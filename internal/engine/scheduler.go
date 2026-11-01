package engine

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

type Task struct {
	ExecutionID, NodeID string
	Number              int
	Request             Request
	Policy              Policy
	Remaining           time.Duration
}
type Outcome struct {
	Value   any
	Error   *Problem
	Runtime map[string]any
}

func (s *Store) Claim(ctx context.Context) (*Task, error) {
	tx, e := s.begin(ctx)
	if e != nil {
		return nil, e
	}
	defer rollback(tx)
	var id string
	e = tx.QueryRow(ctx, `SELECT e.id::text FROM executions e WHERE e.state='RUNNING' AND e.deadline_at>clock_timestamp() AND EXISTS(SELECT 1 FROM nodes n WHERE n.execution_id=e.id AND n.state IN ('PENDING','RETRY_WAIT') AND n.ready_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM nodes p WHERE p.execution_id=e.id AND p.node_id=ANY(n.dependencies) AND p.state<>'COMPLETED')) AND (SELECT count(*) FROM nodes r WHERE r.execution_id=e.id AND r.state='RUNNING')<(e.policy->>'slots')::int ORDER BY e.accepted_at,e.id FOR UPDATE OF e SKIP LOCKED LIMIT 1`).Scan(&id)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var defData, triggerData, policyData []byte
	var now, deadline time.Time
	var inputBytes int64
	if e = tx.QueryRow(ctx, `SELECT definition,trigger,policy,clock_timestamp(),deadline_at,input_bytes FROM executions WHERE id=$1`, id).Scan(&defData, &triggerData, &policyData, &now, &deadline, &inputBytes); e != nil {
		return nil, e
	}
	var p Policy
	if e = json.Unmarshal(policyData, &p); e != nil {
		return nil, e
	}
	if !now.Before(deadline) {
		if e = s.finish(ctx, tx, id, "EXPIRED", p); e != nil {
			return nil, e
		}
		return nil, tx.Commit(ctx)
	}
	var nodeID string
	var number int
	var frozen []byte
	e = tx.QueryRow(ctx, `SELECT n.node_id,n.try_number,n.resolved_input FROM nodes n WHERE n.execution_id=$1 AND n.state IN ('PENDING','RETRY_WAIT') AND n.ready_at<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM nodes p WHERE p.execution_id=n.execution_id AND p.node_id=ANY(n.dependencies) AND p.state<>'COMPLETED') ORDER BY n.ready_at,n.node_id LIMIT 1 FOR UPDATE`, id).Scan(&nodeID, &number, &frozen)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	var req Request
	if frozen == nil {
		var d Definition
		var t Trigger
		if e = json.Unmarshal(defData, &d); e != nil {
			return nil, e
		}
		if e = decodeJSON(triggerData, &t); e != nil {
			return nil, e
		}
		n, e := d.node(nodeID)
		if e != nil {
			return nil, e
		}
		outputs := map[string]any{}
		rows, e := tx.Query(ctx, `SELECT node_id,output FROM nodes WHERE execution_id=$1 AND has_output`, id)
		if e != nil {
			return nil, e
		}
		for rows.Next() {
			var name string
			var raw []byte
			if e = rows.Scan(&name, &raw); e != nil {
				rows.Close()
				return nil, e
			}
			v, e := native(raw)
			if e != nil {
				rows.Close()
				return nil, e
			}
			outputs[name] = v
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		limit := p.ValueBytes
		if n.Handler == "output" {
			limit = p.ExecutionBytes
		}
		req, e = Resolve(n, t, outputs, limit)
		if e == nil {
			frozen = valueJSON(req)
			if inputBytes+int64(len(frozen)) > int64(p.ExecutionBytes) {
				e = problem(422, "INPUT_BUDGET", "Execution input capacity is full.")
			}
		}
		if e != nil {
			failure := problem(422, "TEMPLATE_ERROR", "Node input resolution failed.")
			var pe *Problem
			if errors.As(e, &pe) {
				failure = pe
			}
			if _, e = tx.Exec(ctx, `UPDATE nodes SET state='FAILED',reason='TEMPLATE_ERROR',error=$3 WHERE execution_id=$1 AND node_id=$2`, id, nodeID, valueJSON(failure)); e != nil {
				return nil, e
			}
			if e = s.settle(ctx, tx, id, p); e != nil {
				return nil, e
			}
			return nil, tx.Commit(ctx)
		}
		if _, e = tx.Exec(ctx, `UPDATE executions SET input_bytes=input_bytes+$2 WHERE id=$1`, id, len(frozen)); e != nil {
			return nil, e
		}
	} else {
		if e = decodeJSON(frozen, &req); e != nil {
			return nil, e
		}
	}
	number++
	if number > p.MaxTries {
		return nil, errors.New("ready node exceeds retry policy")
	}
	lease := now.Add(time.Duration(p.HandlerSeconds * float64(time.Second)))
	if deadline.Before(lease) {
		lease = deadline
	}
	if _, e = tx.Exec(ctx, `UPDATE nodes SET state='RUNNING',reason='HANDLER_ACTIVE',try_number=$3,resolved_input=$4,error=NULL WHERE execution_id=$1 AND node_id=$2`, id, nodeID, number, frozen); e != nil {
		return nil, e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO tries(execution_id,node_id,try_number,owner,started_at,lease_until) VALUES($1,$2,$3,$4,$5,$6)`, id, nodeID, number, s.owner, now, lease); e != nil {
		return nil, e
	}
	if e = tx.Commit(ctx); e != nil {
		return nil, e
	}
	return &Task{id, nodeID, number, req, p, lease.Sub(now)}, nil
}
func (s *Store) Complete(ctx context.Context, t Task, out Outcome) error {
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer rollback(tx)
	var state string
	var deadline, now time.Time
	e = tx.QueryRow(ctx, `SELECT state,deadline_at,clock_timestamp() FROM executions WHERE id=$1 FOR UPDATE`, t.ExecutionID).Scan(&state, &deadline, &now)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	if state != "RUNNING" {
		return nil
	}
	if !now.Before(deadline) {
		if e = s.finish(ctx, tx, t.ExecutionID, "EXPIRED", t.Policy); e != nil {
			return e
		}
		return tx.Commit(ctx)
	}
	var current int
	var nodeState, owner string
	var lease time.Time
	var finished *time.Time
	e = tx.QueryRow(ctx, `SELECT n.try_number,n.state,t.owner::text,t.lease_until,t.finished_at FROM nodes n JOIN tries t ON t.execution_id=n.execution_id AND t.node_id=n.node_id AND t.try_number=n.try_number WHERE n.execution_id=$1 AND n.node_id=$2`, t.ExecutionID, t.NodeID).Scan(&current, &nodeState, &owner, &lease, &finished)
	if e != nil {
		return e
	}
	if current != t.Number || nodeState != "RUNNING" || owner != s.owner || finished != nil {
		return nil
	}
	if out.Error == nil && !now.Before(lease) {
		out.Error = &Problem{Code: "HANDLER_TIMEOUT", Message: "The handler deadline elapsed.", Retryable: true}
	}
	var value []byte
	if out.Error == nil {
		if e = examineValue(out.Value, 0); e != nil {
			out.Error = problem(422, "INVALID_HANDLER_OUTPUT", "The handler output is invalid.")
		} else {
			value = valueJSON(out.Value)
			limit := t.Policy.ValueBytes
			if t.Request.Handler == "output" {
				limit = t.Policy.ExecutionBytes
			}
			if len(value) > limit {
				out.Error = problem(422, "OUTPUT_TOO_LARGE", "The handler output exceeds its byte limit.")
			}
			var bytes int64
			if e = tx.QueryRow(ctx, `SELECT output_bytes FROM executions WHERE id=$1`, t.ExecutionID).Scan(&bytes); e != nil {
				return e
			}
			if bytes+int64(len(value)) > int64(t.Policy.ExecutionBytes) {
				out.Error = problem(422, "OUTPUT_BUDGET", "Execution output capacity is full.")
			}
		}
	}
	if _, e = tx.Exec(ctx, `UPDATE tries SET finished_at=$4,success=$5,error=$6,runtime=$7 WHERE execution_id=$1 AND node_id=$2 AND try_number=$3`, t.ExecutionID, t.NodeID, t.Number, now, out.Error == nil, nullableJSON(out.Error), nullableJSON(out.Runtime)); e != nil {
		return e
	}
	if out.Error == nil {
		if _, e = tx.Exec(ctx, `UPDATE nodes SET state='COMPLETED',reason='SUCCESS',output=$3,has_output=true,error=NULL WHERE execution_id=$1 AND node_id=$2`, t.ExecutionID, t.NodeID, value); e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, `UPDATE executions SET output_bytes=output_bytes+$2 WHERE id=$1`, t.ExecutionID, len(value)); e != nil {
			return e
		}
	} else {
		if e = s.failTry(ctx, tx, t.ExecutionID, t.NodeID, t.Number, t.Policy, out.Error, now, deadline); e != nil {
			return e
		}
	}
	if e = s.settle(ctx, tx, t.ExecutionID, t.Policy); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func nullableJSON(v any) any {
	switch p := v.(type) {
	case *Problem:
		if p == nil {
			return nil
		}
	case map[string]any:
		if p == nil {
			return nil
		}
	}
	return valueJSON(v)
}
func (s *Store) failTry(ctx context.Context, tx pgx.Tx, id, node string, number int, p Policy, failure *Problem, now, deadline time.Time) error {
	state, reason := "FAILED", "HANDLER_FAILURE"
	retryAt := now
	if failure.Retryable && number < p.MaxTries {
		delay := math.Min(10, p.RetrySeconds*math.Pow(2, float64(number-1))) * rand.Float64()
		retryAt = now.Add(time.Duration(delay * float64(time.Second)))
		if retryAt.Before(deadline) {
			state, reason = "RETRY_WAIT", "RETRY_DELAY"
		}
	}
	_, e := tx.Exec(ctx, `UPDATE nodes SET state=$3,reason=$4,error=$5,ready_at=$6 WHERE execution_id=$1 AND node_id=$2`, id, node, state, reason, valueJSON(failure), retryAt)
	return e
}
func (s *Store) Maintain(ctx context.Context, recover bool) error {
	ids := []string{}
	query := `SELECT id::text FROM executions WHERE (state='PENDING' AND pending_until<=clock_timestamp()) OR (state='RUNNING' AND deadline_at<=clock_timestamp()) ORDER BY created_at LIMIT 100`
	if recover {
		query = `SELECT e.id::text FROM executions e WHERE (e.state='PENDING' AND e.pending_until<=clock_timestamp()) OR (e.state='RUNNING' AND (e.deadline_at<=clock_timestamp() OR EXISTS(SELECT 1 FROM nodes n WHERE n.execution_id=e.id AND n.state='RUNNING'))) ORDER BY e.created_at`
	}
	rows, e := s.pool.Query(ctx, query)
	if e != nil {
		return e
	}
	for rows.Next() {
		var id string
		if e = rows.Scan(&id); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, id)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for _, id := range ids {
		if e = s.maintainExecution(ctx, id, recover); e != nil {
			return e
		}
	}
	_, e = s.pool.Exec(ctx, `DELETE FROM executions WHERE id IN (SELECT id FROM executions WHERE retention_until<=clock_timestamp() ORDER BY retention_until LIMIT 100)`)
	return e
}
func (s *Store) maintainExecution(ctx context.Context, id string, recover bool) error {
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer rollback(tx)
	var state string
	var pData []byte
	var pending, now time.Time
	var deadline *time.Time
	e = tx.QueryRow(ctx, `SELECT state,policy,pending_until,deadline_at,clock_timestamp() FROM executions WHERE id=$1 FOR UPDATE`, id).Scan(&state, &pData, &pending, &deadline, &now)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil
	}
	if e != nil {
		return e
	}
	var p Policy
	if e = json.Unmarshal(pData, &p); e != nil {
		return e
	}
	if state == "PENDING" && !now.Before(pending) || state == "RUNNING" && deadline != nil && !now.Before(*deadline) {
		if e = s.finish(ctx, tx, id, "EXPIRED", p); e != nil {
			return e
		}
	} else if state == "RUNNING" && recover {
		rows, e := tx.Query(ctx, `SELECT node_id,try_number FROM nodes WHERE execution_id=$1 AND state='RUNNING'`, id)
		if e != nil {
			return e
		}
		type interrupted struct {
			id     string
			number int
		}
		var work []interrupted
		for rows.Next() {
			var n interrupted
			if e = rows.Scan(&n.id, &n.number); e != nil {
				rows.Close()
				return e
			}
			work = append(work, n)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		for _, n := range work {
			failure := &Problem{Code: "PROCESS_INTERRUPTED", Message: "The previous process stopped before result commit.", Retryable: true}
			if _, e = tx.Exec(ctx, `UPDATE tries SET finished_at=$4,success=false,error=$5 WHERE execution_id=$1 AND node_id=$2 AND try_number=$3 AND finished_at IS NULL`, id, n.id, n.number, now, valueJSON(failure)); e != nil {
				return e
			}
			if e = s.failTry(ctx, tx, id, n.id, n.number, p, failure, now, *deadline); e != nil {
				return e
			}
		}
		if e = s.settle(ctx, tx, id, p); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}

type Engine struct {
	Store  *Store
	cfg    Config
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	wake   chan struct{}
	ready  atomic.Bool
	fatal  chan error
}

func Start(ctx context.Context, c Config) (*Engine, error) {
	store, e := Open(ctx, c)
	if e != nil {
		return nil, e
	}
	run, cancel := context.WithCancel(ctx)
	engine := &Engine{Store: store, cfg: c, ctx: run, cancel: cancel, wake: make(chan struct{}, 1), fatal: make(chan error, 1)}
	recoverCtx, done := dbContext(ctx, 30*time.Second)
	e = store.Maintain(recoverCtx, true)
	done()
	if e != nil {
		cancel()
		store.Close()
		return nil, e
	}
	engine.ready.Store(true)
	for i := 0; i < c.Concurrency; i++ {
		engine.wg.Go(engine.worker)
	}
	engine.wg.Go(engine.monitor)
	return engine, nil
}
func (e *Engine) Wake() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}
func (e *Engine) Fatal() <-chan error { return e.fatal }
func (e *Engine) Close()              { e.ready.Store(false); e.cancel(); e.wg.Wait(); e.Store.Close() }
func (e *Engine) stop(err error) {
	e.ready.Store(false)
	select {
	case e.fatal <- err:
	default:
	}
	e.cancel()
}
func (e *Engine) worker() {
	for e.ctx.Err() == nil {
		ctx, cancel := dbContext(e.ctx, e.cfg.DBTimeout)
		task, err := e.Store.Claim(ctx)
		cancel()
		if err != nil {
			if errors.Is(err, ErrFenced) {
				e.stop(err)
				return
			}
			slog.Warn("claim_failed", "error", err)
		}
		if task != nil {
			out := runTask(e.ctx, *task, e.cfg.TestFixtures)
			if e.ctx.Err() != nil {
				return
			}
			for e.ctx.Err() == nil {
				ctx, cancel := dbContext(e.ctx, e.cfg.DBTimeout)
				err = e.Store.Complete(ctx, *task, out)
				cancel()
				if err == nil {
					e.Wake()
					break
				}
				if errors.Is(err, ErrFenced) {
					e.stop(err)
					return
				}
				slog.Warn("completion_retry", "error", err)
				select {
				case <-e.ctx.Done():
					return
				case <-time.After(100 * time.Millisecond):
				}
			}
			continue
		}
		select {
		case <-e.ctx.Done():
			return
		case <-e.wake:
		case <-time.After(e.cfg.PollInterval):
		}
	}
}
func (e *Engine) monitor() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := dbContext(e.ctx, e.cfg.DBTimeout)
			err := e.Store.Heartbeat(ctx)
			cancel()
			if err != nil {
				e.stop(err)
				return
			}
			ctx, cancel = dbContext(e.ctx, e.cfg.DBTimeout)
			err = e.Store.Maintain(ctx, false)
			cancel()
			if err != nil {
				slog.Warn("maintenance_failed", "error", err)
				if errors.Is(err, ErrFenced) {
					e.stop(err)
					return
				}
			}
		}
	}
}
