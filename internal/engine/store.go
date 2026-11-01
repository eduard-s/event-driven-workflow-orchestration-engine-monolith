package engine

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schema string
var ErrFenced = errors.New("engine ownership changed")

type Store struct {
	pool   *pgxpool.Pool
	leader *pgx.Conn
	cfg    Config
	owner  string
}

func Open(ctx context.Context, c Config) (*Store, error) {
	pc, e := pgxpool.ParseConfig(c.DSN)
	if e != nil {
		return nil, e
	}
	pc.MaxConns = int32(c.PoolSize)
	pc.MinConns = 1
	pc.ConnConfig.ConnectTimeout = c.DBTimeout
	pc.ConnConfig.RuntimeParams["statement_timeout"] = fmt.Sprint(c.DBTimeout.Milliseconds())
	pc.ConnConfig.RuntimeParams["lock_timeout"] = "2000"
	pc.ConnConfig.RuntimeParams["synchronous_commit"] = "on"
	pool, e := pgxpool.NewWithConfig(ctx, pc)
	if e != nil {
		return nil, e
	}
	s := &Store{pool: pool, cfg: c, owner: uuid()}
	fail := func(e error) (*Store, error) { s.Close(); return nil, e }
	s.leader, e = pgx.ConnectConfig(ctx, pc.ConnConfig.Copy())
	if e != nil {
		return fail(e)
	}
	var locked bool
	if e = s.leader.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended(current_schema()||':workflow-monolith',0))`).Scan(&locked); e != nil {
		return fail(e)
	}
	if !locked {
		return fail(errors.New("another monolith owns this database schema"))
	}
	if _, e = s.leader.Exec(ctx, schema); e != nil {
		return fail(e)
	}
	tx, e := pool.Begin(ctx)
	if e != nil {
		return fail(e)
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, `INSERT INTO engine_control(singleton,owner) VALUES(true,$1) ON CONFLICT(singleton) DO UPDATE SET owner=excluded.owner`, s.owner); e != nil {
		rollback(tx)
		return fail(e)
	}
	if e = tx.Commit(ctx); e != nil {
		return fail(e)
	}
	return s, nil
}
func (s *Store) Close() {
	if s.leader != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.leader.Close(ctx)
	}
	if s.pool != nil {
		s.pool.Close()
	}
}
func (s *Store) guard(ctx context.Context, tx pgx.Tx) error {
	var owner string
	if e := tx.QueryRow(ctx, `SELECT owner::text FROM engine_control WHERE singleton FOR SHARE`).Scan(&owner); e != nil {
		return e
	}
	if owner != s.owner {
		return ErrFenced
	}
	return nil
}
func (s *Store) begin(ctx context.Context) (pgx.Tx, error) {
	tx, e := s.pool.Begin(ctx)
	if e != nil {
		return nil, e
	}
	if e = s.guard(ctx, tx); e != nil {
		tx.Rollback(ctx)
		return nil, e
	}
	return tx, nil
}
func (s *Store) Ping(ctx context.Context) error {
	var owner string
	return s.pool.QueryRow(ctx, `SELECT owner::text FROM engine_control WHERE singleton AND owner=$1`, s.owner).Scan(&owner)
}
func (s *Store) Heartbeat(ctx context.Context) error {
	var n int
	return s.leader.QueryRow(ctx, `SELECT 1`).Scan(&n)
}
func (s *Store) Submit(ctx context.Context, d Definition, key string) (string, error) {
	tx, e := s.begin(ctx)
	if e != nil {
		return "", e
	}
	defer tx.Rollback(ctx)
	if key != "" {
		if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,1))`, key); e != nil {
			return "", e
		}
		var id string
		e = tx.QueryRow(ctx, `SELECT execution_id::text FROM submission_keys WHERE key=$1`, key).Scan(&id)
		if e == nil {
			return id, tx.Commit(ctx)
		}
		if !errors.Is(e, pgx.ErrNoRows) {
			return "", e
		}
	}
	// Admission remains exact across API goroutines. This lock is independent of execution locks.
	if _, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(73128,1)`); e != nil {
		return "", e
	}
	var total, active int
	if e = tx.QueryRow(ctx, `SELECT count(*),count(*) FILTER(WHERE state IN ('PENDING','RUNNING')) FROM executions`).Scan(&total, &active); e != nil {
		return "", e
	}
	if total >= s.cfg.MaxRecords || active >= s.cfg.MaxExecutions {
		return "", problem(503, "EXECUTION_CAPACITY", "Execution storage capacity is full.")
	}
	id := uuid()
	p := s.cfg.policy()
	if _, e = tx.Exec(ctx, `INSERT INTO executions(id,definition,policy,state,pending_until) VALUES($1,$2,$3,'PENDING',clock_timestamp()+$4*interval '1 second')`, id, valueJSON(d), valueJSON(p), p.PendingSeconds); e != nil {
		return "", e
	}
	batch := &pgx.Batch{}
	for _, n := range d.DAG.Nodes {
		batch.Queue(`INSERT INTO nodes(execution_id,node_id,handler,dependencies,state) VALUES($1,$2,$3,$4,'PENDING')`, id, n.ID, n.Handler, n.Dependencies)
	}
	br := tx.SendBatch(ctx, batch)
	if e = br.Close(); e != nil {
		return "", e
	}
	if key != "" {
		if _, e = tx.Exec(ctx, `INSERT INTO submission_keys(key,execution_id) VALUES($1,$2)`, key, id); e != nil {
			return "", e
		}
	}
	return id, tx.Commit(ctx)
}
func (s *Store) Definition(ctx context.Context, id string) (Definition, error) {
	var data []byte
	var d Definition
	e := s.pool.QueryRow(ctx, `SELECT definition FROM executions WHERE id=$1`, id).Scan(&data)
	if errors.Is(e, pgx.ErrNoRows) {
		return d, problem(404, "NOT_FOUND", "Execution does not exist.")
	}
	if e != nil {
		return d, e
	}
	e = json.Unmarshal(data, &d)
	return d, e
}
func (s *Store) Trigger(ctx context.Context, id string, t Trigger) error {
	tx, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	var state string
	var pending, now time.Time
	var policy []byte
	e = tx.QueryRow(ctx, `SELECT state,pending_until,clock_timestamp(),policy FROM executions WHERE id=$1 FOR UPDATE`, id).Scan(&state, &pending, &now, &policy)
	if errors.Is(e, pgx.ErrNoRows) {
		return problem(404, "NOT_FOUND", "Execution does not exist.")
	}
	if e != nil {
		return e
	}
	if state != "PENDING" {
		return problem(409, "EXECUTION_ALREADY_TRIGGERED", "Execution already started or expired.")
	}
	var p Policy
	if e = json.Unmarshal(policy, &p); e != nil {
		return e
	}
	if !now.Before(pending) {
		if e = s.finish(ctx, tx, id, "EXPIRED", p); e != nil {
			return e
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
		return problem(409, "EXECUTION_EXPIRED", "The execution expired before its trigger.")
	}
	_, e = tx.Exec(ctx, `UPDATE executions SET state='RUNNING',trigger=$2,accepted_at=$3::timestamptz,deadline_at=$3::timestamptz+$4::double precision*interval '1 second' WHERE id=$1`, id, valueJSON(t), now, p.ExecutionSeconds)
	if e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Store) finish(ctx context.Context, tx pgx.Tx, id, state string, p Policy) error {
	_, e := tx.Exec(ctx, `UPDATE executions SET state=$2,terminal_at=clock_timestamp(),retention_until=clock_timestamp()+$3*interval '1 second' WHERE id=$1`, id, state, p.RetentionSeconds)
	if e != nil {
		return e
	}
	if state == "EXPIRED" {
		_, e = tx.Exec(ctx, `UPDATE nodes SET state='EXPIRED',reason='EXECUTION_DEADLINE',error=$2 WHERE execution_id=$1 AND state IN ('PENDING','RETRY_WAIT','RUNNING')`, id, valueJSON(problem(409, "EXECUTION_DEADLINE", "The execution deadline elapsed.")))
	}
	return e
}
func (s *Store) settle(ctx context.Context, tx pgx.Tx, id string, p Policy) error {
	_, e := tx.Exec(ctx, `WITH RECURSIVE blocked(node_id) AS (SELECT node_id FROM nodes WHERE execution_id=$1 AND state IN ('FAILED','BLOCKED','EXPIRED') UNION SELECT n.node_id FROM nodes n JOIN blocked b ON b.node_id=ANY(n.dependencies) WHERE n.execution_id=$1) UPDATE nodes SET state='BLOCKED',reason='UPSTREAM_FAILED',error=$2 WHERE execution_id=$1 AND state IN ('PENDING','RETRY_WAIT') AND node_id IN (SELECT node_id FROM blocked)`, id, valueJSON(problem(409, "UPSTREAM_FAILED", "A dependency failed.")))
	if e != nil {
		return e
	}
	var unfinished, failed int
	if e = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state IN ('PENDING','RUNNING','RETRY_WAIT')),count(*) FILTER(WHERE state IN ('FAILED','BLOCKED','EXPIRED')) FROM nodes WHERE execution_id=$1`, id).Scan(&unfinished, &failed); e != nil {
		return e
	}
	if unfinished == 0 {
		state := "COMPLETED"
		if failed > 0 {
			state = "FAILED"
		}
		return s.finish(ctx, tx, id, state, p)
	}
	return nil
}
func dbContext(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := tx.Rollback(ctx); e != nil && !errors.Is(e, pgx.ErrTxClosed) {
		slog.Debug("rollback", "error", e)
	}
}
