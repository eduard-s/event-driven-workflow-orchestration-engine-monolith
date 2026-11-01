package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

type API struct {
	engine *Engine
	slots  chan struct{}
	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func NewHandler(e *Engine) http.Handler {
	a := &API{engine: e, slots: make(chan struct{}, e.cfg.RequestConcurrency), tokens: float64(e.cfg.RequestsPerSecond), last: time.Now()}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"state": "alive"}) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := dbContext(r.Context(), e.cfg.DBTimeout)
		defer cancel()
		if !e.ready.Load() || e.Store.Ping(ctx) != nil {
			writeError(w, problem(503, "STORAGE_UNAVAILABLE", "Persistent storage is unavailable."))
			return
		}
		writeJSON(w, 200, map[string]string{"state": "ready"})
	})
	mux.HandleFunc("POST /workflow", a.submit)
	mux.HandleFunc("POST /workflow/trigger/{id}", a.trigger)
	mux.HandleFunc("GET /workflows/{id}", a.status)
	mux.HandleFunc("GET /workflows/{id}/results", a.results)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				slog.Error("http_panic", "panic", v)
				writeError(w, problem(500, "INTERNAL_ERROR", "The request failed."))
			}
		}()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if strings.HasPrefix(r.URL.Path, "/health/") {
			mux.ServeHTTP(w, r)
			return
		}
		if !e.ready.Load() {
			writeError(w, problem(503, "NOT_READY", "The engine is unavailable."))
			return
		}
		select {
		case a.slots <- struct{}{}:
			defer func() { <-a.slots }()
		default:
			writeError(w, problem(503, "REQUEST_CAPACITY", "Request capacity is full."))
			return
		}
		if !a.admit() {
			w.Header().Set("Retry-After", "1")
			writeError(w, problem(429, "RATE_LIMIT", "The request rate exceeds its limit."))
			return
		}
		mux.ServeHTTP(w, r)
	})
}
func (a *API) admit() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	a.tokens += now.Sub(a.last).Seconds() * float64(a.engine.cfg.RequestsPerSecond)
	maximum := float64(a.engine.cfg.RequestsPerSecond)
	if a.tokens > maximum {
		a.tokens = maximum
	}
	a.last = now
	if a.tokens < 1 {
		return false
	}
	a.tokens--
	return true
}
func (a *API) body(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, int64(a.engine.cfg.MaxBodyBytes))
	data, e := io.ReadAll(r.Body)
	if e != nil {
		var large *http.MaxBytesError
		if errors.As(e, &large) {
			return nil, problem(413, "BODY_TOO_LARGE", "The request body exceeds its byte limit.")
		}
		return nil, problem(400, "INVALID_BODY", "The request body is incomplete.")
	}
	return data, nil
}
func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	data, e := a.body(w, r)
	if e != nil {
		writeError(w, e)
		return
	}
	d, e := Validate(data, a.engine.cfg)
	if e != nil {
		writeError(w, e)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 128 || r.Header.Values("Idempotency-Key") != nil && key == "" {
		writeError(w, problem(422, "INVALID_IDEMPOTENCY_KEY", "Use a key with 1 to 128 bytes."))
		return
	}
	ctx, cancel := dbContext(r.Context(), a.engine.cfg.DBTimeout)
	defer cancel()
	id, e := a.engine.Store.Submit(ctx, d, key)
	if e != nil {
		writeError(w, e)
		return
	}
	writeJSON(w, 201, map[string]string{"execution_id": id})
}
func (a *API) identifier(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if !uuidPattern.MatchString(id) {
		writeError(w, problem(422, "INVALID_EXECUTION_ID", "The execution ID must be a UUID."))
		return "", false
	}
	return id, true
}
func (a *API) trigger(w http.ResponseWriter, r *http.Request) {
	id, ok := a.identifier(w, r)
	if !ok {
		return
	}
	data, e := a.body(w, r)
	if e != nil {
		writeError(w, e)
		return
	}
	ctx, cancel := dbContext(r.Context(), a.engine.cfg.DBTimeout)
	defer cancel()
	d, e := a.engine.Store.Definition(ctx, id)
	if e != nil {
		writeError(w, e)
		return
	}
	t, e := ValidateTrigger(data, d)
	if e != nil {
		writeError(w, e)
		return
	}
	if e = a.engine.Store.Trigger(ctx, id, t); e != nil {
		writeError(w, e)
		return
	}
	a.engine.Wake()
	w.Header().Set("Location", "/workflows/"+id)
	w.Header().Set("Retry-After", "1")
	writeJSON(w, 202, map[string]string{"execution_id": id})
}
func (a *API) status(w http.ResponseWriter, r *http.Request) {
	id, ok := a.identifier(w, r)
	if !ok {
		return
	}
	offset, limit := 0, 100
	for name, p := range map[string]*int{"offset": &offset, "limit": &limit} {
		if v := r.URL.Query().Get(name); v != "" {
			n, e := strconv.Atoi(v)
			if e != nil || n < 0 || name == "limit" && (n < 1 || n > 100) || name == "offset" && n > a.engine.cfg.MaxNodes {
				writeError(w, problem(422, "INVALID_PAGE", "The status page is invalid."))
				return
			}
			*p = n
		}
	}
	ctx, cancel := dbContext(r.Context(), a.engine.cfg.DBTimeout)
	defer cancel()
	v, e := a.engine.Store.Status(ctx, id, offset, limit)
	if e != nil {
		writeError(w, e)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Retry-After", "1")
	writeJSON(w, 200, v)
}
func (a *API) results(w http.ResponseWriter, r *http.Request) {
	id, ok := a.identifier(w, r)
	if !ok {
		return
	}
	ctx, cancel := dbContext(r.Context(), a.engine.cfg.DBTimeout)
	defer cancel()
	v, e := a.engine.Store.Results(ctx, id)
	if e != nil {
		writeError(w, e)
		return
	}
	status := 200
	if v["state"] != "COMPLETED" {
		status = 409
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, status, v)
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(v)
}
func writeError(w http.ResponseWriter, e error) {
	var p *Problem
	if !errors.As(e, &p) {
		slog.Warn("storage_request_failed", "error", e)
		p = problem(503, "STORAGE_UNAVAILABLE", "Persistent storage is unavailable.")
	}
	if p.Status == 0 {
		p.Status = 500
	}
	writeJSON(w, p.Status, map[string]any{"error": p})
}
func (s *Store) readTx(ctx context.Context) (pgx.Tx, error) {
	return s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
}
func (s *Store) Status(ctx context.Context, id string, offset, limit int) (map[string]any, error) {
	tx, e := s.readTx(ctx)
	if e != nil {
		return nil, e
	}
	defer rollback(tx)
	v, e := executionSummary(ctx, tx, id)
	if e != nil {
		return nil, e
	}
	rows, e := tx.Query(ctx, `SELECT node_id,state,reason,try_number,error FROM nodes WHERE execution_id=$1 ORDER BY node_id OFFSET $2 LIMIT $3`, id, offset, limit)
	if e != nil {
		return nil, e
	}
	nodes := []map[string]any{}
	for rows.Next() {
		var name, state, reason string
		var number int
		var raw []byte
		if e = rows.Scan(&name, &state, &reason, &number, &raw); e != nil {
			rows.Close()
			return nil, e
		}
		var failure any
		if raw != nil {
			failure, _ = native(raw)
		}
		nodes = append(nodes, map[string]any{"id": name, "state": state, "reason": reason, "try_number": number, "starts": number, "error": failure, "tries": []map[string]any{}})
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	for _, n := range nodes {
		rows, e = tx.Query(ctx, `SELECT try_number,started_at,finished_at,lease_until,success,error,runtime FROM tries WHERE execution_id=$1 AND node_id=$2 ORDER BY try_number`, id, n["id"])
		if e != nil {
			return nil, e
		}
		tries := []map[string]any{}
		for rows.Next() {
			var number int
			var started, lease time.Time
			var finished *time.Time
			var success *bool
			var failure, runtime []byte
			if e = rows.Scan(&number, &started, &finished, &lease, &success, &failure, &runtime); e != nil {
				rows.Close()
				return nil, e
			}
			var f, rt any
			if failure != nil {
				f, _ = native(failure)
			}
			if runtime != nil {
				rt, _ = native(runtime)
			}
			tries = append(tries, map[string]any{"try_number": number, "started_at": started, "finished_at": finished, "lease_until": lease, "success": success, "error": f, "runtime": rt})
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return nil, e
		}
		n["tries"] = tries
	}
	var total int
	if e = tx.QueryRow(ctx, `SELECT count(*) FROM nodes WHERE execution_id=$1`, id).Scan(&total); e != nil {
		return nil, e
	}
	v["nodes"], v["offset"], v["limit"], v["total_nodes"] = nodes, offset, limit, total
	return v, tx.Commit(ctx)
}
func executionSummary(ctx context.Context, tx pgx.Tx, id string) (map[string]any, error) {
	var state string
	var created time.Time
	var accepted, deadline, terminal *time.Time
	e := tx.QueryRow(ctx, `SELECT state,created_at,accepted_at,deadline_at,terminal_at FROM executions WHERE id=$1`, id).Scan(&state, &created, &accepted, &deadline, &terminal)
	if errors.Is(e, pgx.ErrNoRows) {
		return nil, problem(404, "NOT_FOUND", "Execution does not exist.")
	}
	if e != nil {
		return nil, e
	}
	return map[string]any{"execution_id": id, "state": state, "has_failure": state == "FAILED" || state == "EXPIRED", "created_at": created, "accepted_at": accepted, "deadline_at": deadline, "terminal_at": terminal}, nil
}
func (s *Store) Results(ctx context.Context, id string) (map[string]any, error) {
	tx, e := s.readTx(ctx)
	if e != nil {
		return nil, e
	}
	defer rollback(tx)
	summary, e := executionSummary(ctx, tx, id)
	if e != nil {
		return nil, e
	}
	state := summary["state"].(string)
	rows, e := tx.Query(ctx, `SELECT node_id,handler,dependencies,has_output,output,error FROM nodes WHERE execution_id=$1 ORDER BY node_id`, id)
	if e != nil {
		return nil, e
	}
	outputs, failures := map[string]any{}, map[string]any{}
	explicit := []string{}
	all := []string{}
	parents := map[string]bool{}
	for rows.Next() {
		var name, handler string
		var deps []string
		var has bool
		var raw, failure []byte
		if e = rows.Scan(&name, &handler, &deps, &has, &raw, &failure); e != nil {
			rows.Close()
			return nil, e
		}
		all = append(all, name)
		for _, p := range deps {
			parents[p] = true
		}
		if handler == "output" {
			explicit = append(explicit, name)
		}
		if has {
			v, e := native(raw)
			if e != nil {
				rows.Close()
				return nil, e
			}
			outputs[name] = v
		}
		if failure != nil {
			failures[name], _ = native(failure)
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	result := map[string]any{"execution_id": id, "state": state}
	if state != "COMPLETED" {
		result["partial_outputs"], result["errors"], result["error"] = outputs, failures, problem(409, "RESULTS_"+state, "The workflow is not complete.")
	} else {
		if len(explicit) == 0 {
			for _, name := range all {
				if !parents[name] {
					explicit = append(explicit, name)
				}
			}
		}
		selected := map[string]any{}
		for _, name := range explicit {
			v, ok := outputs[name]
			if !ok {
				return nil, problem(500, "MISSING_RESULT", "A completed workflow lacks an output.")
			}
			selected[name] = v
		}
		result["outputs"] = selected
	}
	return result, tx.Commit(ctx)
}
