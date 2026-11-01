package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func integrationConfig(t *testing.T) Config {
	t.Helper()
	dsn := os.Getenv("WF_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("WF_TEST_POSTGRES_DSN enables PostgreSQL integration tests")
	}
	ctx := context.Background()
	conn, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	name := "test_" + strings.ReplaceAll(uuid(), "-", "")
	if _, e = conn.Exec(ctx, `CREATE SCHEMA `+name); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _, _ = conn.Exec(ctx, `DROP SCHEMA `+name+` CASCADE`); conn.Close(ctx) })
	u, e := url.Parse(dsn)
	if e != nil {
		t.Fatal(e)
	}
	q := u.Query()
	q.Set("search_path", name)
	u.RawQuery = q.Encode()
	c := DefaultConfig()
	c.DSN = u.String()
	c.RequestsPerSecond = 10000
	c.TestFixtures = true
	c.RetryBase = 10 * time.Millisecond
	c.PollInterval = 2 * time.Millisecond
	c.Concurrency = 8
	return c
}
func startTest(t *testing.T, c Config) *Engine {
	t.Helper()
	e, err := Start(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(e.Close)
	return e
}
func accepted(t *testing.T, e *Engine, d Definition, tr Trigger) string {
	t.Helper()
	d = mustValid(t, d)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	id, err := e.Store.Submit(ctx, d, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = e.Store.Trigger(ctx, id, tr); err != nil {
		t.Fatal(err)
	}
	e.Wake()
	return id
}
func await(t *testing.T, e *Engine, id, state string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		v, err := e.Store.Status(context.Background(), id, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		if v["state"] == state {
			return v
		}
		if v["state"] == "FAILED" || v["state"] == "EXPIRED" {
			t.Fatalf("unexpected terminal state: %s", valueJSON(v))
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("workflow did not reach " + state)
	return nil
}
func TestIntegrationParallelFanIn(t *testing.T) {
	e := startTest(t, integrationConfig(t))
	a := node("a", "input")
	b := node("b", "call_external_service", "a")
	b.Data = []byte(`{"received":"{{ a.id }}"}`)
	c := b
	c.ID = "c"
	d := node("d", "output", "b", "c")
	started := time.Now()
	id := accepted(t, e, graph(a, b, c, d), Trigger{Input: map[string]any{"id": json.Number("9007199254740993")}})
	v := await(t, e, id, "COMPLETED")
	if time.Since(started) > 1900*time.Millisecond {
		t.Fatal("branches did not overlap")
	}
	for _, raw := range v["nodes"].([]map[string]any) {
		if raw["starts"].(int) != 1 {
			t.Fatalf("duplicate start: %+v", raw)
		}
	}
	result, err := e.Store.Results(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(valueJSON(result)), "9007199254740993") {
		t.Fatal("integer precision lost")
	}
}
func TestIntegrationIdempotencyAndTriggerRace(t *testing.T) {
	e := startTest(t, integrationConfig(t))
	d := mustValid(t, graph(node("a", "input")))
	ids := make(chan string, 20)
	errs := make(chan error, 20)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Go(func() { id, err := e.Store.Submit(context.Background(), d, "same-key"); ids <- id; errs <- err })
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	id := ""
	for v := range ids {
		if id != "" && v != id {
			t.Fatal("different receipts")
		}
		id = v
	}
	success := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		wg.Go(func() { err := e.Store.Trigger(context.Background(), id, Trigger{}); success <- err == nil })
	}
	wg.Wait()
	close(success)
	count := 0
	for ok := range success {
		if ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("accepted %d triggers", count)
	}
	await(t, e, id, "COMPLETED")
}
func TestIntegrationRetryAndFailure(t *testing.T) {
	e := startTest(t, integrationConfig(t))
	for _, permanent := range []bool{false, true} {
		n := node("a", "output")
		fail := "transient"
		expected := "COMPLETED"
		if permanent {
			fail = "permanent"
			expected = "FAILED"
		}
		n.Data = valueJSON(map[string]any{"__test": map[string]any{"fail": fail, "fail_until": 1, "output": "ok"}})
		id := accepted(t, e, graph(n), Trigger{})
		deadline := time.Now().Add(5 * time.Second)
		for {
			v, err := e.Store.Status(context.Background(), id, 0, 100)
			if err != nil {
				t.Fatal(err)
			}
			if v["state"] == expected {
				want := 2
				if permanent {
					want = 1
				}
				if v["nodes"].([]map[string]any)[0]["starts"] != want {
					t.Fatalf("unexpected tries: %+v", v)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("timeout: %+v", v)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}
func TestIntegrationRecoveryAndSingleton(t *testing.T) {
	c := integrationConfig(t)
	s, err := Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	d := mustValid(t, graph(node("a", "input")))
	id, err := s.Submit(context.Background(), d, "receipt")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Trigger(context.Background(), id, Trigger{Input: "saved"}); err != nil {
		t.Fatal(err)
	}
	task, err := s.Claim(context.Background())
	if err != nil || task == nil {
		t.Fatalf("%v %+v", err, task)
	}
	if other, err := Open(context.Background(), c); err == nil {
		other.Close()
		t.Fatal("second owner started")
	}
	s.Close()
	e := startTest(t, c)
	await(t, e, id, "COMPLETED")
	v, err := e.Store.Status(context.Background(), id, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	n := v["nodes"].([]map[string]any)[0]
	if n["starts"] != 2 {
		t.Fatalf("want interrupted try and retry: %+v", n)
	}
	repeated, err := e.Store.Submit(context.Background(), d, "receipt")
	if err != nil || repeated != id {
		t.Fatal("receipt lost")
	}
}
func TestIntegrationDuplicateAndStaleCompletion(t *testing.T) {
	c := integrationConfig(t)
	s, err := Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	d := mustValid(t, graph(node("a", "input")))
	id, _ := s.Submit(context.Background(), d, "")
	if err = s.Trigger(context.Background(), id, Trigger{}); err != nil {
		t.Fatal(err)
	}
	task, err := s.Claim(context.Background())
	if err != nil || task == nil {
		t.Fatalf("%v %+v", err, task)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Go(func() { errs <- s.Complete(context.Background(), *task, Outcome{Value: "one"}) })
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err = s.Complete(context.Background(), *task, Outcome{Value: "different"}); err != nil {
		t.Fatal(err)
	}
	v, err := s.Results(context.Background(), id)
	if err != nil || v["outputs"].(map[string]any)["a"] != "one" {
		t.Fatalf("%+v %v", v, err)
	}
	if _, err = s.pool.Exec(context.Background(), `UPDATE engine_control SET owner=$1`, uuid()); err != nil {
		t.Fatal(err)
	}
	if err = s.Complete(context.Background(), *task, Outcome{}); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale owner: %v", err)
	}
}
func TestIntegrationDeadlineAndBlockedBranch(t *testing.T) {
	c := integrationConfig(t)
	c.ExecutionTimeout = 150 * time.Millisecond
	c.HandlerTimeout = 100 * time.Millisecond
	e := startTest(t, c)
	n := node("a", "output")
	n.Data = []byte(`{"__test":{"delay":2}}`)
	id := accepted(t, e, graph(n), Trigger{})
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		v, _ := e.Store.Status(context.Background(), id, 0, 100)
		if v["state"] == "EXPIRED" || v["state"] == "FAILED" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("deadline did not settle")
}
func TestIntegrationHTTP(t *testing.T) {
	e := startTest(t, integrationConfig(t))
	server := httptest.NewServer(NewHandler(e))
	defer server.Close()
	d := graph(node("a", "input"))
	r, err := http.Post(server.URL+"/workflow", "application/json", strings.NewReader(string(valueJSON(d))))
	if err != nil {
		t.Fatal(err)
	}
	var receipt map[string]string
	json.NewDecoder(r.Body).Decode(&receipt)
	r.Body.Close()
	if r.StatusCode != 201 {
		t.Fatalf("%d %+v", r.StatusCode, receipt)
	}
	id := receipt["execution_id"]
	r, err = http.Post(server.URL+"/workflow/trigger/"+id, "application/json", strings.NewReader(`{"input":{"literal":"{{ not_a_template }}"}}`))
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != 202 {
		t.Fatal(r.StatusCode)
	}
	await(t, e, id, "COMPLETED")
	for _, path := range []string{"/workflows/" + id, "/workflows/" + id + "/results", "/health/ready"} {
		r, err = http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			t.Fatalf("%s: %d", path, r.StatusCode)
		}
	}
	r, _ = http.Get(server.URL + "/workflows/bad")
	r.Body.Close()
	if r.StatusCode != 422 {
		t.Fatal(fmt.Sprint(r.StatusCode))
	}
}

func TestIntegrationPermanentFailureBlocksOnlyDescendants(t *testing.T) {
	e := startTest(t, integrationConfig(t))
	a := node("a", "input")
	b := node("b", "call_external_service", "a")
	b.Data = []byte(`{"__test":{"delay":0,"fail":"permanent"}}`)
	c := node("c", "llm", "a")
	d := node("d", "output", "b", "c")
	id := accepted(t, e, graph(a, b, c, d), Trigger{Input: "input"})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		v, err := e.Store.Status(context.Background(), id, 0, 100)
		if err != nil {
			t.Fatal(err)
		}
		if v["state"] == "FAILED" {
			states := map[string]any{}
			for _, n := range v["nodes"].([]map[string]any) {
				states[n["id"].(string)] = n["state"]
			}
			if states["b"] != "FAILED" || states["c"] != "COMPLETED" || states["d"] != "BLOCKED" {
				t.Fatalf("%+v", states)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("failure did not settle")
}
func TestIntegrationPendingExpiryRetentionAndKeyLifetime(t *testing.T) {
	c := integrationConfig(t)
	c.PendingTimeout = 10 * time.Millisecond
	c.Retention = 20 * time.Millisecond
	s, err := Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	d := mustValid(t, graph(node("a", "input")))
	id, err := s.Submit(context.Background(), d, "key")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(15 * time.Millisecond)
	if err = s.Maintain(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	v, err := s.Status(context.Background(), id, 0, 100)
	if err != nil || v["state"] != "EXPIRED" {
		t.Fatalf("%+v %v", v, err)
	}
	time.Sleep(25 * time.Millisecond)
	if err = s.Maintain(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Status(context.Background(), id, 0, 100); err == nil {
		t.Fatal("retained expired execution")
	}
	other, err := s.Submit(context.Background(), d, "key")
	if err != nil || other == id {
		t.Fatalf("key did not expire: %s %v", other, err)
	}
}
func TestIntegrationExactAdmissionAndHTTPBounds(t *testing.T) {
	c := integrationConfig(t)
	c.MaxExecutions = 3
	c.MaxRecords = 3
	e := startTest(t, c)
	d := mustValid(t, graph(node("a", "input")))
	var wg sync.WaitGroup
	results := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Go(func() { _, err := e.Store.Submit(context.Background(), d, uuid()); results <- err })
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else {
			var p *Problem
			if !errors.As(err, &p) || p.Code != "EXECUTION_CAPACITY" {
				t.Fatal(err)
			}
		}
	}
	if accepted != 3 {
		t.Fatalf("accepted %d", accepted)
	}
	h := NewHandler(e)
	r := httptest.NewRequest("POST", "/workflow", strings.NewReader(strings.Repeat("x", c.MaxBodyBytes+1)))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatalf("body limit: %d", w.Code)
	}
}
func TestIntegrationExpiredRecoveryDoesNotRestartHandlers(t *testing.T) {
	c := integrationConfig(t)
	c.ExecutionTimeout = 50 * time.Millisecond
	c.HandlerTimeout = 40 * time.Millisecond
	s, err := Open(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	d := mustValid(t, graph(node("a", "input")))
	id, _ := s.Submit(context.Background(), d, "")
	if err = s.Trigger(context.Background(), id, Trigger{}); err != nil {
		t.Fatal(err)
	}
	task, err := s.Claim(context.Background())
	if err != nil || task == nil {
		t.Fatalf("%+v %v", task, err)
	}
	s.Close()
	time.Sleep(70 * time.Millisecond)
	e := startTest(t, c)
	v, err := e.Store.Status(context.Background(), id, 0, 100)
	if err != nil || v["state"] != "EXPIRED" || v["nodes"].([]map[string]any)[0]["starts"] != 1 {
		t.Fatalf("%+v %v", v, err)
	}
}
