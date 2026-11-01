package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"time"
)

// Built-in handlers are trusted, cooperative mocks. Arbitrary code needs process isolation.
func runTask(parent context.Context, t Task, fixtures bool) (out Outcome) {
	started := time.Now()
	out.Runtime = map[string]any{"pid": os.Getpid(), "started_at": float64(started.UnixNano()) / 1e9}
	defer func() {
		out.Runtime["finished_at"] = float64(time.Now().UnixNano()) / 1e9
		if r := recover(); r != nil {
			out.Error = &Problem{Code: "HANDLER_PANIC", Message: "The handler panicked.", Retryable: true}
			_ = debug.Stack()
		}
	}()
	ctx, cancel := context.WithTimeout(parent, t.Remaining)
	defer cancel()
	delay := time.Duration(0)
	if t.Request.Handler == "call_external_service" {
		delay = time.Second
	}
	fixture := map[string]any{}
	if fixtures {
		if data, ok := t.Request.Data.(map[string]any); ok {
			fixture, _ = data["__test"].(map[string]any)
		}
	}
	if v, ok := fixture["delay"]; ok {
		if n, ok := v.(json.Number); ok {
			f, _ := n.Float64()
			delay = time.Duration(f * float64(time.Second))
		}
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			out.Error = &Problem{Code: "HANDLER_TIMEOUT", Message: "The handler deadline elapsed.", Retryable: true}
			return
		case <-timer.C:
		}
	}
	if ctx.Err() != nil {
		out.Error = &Problem{Code: "HANDLER_TIMEOUT", Message: "The handler deadline elapsed.", Retryable: true}
		return
	}
	if fixture["panic"] == true {
		panic("fixture panic")
	}
	if fail, ok := fixture["fail"].(string); ok {
		retry := fail == "transient"
		until := 0
		if n, ok := fixture["fail_until"].(json.Number); ok {
			i, _ := n.Int64()
			until = int(i)
		}
		if until == 0 || t.Number <= until {
			out.Error = &Problem{Code: "MOCK_" + fail, Message: "The mock handler failed.", Retryable: retry}
			return
		}
	}
	if v, ok := fixture["output"]; ok {
		out.Value = v
		return
	}
	switch t.Request.Handler {
	case "input", "output":
		out.Value = t.Request.Data
	case "llm":
		prompt, ok := t.Request.Config["prompt"].(string)
		if !ok {
			out.Error = problem(422, "INVALID_HANDLER_INPUT", "The prompt must be text.")
			return
		}
		out.Value = "mock: " + prompt
	case "call_external_service":
		out.Value = map[string]any{"url": t.Request.Config["url"], "data": t.Request.Data}
	default:
		out.Error = problem(422, "UNKNOWN_HANDLER", fmt.Sprintf("Handler %s is unsupported.", t.Request.Handler))
	}
	return
}
