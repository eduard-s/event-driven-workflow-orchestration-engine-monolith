package engine

import (
	"encoding/json"
	"strings"
	"testing"
)

func graph(nodes ...Node) Definition { d := Definition{Name: "test"}; d.DAG.Nodes = nodes; return d }
func node(id, handler string, parents ...string) Node {
	if parents == nil {
		parents = []string{}
	}
	n := Node{ID: id, Handler: handler, Dependencies: parents}
	if handler == "llm" {
		n.Config = map[string]any{"prompt": "hello"}
	}
	if handler == "call_external_service" {
		n.Config = map[string]any{"url": "http://mock"}
	}
	return n
}
func mustValid(t *testing.T, d Definition) Definition {
	t.Helper()
	v, e := Validate(valueJSON(d), DefaultConfig())
	if e != nil {
		t.Fatal(e)
	}
	return v
}
func TestGraphValidation(t *testing.T) {
	cases := []struct {
		name string
		d    Definition
		code string
	}{
		{"linear", graph(node("a", "input"), node("b", "llm", "a"), node("c", "output", "b")), ""},
		{"cycle", graph(node("a", "llm", "b"), node("b", "llm", "a")), "GRAPH_CYCLE"},
		{"self", graph(node("a", "input", "a")), "SELF_DEPENDENCY"},
		{"absent", graph(node("a", "input", "missing")), "MISSING_DEPENDENCY"},
		{"duplicate", graph(node("a", "input"), node("a", "input")), "DUPLICATE_NODE"},
		{"duplicate_edge", graph(node("a", "input"), node("b", "llm", "a", "a")), "DUPLICATE_DEPENDENCY"},
		{"output_parent", graph(node("a", "output"), node("b", "llm", "a")), "OUTPUT_NOT_TERMINAL"},
		{"unknown", graph(node("a", "unknown")), "UNKNOWN_HANDLER"},
		{"bad_id", graph(node("2a", "input")), "INVALID_SCHEMA"},
		{"empty", graph(), "INVALID_SCHEMA"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			_, e := Validate(valueJSON(tt.d), DefaultConfig())
			if tt.code == "" {
				if e != nil {
					t.Fatal(e)
				}
			} else if p, ok := e.(*Problem); !ok || p.Code != tt.code {
				t.Fatalf("want %s, got %v", tt.code, e)
			}
		})
	}
}
func TestStrictJSON(t *testing.T) {
	for _, s := range []string{`{"name":"x","dag":{"nodes":[]},"extra":1}`, `{} {}`, `{"name":3}`, `null`, `{"name":"x","dag":{"nodes":[{"id":"a","handler":"input","dependencies":null}]}}`} {
		if _, e := Validate([]byte(s), DefaultConfig()); e == nil {
			t.Errorf("accepted %s", s)
		}
	}
	deep := strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34)
	var v any
	if e := decodeJSON([]byte(deep), &v); e == nil {
		t.Fatal("accepted excessive depth")
	}
}
func TestTemplateGrammarAndTypes(t *testing.T) {
	outputs := map[string]any{"a": map[string]any{"value": json.Number("9007199254740993"), "array": []any{nil, true, "text"}, "a.b": "quoted", "}}": "braces"}}
	cases := []struct {
		input string
		want  any
	}{
		{"{{ a.value }}", json.Number("9007199254740993")},
		{"x {{ a.array[1] }}", "x true"},
		{`{{ a["a.b"] }}`, "quoted"},
		{`{{ a["}}"] }}`, "braces"},
		{"{{ a.array[0] }}", nil},
		{"{{{{ a }}}}", "{{ a }}"},
		{" {{ a.array[2] }} ", "text"},
	}
	for _, tt := range cases {
		t.Run(tt.input, func(t *testing.T) {
			r := resolver{remaining: 10000}
			v, e := r.text(tt.input, outputs, false)
			if e != nil || string(valueJSON(v)) != string(valueJSON(tt.want)) {
				t.Fatalf("%v %v", v, e)
			}
		})
	}
	for _, s := range []string{"{{ a[01] }}", "{{ a[-1] }}", "{{ a..x }}", "{{ a.x", "}}", "{{ a['x'] }}", "{{ 1 }}"} {
		if _, e := parseTemplate(s); e == nil {
			t.Errorf("accepted %s", s)
		}
	}
}
func TestReferenceAndOverrideRules(t *testing.T) {
	a := node("a", "input")
	b := node("b", "llm", "a")
	b.Config["prompt"] = "{{ a.id }}"
	d := mustValid(t, graph(a, b))
	for _, raw := range []string{`{"node_params":{"b":{"prompt":"{{ b }}"}}}`, `{"node_params":{"b":{"url":"x"}}}`, `{"node_params":{"absent":{}}}`, `{"node_params":{"b":{"prompt":3}}}`} {
		if _, e := ValidateTrigger([]byte(raw), d); e == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	b.Config["prompt"] = "{{ absent }}"
	if _, e := Validate(valueJSON(graph(a, b)), DefaultConfig()); e == nil {
		t.Fatal("accepted invalid ancestor")
	}
}
func TestResolutionBudgetsAndAbsentData(t *testing.T) {
	n := node("o", "output", "a")
	req, e := Resolve(n, Trigger{}, map[string]any{"a": nil}, 1000)
	if e != nil || string(valueJSON(req.Data)) != `{"a":null}` {
		t.Fatalf("%+v %v", req, e)
	}
	n.Data = []byte(`null`)
	req, e = Resolve(n, Trigger{}, nil, 1000)
	if e != nil || req.Data != nil {
		t.Fatalf("%+v %v", req, e)
	}
	n.Data = []byte(`"{{ a }}{{ a }}{{ a }}"`)
	if _, e = Resolve(n, Trigger{}, map[string]any{"a": strings.Repeat("x", 100)}, 200); e == nil {
		t.Fatal("unbounded resolution")
	}
}
func TestConfigLimits(t *testing.T) {
	t.Setenv("WF_WORKER_CONCURRENCY", "0")
	if _, e := ConfigFromEnv(); e == nil {
		t.Fatal("accepted invalid concurrency")
	}
}

func FuzzTemplateParser(f *testing.F) {
	for _, s := range []string{"{{ a.id }}", `{{ a["}}"] }}`, "{{{{", "{{ a[0] }}", "{{ a[", "text"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		if len(s) > 4096 {
			t.Skip()
		}
		_, _ = parseTemplate(s)
	})
}

func TestUnicodeAndNumberSafety(t *testing.T) {
	for _, raw := range []string{`{"input":"\ud800"}`, `{"input":"\udc00"}`, `{"input":1e99999}`, `null`, `{"node_params":null}`, `{"node_params":{"a":null}}`} {
		if _, e := ValidateTrigger([]byte(raw), graph(node("a", "input"))); e == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	for _, raw := range []string{`{"input":"\ud83d\ude00"}`, `{"input":"\\ud800"}`, `{"input":9007199254740993}`} {
		if _, e := ValidateTrigger([]byte(raw), graph(node("a", "input"))); e != nil {
			t.Errorf("rejected %s: %v", raw, e)
		}
	}
}
