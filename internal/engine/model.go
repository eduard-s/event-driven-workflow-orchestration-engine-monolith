package engine

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

type Problem struct {
	Status    int    `json:"-"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Path      string `json:"path,omitempty"`
	Retryable bool   `json:"retryable,omitempty"`
}

func (p *Problem) Error() string { return p.Code + ": " + p.Message }
func problem(status int, code, msg string) *Problem {
	return &Problem{Status: status, Code: code, Message: msg}
}

type Definition struct {
	Name string `json:"name"`
	DAG  struct {
		Nodes []Node `json:"nodes"`
	} `json:"dag"`
}
type Node struct {
	ID           string          `json:"id"`
	Handler      string          `json:"handler"`
	Dependencies []string        `json:"dependencies"`
	Config       map[string]any  `json:"config,omitempty"`
	Data         json.RawMessage `json:"data,omitempty"`
}
type Trigger struct {
	Input      any                       `json:"input"`
	NodeParams map[string]map[string]any `json:"node_params"`
}
type Request struct {
	Handler string         `json:"handler"`
	Data    any            `json:"data"`
	Config  map[string]any `json:"config"`
	Fixture map[string]any `json:"fixture,omitempty"`
}
type Policy struct {
	HandlerSeconds   float64 `json:"handler_seconds"`
	ExecutionSeconds float64 `json:"execution_seconds"`
	PendingSeconds   float64 `json:"pending_seconds"`
	RetentionSeconds float64 `json:"retention_seconds"`
	RetrySeconds     float64 `json:"retry_seconds"`
	MaxTries         int     `json:"max_tries"`
	Slots            int     `json:"slots"`
	ValueBytes       int     `json:"value_bytes"`
	ExecutionBytes   int     `json:"execution_bytes"`
}

func (c Config) policy() Policy {
	return Policy{c.HandlerTimeout.Seconds(), c.ExecutionTimeout.Seconds(), c.PendingTimeout.Seconds(), c.Retention.Seconds(), c.RetryBase.Seconds(), c.MaxTries, c.ExecutionSlots, c.MaxValueBytes, c.MaxExecutionBytes}
}
func uuid() string {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic(e)
	}
	b[6] = b[6]&15 | 64
	b[8] = b[8]&63 | 128
	s := hex.EncodeToString(b[:])
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}

var idPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,63}$`)
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func decodeJSON(data []byte, target any) error {
	if !utf8.Valid(data) || !validJSONEscapes(data) {
		return problem(422, "INVALID_UNICODE", "JSON text must use UTF-8.")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	d.DisallowUnknownFields()
	if e := d.Decode(target); e != nil {
		return problem(422, "INVALID_SCHEMA", "The request does not match the schema.")
	}
	if e := d.Decode(new(any)); e != io.EOF {
		return problem(422, "INVALID_JSON", "Use one JSON document.")
	}
	var value any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if e := dec.Decode(&value); e != nil {
		return e
	}
	return examineValue(value, 0)
}
func examineValue(v any, depth int) error {
	switch x := v.(type) {
	case json.Number:
		text := string(x)
		if len(text) > 4300 {
			return problem(422, "INVALID_NUMBER", "The number exceeds its length limit.")
		}
		if strings.ContainsAny(text, ".eE") {
			n, err := strconv.ParseFloat(text, 64)
			if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
				return problem(422, "INVALID_NUMBER", "JSON numbers must be finite.")
			}
		}
	case string:
		if strings.ContainsRune(x, 0) {
			return problem(422, "INVALID_UNICODE", "JSON text cannot contain NUL.")
		}
	case map[string]any:
		if depth >= 32 {
			return problem(422, "JSON_DEPTH_LIMIT", "The JSON depth exceeds the limit.")
		}
		for k, v := range x {
			if e := examineValue(k, depth); e != nil {
				return e
			}
			if e := examineValue(v, depth+1); e != nil {
				return e
			}
		}
	case []any:
		if depth >= 32 {
			return problem(422, "JSON_DEPTH_LIMIT", "The JSON depth exceeds the limit.")
		}
		for _, v := range x {
			if e := examineValue(v, depth+1); e != nil {
				return e
			}
		}
	}
	return nil
}
func valueJSON(v any) []byte {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(v); err != nil {
		panic(err)
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n"))
}

func native(data []byte) (any, error) {
	var v any
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	e := d.Decode(&v)
	return v, e
}

func Validate(data []byte, c Config) (Definition, error) {
	var d Definition
	if e := decodeJSON(data, &d); e != nil {
		return d, e
	}
	if strings.TrimSpace(d.Name) == "" || len(d.DAG.Nodes) == 0 || len(d.DAG.Nodes) > c.MaxNodes {
		return d, problem(422, "INVALID_SCHEMA", "Supply a name and a bounded nonempty graph.")
	}
	nodes := map[string]Node{}
	edges := 0
	for _, n := range d.DAG.Nodes {
		if !idPattern.MatchString(n.ID) || n.Dependencies == nil {
			return d, problem(422, "INVALID_SCHEMA", "Node IDs and dependencies are necessary.")
		}
		if _, ok := nodes[n.ID]; ok {
			return d, problem(422, "DUPLICATE_NODE", "Node IDs must be unique.")
		}
		nodes[n.ID] = n
		edges += len(n.Dependencies)
		if e := validateNode(n); e != nil {
			return d, e
		}
	}
	if edges > c.MaxEdges {
		return d, problem(422, "EDGE_LIMIT", "The graph exceeds the edge limit.")
	}
	for _, n := range d.DAG.Nodes {
		seen := map[string]bool{}
		for _, p := range n.Dependencies {
			pn, ok := nodes[p]
			if !ok {
				return d, problem(422, "MISSING_DEPENDENCY", "A dependency does not name a node.")
			}
			if p == n.ID {
				return d, problem(422, "SELF_DEPENDENCY", "A node cannot depend on itself.")
			}
			if seen[p] {
				return d, problem(422, "DUPLICATE_DEPENDENCY", "Dependencies must be unique.")
			}
			if pn.Handler == "output" {
				return d, problem(422, "OUTPUT_NOT_TERMINAL", "Output nodes cannot have dependents.")
			}
			seen[p] = true
		}
	}
	ancestors, e := ancestorSets(d)
	if e != nil {
		return d, e
	}
	for _, n := range d.DAG.Nodes {
		for _, v := range templateValues(n, nil) {
			if e := validateReferences(v, ancestors[n.ID]); e != nil {
				return d, e
			}
		}
	}
	return d, nil
}
func validateNode(n Node) error {
	switch n.Handler {
	case "input":
		if n.Config != nil || n.Data != nil {
			return problem(422, "INVALID_SCHEMA", "Input nodes contain no data or config.")
		}
	case "output":
		if n.Config != nil {
			return problem(422, "INVALID_SCHEMA", "Output nodes contain no config.")
		}
	case "llm":
		if len(n.Config) != 1 || n.Data != nil {
			return problem(422, "INVALID_SCHEMA", "LLM nodes need a prompt config.")
		}
		if _, ok := n.Config["prompt"].(string); !ok {
			return problem(422, "INVALID_SCHEMA", "The prompt must be text.")
		}
	case "call_external_service":
		s, ok := n.Config["url"].(string)
		u, e := url.Parse(s)
		if !ok || len(n.Config) != 1 || e != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || strings.IndexFunc(s, unicode.IsSpace) >= 0 {
			return problem(422, "INVALID_SCHEMA", "The external URL must use HTTP or HTTPS.")
		}
	default:
		return problem(422, "UNKNOWN_HANDLER", "The handler is unsupported.")
	}
	return nil
}
func ancestorSets(d Definition) (map[string]map[string]bool, error) {
	nodes := map[string]Node{}
	for _, n := range d.DAG.Nodes {
		nodes[n.ID] = n
	}
	result := map[string]map[string]bool{}
	visiting := map[string]bool{}
	var visit func(string) (map[string]bool, error)
	visit = func(id string) (map[string]bool, error) {
		if a, ok := result[id]; ok {
			return a, nil
		}
		if visiting[id] {
			return nil, problem(422, "GRAPH_CYCLE", "The workflow contains a dependency cycle.")
		}
		visiting[id] = true
		a := map[string]bool{}
		for _, p := range nodes[id].Dependencies {
			pa, e := visit(p)
			if e != nil {
				return nil, e
			}
			a[p] = true
			for k := range pa {
				a[k] = true
			}
		}
		visiting[id] = false
		result[id] = a
		return a, nil
	}
	for id := range nodes {
		if _, e := visit(id); e != nil {
			return nil, e
		}
	}
	return result, nil
}
func templateValues(n Node, params map[string]any) []any {
	if n.Handler == "input" {
		return nil
	}
	if n.Handler == "llm" {
		if v, ok := params["prompt"]; ok {
			return []any{v}
		}
		return []any{n.Config["prompt"]}
	}
	if v, ok := params["data"]; ok {
		return []any{v}
	}
	if n.Data != nil {
		v, _ := native(n.Data)
		return []any{v}
	}
	return nil
}
func ValidateTrigger(data []byte, d Definition) (Trigger, error) {
	var t Trigger
	if len(bytes.TrimSpace(data)) == 0 {
		data = []byte(`{}`)
	}
	if bytes.TrimSpace(data)[0] != '{' {
		return t, problem(422, "INVALID_SCHEMA", "The trigger must be an object.")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return t, problem(422, "INVALID_JSON", "The trigger is invalid JSON.")
	}
	if raw, ok := fields["node_params"]; ok {
		var params map[string]json.RawMessage
		if err := json.Unmarshal(raw, &params); err != nil || params == nil {
			return t, problem(422, "INVALID_OVERRIDE", "Node parameters must be objects.")
		}
		for _, entry := range params {
			var object map[string]json.RawMessage
			if err := json.Unmarshal(entry, &object); err != nil || object == nil {
				return t, problem(422, "INVALID_OVERRIDE", "Node parameters must be objects.")
			}
		}
	}
	if e := decodeJSON(data, &t); e != nil {
		return t, e
	}
	if t.NodeParams == nil {
		t.NodeParams = map[string]map[string]any{}
	}
	a, _ := ancestorSets(d)
	nodes := map[string]Node{}
	for _, n := range d.DAG.Nodes {
		nodes[n.ID] = n
	}
	for id, params := range t.NodeParams {
		n, ok := nodes[id]
		if !ok {
			return t, problem(422, "UNKNOWN_OVERRIDE_NODE", "The override does not name a node.")
		}
		for k, v := range params {
			if n.Handler == "llm" && k == "prompt" {
				if _, ok := v.(string); !ok {
					return t, problem(422, "INVALID_OVERRIDE", "The prompt override must be text.")
				}
			} else if (n.Handler != "output" && n.Handler != "call_external_service") || k != "data" {
				return t, problem(422, "INVALID_OVERRIDE", "The node override contains an unsupported field.")
			}
			if e := validateReferences(v, a[id]); e != nil {
				return t, e
			}
		}
	}
	return t, nil
}
func (d Definition) node(id string) (Node, error) {
	for _, n := range d.DAG.Nodes {
		if n.ID == id {
			return n, nil
		}
	}
	return Node{}, fmt.Errorf("node %s is absent", id)
}

// Reject unpaired Unicode surrogate escapes instead of silently replacing them.
func validJSONEscapes(data []byte) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != '\\' {
			continue
		}
		i++
		if i >= len(data) {
			return false
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return false
		}
		n, e := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if e != nil {
			return false
		}
		i += 4
		if n >= 0xDC00 && n <= 0xDFFF {
			return false
		}
		if n >= 0xD800 && n <= 0xDBFF {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return false
			}
			next, e := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if e != nil || next < 0xDC00 || next > 0xDFFF {
				return false
			}
			i += 6
		}
	}
	return true
}
