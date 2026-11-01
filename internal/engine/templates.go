package engine

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

type reference struct {
	node  string
	steps []any
}
type token struct {
	literal string
	ref     *reference
}

func parseTemplate(text string) ([]token, error) {
	var result []token
	var literal strings.Builder
	flush := func() {
		if literal.Len() > 0 {
			result = append(result, token{literal: literal.String()})
			literal.Reset()
		}
	}
	bad := func() ([]token, error) {
		return nil, problem(422, "INVALID_TEMPLATE", "The template reference has invalid syntax.")
	}
	for i := 0; i < len(text); {
		switch {
		case strings.HasPrefix(text[i:], "{{{{"):
			literal.WriteString("{{")
			i += 4
		case strings.HasPrefix(text[i:], "}}}}"):
			literal.WriteString("}}")
			i += 4
		case strings.HasPrefix(text[i:], "}}"):
			return bad()
		case strings.HasPrefix(text[i:], "{{"):
			flush()
			start := i + 2
			j := start
			quoted, escaped := false, false
			for ; j < len(text); j++ {
				c := text[j]
				if quoted {
					if escaped {
						escaped = false
					} else if c == '\\' {
						escaped = true
					} else if c == '"' {
						quoted = false
					}
				} else if c == '"' {
					quoted = true
				} else if strings.HasPrefix(text[j:], "}}") {
					break
				}
			}
			if j >= len(text) {
				return bad()
			}
			r, e := parseReference(strings.TrimSpace(text[start:j]))
			if e != nil {
				return nil, e
			}
			result = append(result, token{ref: &r})
			i = j + 2
		default:
			literal.WriteByte(text[i])
			i++
		}
	}
	flush()
	return result, nil
}
func parseReference(s string) (reference, error) {
	r := reference{}
	bad := func() (reference, error) {
		return r, problem(422, "INVALID_TEMPLATE", "The template reference has invalid syntax.")
	}
	i := 0
	for i < len(s) && ((s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= 'a' && s[i] <= 'z') || (s[i] >= '0' && s[i] <= '9') || s[i] == '_' || s[i] == '-') {
		i++
	}
	if !idPattern.MatchString(s[:i]) {
		return bad()
	}
	r.node = s[:i]
	for i < len(s) {
		if s[i] == '.' {
			i++
			start := i
			for i < len(s) && ((s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= 'a' && s[i] <= 'z') || (s[i] >= '0' && s[i] <= '9') || s[i] == '_') {
				i++
			}
			if start == i || s[start] >= '0' && s[start] <= '9' {
				return bad()
			}
			r.steps = append(r.steps, s[start:i])
		} else if s[i] == '[' {
			i++
			if i >= len(s) {
				return bad()
			}
			if s[i] == '"' {
				start := i
				i++
				escaped := false
				for i < len(s) {
					if !escaped && s[i] == '"' {
						break
					}
					if !escaped && s[i] == '\\' {
						escaped = true
					} else {
						escaped = false
					}
					i++
				}
				if i >= len(s) {
					return bad()
				}
				var key string
				if e := json.Unmarshal([]byte(s[start:i+1]), &key); e != nil {
					return bad()
				}
				r.steps = append(r.steps, key)
				i++
			} else {
				start := i
				for i < len(s) && s[i] >= '0' && s[i] <= '9' {
					i++
				}
				if start == i || i-start > 1 && s[start] == '0' {
					return bad()
				}
				index, e := strconv.Atoi(s[start:i])
				if e != nil {
					return bad()
				}
				r.steps = append(r.steps, index)
			}
			if i >= len(s) || s[i] != ']' {
				return bad()
			}
			i++
		} else {
			return bad()
		}
	}
	return r, nil
}
func validateReferences(v any, ancestors map[string]bool) error {
	switch x := v.(type) {
	case string:
		tokens, e := parseTemplate(x)
		if e != nil {
			return e
		}
		for _, t := range tokens {
			if t.ref != nil && !ancestors[t.ref.node] {
				return problem(422, "INVALID_REFERENCE", "A template must reference a strict ancestor.")
			}
		}
	case []any:
		for _, v := range x {
			if e := validateReferences(v, ancestors); e != nil {
				return e
			}
		}
	case map[string]any:
		for _, v := range x {
			if e := validateReferences(v, ancestors); e != nil {
				return e
			}
		}
	}
	return nil
}
func lookup(r reference, outputs map[string]any) (any, error) {
	v, ok := outputs[r.node]
	if !ok {
		return nil, problem(422, "MISSING_OUTPUT", "An ancestor output is unavailable.")
	}
	for _, step := range r.steps {
		switch s := step.(type) {
		case string:
			m, ok := v.(map[string]any)
			if !ok {
				return nil, problem(422, "REFERENCE_TYPE", "The reference needs a JSON object.")
			}
			v, ok = m[s]
			if !ok {
				return nil, problem(422, "MISSING_FIELD", "The referenced field is absent.")
			}
		case int:
			a, ok := v.([]any)
			if !ok {
				return nil, problem(422, "REFERENCE_TYPE", "The reference needs a JSON array.")
			}
			if s >= len(a) {
				return nil, problem(422, "INVALID_INDEX", "The array index is outside the output.")
			}
			v = a[s]
		}
	}
	return v, nil
}

type resolver struct{ remaining int }

func (r *resolver) take(n int) error {
	r.remaining -= n
	if r.remaining < 0 {
		return problem(422, "RESOLVED_INPUT_LIMIT", "The resolved input exceeds its byte limit.")
	}
	return nil
}
func (r *resolver) literal(v any) (any, error) {
	b := valueJSON(v)
	if e := r.take(len(b)); e != nil {
		return nil, e
	}
	return v, nil
}
func (r *resolver) text(s string, outputs map[string]any, force bool) (any, error) {
	tokens, e := parseTemplate(s)
	if e != nil {
		return nil, e
	}
	var significant []token
	for _, t := range tokens {
		if t.ref != nil || strings.TrimSpace(t.literal) != "" {
			significant = append(significant, t)
		}
	}
	if len(significant) == 1 && significant[0].ref != nil {
		v, e := lookup(*significant[0].ref, outputs)
		if e != nil {
			return nil, e
		}
		if !force {
			return r.literal(v)
		}
		if text, ok := v.(string); ok {
			return r.literal(text)
		}
		return r.literal(string(valueJSON(v)))
	}
	var b strings.Builder
	for _, t := range tokens {
		piece := t.literal
		if t.ref != nil {
			v, e := lookup(*t.ref, outputs)
			if e != nil {
				return nil, e
			}
			if s, ok := v.(string); ok {
				piece = s
			} else {
				piece = string(valueJSON(v))
			}
		}
		if e := r.take(len(valueJSON(piece))); e != nil {
			return nil, e
		}
		b.WriteString(piece)
	}
	return b.String(), nil
}
func (r *resolver) resolve(v any, outputs map[string]any) (any, error) {
	switch x := v.(type) {
	case string:
		return r.text(x, outputs, false)
	case map[string]any:
		if e := r.take(2 + len(x)); e != nil {
			return nil, e
		}
		m := make(map[string]any, len(x))
		for k, v := range x {
			if e := r.take(len(valueJSON(k)) + 1); e != nil {
				return nil, e
			}
			rv, e := r.resolve(v, outputs)
			if e != nil {
				return nil, e
			}
			m[k] = rv
		}
		return m, nil
	case []any:
		if e := r.take(2 + len(x)); e != nil {
			return nil, e
		}
		a := make([]any, len(x))
		for i, v := range x {
			rv, e := r.resolve(v, outputs)
			if e != nil {
				return nil, e
			}
			a[i] = rv
		}
		return a, nil
	default:
		return r.literal(v)
	}
}
func Resolve(n Node, t Trigger, outputs map[string]any, limit int) (Request, error) {
	r := resolver{remaining: limit}
	req := Request{Handler: n.Handler, Config: map[string]any{}}
	params := t.NodeParams[n.ID]
	switch n.Handler {
	case "input":
		v, e := r.literal(t.Input)
		if e != nil {
			return req, e
		}
		req.Data = v
	case "llm":
		prompt := n.Config["prompt"]
		if p, ok := params["prompt"]; ok {
			prompt = p
		}
		v, e := r.text(prompt.(string), outputs, true)
		if e != nil {
			return req, e
		}
		req.Config["prompt"] = v
	default:
		if n.Handler == "call_external_service" {
			req.Config["url"] = n.Config["url"]
		}
		var raw any
		var exists bool
		if raw, exists = params["data"]; !exists && n.Data != nil {
			var e error
			raw, e = native(n.Data)
			if e != nil {
				return req, e
			}
			exists = true
		}
		if n.Handler == "output" && !exists {
			m := map[string]any{}
			for _, p := range n.Dependencies {
				v, ok := outputs[p]
				if !ok {
					return req, fmt.Errorf("missing output %s", p)
				}
				m[p] = v
			}
			v, e := r.literal(m)
			if e != nil {
				return req, e
			}
			req.Data = v
		} else {
			v, e := r.resolve(raw, outputs)
			if e != nil {
				return req, e
			}
			req.Data = v
		}
	}
	if e := examineValue(map[string]any{"handler": req.Handler, "config": req.Config, "data": req.Data}, 0); e != nil {
		return req, e
	}
	if len(valueJSON(req)) > limit {
		return req, problem(422, "RESOLVED_INPUT_LIMIT", "The resolved input exceeds its byte limit.")
	}
	return req, nil
}
