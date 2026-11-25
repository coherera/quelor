package mcpproxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode"
)

// object is a JSON object read key by key. encoding/json folds key case and
// lets a later duplicate win, but servers written against an exact-key parser
// (Python, Node) do neither, so the two sides could read different methods
// from one line. The proxy therefore reads only exact keys and refuses any
// object whose keys collide under case folding.
type object struct {
	fields   map[string]json.RawMessage // exact key -> raw value
	collided map[string]bool            // folded keys that occur more than once
}

// scanObject reads raw as one JSON object with nothing after it.
func scanObject(raw []byte) (*object, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, errors.New("not a JSON object")
	}
	o := &object{fields: map[string]json.RawMessage{}, collided: map[string]bool{}}
	seen := map[string]bool{}
	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, _ := kt.(string)
		var val json.RawMessage
		if err := dec.Decode(&val); err != nil {
			return nil, err
		}
		folded := foldKey(key)
		if seen[folded] {
			o.collided[folded] = true
		}
		seen[folded] = true
		o.fields[key] = val
	}
	if _, err := dec.Token(); err != nil { // the closing brace
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("trailing data after JSON object")
	}
	return o, nil
}

// foldKey maps every key in a case-folding class to one representative, so two
// keys are equal under strings.EqualFold exactly when their foldKey is equal.
func foldKey(s string) string {
	return strings.Map(func(r rune) rune {
		lowest := r
		for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
			if f < lowest {
				lowest = f
			}
		}
		return lowest
	}, s)
}

// envelopeKeys and paramsKeys are the keys the proxy reads. A server whose
// JSON decoder folds key case (Go's encoding/json does) reads "METHOD" or
// "Method" as "method", while the proxy reads only the exact key, so such a
// key must be refused rather than passed through.
var (
	envelopeKeys = []string{"jsonrpc", "id", "method", "params"}
	paramsKeys   = []string{"name", "arguments"}
)

// nonCanonicalKey returns a key that equals one of reserved under case
// folding but is not spelled exactly like it.
func (o *object) nonCanonicalKey(reserved []string) (string, bool) {
	for k := range o.fields {
		for _, r := range reserved {
			if k != r && foldKey(k) == foldKey(r) {
				return k, true
			}
		}
	}
	return "", false
}

// hasCollision reports whether any two keys are equal under case folding,
// exact duplicates included.
func (o *object) hasCollision() bool { return len(o.collided) > 0 }

// str returns the value at the exact key when it is a JSON string.
func (o *object) str(key string) (string, bool) {
	raw, ok := o.fields[key]
	if !ok {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	return s, true
}

// echoID returns the request id to put in a refusal: the value at the exact
// key "id" when it is a string or a number and no other key shadows it, else
// nil (JSON null).
func (o *object) echoID() json.RawMessage {
	raw, ok := o.fields["id"]
	if !ok || o.collided[foldKey("id")] {
		return nil
	}
	b := bytes.TrimSpace(raw)
	if len(b) > 0 && (b[0] == '"' || b[0] == '-' || (b[0] >= '0' && b[0] <= '9')) {
		return json.RawMessage(b)
	}
	return nil
}

// mentionsToolsCall reports whether any key that folds to "method" carries a
// string that folds to "tools/call". It is deliberately loose: it only decides
// whether a batch element must be refused.
func mentionsToolsCall(raw []byte) bool {
	o, err := scanObject(raw)
	if err != nil {
		return false
	}
	for k, v := range o.fields {
		var s string
		if strings.EqualFold(k, "method") && json.Unmarshal(v, &s) == nil && strings.EqualFold(s, "tools/call") {
			return true
		}
	}
	return false
}
