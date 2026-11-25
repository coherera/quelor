package mcpproxy

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// FuzzNoUngatedToolsCall checks the proxy's one invariant against several
// server readings: whatever reaches the child holds no more tools/call
// messages than the proxy admitted, whether the child splits lines on LF or
// CR, drops invalid UTF-8, folds key case, or reads exact keys.
func FuzzNoUngatedToolsCall(f *testing.F) {
	call := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"x"}}`
	for _, s := range []string{
		call,
		`{"jsonrpc":"2.0","method":"ping","params":` + "\r" + call + "\r}",
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x","arguments":[` + "\r" + call + "\r]}}",
		`[{"method":"tools/call","method":"ping"}]`,
		`[[` + call + `]]`,
		"{\"meth\xffod\":\"tools/call\"}",
		`{"METHOD":"tools/call"}`,
		`{"method":"tools/call "}`,
		`{"method":"ping"}` + call,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, line []byte) {
		if bytes.IndexByte(line, '\n') >= 0 {
			t.Skip()
		}
		_, child, _, p := ungatedCase(t, Config{}, string(line)+"\n", nil)
		worst := 0
		for _, sep := range []string{"\n", "\r"} {
			n := 0
			for _, frag := range bytes.Split([]byte(child), []byte(sep)) {
				n += max(readsAsCalls(frag), readsAsCalls(bytes.ToValidUTF8(frag, nil)))
			}
			worst = max(worst, n)
		}
		p.mu.Lock()
		admitted := len(p.inflight)
		p.mu.Unlock()
		if worst > admitted {
			t.Fatalf("%d tools/call readable by a child, %d admitted: %q", worst, admitted, child)
		}
	})
}

// readsAsCalls counts the tools/call messages some server would read in b,
// including a streaming reader that decodes every value on the line.
func readsAsCalls(b []byte) int {
	dec := json.NewDecoder(bytes.NewReader(b))
	n, values := 0, 0
	for {
		var v json.RawMessage
		if dec.Decode(&v) != nil {
			break
		}
		values++
		n += readsAsCallsOne(v)
	}
	if values > 1 {
		return n
	}
	return readsAsCallsOne(b)
}

func readsAsCallsOne(b []byte) int {
	var arr []json.RawMessage
	if json.Unmarshal(b, &arr) == nil {
		n := 0
		for _, e := range arr {
			n += readsAsCallsOne(e)
		}
		return n
	}
	isCall := func(s string) bool { return strings.EqualFold(strings.TrimSpace(s), "tools/call") }
	var folded struct{ Method string } // case-folding, last key wins
	if json.NewDecoder(bytes.NewReader(b)).Decode(&folded) == nil && isCall(folded.Method) {
		return 1
	}
	dec := json.NewDecoder(bytes.NewReader(b)) // exact key, any occurrence
	if tok, _ := dec.Token(); tok != json.Delim('{') {
		return 0
	}
	for dec.More() {
		k, _ := dec.Token()
		var v any
		if dec.Decode(&v) != nil {
			return 0
		}
		if s, _ := v.(string); k == "method" && isCall(s) {
			return 1
		}
	}
	return 0
}
