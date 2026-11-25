package mcpproxy

import (
	"strings"
	"testing"
)

// A reserved key spelled in another case must not pass as "not a tools/call":
// a server whose decoder folds key case would run it ungated.
func TestNonCanonicalReservedKeysAreRefused(t *testing.T) {
	tests := []struct {
		name string
		line string
		want int
	}{
		{"METHOD only", `{"jsonrpc":"2.0","id":1,"METHOD":"tools/call","params":{"name":"x","arguments":{}}}`, rpcInvalidRequest},
		{"Method only", `{"jsonrpc":"2.0","id":2,"Method":"tools/call","params":{"name":"x","arguments":{}}}`, rpcInvalidRequest},
		{"long s in params key", "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"tools/call\",\"param\u017f\":{\"name\":\"x\"}}", rpcInvalidRequest},
		{"JSONRPC", `{"JSONRPC":"2.0","id":4,"method":"tools/call","params":{"name":"x","arguments":{}}}`, rpcInvalidRequest},
		{"method value case", `{"jsonrpc":"2.0","id":5,"method":"Tools/Call","params":{"name":"x","arguments":{}}}`, rpcInvalidRequest},
		{"NAME in params", `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"x","ARGUMENTS":{}}}`, rpcInvalidParams},
		{"canonical call passes", `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"x","arguments":{}}}`, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := newFakeClock()
			h := newHarness(t, Config{Clock: clk.Now}, okExcept())
			h.send(tc.line + "\n")
			if tc.want == 0 {
				if got := h.readLine(); !strings.Contains(got, `"result"`) {
					t.Fatalf("canonical call: got %q, want a result", got)
				}
			} else if code, _ := h.readError(); code != tc.want {
				t.Fatalf("got code %d, want %d", code, tc.want)
			}
			h.close()
			want := 0
			if tc.want == 0 {
				want = 1
			}
			if n := len(h.serverSeen()); n != want {
				t.Fatalf("child received %d lines, want %d: %q", n, want, h.serverSeen())
			}
		})
	}
}
