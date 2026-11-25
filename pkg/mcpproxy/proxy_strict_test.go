package mcpproxy

import (
	"encoding/json"
	"strings"
	"testing"
)

// readError reads one reply and returns its JSON-RPC error code and raw id.
func (h *testHarness) readError() (int, string) {
	h.t.Helper()
	line := h.readLine()
	var msg rpcMessage
	if err := json.Unmarshal([]byte(line), &msg); err != nil {
		h.t.Fatalf("unmarshal reply %q: %v", line, err)
	}
	if msg.Error == nil {
		h.t.Fatalf("reply is not a JSON-RPC error: %s", line)
	}
	return msg.Error.Code, string(msg.ID)
}

func toolsCallsSeen(h *testHarness) int {
	n := 0
	for _, l := range h.serverSeen() {
		if strings.Contains(strings.ToLower(l), `"tools/call"`) {
			n++
		}
	}
	return n
}

// A key that differs only in case, or repeats, must not let the proxy and the
// server read different methods from one line.
func TestStrictEnvelopeRefusesKeyShadows(t *testing.T) {
	tests := []struct {
		name string
		line string
		want int // JSON-RPC error code; 0 means the call must pass
	}{
		{"METHOD shadow", `{"jsonrpc":"2.0","id":1,"method":"tools/call","METHOD":"ping","params":{"name":"echo","arguments":{}}}`, rpcInvalidRequest},
		{"Method shadow before method", `{"jsonrpc":"2.0","id":1,"Method":"ping","method":"tools/call","params":{"name":"echo","arguments":{}}}`, rpcInvalidRequest},
		{"exact duplicate method", `{"jsonrpc":"2.0","id":1,"method":"tools/call","method":"ping","params":{"name":"echo","arguments":{}}}`, rpcInvalidRequest},
		{"NAME shadow in params", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","NAME":"safe","arguments":{}}}`, rpcInvalidRequest},
		{"unrelated key collision", `{"jsonrpc":"2.0","id":1,"method":"tools/list","Foo":1,"foo":2}`, rpcInvalidRequest},
		{"normal call", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"echo","arguments":{}}}`, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, Config{Clock: newFakeClock().Now}, okExcept())
			h.send(tc.line + "\n")
			if tc.want == 0 {
				var msg rpcMessage
				if err := json.Unmarshal([]byte(h.readLine()), &msg); err != nil || msg.Error != nil {
					t.Fatalf("normal call was not answered by the child: %v %+v", err, msg.Error)
				}
				h.close()
				if got := toolsCallsSeen(h); got != 1 {
					t.Fatalf("child saw %d tools/call, want 1", got)
				}
				return
			}
			code, id := h.readError()
			if code != tc.want {
				t.Fatalf("code = %d, want %d", code, tc.want)
			}
			if id != "1" {
				t.Fatalf("id = %s, want 1 (a refusal must answer the request)", id)
			}
			h.close()
			if got := len(h.serverSeen()); got != 0 {
				t.Fatalf("child received %d lines %q, want none", got, h.serverSeen())
			}
		})
	}
}

// A shadow must not spend the rate window or concurrency cap either: after
// three shadowed calls the one normal call still gets in.
func TestStrictEnvelopeShadowsDoNotReachChildUnderTightRate(t *testing.T) {
	h := newHarness(t, Config{Clock: newFakeClock().Now, RateCount: 1, RateWindow: 60e9, MaxConcurrent: 1}, okExcept())
	for i := 0; i < 3; i++ {
		h.send(`{"jsonrpc":"2.0","id":1,"method":"tools/call","METHOD":"ping","params":{"name":"echo","arguments":{}}}` + "\n")
		h.readError()
	}
	if res := h.call("2", "echo", `{}`); res.IsError {
		t.Fatalf("normal call refused: %+v", res)
	}
	h.close()
	if got := toolsCallsSeen(h); got != 1 {
		t.Fatalf("child saw %d tools/call, want exactly the 1 normal call", got)
	}
}

func TestDuplicateInFlightIDRefusedAndOlderLeaseKept(t *testing.T) {
	h := newHarness(t, Config{Clock: newFakeClock().Now, MaxConcurrent: 2}, okExcept("7", "8"))
	h.send(callLine("7", "slow", `{}`))
	for i := 0; i < 2; i++ {
		h.send(callLine("7", "slow", `{}`))
		code, id := h.readError()
		if code != rpcInvalidRequest || id != "7" {
			t.Fatalf("duplicate %d: code=%d id=%s, want %d and 7", i, code, id, rpcInvalidRequest)
		}
	}
	// The first call still holds its lease, so with a cap of 2 one slot is free
	// and a second distinct call is admitted, a third is not.
	h.send(callLine("8", "slow", `{}`))
	h.send(callLine("9", "slow", `{}`))
	res := h.readLine()
	if !strings.Contains(res, `"id":9`) || !strings.Contains(res, "isError") {
		t.Fatalf("third distinct in-flight call should be deferred by the cap of 2, got %s", res)
	}
	h.close()
	if got := toolsCallsSeen(h); got != 2 {
		t.Fatalf("child saw %d tools/call, want 2 (ids 7 and 8)", got)
	}
}

func TestRefusedEnvelopeEchoesID(t *testing.T) {
	tests := []struct{ name, line, wantID string }{
		{"numeric jsonrpc, number id", `{"jsonrpc":2.0,"id":1,"method":"tools/call","params":{"name":"echo"}}`, "1"},
		{"numeric jsonrpc, string id", `{"jsonrpc":2.0,"id":"abc","method":"tools/call","params":{"name":"echo"}}`, `"abc"`},
		{"no readable id", `{"jsonrpc":2.0,"id":{"x":1},"method":"tools/call","params":{"name":"echo"}}`, "null"},
		{"id shadowed", `{"jsonrpc":"2.0","id":1,"ID":2,"method":"tools/call","params":{"name":"echo"}}`, "null"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, Config{Clock: newFakeClock().Now}, okExcept())
			h.send(tc.line + "\n")
			code, id := h.readError()
			if code != rpcInvalidRequest || id != tc.wantID {
				t.Fatalf("code=%d id=%s, want %d and %s", code, id, rpcInvalidRequest, tc.wantID)
			}
			h.close()
			if got := len(h.serverSeen()); got != 0 {
				t.Fatalf("child received %q, want nothing", h.serverSeen())
			}
		})
	}
}

func TestToolsCallNeedsObjectParamsAndStringName(t *testing.T) {
	tests := []struct{ name, params string }{
		{"missing params", ``},
		{"null params", `,"params":null`},
		{"array params", `,"params":[]`},
		{"missing name", `,"params":{"arguments":{}}`},
		{"empty name", `,"params":{"name":"","arguments":{}}`},
		{"numeric name", `,"params":{"name":5,"arguments":{}}`},
		{"null name", `,"params":{"name":null}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, Config{Clock: newFakeClock().Now}, okExcept())
			h.send(`{"jsonrpc":"2.0","id":3,"method":"tools/call"` + tc.params + "}\n")
			code, id := h.readError()
			if code != rpcInvalidParams || id != "3" {
				t.Fatalf("code=%d id=%s, want %d and 3", code, id, rpcInvalidParams)
			}
			h.close()
			if got := len(h.serverSeen()); got != 0 {
				t.Fatalf("child received %q, want nothing", h.serverSeen())
			}
		})
	}
}

func TestCancelWithShadowedRequestIDIsIgnored(t *testing.T) {
	h := newHarness(t, Config{Clock: newFakeClock().Now, MaxConcurrent: 1}, okExcept("1"))
	h.send(callLine("1", "slow", `{}`))
	// A case-folding parser lets "REQUESTID" win and cancel id 1; an exact-key
	// parser reads 99. The proxy cannot tell which, so it must settle neither.
	h.send(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":99,"REQUESTID":1}}` + "\n")
	h.send(callLine("2", "slow", `{}`))
	res := h.readLine()
	if !strings.Contains(res, `"id":2`) || !strings.Contains(res, "isError") {
		t.Fatalf("call 2 should be deferred because call 1 still holds the only slot, got %s", res)
	}
	h.close()
}

func TestCanonicalIDNumericForms(t *testing.T) {
	same := [][]string{{"1", "1.0", "1e0", "10e-1"}, {"-3", "-3.0"}}
	for _, g := range same {
		want, _, _, _ := canonicalID(json.RawMessage(g[0]))
		for _, s := range g[1:] {
			if got, _, _, _ := canonicalID(json.RawMessage(s)); got != want {
				t.Errorf("canonicalID(%s) = %q, want %q", s, got, want)
			}
		}
	}
	a, _, _, _ := canonicalID(json.RawMessage("9007199254740993"))
	b, _, _, _ := canonicalID(json.RawMessage("9007199254740992"))
	if a == b {
		t.Errorf("large distinct integers collapsed to one key %q", a)
	}
	n, _, _, _ := canonicalID(json.RawMessage("1"))
	s, _, _, _ := canonicalID(json.RawMessage(`"1"`))
	if n == s {
		t.Errorf("number 1 and string \"1\" share key %q", n)
	}
}
