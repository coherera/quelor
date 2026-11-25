package mcpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// Each line below is one value to the proxy but can be read as a tools/call
// (or as more than one message) by some real server. None may reach the child.
func TestLinesServersReadDifferentlyAreRefused(t *testing.T) {
	call := `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"x"}}`
	cases := []struct {
		name, line string
		code       int
	}{
		{"CR inside a ping", `{"jsonrpc":"2.0","method":"ping","params":` + "\r" + call + "\r}", rpcParseError},
		{"CR inside gated arguments", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"x","arguments":[` + "\r" + call + "\r]}}", rpcParseError},
		{"invalid UTF-8 in a key", "{\"jsonrpc\":\"2.0\",\"id\":3,\"meth\xffod\":\"tools/call\",\"params\":{\"name\":\"x\"}}", rpcParseError},
		{"overlong UTF-8 in the method", "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"tools/ca\xc0\xafll\",\"params\":{\"name\":\"x\"}}", rpcParseError},
		{"trailing space in the method", `{"jsonrpc":"2.0","id":5,"method":"tools/call ","params":{"name":"x"}}`, rpcInvalidRequest},
		{"NUL in the method", `{"jsonrpc":"2.0","id":6,"method":"tools/call\u0000","params":{"name":"x"}}`, rpcInvalidRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, child, _, _ := ungatedCase(t, Config{}, tc.line+"\n", nil)
			if child != "" {
				t.Fatalf("forwarded to child: %q", child)
			}
			var reply struct {
				Error struct{ Code int } `json:"error"`
			}
			if json.Unmarshal([]byte(client), &reply) != nil || reply.Error.Code != tc.code {
				t.Fatalf("client got %q, want code %d", client, tc.code)
			}
		})
	}
}

// A CRLF line ending is still one line and passes.
func TestCRLFLineEndingIsAccepted(t *testing.T) {
	line := `{"jsonrpc":"2.0","id":1,"method":"tools/list"}` + "\r\n"
	client, child, _, _ := ungatedCase(t, Config{}, line, nil)
	if client != "" || child != line {
		t.Fatalf("CRLF line: client=%q child=%q", client, child)
	}
}

// A non-tools/call request reusing an in-flight call's id is refused: the
// child's reply to it would settle the running call's lease.
func TestRequestReusingAnInFlightIDIsRefused(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{Clock: clk.Now}, okExcept("7"))
	h.send(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"x","arguments":{}}}` + "\n")
	h.send(`{"jsonrpc":"2.0","id":7,"method":"ping"}` + "\n")
	if code, _ := h.readError(); code != rpcInvalidRequest {
		t.Fatalf("got code %d, want %d", code, rpcInvalidRequest)
	}
	h.close()
	for _, l := range h.serverSeen() {
		if strings.Contains(l, `"ping"`) {
			t.Fatalf("ping with an in-flight id reached the child: %q", h.serverSeen())
		}
	}
}

// A client response (no method) may carry any id: ids of server-to-client
// requests are a separate space.
func TestClientResponseWithAnInFlightIDIsForwarded(t *testing.T) {
	p, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	k, _, _, _ := canonicalID(json.RawMessage("7"))
	p.inflight[k] = "lease-under-test"
	var out, in bytes.Buffer
	line := `{"jsonrpc":"2.0","id":7,"result":{}}` + "\n"
	if err := p.handleClientLine(context.Background(), []byte(line), &out, &in); err != nil {
		t.Fatal(err)
	}
	if in.String() != line || out.Len() != 0 {
		t.Fatalf("response: child=%q client=%q", in.String(), out.String())
	}
}

// A message whose method is empty or not a string, reusing an in-flight id,
// must not reach the child: a server answering it would settle the call.
func TestBadMethodWithAnInFlightIDIsRefused(t *testing.T) {
	for _, m := range []string{`""`, `null`, `123`} {
		clk := newFakeClock()
		h := newHarness(t, Config{Clock: clk.Now}, okExcept("7"))
		h.send(`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"x","arguments":{}}}` + "\n")
		h.send(`{"jsonrpc":"2.0","id":7,"method":` + m + `}` + "\n")
		if code, _ := h.readError(); code != rpcInvalidRequest {
			t.Fatalf("method %s: got code %d, want %d", m, code, rpcInvalidRequest)
		}
		h.close()
		if n := len(h.serverSeen()); n != 1 {
			t.Fatalf("method %s: child received %d lines, want 1: %q", m, n, h.serverSeen())
		}
	}
}

// -0 and 0 are the same id to a server that echoes numbers.
func TestMinusZeroIDMatchesZero(t *testing.T) {
	a, _, _, _ := canonicalID(json.RawMessage("0"))
	b, _, _, _ := canonicalID(json.RawMessage("-0"))
	if a != b {
		t.Fatalf("canonical ids differ: %q vs %q", a, b)
	}
}
