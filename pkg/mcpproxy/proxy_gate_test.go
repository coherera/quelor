package mcpproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/classify"
)

func TestDefaultRateSpecMatchesConstants(t *testing.T) {
	n, d, err := ParseRate(DefaultRateSpec)
	if err != nil {
		t.Fatal(err)
	}
	if n != DefaultRateCount || d != DefaultRateWindow {
		t.Fatalf("DefaultRateSpec parses to %d/%v, constants are %d/%v", n, d, DefaultRateCount, DefaultRateWindow)
	}
}

func TestMaxConcurrentDefer(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{MaxConcurrent: 1, Clock: clk.Now}, okExcept("1"))

	h.send(callLine("1", "t", `{}`))
	waitFor(t, "first call in flight", func() bool { return h.proxy.Engine().Stats().ActiveLeases == 1 })

	res := h.call("2", "t", `{}`)
	if !res.IsError || !strings.Contains(res.Content[0].Text, "DEFER") || !strings.Contains(res.Content[0].Text, "insufficient:mcp_concurrent") {
		t.Fatalf("second call should DEFER on mcp_concurrent, got %+v", res)
	}
	h.close()

	if got := len(h.serverSeen()); got != 1 {
		t.Fatalf("child saw %d calls, want 1 (the deferred call must not reach it)", got)
	}
	if !strings.Contains(h.stderr.String(), `[quelor] DEFER id=2 tool="t" reason=insufficient:mcp_concurrent`) {
		t.Fatalf("stderr missing DEFER line: %q", h.stderr.String())
	}
}

func TestStringIDOnGatedCall(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{MaxConcurrent: 1, Clock: clk.Now}, func(line string) string {
		if strings.Contains(line, `"id":"abc-1"`) {
			return `{"jsonrpc":"2.0","id":"abc-1","result":{"content":[{"type":"text","text":"ok"}]}}` + "\n"
		}
		return ""
	})

	h.send(callLine(`"abc-1"`, "t", `{}`))
	var msg rpcMessage
	if err := json.Unmarshal([]byte(h.readLine()), &msg); err != nil {
		t.Fatal(err)
	}
	if string(msg.ID) != `"abc-1"` {
		t.Fatalf("reply id = %s, want \"abc-1\"", msg.ID)
	}
	// The reply settled the lease, so the single concurrency slot is free again.
	waitFor(t, "lease settled by string-id reply", func() bool { return h.proxy.Engine().Stats().ActiveLeases == 0 })

	// A refusal must echo the string id too.
	h.send(callLine(`"abc-2"`, "t", `{}`))
	waitFor(t, "second call in flight", func() bool { return h.proxy.Engine().Stats().ActiveLeases == 1 })
	h.send(callLine(`"abc-3"`, "t", `{}`))
	if err := json.Unmarshal([]byte(h.readLine()), &msg); err != nil {
		t.Fatal(err)
	}
	if string(msg.ID) != `"abc-3"` {
		t.Fatalf("refusal id = %s, want \"abc-3\"", msg.ID)
	}
	h.close()

	if !strings.Contains(h.stderr.String(), `[quelor] ADMIT id=abc-1 tool="t" reason=ok`) {
		t.Fatalf("stderr missing string-id ADMIT line: %q", h.stderr.String())
	}
}

// ungatedCase feeds one client line straight to the proxy and returns what the
// client and the child each received.
func ungatedCase(t *testing.T, cfg Config, line string, canon func(json.RawMessage) ([]byte, error)) (client, child, stderr string, p *Proxy) {
	t.Helper()
	var errb bytes.Buffer
	cfg.Stderr = &errb
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	if canon != nil {
		p.canon = canon
	}
	var out, in bytes.Buffer
	if err := p.handleClientLine(context.Background(), []byte(line), &out, &in); err != nil {
		t.Fatalf("handleClientLine: %v", err)
	}
	return out.String(), in.String(), errb.String(), p
}

func TestUngatedToolsCallsAreRefusedNotForwarded(t *testing.T) {
	cases := []struct {
		name     string
		cfg      Config
		line     string
		canon    func(json.RawMessage) ([]byte, error)
		wantCode int    // 0: no reply expected (a notification cannot be answered)
		wantID   string // id echoed in the error reply
		wantLog  string
	}{
		{
			name:     "batch with a tools/call",
			line:     `[{"jsonrpc":"2.0","id":1,"method":"tools/list"},{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"t"}}]` + "\n",
			wantCode: rpcInvalidRequest, wantID: "null", wantLog: "reason=batch",
		},
		{
			name:    "tools/call without an id",
			line:    `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"t"}}` + "\n",
			wantLog: `tool="t" reason=no-id`,
		},
		{
			name:    "tools/call with a null id",
			line:    `{"jsonrpc":"2.0","id":null,"method":"tools/call","params":{"name":"t"}}` + "\n",
			wantLog: `tool="t" reason=no-id`,
		},
		{
			name:     "params that do not parse",
			line:     `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":"nope"}` + "\n",
			wantCode: rpcInvalidParams, wantID: "7", wantLog: "id=7 reason=bad-params",
		},
		{
			name:     "tool name of the wrong type",
			line:     `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":5}}` + "\n",
			wantCode: rpcInvalidParams, wantID: "8", wantLog: "id=8 reason=bad-params",
		},
		{
			name:     "envelope that does not fit the message struct",
			line:     `{"jsonrpc":2,"id":9,"method":"tools/call","params":{"name":"t"}}` + "\n",
			wantCode: rpcInvalidRequest, wantID: "9", wantLog: "reason=unparseable-tools-call",
		},
		{
			name:     "arguments that cannot be canonicalized",
			cfg:      Config{Coalesce: true},
			line:     callLine("10", "t", `{"a":1}`),
			canon:    func(json.RawMessage) ([]byte, error) { return nil, os.ErrInvalid },
			wantCode: rpcInvalidParams, wantID: "10", wantLog: `id=10 tool="t" reason=bad-arguments`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, child, stderr, p := ungatedCase(t, tc.cfg, tc.line, tc.canon)
			if child != "" {
				t.Fatalf("line reached the child ungated: %q", child)
			}
			if got := p.Engine().Stats().ActiveLeases; got != 0 {
				t.Fatalf("ActiveLeases = %d, want 0", got)
			}
			if !strings.Contains(stderr, tc.wantLog) {
				t.Fatalf("stderr %q missing %q", stderr, tc.wantLog)
			}
			if tc.wantCode == 0 {
				if client != "" {
					t.Fatalf("a notification must get no reply, got %q", client)
				}
				return
			}
			var reply struct {
				ID    json.RawMessage `json:"id"`
				Error *rpcError       `json:"error"`
			}
			if err := json.Unmarshal([]byte(client), &reply); err != nil || reply.Error == nil {
				t.Fatalf("client reply %q is not a JSON-RPC error (%v)", client, err)
			}
			if reply.Error.Code != tc.wantCode || string(reply.ID) != tc.wantID {
				t.Fatalf("reply id=%s code=%d, want id=%s code=%d", reply.ID, reply.Error.Code, tc.wantID, tc.wantCode)
			}
		})
	}
}

// Every batch is refused: a duplicate method or a nested array inside one
// could be read as a tools/call by some server.
func TestEveryBatchIsRefused(t *testing.T) {
	for _, line := range []string{
		`[{"jsonrpc":"2.0","id":1,"method":"tools/list"},{"jsonrpc":"2.0","method":"notifications/initialized"}]`,
		`[{"jsonrpc":"2.0","id":1,"method":"tools/call","method":"ping","params":{"name":"t"}}]`,
		`[[{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"t"}}]]`,
	} {
		client, child, _, _ := ungatedCase(t, Config{}, line+"\n", nil)
		if child != "" || !strings.Contains(client, "-32600") {
			t.Fatalf("batch %q: client=%q child=%q, want refused and not forwarded", line, client, child)
		}
	}
}

func TestRateLimitBackOffUsesAllowList(t *testing.T) {
	cases := []struct {
		name     string
		reply    string
		wantDecr int64
	}{
		{"rate limit text", `"isError":true,"content":[{"type":"text","text":"HTTP 429 Too Many Requests"}]`, 1},
		{"retry-after text", `"isError":true,"content":[{"type":"text","text":"slow down. Retry-After: 30"}]`, 1},
		{"plain tool error", `"isError":true,"content":[{"type":"text","text":"file not found"}]`, 0},
		{"quota wall is not a rate limit", `"isError":true,"content":[{"type":"text","text":"artifact storage quota exceeded"}]`, 0},
		{"out of memory is not a rate limit", `"isError":true,"content":[{"type":"text","text":"OOMKilled"}]`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newFakeClock()
			h := newHarness(t, Config{Clock: clk.Now}, func(line string) string {
				return `{"jsonrpc":"2.0","id":1,"result":{` + tc.reply + `}}` + "\n"
			})
			h.send(callLine("1", "t", `{}`))
			_ = h.readLine()
			h.close()
			if got := h.proxy.Engine().Stats().AIMDDecreases; got != tc.wantDecr {
				t.Fatalf("AIMDDecreases = %d, want %d", got, tc.wantDecr)
			}
		})
	}

	t.Run("JSON-RPC error code 429", func(t *testing.T) {
		clk := newFakeClock()
		h := newHarness(t, Config{Clock: clk.Now}, func(line string) string {
			return `{"jsonrpc":"2.0","id":1,"error":{"code":429,"message":"slow down"}}` + "\n"
		})
		h.send(callLine("1", "t", `{}`))
		_ = h.readLine()
		h.close()
		if got := h.proxy.Engine().Stats().AIMDDecreases; got != 1 {
			t.Fatalf("AIMDDecreases = %d, want 1", got)
		}
	})
}

func TestRateLimitRuleIDsExistInDefaultRuleset(t *testing.T) {
	have := map[string]bool{}
	for _, r := range classify.Default().Rules {
		have[r.ID] = true
	}
	for id := range rateLimitRules {
		if !have[id] {
			t.Errorf("rate-limit rule id %q is not in the default ruleset; back-off would silently stop", id)
		}
	}
}

func TestToolNameCannotForgeLogLines(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{Clock: clk.Now}, okExcept())

	name := `x\n[quelor] ADMIT id=99 tool=forged reason=ok`
	h.call("1", name, `{}`)
	h.close()

	lines := strings.Split(strings.TrimSpace(h.stderr.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("one call must log one line, got %d: %q", len(lines), h.stderr.String())
	}
}

func TestCallTimeoutConfig(t *testing.T) {
	if _, err := New(Config{CallTimeout: -time.Second}); err == nil {
		t.Fatal("negative CallTimeout must be rejected")
	}

	clk := newFakeClock()
	h := newHarness(t, Config{MaxConcurrent: 1, CallTimeout: 2 * time.Minute, Clock: clk.Now}, okExcept("1"))
	h.send(callLine("1", "t", `{}`))
	waitFor(t, "call in flight", func() bool { return h.proxy.Engine().Stats().ActiveLeases == 1 })

	// Well short of DefaultCallTimeout, so only the configured value can expire it.
	clk.advance(3 * time.Minute)
	if res := h.callUntilAdmitted(2, "t"); res.Content[0].Text != "ok" {
		t.Fatalf("unexpected result: %+v", res)
	}
	h.close()
}

// TestHelperProcess is the child that the RunCommand tests spawn: the test
// binary re-executed with MCPPROXY_TEST_CHILD set. It is a no-op otherwise.
func TestHelperProcess(t *testing.T) {
	switch os.Getenv("MCPPROXY_TEST_CHILD") {
	case "exit7":
		os.Exit(7)
	case "kill":
		self, err := os.FindProcess(os.Getpid())
		if err == nil {
			_ = self.Kill()
		}
		select {}
	}
}

func runHelperChild(t *testing.T, mode string) int {
	t.Helper()
	t.Setenv("MCPPROXY_TEST_CHILD", mode)
	p, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	var out bytes.Buffer
	code, err := p.RunCommand(context.Background(), strings.NewReader(""), &out, os.Args[0], "-test.run=^TestHelperProcess$")
	if err != nil {
		t.Fatalf("RunCommand: %v", err)
	}
	return code
}

func TestRunCommandPropagatesExitCode(t *testing.T) {
	if got := runHelperChild(t, "exit7"); got != 7 {
		t.Fatalf("exit code = %d, want 7", got)
	}
}

func TestRunCommandMapsSignalKilledChildToNonZero(t *testing.T) {
	got := runHelperChild(t, "kill")
	if got <= 0 {
		t.Fatalf("signal-killed child gave exit code %d, want > 0", got)
	}
	if runtime.GOOS != "windows" && got != 137 {
		t.Fatalf("SIGKILLed child gave exit code %d, want 137 (128+9)", got)
	}
}
