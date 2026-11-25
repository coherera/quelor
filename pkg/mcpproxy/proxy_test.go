package mcpproxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/trace"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type testHarness struct {
	t          *testing.T
	proxy      *Proxy
	stderr     *bytes.Buffer
	clientInW  *io.PipeWriter
	clientOutR *bufio.Reader
	serveDone  chan error
	serverDone chan struct{}

	mu   sync.Mutex
	seen []string
}

func newHarness(t *testing.T, cfg Config, handler func(line string) string) *testHarness {
	t.Helper()
	var stderr bytes.Buffer
	if cfg.Stderr == nil {
		cfg.Stderr = &stderr
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	cInR, cInW := io.Pipe()
	cOutR, cOutW := io.Pipe()
	sInR, sInW := io.Pipe()
	sOutR, sOutW := io.Pipe()

	h := &testHarness{
		t:          t,
		proxy:      p,
		stderr:     &stderr,
		clientInW:  cInW,
		clientOutR: bufio.NewReader(cOutR),
		serveDone:  make(chan error, 1),
		serverDone: make(chan struct{}),
	}

	go func() {
		defer close(h.serverDone)
		defer sOutW.Close()
		br := bufio.NewReader(sInR)
		for {
			line, rerr := br.ReadString('\n')
			if len(line) > 0 {
				h.mu.Lock()
				h.seen = append(h.seen, line)
				h.mu.Unlock()
				if handler != nil {
					if resp := handler(line); resp != "" {
						if _, werr := io.WriteString(sOutW, resp); werr != nil {
							return
						}
					}
				}
			}
			if rerr != nil {
				return
			}
		}
	}()

	go func() {
		err := p.Serve(context.Background(), cInR, cOutW, sInW, sOutR)
		_ = cOutW.Close()
		h.serveDone <- err
	}()

	t.Cleanup(func() {
		_ = cInW.Close()
		_ = cOutR.Close()
		_ = sInR.Close()
		_ = sOutW.Close()
		_ = p.Close()
	})

	return h
}

func (h *testHarness) send(line string) {
	h.t.Helper()
	if _, err := io.WriteString(h.clientInW, line); err != nil {
		h.t.Fatalf("write client line: %v", err)
	}
}

func (h *testHarness) readLine() string {
	h.t.Helper()
	line, err := h.clientOutR.ReadString('\n')
	if err != nil {
		h.t.Fatalf("read client line: %v", err)
	}
	return line
}

func (h *testHarness) serverSeen() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.seen))
	copy(out, h.seen)
	return out
}

func (h *testHarness) close() {
	h.t.Helper()
	_ = h.clientInW.Close()
	select {
	case err := <-h.serveDone:
		if err != nil {
			h.t.Fatalf("Serve error: %v", err)
		}
	case <-time.After(time.Second):
		h.t.Fatal("timeout waiting for Serve to finish")
	}
	select {
	case <-h.serverDone:
	case <-time.After(time.Second):
		h.t.Fatal("timeout waiting for fake server to finish")
	}
}

func TestPassThroughByteForByte(t *testing.T) {
	clk := newFakeClock()
	initReq := `{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2024-11-05"}}` + "\n"
	listReq := `{"jsonrpc":"2.0",  "id":"list-1",  "method":"tools/list"}` + "\n"
	initResp := `{"jsonrpc": "2.0", "id": 1, "result": {"serverInfo": {"name": "fake"}}}` + "\n"
	listResp := `{"jsonrpc":"2.0",  "id":"list-1",  "result": {"tools": []}}` + "\n"

	h := newHarness(t, Config{Clock: clk.Now}, func(line string) string {
		switch line {
		case initReq:
			return initResp
		case listReq:
			return listResp
		default:
			return ""
		}
	})

	h.send(initReq)
	if got := h.readLine(); got != initResp {
		t.Fatalf("initialize response mismatch: got %q want %q", got, initResp)
	}

	h.send(listReq)
	if got := h.readLine(); got != listResp {
		t.Fatalf("tools/list response mismatch: got %q want %q", got, listResp)
	}

	h.close()

	seen := h.serverSeen()
	if len(seen) != 2 || seen[0] != initReq || seen[1] != listReq {
		t.Fatalf("child received %q, want [%q, %q]", seen, initReq, listReq)
	}
	if h.stderr.Len() != 0 {
		t.Fatalf("expected empty stderr for pass-through, got %q", h.stderr.String())
	}
}

func TestRateLimitDefer(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{
		RateCount:     2,
		RateWindow:    time.Minute,
		MaxConcurrent: 4,
		Clock:         clk.Now,
	}, func(line string) string {
		var req rpcMessage
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			return ""
		}
		return `{"jsonrpc":"2.0","id":` + string(bytes.TrimSpace(req.ID)) + `,"result":{"content":[{"type":"text","text":"ok"}]}}` + "\n"
	})

	for _, id := range []string{"1", "2"} {
		h.send(`{"jsonrpc":"2.0","id":` + id + `,"method":"tools/call","params":{"name":"search_code","arguments":{"q":"` + id + `"}}}` + "\n")
		respLine := h.readLine()
		var msg rpcMessage
		if err := json.Unmarshal([]byte(respLine), &msg); err != nil {
			t.Fatalf("unmarshal response %s: %v", id, err)
		}
		var res toolCallResult
		if err := json.Unmarshal(msg.Result, &res); err != nil {
			t.Fatalf("unmarshal result %s: %v", id, err)
		}
		if res.IsError {
			t.Fatalf("call %s unexpectedly refused: %s", id, respLine)
		}
	}

	// Third call exceeds --rate 2/1m and must be DEFERred without reaching the child.
	h.send(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search_code","arguments":{"q":"3"}}}` + "\n")
	respLine := h.readLine()

	var msg rpcMessage
	if err := json.Unmarshal([]byte(respLine), &msg); err != nil {
		t.Fatalf("unmarshal defer response: %v", err)
	}
	if string(bytes.TrimSpace(msg.ID)) != "3" {
		t.Fatalf("defer response id = %s, want 3", string(msg.ID))
	}
	var res toolCallResult
	if err := json.Unmarshal(msg.Result, &res); err != nil {
		t.Fatalf("unmarshal defer result: %v", err)
	}
	if !res.IsError || len(res.Content) != 1 || res.Content[0].Type != "text" {
		t.Fatalf("unexpected defer result: %+v", res)
	}
	text := res.Content[0].Text
	if !strings.Contains(text, "DEFER") || !strings.Contains(text, "insufficient:mcp_rate") || !strings.Contains(text, "wake_at=") {
		t.Fatalf("defer text %q missing verdict, reason, or wake_at", text)
	}

	h.close()

	if got := len(h.serverSeen()); got != 2 {
		t.Fatalf("child saw %d calls, want 2", got)
	}
	if !strings.Contains(h.stderr.String(), "[quelor] DEFER id=3 tool=\"search_code\" reason=insufficient:mcp_rate") {
		t.Fatalf("stderr missing DEFER line: %q", h.stderr.String())
	}
}

func TestCoalesceInFlightDuplicate(t *testing.T) {
	clk := newFakeClock()
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	h := newHarness(t, Config{
		RateCount:     10,
		RateWindow:    time.Minute,
		MaxConcurrent: 4,
		Coalesce:      true,
		Clock:         clk.Now,
	}, func(line string) string {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		var req rpcMessage
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			return ""
		}
		return `{"jsonrpc":"2.0","id":` + string(bytes.TrimSpace(req.ID)) + `,"result":{"content":[{"type":"text","text":"done"}]}}` + "\n"
	})

	// Send first call and wait until it is in-flight at the child.
	h.send(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_code","arguments":{"b":2,"a":1}}}` + "\n")
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for child to receive first call")
	}

	// Send duplicate with differently ordered JSON keys; must be SHED immediately.
	h.send(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"search_code","arguments":{"a": 1, "b": 2}}}` + "\n")
	shedLine := h.readLine()

	var shedMsg rpcMessage
	if err := json.Unmarshal([]byte(shedLine), &shedMsg); err != nil {
		t.Fatalf("unmarshal shed response: %v", err)
	}
	if string(bytes.TrimSpace(shedMsg.ID)) != "2" {
		t.Fatalf("shed response id = %s, want 2", string(shedMsg.ID))
	}
	var shedRes toolCallResult
	if err := json.Unmarshal(shedMsg.Result, &shedRes); err != nil {
		t.Fatalf("unmarshal shed result: %v", err)
	}
	if !shedRes.IsError || len(shedRes.Content) != 1 {
		t.Fatalf("expected isError result, got %+v", shedRes)
	}
	if !strings.Contains(shedRes.Content[0].Text, "SHED") || !strings.Contains(shedRes.Content[0].Text, "coalesced") {
		t.Fatalf("shed text %q missing SHED or coalesced", shedRes.Content[0].Text)
	}

	// Release first call and verify its normal reply arrives.
	close(release)
	okLine := h.readLine()
	var okMsg rpcMessage
	if err := json.Unmarshal([]byte(okLine), &okMsg); err != nil {
		t.Fatalf("unmarshal first response: %v", err)
	}
	if string(bytes.TrimSpace(okMsg.ID)) != "1" {
		t.Fatalf("first response id = %s, want 1", string(okMsg.ID))
	}

	h.close()

	if got := len(h.serverSeen()); got != 1 {
		t.Fatalf("child saw %d calls, want 1", got)
	}
	if !strings.Contains(h.stderr.String(), "[quelor] SHED id=2 tool=\"search_code\" reason=coalesced") {
		t.Fatalf("stderr missing SHED line: %q", h.stderr.String())
	}
}

func TestChild429ProducesObservation(t *testing.T) {
	clk := newFakeClock()
	var mem trace.MemoryWriter

	h := newHarness(t, Config{
		RateCount:     10,
		RateWindow:    time.Minute,
		MaxConcurrent: 4,
		Trace:         &mem,
		Clock:         clk.Now,
	}, func(line string) string {
		var req rpcMessage
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			return ""
		}
		return `{"jsonrpc":"2.0","id":` + string(bytes.TrimSpace(req.ID)) + `,"result":{"content":[{"type":"text","text":"upstream error: HTTP 429 Too Many Requests"}],"isError":true}}` + "\n"
	})

	evs := h.proxy.Engine().Subscribe()

	h.send(`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_code","arguments":{"q":"auth"}}}` + "\n")
	_ = h.readLine()

	st := h.proxy.Engine().Stats()
	if st.AIMDDecreases != 1 {
		t.Fatalf("AIMDDecreases = %d, want 1", st.AIMDDecreases)
	}

	var gotAIMD bool
	for {
		select {
		case ev := <-evs:
			if ev.Kind == "aimd-decrease" && ev.Resource == ResourceRate {
				gotAIMD = true
			}
		default:
			goto drained
		}
	}
drained:
	if !gotAIMD {
		t.Fatalf("expected aimd-decrease event for %s on Subscribe()", ResourceRate)
	}

	// The 429 rides on the settle, so the trace shows it as a failed,
	// rate-limit-classified settle record.
	var gotSettle bool
	for _, rec := range mem.Records() {
		if rec.Op == trace.OpSettle && rec.Outcome != nil && rec.Outcome.Failed &&
			len(rec.RuleIDs) == 1 && rec.RuleIDs[0] == "gha-rate-limit" {
			gotSettle = true
		}
	}
	if !gotSettle {
		t.Fatal("expected a failed settle trace record classified gha-rate-limit")
	}

	h.close()
}

func TestLineThatIsNotOneJSONValueIsRefused(t *testing.T) {
	clk := newFakeClock()
	listReq := `{"jsonrpc":"2.0","id":9,"method":"tools/list"}` + "\n"
	listResp := `{"jsonrpc":"2.0","id":9,"result":{"tools":[]}}` + "\n"

	h := newHarness(t, Config{Clock: clk.Now}, func(line string) string {
		if line == listReq {
			return listResp
		}
		return ""
	})

	bad := []string{
		"not valid json {{{\n",
		// A second object on the same line hides a tools/call from a
		// line-at-a-time reader; a streaming server would still run it.
		`{"jsonrpc":"2.0","id":1,"method":"ping"}{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"x","arguments":{}}}` + "\n",
		`{"jsonrpc":"2.0","id":3,"method":"ping"} {"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"x","arguments":{}}}` + "\n",
		`{"jsonrpc":"2.0","id":5,"method":"ping"} trailing` + "\n",
	}
	for _, line := range bad {
		h.send(line)
		if code, id := h.readError(); code != rpcParseError || id != "null" {
			t.Fatalf("line %q: got code=%d id=%s, want %d and null", line, code, id, rpcParseError)
		}
	}
	h.send(listReq)
	if got := h.readLine(); got != listResp {
		t.Fatalf("tools/list response = %q, want %q", got, listResp)
	}

	h.close()

	if seen := h.serverSeen(); len(seen) != 1 || seen[0] != listReq {
		t.Fatalf("child received %q, want only %q", seen, listReq)
	}
}

// okExcept answers every request with an ok tool result except those whose id
// is listed in silent, which the fake child never answers.
func okExcept(silent ...string) func(string) string {
	return func(line string) string {
		var req rpcMessage
		if err := json.Unmarshal([]byte(line), &req); err != nil || len(req.ID) == 0 {
			return ""
		}
		id := string(bytes.TrimSpace(req.ID))
		for _, s := range silent {
			if id == s {
				return ""
			}
		}
		return `{"jsonrpc":"2.0","id":` + id + `,"result":{"content":[{"type":"text","text":"ok"}]}}` + "\n"
	}
}

func callLine(id, tool, args string) string {
	return `{"jsonrpc":"2.0","id":` + id + `,"method":"tools/call","params":{"name":"` + tool + `","arguments":` + args + `}}` + "\n"
}

// call sends one tools/call and returns the single reply it produces.
func (h *testHarness) call(id, tool, args string) toolCallResult {
	h.t.Helper()
	h.send(callLine(id, tool, args))
	var msg rpcMessage
	if err := json.Unmarshal([]byte(h.readLine()), &msg); err != nil {
		h.t.Fatalf("unmarshal reply to %s: %v", id, err)
	}
	var res toolCallResult
	if err := json.Unmarshal(msg.Result, &res); err != nil {
		h.t.Fatalf("unmarshal result of %s: %v (%s)", id, err, msg.Result)
	}
	return res
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// callUntilAdmitted retries a call (ids start, start+1, ...) until the proxy
// admits it. It never calls Engine.Advance: only the proxy's own timer loop can
// make the retry succeed.
func (h *testHarness) callUntilAdmitted(start int, tool string) toolCallResult {
	h.t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for id := start; ; id++ {
		res := h.call(strconv.Itoa(id), tool, `{}`)
		if !res.IsError {
			return res
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("call still refused after the clock moved past the window: %+v", res)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestRateWindowRefillsWithoutManualAdvance(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{RateCount: 2, RateWindow: time.Minute, Clock: clk.Now}, okExcept())

	for _, id := range []string{"1", "2"} {
		if res := h.call(id, "t", `{}`); res.IsError {
			t.Fatalf("call %s refused: %+v", id, res)
		}
	}
	if res := h.call("3", "t", `{}`); !res.IsError || !strings.Contains(res.Content[0].Text, "DEFER") {
		t.Fatalf("third call should DEFER, got %+v", res)
	}

	clk.advance(61 * time.Second)
	if res := h.callUntilAdmitted(4, "t"); res.Content[0].Text != "ok" {
		t.Fatalf("unexpected result after refill: %+v", res)
	}
	h.close()
}

func TestLeaseExpiresWhenChildNeverAnswers(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{MaxConcurrent: 1, Clock: clk.Now}, okExcept("1"))

	h.send(callLine("1", "t", `{}`))
	waitFor(t, "first call in flight", func() bool { return h.proxy.Engine().Stats().ActiveLeases == 1 })
	if res := h.call("2", "t", `{}`); !res.IsError || !strings.Contains(res.Content[0].Text, "mcp_concurrent") {
		t.Fatalf("second call should DEFER on mcp_concurrent, got %+v", res)
	}

	clk.advance(DefaultCallTimeout + time.Minute)
	if res := h.callUntilAdmitted(3, "t"); res.Content[0].Text != "ok" {
		t.Fatalf("unexpected result after expiry: %+v", res)
	}
	h.close()
}

func TestInFlightLeaseSettledWhenServeReturns(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{Clock: clk.Now}, okExcept("1"))

	h.send(callLine("1", "t", `{}`))
	waitFor(t, "call in flight", func() bool { return h.proxy.Engine().Stats().ActiveLeases == 1 })
	h.close()

	if got := h.proxy.Engine().Stats().ActiveLeases; got != 0 {
		t.Fatalf("ActiveLeases after Serve returned = %d, want 0", got)
	}
}

func TestCancelledNotificationSettlesLease(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{Coalesce: true, Clock: clk.Now}, okExcept("1"))

	h.send(callLine("1", "t", `{"a":1}`))
	waitFor(t, "call in flight", func() bool { return h.proxy.Engine().Stats().ActiveLeases == 1 })
	h.send(`{"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":1,"reason":"user"}}` + "\n")
	waitFor(t, "lease settled by cancel", func() bool { return h.proxy.Engine().Stats().ActiveLeases == 0 })

	// The identical call must now be admitted, not SHED as a duplicate.
	if res := h.call("2", "t", `{"a":1}`); res.IsError {
		t.Fatalf("retry after cancel refused: %+v", res)
	}
	h.close()

	seen := h.serverSeen()
	if len(seen) != 3 || !strings.Contains(seen[1], "notifications/cancelled") {
		t.Fatalf("cancel notification must still reach the child, child saw %q", seen)
	}
}

func TestAIMDRecoversAfterSuccesses(t *testing.T) {
	clk := newFakeClock()
	h := newHarness(t, Config{RateCount: 20, RateWindow: time.Minute, Clock: clk.Now}, func(line string) string {
		var req rpcMessage
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			return ""
		}
		if string(bytes.TrimSpace(req.ID)) == "1" {
			return `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"HTTP 429 Too Many Requests"}],"isError":true}}` + "\n"
		}
		return okExcept()(line)
	})
	evs := h.proxy.Engine().Subscribe()

	h.call("1", "t", `{}`)
	for _, id := range []string{"2", "3", "4"} {
		if res := h.call(id, "t", `{}`); res.IsError {
			t.Fatalf("call %s refused: %+v", id, res)
		}
	}
	h.close()

	var dec, inc int
	for drained := false; !drained; {
		select {
		case ev := <-evs:
			switch ev.Kind {
			case "aimd-decrease":
				dec++
			case "aimd-increase":
				inc++
			}
		default:
			drained = true
		}
	}
	if dec != 1 || inc != 3 {
		t.Fatalf("aimd decreases=%d increases=%d, want 1 and 3", dec, inc)
	}
}
