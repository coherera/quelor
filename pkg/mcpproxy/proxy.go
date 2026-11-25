// Package mcpproxy implements a stdio JSON-RPC 2.0 admission proxy for MCP
// servers backed by the embedded Quelor engine.
package mcpproxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode"

	"github.com/coherera/quelor/pkg/classify"
	"github.com/coherera/quelor/pkg/engine"
	"github.com/coherera/quelor/pkg/quelor"
	"github.com/coherera/quelor/pkg/resource"
	"github.com/coherera/quelor/pkg/trace"
)

const (
	DefaultRateCount     int64 = 30
	DefaultRateWindow          = time.Minute
	DefaultMaxConcurrent int64 = 4
	DefaultHolderID            = "mcp-cli"
	DefaultTenantID            = "mcp"

	// DefaultRateSpec is DefaultRateCount/DefaultRateWindow in the form
	// ParseRate accepts. A test keeps the three in step.
	DefaultRateSpec = "30/1m"

	// DefaultCallTimeout is how long a tools/call may stay in flight before the
	// engine expires its lease and frees the rate and concurrency capacity it
	// holds. Tool calls can be slow (builds, searches), so it is generous.
	DefaultCallTimeout = 10 * time.Minute

	ResourceRate       = "mcp_rate"
	ResourceConcurrent = "mcp_concurrent"
)

// Config configures a stdio MCP admission Proxy.
type Config struct {
	RateCount     int64
	RateWindow    time.Duration
	MaxConcurrent int64
	// CallTimeout is the per-call lease lifetime; 0 means DefaultCallTimeout.
	CallTimeout time.Duration
	Coalesce    bool
	Trace       trace.Writer
	Classifier  *classify.Ruleset
	Clock       func() time.Time
	Stderr      io.Writer
}

// ParseRate parses a "<count>/<duration>" rate specification such as "30/1m".
func ParseRate(s string) (int64, time.Duration, error) {
	countStr, durStr, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok {
		return 0, 0, fmt.Errorf("invalid rate %q: expected <count>/<duration> (e.g. 30/1m)", s)
	}
	n, err := strconv.ParseInt(strings.TrimSpace(countStr), 10, 64)
	if err != nil || n <= 0 {
		return 0, 0, fmt.Errorf("invalid rate %q: count must be a positive integer", s)
	}
	d, err := time.ParseDuration(strings.TrimSpace(durStr))
	if err != nil || d <= 0 {
		return 0, 0, fmt.Errorf("invalid rate %q: duration must be positive", s)
	}
	return n, d, nil
}

// Proxy gates client-to-server MCP tools/call messages through an embedded
// Quelor engine and forwards all other lines in both directions verbatim.
type Proxy struct {
	cfg    Config
	engine *engine.Engine
	canon  func(json.RawMessage) ([]byte, error)

	seq      atomic.Uint64
	mu       sync.Mutex
	inflight map[string]string // canonical request id -> lease id
	closed   bool              // set once Serve has returned; nothing new may stay in flight

	clientOutMu sync.Mutex
	stderrMu    sync.Mutex
}

// New builds a Proxy with an embedded engine configured from cfg.
func New(cfg Config) (*Proxy, error) {
	if cfg.RateCount == 0 && cfg.RateWindow == 0 {
		cfg.RateCount = DefaultRateCount
		cfg.RateWindow = DefaultRateWindow
	}
	if cfg.RateCount <= 0 {
		return nil, fmt.Errorf("mcpproxy: rate count must be > 0, got %d", cfg.RateCount)
	}
	if cfg.RateWindow <= 0 {
		return nil, fmt.Errorf("mcpproxy: rate window must be > 0, got %v", cfg.RateWindow)
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = DefaultMaxConcurrent
	}
	if cfg.MaxConcurrent < 0 {
		return nil, fmt.Errorf("mcpproxy: max-concurrent must be > 0, got %d", cfg.MaxConcurrent)
	}
	if cfg.CallTimeout == 0 {
		cfg.CallTimeout = DefaultCallTimeout
	}
	if cfg.CallTimeout < 0 {
		return nil, fmt.Errorf("mcpproxy: call timeout must be > 0, got %v", cfg.CallTimeout)
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Classifier == nil {
		cfg.Classifier = classify.Default()
	}
	if cfg.Trace == nil {
		cfg.Trace = trace.Nop
	}
	if cfg.Stderr == nil {
		cfg.Stderr = io.Discard
	}

	reg := resource.NewRegistry(cfg.Clock)
	if err := reg.Register(resource.Manifest{
		ID:            ResourceRate,
		CapacityModel: resource.ModelWindowBucket,
		Capacity:      cfg.RateCount,
		Window:        resource.Duration(cfg.RateWindow),
	}, nil); err != nil {
		return nil, fmt.Errorf("mcpproxy: register %s: %w", ResourceRate, err)
	}
	if err := reg.Register(resource.Manifest{
		ID:            ResourceConcurrent,
		CapacityModel: resource.ModelLease,
		Capacity:      cfg.MaxConcurrent,
	}, nil); err != nil {
		return nil, fmt.Errorf("mcpproxy: register %s: %w", ResourceConcurrent, err)
	}

	eng, err := engine.New(engine.Config{
		Registry:   reg,
		Trace:      cfg.Trace,
		Classifier: cfg.Classifier,
		Clock:      cfg.Clock,
		LeaseTTL:   cfg.CallTimeout,
	})
	if err != nil {
		return nil, fmt.Errorf("mcpproxy: engine: %w", err)
	}

	return &Proxy{
		cfg:      cfg,
		engine:   eng,
		canon:    canonicalJSON,
		inflight: make(map[string]string),
	}, nil
}

// Engine returns the embedded Quelor engine.
func (p *Proxy) Engine() *engine.Engine { return p.engine }

// Close closes the embedded engine and its trace writer.
func (p *Proxy) Close() error { return p.engine.Close() }

// RunCommand spawns name with args as a stdio MCP child server, proxies traffic
// between stdin/stdout and the child, and returns the child's exit code.
func (p *Proxy) RunCommand(ctx context.Context, stdin io.Reader, stdout io.Writer, name string, args ...string) (int, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stderr = (*stderrWriter)(p)

	serverIn, err := cmd.StdinPipe()
	if err != nil {
		return 1, err
	}
	serverOut, err := cmd.StdoutPipe()
	if err != nil {
		return 1, err
	}
	if err := cmd.Start(); err != nil {
		return 1, err
	}

	serveErr := p.Serve(ctx, stdin, stdout, serverIn, serverOut)
	_ = serverIn.Close()

	waitErr := cmd.Wait()
	if waitErr != nil {
		var exitErr *exec.ExitError
		if errors.As(waitErr, &exitErr) {
			return exitCode(exitErr), nil
		}
		return 1, waitErr
	}
	if serveErr != nil {
		return 1, serveErr
	}
	return 0, nil
}

// exitCode maps a finished child to a process exit code. A child killed by a
// signal reports ExitCode() == -1, which must not become a success or wrap to
// 255; it maps to the shell convention 128+signal.
func exitCode(e *exec.ExitError) int {
	if c := e.ExitCode(); c >= 0 {
		return c
	}
	if ws, ok := e.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return 128 + int(ws.Signal())
	}
	return 1
}

// Serve proxies newline-delimited JSON-RPC 2.0 messages between clientIn/clientOut
// and serverIn/serverOut until serverOut closes or ctx is cancelled.
//
// Serve drives the engine's timers (rate-window refill, lease expiry) for as
// long as it runs and, when it returns, settles every call still in flight so
// no lease outlives the session. A Proxy serves one session; it cannot be
// reused after Serve returns.
func (p *Proxy) Serve(ctx context.Context, clientIn io.Reader, clientOut io.Writer, serverIn io.Writer, serverOut io.Reader) error {
	runCtx, cancel := context.WithCancel(ctx)
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = p.engine.Run(runCtx)
	}()
	defer func() {
		cancel()
		<-runDone
		p.settleAll()
	}()

	var closeOnce sync.Once
	closeServerIn := func() {
		closeOnce.Do(func() {
			if c, ok := serverIn.(io.Closer); ok {
				_ = c.Close()
			}
		})
	}

	c2sErrCh := make(chan error, 1)
	s2cErrCh := make(chan error, 1)

	go func() {
		err := p.pumpClientToServer(ctx, clientIn, clientOut, serverIn)
		closeServerIn()
		c2sErrCh <- err
	}()

	go func() {
		s2cErrCh <- p.pumpServerToClient(ctx, serverOut, clientOut)
	}()

	var (
		c2sDone bool
		c2sErr  error
	)
	for {
		select {
		case err := <-s2cErrCh:
			closeServerIn()
			if err != nil {
				return err
			}
			if c2sDone {
				return c2sErr
			}
			return nil
		case err := <-c2sErrCh:
			c2sDone = true
			c2sErr = err
			c2sErrCh = nil
		case <-ctx.Done():
			closeServerIn()
			return ctx.Err()
		}
	}
}

type rpcMessage struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method,omitempty"`
	Params  json.RawMessage `json:"params,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

type toolCallResult struct {
	Content []contentItem `json:"content"`
	IsError bool          `json:"isError"`
}

type contentItem struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type refusalReply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  toolCallResult  `json:"result"`
}

func (p *Proxy) pumpClientToServer(ctx context.Context, clientIn io.Reader, clientOut, serverIn io.Writer) error {
	br := bufio.NewReader(clientIn)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if herr := p.handleClientLine(ctx, line, clientOut, serverIn); herr != nil {
				return herr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// JSON-RPC error codes used for calls the proxy refuses to forward.
const (
	rpcInvalidRequest = -32600
	rpcInvalidParams  = -32602
)

type rpcErrorReply struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"` // nil marshals as null
	Error   rpcError        `json:"error"`
}

// handleClientLine forwards one client line to the child. Every tools/call is
// either gated through the engine or refused here; none is forwarded ungated.
func (p *Proxy) handleClientLine(ctx context.Context, line []byte, clientOut, serverIn io.Writer) error {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) == 0 {
		_, err := serverIn.Write(line)
		return err
	}

	if trimmed[0] == '[' {
		if batchHasToolsCall(trimmed) {
			p.logf("[quelor] REFUSE reason=batch-with-tools-call\n")
			return p.writeRPCError(clientOut, nil, rpcInvalidRequest, "quelor: JSON-RPC batches containing tools/call are not supported; send each call as its own message")
		}
		_, err := serverIn.Write(line)
		return err
	}

	obj, err := scanObject(trimmed)
	if err != nil {
		// Text that is not a JSON object cannot be a tools/call and is
		// forwarded unchanged.
		_, werr := serverIn.Write(line)
		return werr
	}
	if obj.hasCollision() {
		// Two keys that differ only in case (or repeat) are read differently
		// by different JSON parsers, so the proxy cannot know which method the
		// server will run. Refuse rather than guess.
		p.logf("[quelor] REFUSE reason=key-collision\n")
		return p.writeRPCError(clientOut, obj.echoID(), rpcInvalidRequest, "quelor: request has duplicate or case-colliding keys")
	}

	method, _ := obj.str("method")
	switch method {
	case "notifications/cancelled":
		p.cancelInflight(obj.fields["params"])
	case "tools/call":
		return p.gateToolsCall(ctx, line, obj, clientOut, serverIn)
	}
	_, werr := serverIn.Write(line)
	return werr
}

func (p *Proxy) gateToolsCall(ctx context.Context, line []byte, obj *object, clientOut, serverIn io.Writer) error {
	if v, ok := obj.str("jsonrpc"); !ok || v != "2.0" {
		p.logf("[quelor] REFUSE reason=unparseable-tools-call\n")
		return p.writeRPCError(clientOut, obj.echoID(), rpcInvalidRequest, "quelor: tools/call is not a valid JSON-RPC request")
	}
	// The params object gets the same strict reading as the envelope, so a
	// shadowed "name" cannot gate one tool while the server runs another.
	pobj, perr := scanObject(obj.fields["params"])
	var params toolCallParams
	if perr == nil {
		params.Name, _ = pobj.str("name")
		params.Arguments = pobj.fields["arguments"]
	}

	idKey, idDisplay, rawID, hasID := canonicalID(obj.fields["id"])
	if !hasID {
		// A call without an id is a notification: it gets no reply, so it cannot
		// be answered with a refusal. Dropping it is the only gated outcome.
		p.logf("[quelor] REFUSE tool=%q reason=no-id\n", params.Name)
		return nil
	}
	if perr != nil || params.Name == "" {
		p.logf("[quelor] REFUSE id=%s reason=bad-params\n", idDisplay)
		return p.writeRPCError(clientOut, rawID, rpcInvalidParams, "quelor: tools/call params need an object with a non-empty string name")
	}
	if pobj.hasCollision() {
		p.logf("[quelor] REFUSE id=%s reason=key-collision\n", idDisplay)
		return p.writeRPCError(clientOut, rawID, rpcInvalidRequest, "quelor: tools/call params have duplicate or case-colliding keys")
	}

	p.mu.Lock()
	_, inflight := p.inflight[idKey]
	p.mu.Unlock()
	if inflight {
		// Replacing the entry would drop the older call's lease while that call
		// still runs, so the concurrency cap would no longer hold.
		p.logf("[quelor] REFUSE id=%s tool=%q reason=duplicate-id\n", idDisplay, params.Name)
		return p.writeRPCError(clientOut, rawID, rpcInvalidRequest, "quelor: duplicate request id in flight")
	}

	env := quelor.Envelope{
		UnitID:      fmt.Sprintf("mcp-%d-%s", p.seq.Add(1), idDisplay),
		TenantID:    DefaultTenantID,
		HolderID:    DefaultHolderID,
		AffinityKey: params.Name,
		Demands: map[string]int64{
			ResourceRate:       1,
			ResourceConcurrent: 1,
		},
	}

	if p.cfg.Coalesce {
		canonArgs, err := p.canon(params.Arguments)
		if err != nil {
			p.logf("[quelor] REFUSE id=%s tool=%q reason=bad-arguments\n", idDisplay, params.Name)
			return p.writeRPCError(clientOut, rawID, rpcInvalidParams, "quelor: tools/call arguments cannot be canonicalized")
		}
		env.CoalesceKey = coalesceHash(params.Name, canonArgs)
		env.CoalescePolicy = quelor.CoalesceDropNew
	}

	res, err := p.engine.AcquireLease(ctx, quelor.AcquireRequest{Envelope: env})
	if err != nil {
		return err
	}

	reason := res.Verdict.Reason
	if reason == "" && res.Verdict.Kind == quelor.Admit {
		reason = "ok"
	}
	p.logDecision(res.Verdict.Kind, idDisplay, params.Name, reason)

	if res.Verdict.Kind != quelor.Admit || res.Lease == nil {
		return p.writeRefusal(clientOut, rawID, res.Verdict)
	}

	// Register in-flight before writing to the child so a fast reply always matches.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.release(res.Lease.ID)
		return errors.New("mcpproxy: session ended")
	}
	p.inflight[idKey] = res.Lease.ID
	p.mu.Unlock()

	if _, err := serverIn.Write(line); err != nil {
		p.mu.Lock()
		if p.inflight[idKey] == res.Lease.ID {
			delete(p.inflight, idKey)
		}
		p.mu.Unlock()
		p.release(res.Lease.ID)
		return err
	}
	return nil
}

// cancelInflight settles the call a client notifications/cancelled names. The
// notification itself is still forwarded to the child.
func (p *Proxy) cancelInflight(params json.RawMessage) {
	o, err := scanObject(params)
	if err != nil || o.hasCollision() {
		return
	}
	idKey, _, _, ok := canonicalID(o.fields["requestId"])
	if !ok {
		return
	}
	p.mu.Lock()
	leaseID, found := p.inflight[idKey]
	delete(p.inflight, idKey)
	p.mu.Unlock()
	if found {
		p.release(leaseID)
	}
}

// settleAll ends the session's accounting: it settles every call still in
// flight (the child never answered, or exited mid-call) and refuses any call
// that would register afterwards.
func (p *Proxy) settleAll() {
	p.mu.Lock()
	p.closed = true
	leases := p.inflight
	p.inflight = make(map[string]string)
	p.mu.Unlock()
	for _, id := range leases {
		p.release(id)
	}
}

// release settles a lease with no outcome, returning its capacity untouched by
// classification or AIMD. It is idempotent in the engine.
func (p *Proxy) release(leaseID string) {
	_, _ = p.engine.SettleLease(context.Background(), quelor.SettleRequest{
		LeaseID:  leaseID,
		HolderID: DefaultHolderID,
	})
}

// batchHasToolsCall reports whether a JSON-array batch carries a tools/call.
func batchHasToolsCall(raw []byte) bool {
	var batch []json.RawMessage
	if json.Unmarshal(raw, &batch) != nil {
		return false
	}
	for _, el := range batch {
		if mentionsToolsCall(el) {
			return true
		}
	}
	return false
}

func (p *Proxy) pumpServerToClient(ctx context.Context, serverOut io.Reader, clientOut io.Writer) error {
	br := bufio.NewReader(serverOut)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if herr := p.handleServerLine(ctx, line, clientOut); herr != nil {
				return herr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func (p *Proxy) handleServerLine(ctx context.Context, line []byte, clientOut io.Writer) error {
	trimmed := bytes.TrimSpace(line)
	if len(trimmed) > 0 {
		var msg rpcMessage
		if err := json.Unmarshal(trimmed, &msg); err == nil && msg.Method == "" {
			if idKey, _, _, ok := canonicalID(msg.ID); ok {
				p.mu.Lock()
				leaseID, found := p.inflight[idKey]
				if found {
					delete(p.inflight, idKey)
				}
				p.mu.Unlock()

				if found {
					p.settleResponse(ctx, leaseID, msg)
				}
			}
		}
	}
	return p.writeClient(clientOut, line)
}

// settleResponse settles the lease for a child reply. The rate resource is told
// what the call showed: a 2xx on success (so AIMD can regain capacity), a 429
// on a rate-limited failure (so it backs off). Settle observes only the
// resources named in Observations, so the concurrency lease is never observed.
func (p *Proxy) settleResponse(ctx context.Context, leaseID string, msg rpcMessage) {
	isErr, errText, httpStatus := inspectResponseError(msg)
	if !isErr {
		_, _ = p.engine.SettleLease(ctx, quelor.SettleRequest{
			LeaseID:      leaseID,
			HolderID:     DefaultHolderID,
			Outcome:      &quelor.ExecutionOutcome{ExitCode: 0},
			Observations: []quelor.Observation{{Resource: ResourceRate, HTTPStatus: 200}},
		})
		return
	}

	cr := p.cfg.Classifier.Classify(quelor.ExecutionOutcome{
		ExitCode:   1,
		HTTPStatus: httpStatus,
		Log:        errText,
	})
	req := quelor.SettleRequest{
		LeaseID:  leaseID,
		HolderID: DefaultHolderID,
		Outcome:  &quelor.ExecutionOutcome{ExitCode: 1, Log: errText},
	}
	if isRateLimit(cr) {
		req.Observations = []quelor.Observation{{Resource: ResourceRate, HTTPStatus: 429}}
	}
	_, _ = p.engine.SettleLease(ctx, req)
}

func inspectResponseError(msg rpcMessage) (bool, string, int) {
	if msg.Error != nil {
		var parts []string
		if msg.Error.Message != "" {
			parts = append(parts, msg.Error.Message)
		}
		if len(msg.Error.Data) > 0 && !bytes.Equal(bytes.TrimSpace(msg.Error.Data), []byte("null")) {
			parts = append(parts, string(bytes.TrimSpace(msg.Error.Data)))
		}
		status := 0
		if msg.Error.Code == 429 {
			status = 429
		}
		return true, strings.Join(parts, " "), status
	}

	if len(msg.Result) > 0 {
		var res toolCallResult
		if err := json.Unmarshal(msg.Result, &res); err == nil && res.IsError {
			var parts []string
			for _, item := range res.Content {
				if item.Text != "" {
					parts = append(parts, item.Text)
				}
			}
			return true, strings.Join(parts, "\n"), 0
		}
	}

	return false, "", 0
}

// rateLimitRules are the rule ids, in the bundled default ruleset, that mean
// "the upstream is rate limiting us": a 429 status or HTTP 429, "rate limit
// exceeded" or Retry-After wording. That wording is generic HTTP/API text, not
// specific to GitHub Actions, so it is the right signal for any MCP server. The
// resource-exhausted class alone is not: quota, out-of-memory and wall-clock
// rules share it and must not shrink the call rate. A custom Classifier has to
// reuse these ids to trigger back-off.
var rateLimitRules = map[string]bool{
	"gha-rate-limit":      true,
	"gha-rate-limit-text": true,
}

func isRateLimit(cr classify.Result) bool {
	return cr.Failed && cr.Class == quelor.ResourceExhausted && rateLimitRules[cr.RuleID]
}

func (p *Proxy) writeRefusal(clientOut io.Writer, rawID json.RawMessage, v quelor.Verdict) error {
	text := formatRefusalText(v)
	reply := refusalReply{
		JSONRPC: "2.0",
		ID:      rawID,
		Result: toolCallResult{
			Content: []contentItem{{Type: "text", Text: text}},
			IsError: true,
		},
	}
	b, err := json.Marshal(reply)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return p.writeClient(clientOut, b)
}

func (p *Proxy) writeRPCError(clientOut io.Writer, rawID json.RawMessage, code int, message string) error {
	b, err := json.Marshal(rpcErrorReply{
		JSONRPC: "2.0",
		ID:      rawID,
		Error:   rpcError{Code: code, Message: message},
	})
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return p.writeClient(clientOut, b)
}

func formatRefusalText(v quelor.Verdict) string {
	var b strings.Builder
	b.WriteString("quelor ")
	b.WriteString(string(v.Kind))
	if v.Reason != "" {
		b.WriteString(": ")
		b.WriteString(v.Reason)
	}
	if v.WakeAt != nil && !v.WakeAt.IsZero() {
		b.WriteString(" wake_at=")
		b.WriteString(v.WakeAt.UTC().Format(time.RFC3339))
	}
	return b.String()
}

func (p *Proxy) writeClient(clientOut io.Writer, b []byte) error {
	p.clientOutMu.Lock()
	defer p.clientOutMu.Unlock()
	_, err := clientOut.Write(b)
	return err
}

func (p *Proxy) logDecision(kind quelor.VerdictKind, id, tool, reason string) {
	p.logf("[quelor] %s id=%s tool=%q reason=%s\n", kind, id, tool, reason)
}

// stderrWriter writes through the same lock as the proxy's own log lines, so
// the child's stderr and ours never interleave inside one write.
type stderrWriter Proxy

func (w *stderrWriter) Write(b []byte) (int, error) {
	w.stderrMu.Lock()
	defer w.stderrMu.Unlock()
	return w.cfg.Stderr.Write(b)
}

func (p *Proxy) logf(format string, args ...any) {
	p.stderrMu.Lock()
	defer p.stderrMu.Unlock()
	_, _ = fmt.Fprintf(p.cfg.Stderr, format, args...)
}

func canonicalID(raw json.RawMessage) (key, display string, trimmed json.RawMessage, ok bool) {
	b := bytes.TrimSpace(raw)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		return "", "", nil, false
	}
	if b[0] == '"' {
		var s string
		if err := json.Unmarshal(b, &s); err != nil {
			return "", "", nil, false
		}
		return "s:" + s, displayID(s), json.RawMessage(b), true
	}
	s := string(b)
	return "n:" + canonicalNumber(s), displayID(s), json.RawMessage(b), true
}

// canonicalNumber maps the spellings of one whole number (1, 1.0, 1e0) to a
// single form, because a server may re-serialise an id and still mean the same
// request. Integer literals and anything beyond exact float range stay as sent,
// so distinct large ids never merge.
func canonicalNumber(s string) string {
	if !strings.ContainsAny(s, ".eE") {
		return s
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f != math.Trunc(f) || math.Abs(f) > 1<<53 {
		return s
	}
	return strconv.FormatInt(int64(f), 10)
}

// displayID renders an id for a stderr line. An id with a space or a
// non-printable character is quoted so it cannot forge or split a log line.
func displayID(s string) string {
	if strings.IndexFunc(s, func(r rune) bool { return r == ' ' || r == '"' || !unicode.IsPrint(r) }) >= 0 {
		return strconv.Quote(s)
	}
	return s
}

func canonicalJSON(raw json.RawMessage) ([]byte, error) {
	b := bytes.TrimSpace(raw)
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		return []byte("{}"), nil
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return json.Marshal(v)
}

func coalesceHash(tool string, canonArgs []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(tool))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(canonArgs)
	return hex.EncodeToString(h.Sum(nil))
}
