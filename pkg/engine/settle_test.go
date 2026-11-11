package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/coherera/quelor/pkg/decider"
	"github.com/coherera/quelor/pkg/quelor"
	"github.com/coherera/quelor/pkg/resource"
	"github.com/coherera/quelor/pkg/trace"
)

type testClock struct {
	t time.Time
}

func (c *testClock) Now() time.Time { return c.t }

func admitResponse(ref string) decider.Response {
	return decider.Response{
		Status:      decider.StatusAnswered,
		Mode:        decider.ModeEnforce,
		Verdict:     quelor.Verdict{Kind: quelor.Admit},
		DecisionRef: ref,
		DeciderID:   "test",
		Provenance:  decider.Provenance{Step: "test", Tier: "unit", EvidenceClass: "synthetic"},
	}
}

func admitAll() decider.Decider {
	return decider.Func(func(_ context.Context, req decider.Request) (decider.Response, error) {
		return admitResponse("dec-" + string(req.Op)), nil
	})
}

func newTestEngine(t *testing.T, clk *testClock, manifests []resource.Manifest, d decider.Decider, mod func(*Config)) (*Engine, *trace.MemoryWriter, *tracetest.SpanRecorder) {
	t.Helper()
	reg := resource.NewRegistry(nil)
	for _, m := range manifests {
		m.WithDefaults()
		if err := reg.Register(m, nil); err != nil {
			t.Fatalf("register %s: %v", m.ID, err)
		}
	}
	if d == nil {
		d = admitAll()
	}
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(sr),
	)
	tw := &trace.MemoryWriter{}
	cfg := Config{
		Registry:       reg,
		Decider:        d,
		Trace:          tw,
		Clock:          clk.Now,
		Tracer:         tp.Tracer("quelor"),
		LeaseTTL:       30 * time.Second,
		DeciderTimeout: 5 * time.Second,
		Cell:           "cell-1",
	}
	if mod != nil {
		mod(&cfg)
	}
	e, err := New(cfg)
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() {
		_ = e.Close()
		_ = tp.Shutdown(context.Background())
	})
	return e, tw, sr
}

func manifest(id string, model resource.CapacityModel, cap int64, policy resource.UnknownPolicy) resource.Manifest {
	return resource.Manifest{
		ID:            id,
		CapacityModel: model,
		Capacity:      cap,
		UnknownPolicy: policy,
	}
}

func work(unit, holder string, demand map[string]int64, pri quelor.Priority) quelor.Envelope {
	d := settleCloneI64(demand)
	return quelor.Envelope{
		UnitID:   unit,
		TenantID: "tenant",
		HolderID: holder,
		Demands:  d,
		Priority: pri,
	}
}

func acquireLease(t *testing.T, e *Engine, env quelor.Envelope, token string) quelor.Lease {
	t.Helper()
	res, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{Envelope: env, Token: token})
	if err != nil {
		t.Fatalf("acquire %s: %v", env.UnitID, err)
	}
	if res.Verdict.Kind != quelor.Admit || res.Lease == nil {
		t.Fatalf("acquire %s: kind=%s reason=%s stamps=%v", env.UnitID, res.Verdict.Kind, res.Verdict.Reason, res.Verdict.Stamps)
	}
	return *res.Lease
}

func acquireVerdict(t *testing.T, e *Engine, env quelor.Envelope) quelor.VerdictKind {
	t.Helper()
	res, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{Envelope: env})
	if err != nil {
		t.Fatalf("acquire %s: %v", env.UnitID, err)
	}
	return res.Verdict.Kind
}

func drainEvents(ch <-chan Event) []Event {
	var out []Event
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func wakeTokens(evs []Event) []string {
	var out []string
	for _, ev := range evs {
		if ev.Kind == "wake" {
			out = append(out, ev.Token)
		}
	}
	return out
}

func findTrace(recs []trace.Record, op trace.Op) (trace.Record, bool) {
	var last trace.Record
	found := false
	for _, rec := range recs {
		if rec.Op == op {
			last = rec
			found = true
		}
	}
	return last, found
}

func countTrace(recs []trace.Record, op trace.Op) int {
	n := 0
	for _, rec := range recs {
		if rec.Op == op {
			n++
		}
	}
	return n
}

func spanByName(sr *tracetest.SpanRecorder, name string) sdktrace.ReadOnlySpan {
	for _, sp := range sr.Ended() {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

func spanAttr(sp sdktrace.ReadOnlySpan, key string) (string, bool) {
	if sp == nil {
		return "", false
	}
	for _, a := range sp.Attributes() {
		if string(a.Key) == key {
			return a.Value.AsString(), true
		}
	}
	return "", false
}

func baseClock() *testClock {
	return &testClock{t: time.Date(2026, 10, 31, 15, 0, 0, 0, time.UTC)}
}

func TestSettleDeclaredConsumedAndIdempotent(t *testing.T) {
	clk := baseClock()
	e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 5, resource.UnknownHoldCursor),
	}, nil, nil)
	l1 := acquireLease(t, e, work("u1", "h1", map[string]int64{"slots": 3}, quelor.PriorityChange), "")
	l2 := acquireLease(t, e, work("u2", "h1", map[string]int64{"slots": 2}, quelor.PriorityChange), "")

	got, err := e.SettleLease(context.Background(), quelor.SettleRequest{
		LeaseID:  l1.ID,
		HolderID: "h1",
		Consumed: map[string]int64{"slots": 1},
	})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got.Duplicate {
		t.Fatal("first settle marked duplicate")
	}
	if got.LeaseID != l1.ID || got.Declared["slots"] != 3 || got.Consumed["slots"] != 1 || got.Delta["slots"] != 2 {
		t.Fatalf("settlement = %+v", got)
	}
	if e.Stats().Settles != 1 || e.Stats().ActiveLeases != 1 {
		t.Fatalf("stats %+v", e.Stats())
	}

	sub := e.Subscribe()
	again, err := e.SettleLease(context.Background(), quelor.SettleRequest{
		LeaseID:  l1.ID,
		HolderID: "someone-else",
		Consumed: map[string]int64{"slots": 3},
	})
	if err != nil {
		t.Fatalf("duplicate settle: %v", err)
	}
	if !again.Duplicate || again.Consumed["slots"] != 1 || again.Delta["slots"] != 2 || again.Declared["slots"] != 3 {
		t.Fatalf("duplicate = %+v", again)
	}
	if e.Stats().DuplicateSettles != 1 || e.Stats().Settles != 1 || e.Stats().ActiveLeases != 1 {
		t.Fatalf("stats after duplicate %+v", e.Stats())
	}
	if w := wakeTokens(drainEvents(sub)); len(w) != 0 {
		t.Fatalf("duplicate emitted wakes %v", w)
	}
	if countTrace(tw.Records(), trace.OpSettle) != 1 {
		t.Fatalf("settle traces = %d", countTrace(tw.Records(), trace.OpSettle))
	}

	// Capacity 5, l2 still holds 2, l1's 3 released once. A second release would
	// free enough for both of the probes below.
	if k := acquireVerdict(t, e, work("u3", "h2", map[string]int64{"slots": 3}, "")); k != quelor.Admit {
		t.Fatalf("expected the single release to free 3, got %s", k)
	}
	if k := acquireVerdict(t, e, work("u4", "h2", map[string]int64{"slots": 1}, "")); k == quelor.Admit {
		t.Fatal("capacity grew as if the lease was released twice")
	}

	rec, ok := findTrace(tw.Records(), trace.OpSettle)
	if !ok {
		t.Fatal("missing settle trace")
	}
	if rec.Cell != "cell-1" || rec.LeaseID != l1.ID || rec.DecisionRef != l1.DecisionRef {
		t.Fatalf("trace cell=%s lease=%s ref=%s wantRef=%s", rec.Cell, rec.LeaseID, rec.DecisionRef, l1.DecisionRef)
	}
	if rec.Declared["slots"] != 3 || rec.Consumed["slots"] != 1 {
		t.Fatalf("trace declared/consumed %+v %+v", rec.Declared, rec.Consumed)
	}
	_ = l2
}

func TestSettleErrorsClampAndAuth(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("cpu", resource.ModelLease, 20, resource.UnknownHoldCursor),
		manifest("mem", resource.ModelLease, 20, resource.UnknownHoldCursor),
	}, nil, func(cfg *Config) {
		cfg.SharedToken = "sekret"
	})

	lease := acquireLease(t, e, work("u", "h1", map[string]int64{"cpu": 10, "mem": 7}, quelor.PriorityChange), "sekret")

	_, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: lease.ID, HolderID: "h1"})
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("auth err = %v", err)
	}
	_, err = e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: lease.ID, HolderID: "h1", Token: "nope"})
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("bad token err = %v", err)
	}
	if e.Stats().ActiveLeases != 1 {
		t.Fatal("unauthenticated settle released the lease")
	}

	_, err = e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: lease.ID, HolderID: "other", Token: "sekret"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("holder err = %v", err)
	}
	_, err = e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: "missing", HolderID: "h1", Token: "sekret"})
	if !errors.Is(err, ErrUnknownLease) {
		t.Fatalf("unknown err = %v", err)
	}

	cases := []struct {
		name     string
		consumed map[string]int64
		nilCons  bool
		wantC    map[string]int64
		wantD    map[string]int64
	}{
		{
			name:     "clamp high and negative",
			consumed: map[string]int64{"cpu": 15, "mem": -3, "extra": 9},
			wantC:    map[string]int64{"cpu": 10, "mem": 0},
			wantD:    map[string]int64{"cpu": 0, "mem": 7},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := e.SettleLease(context.Background(), quelor.SettleRequest{
				LeaseID:  lease.ID,
				HolderID: "h1",
				Token:    "sekret",
				Consumed: tc.consumed,
			})
			if err != nil {
				t.Fatalf("settle: %v", err)
			}
			for id, want := range tc.wantC {
				if got.Consumed[id] != want {
					t.Fatalf("consumed[%s]=%d want %d (%+v)", id, got.Consumed[id], want, got)
				}
			}
			for id, want := range tc.wantD {
				if got.Delta[id] != want {
					t.Fatalf("delta[%s]=%d want %d (%+v)", id, got.Delta[id], want, got)
				}
			}
			if _, ok := got.Consumed["extra"]; ok {
				t.Fatal("undeclared consumed key kept")
			}
		})
	}

	// A fresh lease checks nil consumed (fully used) and an explicit empty map.
	full := acquireLease(t, e, work("u-full", "h1", map[string]int64{"cpu": 4}, ""), "sekret")
	got, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: full.ID, HolderID: "h1", Token: "sekret"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Consumed["cpu"] != 4 || got.Delta["cpu"] != 0 {
		t.Fatalf("nil consumed = %+v", got)
	}
	empty := acquireLease(t, e, work("u-empty", "h1", map[string]int64{"cpu": 4}, ""), "sekret")
	got, err = e.SettleLease(context.Background(), quelor.SettleRequest{
		LeaseID:  empty.ID,
		HolderID: "h1",
		Token:    "sekret",
		Consumed: map[string]int64{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Consumed["cpu"] != 0 || got.Delta["cpu"] != 4 {
		t.Fatalf("empty consumed = %+v", got)
	}
}

func TestReleaseLockedDoesNotStore(t *testing.T) {
	clk := baseClock()
	e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 4, resource.UnknownHoldCursor),
	}, nil, nil)
	lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 4}, ""), "")
	before := len(tw.Records())

	e.mu.Lock()
	rec := e.leases[lease.ID]
	got := e.releaseLocked(rec, map[string]int64{"slots": 1}, clk.Now(), "expire")
	e.mu.Unlock()

	if got.Consumed["slots"] != 1 || got.Delta["slots"] != 3 || got.Declared["slots"] != 4 {
		t.Fatalf("release = %+v", got)
	}
	if _, ok := e.settled[lease.ID]; ok {
		t.Fatal("releaseLocked stored a settlement")
	}
	if len(tw.Records()) != before {
		t.Fatal("releaseLocked wrote a trace")
	}
	_, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: lease.ID, HolderID: "h1"})
	if !errors.Is(err, ErrUnknownLease) {
		t.Fatalf("settle after release = %v", err)
	}
	if k := acquireVerdict(t, e, work("u2", "h1", map[string]int64{"slots": 4}, "")); k != quelor.Admit {
		t.Fatalf("capacity not restored: %s", k)
	}
}

func TestHardBudgetRefundsUnconsumed(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("usd", resource.ModelHardBudget, 100, resource.UnknownDeny),
	}, nil, nil)
	lease := acquireLease(t, e, work("u", "h1", map[string]int64{"usd": 40}, ""), "")
	got, err := e.SettleLease(context.Background(), quelor.SettleRequest{
		LeaseID:  lease.ID,
		HolderID: "h1",
		Consumed: map[string]int64{"usd": 25},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Declared["usd"] != 40 || got.Consumed["usd"] != 25 || got.Delta["usd"] != 15 {
		t.Fatalf("settlement = %+v", got)
	}
	// 25 stays spent, 15 returns. 76 must not fit; 75 must.
	if k := acquireVerdict(t, e, work("too-much", "h2", map[string]int64{"usd": 76}, "")); k == quelor.Admit {
		t.Fatal("unconsumed budget was not the only refund")
	}
	if k := acquireVerdict(t, e, work("exact", "h2", map[string]int64{"usd": 75}, "")); k != quelor.Admit {
		t.Fatalf("refunded budget unavailable: %s", k)
	}
}

func TestSettleClassifiesRateLimitLog(t *testing.T) {
	clk := baseClock()
	e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 2, resource.UnknownHoldCursor),
	}, nil, nil)
	lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 1}, ""), "")
	log := strings.Repeat("429 rate limit exceeded; Too Many Requests. ", 20) // > 512 bytes
	if len(log) <= 512 {
		t.Fatalf("fixture log len %d", len(log))
	}
	got, err := e.SettleLease(context.Background(), quelor.SettleRequest{
		LeaseID:  lease.ID,
		HolderID: "h1",
		Outcome: &quelor.ExecutionOutcome{
			ExitCode: 1,
			Log:      log,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Class != quelor.ResourceExhausted {
		t.Fatalf("class %q", got.Class)
	}
	rec, ok := findTrace(tw.Records(), trace.OpSettle)
	if !ok || rec.Outcome == nil {
		t.Fatal("missing settle outcome trace")
	}
	if rec.Outcome.Class != quelor.ResourceExhausted {
		t.Fatalf("trace class %q", rec.Outcome.Class)
	}
	if len(rec.Outcome.LogExcerpt) != 512 || !strings.HasPrefix(log, rec.Outcome.LogExcerpt) {
		t.Fatalf("excerpt len %d", len(rec.Outcome.LogExcerpt))
	}
	if rec.Outcome.ExitCode != 1 {
		t.Fatalf("exit %d", rec.Outcome.ExitCode)
	}
}

func TestAIMDHalvesOnSettleAndObserve(t *testing.T) {
	models := []resource.CapacityModel{resource.ModelWindowBucket, resource.ModelGCRA, resource.ModelLease}
	var problems []string
	settled := false
	observed := false
	for _, model := range models {
		if !settled {
			if err := aimdVia(model, true); err != nil {
				problems = append(problems, "settle/"+string(model)+": "+err.Error())
			} else {
				settled = true
			}
		}
		if !observed {
			if err := aimdVia(model, false); err != nil {
				problems = append(problems, "observe/"+string(model)+": "+err.Error())
			} else {
				observed = true
			}
		}
	}
	if !settled || !observed {
		t.Fatalf("429 did not halve effective capacity (settle=%v observe=%v): %s", settled, observed, strings.Join(problems, "; "))
	}
}

func aimdVia(model resource.CapacityModel, viaSettle bool) error {
	clk := baseClock()
	reg := resource.NewRegistry(nil)
	m := manifest("rpm", model, 100, resource.UnknownHoldCursor)
	m.WithDefaults()
	if err := reg.Register(m, nil); err != nil {
		return err
	}
	e, err := New(Config{
		Registry:       reg,
		Decider:        admitAll(),
		Clock:          clk.Now,
		LeaseTTL:       30 * time.Second,
		DeciderTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer e.Close()

	admit := func(unit string, n int64) error {
		res, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{
			Envelope: work(unit, "h", map[string]int64{"rpm": n}, ""),
		})
		if err != nil {
			return err
		}
		if res.Verdict.Kind != quelor.Admit || res.Lease == nil {
			return fmt.Errorf("acquire %d => %s (%s)", n, res.Verdict.Kind, res.Verdict.Reason)
		}
		_, err = e.SettleLease(context.Background(), quelor.SettleRequest{
			LeaseID:  res.Lease.ID,
			HolderID: "h",
			Consumed: map[string]int64{"rpm": 0},
		})
		return err
	}
	// Two clean cycles prove the model can grant and fully restore 60.
	if err := admit("base-1", 60); err != nil {
		return fmt.Errorf("baseline: %w", err)
	}
	if err := admit("base-2", 60); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	before := e.Stats().AIMDDecreases
	if viaSettle {
		res, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{
			Envelope: work("burn", "h", map[string]int64{"rpm": 1}, ""),
		})
		if err != nil {
			return err
		}
		if res.Verdict.Kind != quelor.Admit || res.Lease == nil {
			return fmt.Errorf("probe acquire %s", res.Verdict.Kind)
		}
		if _, err := e.SettleLease(context.Background(), quelor.SettleRequest{
			LeaseID:  res.Lease.ID,
			HolderID: "h",
			Consumed: map[string]int64{"rpm": 0},
			Outcome:  &quelor.ExecutionOutcome{HTTPStatus: 429, ExitCode: 1},
		}); err != nil {
			return err
		}
	} else if err := e.Observe(context.Background(), quelor.Observation{Resource: "rpm", HTTPStatus: 429}); err != nil {
		return err
	}
	if e.Stats().AIMDDecreases <= before {
		return errors.New("no aimd-decrease counted")
	}
	wide, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{
		Envelope: work("wide", "h", map[string]int64{"rpm": 60}, ""),
	})
	if err != nil {
		return err
	}
	if wide.Verdict.Kind == quelor.Admit {
		return errors.New("60 still admitted after 429")
	}
	half, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{
		Envelope: work("half", "h", map[string]int64{"rpm": 50}, ""),
	})
	if err != nil {
		return err
	}
	if half.Verdict.Kind != quelor.Admit {
		return fmt.Errorf("half capacity not admitted: %s %s", half.Verdict.Kind, half.Verdict.Reason)
	}
	return nil
}

func TestObserveUnknownResource(t *testing.T) {
	clk := baseClock()
	e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 1, resource.UnknownHoldCursor),
	}, nil, nil)
	err := e.Observe(context.Background(), quelor.Observation{Resource: "missing", HTTPStatus: 503})
	if err == nil || !strings.Contains(err.Error(), "unknown resource") {
		t.Fatalf("err = %v", err)
	}
	if countTrace(tw.Records(), trace.OpObserve) != 0 {
		t.Fatal("unknown observe wrote a trace")
	}
}

func TestCircuitProbeSettleCloses(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("up", resource.ModelCircuit, 2, resource.UnknownHoldCursor),
	}, nil, nil)

	opened := false
	for i := 0; i < 20; i++ {
		if err := e.Observe(context.Background(), quelor.Observation{Resource: "up", HTTPStatus: 503}); err != nil {
			t.Fatal(err)
		}
		if acquireVerdict(t, e, work(fmt.Sprintf("closed-%d", i), "h1", map[string]int64{"up": 1}, "")) != quelor.Admit {
			opened = true
			break
		}
	}
	if !opened {
		t.Fatal("503 observations did not open the circuit")
	}

	for i := 0; i < 4; i++ {
		clk.t = clk.t.Add(time.Hour)
		_ = e.Advance(clk.t)
	}
	probe := acquireLease(t, e, work("probe", "h1", map[string]int64{"up": 1}, ""), "")
	if k := acquireVerdict(t, e, work("second", "h1", map[string]int64{"up": 1}, "")); k == quelor.Admit {
		t.Fatal("half-open circuit admitted a second demand beside the probe")
	}
	if _, err := e.SettleLease(context.Background(), quelor.SettleRequest{
		LeaseID:  probe.ID,
		HolderID: "h1",
		Outcome:  &quelor.ExecutionOutcome{ExitCode: 0},
	}); err != nil {
		t.Fatal(err)
	}
	if k := acquireVerdict(t, e, work("after", "h1", map[string]int64{"up": 1}, "")); k != quelor.Admit {
		t.Fatalf("successful probe settle left the circuit closed to work: %s", k)
	}
	// Both slots work again once the breaker is closed.
	if k := acquireVerdict(t, e, work("after-2", "h1", map[string]int64{"up": 1}, "")); k != quelor.Admit {
		t.Fatalf("closed circuit did not restore capacity: %s", k)
	}
}

// Production call sites pass only "settle" (SettleLease) and "expire"
// (Advance). Any other reason that releases an in-flight probe must fail
// closed: re-open the breaker and free the probe slot so half-open cannot
// stick forever if a new release path appears.
func TestReleaseLockedUnknownReasonReopensProbe(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("up", resource.ModelCircuit, 2, resource.UnknownHoldCursor),
	}, nil, nil)

	opened := false
	for i := 0; i < 20; i++ {
		if err := e.Observe(context.Background(), quelor.Observation{Resource: "up", HTTPStatus: 503}); err != nil {
			t.Fatal(err)
		}
		if acquireVerdict(t, e, work(fmt.Sprintf("closed-%d", i), "h1", map[string]int64{"up": 1}, "")) != quelor.Admit {
			opened = true
			break
		}
	}
	if !opened {
		t.Fatal("503 observations did not open the circuit")
	}
	for i := 0; i < 4; i++ {
		clk.t = clk.t.Add(time.Hour)
		_ = e.Advance(clk.t)
	}

	probe := acquireLease(t, e, work("probe", "h1", map[string]int64{"up": 1}, ""), "")
	if k := acquireVerdict(t, e, work("second", "h1", map[string]int64{"up": 1}, "")); k == quelor.Admit {
		t.Fatal("half-open circuit admitted a second demand beside the probe")
	}

	sub := e.Subscribe()
	e.mu.Lock()
	rec := e.leases[probe.ID]
	if rec == nil || rec.probe == nil || !rec.probe["up"] {
		e.mu.Unlock()
		t.Fatal("probe lease missing circuit probe mark")
	}
	_ = e.releaseLocked(rec, nil, clk.Now(), "cancel")
	e.mu.Unlock()

	sawOpen, sawClosed := false, false
	for _, ev := range drainEvents(sub) {
		switch ev.Kind {
		case "circuit-open":
			sawOpen = true
		case "circuit-closed":
			sawClosed = true
		}
	}
	if sawClosed {
		t.Fatal("unknown release reason closed the circuit")
	}
	if !sawOpen {
		t.Fatal("unknown release reason did not re-open the circuit")
	}

	var circ circuitMode
	var inFlight bool
	e.mu.Lock()
	s := e.res["up"]
	circ = s.circ
	inFlight = s.probeInFlight
	e.mu.Unlock()
	if circ != modeOpen {
		t.Fatalf("circuit mode = %v, want open", circ)
	}
	if inFlight {
		t.Fatal("probe slot still held after unknown-reason release")
	}
	if k := acquireVerdict(t, e, work("after-cancel", "h1", map[string]int64{"up": 1}, "")); k == quelor.Admit {
		t.Fatalf("admitted while re-opened after unknown-reason release: %s", k)
	}
}

func TestNotifyCapacityOnSettle(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 10, resource.UnknownHoldCursor),
	}, nil, nil)
	lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 9}, ""), "")
	sub := e.Subscribe()

	e.mu.Lock()
	e.waiters["soon"] = &waiter{token: "soon", resource: "slots", need: 1, wakeAt: clk.t.Add(time.Hour)}
	e.waiters["later"] = &waiter{token: "later", resource: "slots", need: 2, wakeAt: clk.t.Add(-time.Hour)}
	e.notifyCapacityLocked(clk.t)
	e.mu.Unlock()

	if got := wakeTokens(drainEvents(sub)); len(got) != 1 || got[0] != "soon" {
		t.Fatalf("wakes = %v", got)
	}
	e.mu.Lock()
	if _, ok := e.waiters["later"]; !ok {
		e.mu.Unlock()
		t.Fatal("short waiter was dropped")
	}
	if _, ok := e.waiters["soon"]; ok {
		e.mu.Unlock()
		t.Fatal("satisfied waiter stayed queued")
	}
	e.mu.Unlock()

	if _, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: lease.ID, HolderID: "h1"}); err != nil {
		t.Fatal(err)
	}
	if got := wakeTokens(drainEvents(sub)); len(got) != 1 || got[0] != "later" {
		t.Fatalf("settle wakes = %v", got)
	}
}

func TestRenewSuccess(t *testing.T) {
	clk := baseClock()
	var seen atomic.Value
	d := decider.Func(func(_ context.Context, req decider.Request) (decider.Response, error) {
		if req.Op == decider.OpRenew {
			seen.Store(req)
		}
		return admitResponse("dec-" + string(req.Op)), nil
	})
	e, tw, sr := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 10, resource.UnknownHoldCursor),
	}, d, nil)
	lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 4}, quelor.PriorityScheduled), "")
	origExp := lease.ExpiresAt

	clk.t = clk.t.Add(10 * time.Second)
	got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
		LeaseID:  lease.ID,
		HolderID: "h1",
		Delta:    map[string]int64{"slots": 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Verdict.Kind != quelor.Admit || got.Verdict.Hibernate {
		t.Fatalf("verdict %+v", got.Verdict)
	}
	if got.DecisionRef != "dec-renew" || got.Lease == nil {
		t.Fatalf("result %+v", got)
	}
	if got.Lease.Renewals != 1 || got.Lease.Declared["slots"] != 7 {
		t.Fatalf("lease %+v", got.Lease)
	}
	wantExp := clk.t.Add(30 * time.Second)
	if !got.Lease.ExpiresAt.Equal(wantExp) || !origExp.Before(got.Lease.ExpiresAt) {
		t.Fatalf("expiry %s want %s", got.Lease.ExpiresAt, wantExp)
	}
	if e.Stats().Renews != 1 || e.Stats().RenewRefused != 0 || e.Stats().ActiveLeases != 1 {
		t.Fatalf("stats %+v", e.Stats())
	}

	req, _ := seen.Load().(decider.Request)
	if req.Op != decider.OpRenew || req.DeciderAPIVersion != decider.APIVersion {
		t.Fatalf("decider request %+v", req)
	}
	if req.Lease == nil || req.Lease.ID != lease.ID || req.SpendDelta["slots"] != 3 {
		t.Fatalf("decider lease/delta %+v", req)
	}
	if req.Envelope.UnitID != "u" || req.Meta.Now != clk.t.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("meta %+v env %+v", req.Meta, req.Envelope)
	}
	if len(req.Readings) == 0 {
		t.Fatal("renew consulted without readings")
	}

	rec, ok := findTrace(tw.Records(), trace.OpRenew)
	if !ok {
		t.Fatal("missing renew trace")
	}
	if rec.Cell != "cell-1" || rec.DecisionRef != "dec-renew" || rec.Declared["slots"] != 7 {
		t.Fatalf("trace %+v", rec)
	}
	if rec.DeciderStatus != trace.DeciderStatusAnswered || rec.Mode != trace.Mode(decider.ModeEnforce) {
		t.Fatalf("status %s mode %s", rec.DeciderStatus, rec.Mode)
	}
	if rec.Provenance.Step != "test" || rec.Verdict == nil || rec.Verdict.Kind != quelor.Admit {
		t.Fatalf("provenance/verdict %+v %+v", rec.Provenance, rec.Verdict)
	}

	sp := spanByName(sr, "quelor.renew")
	if sp == nil {
		t.Fatal("missing quelor.renew span")
	}
	if v, ok := spanAttr(sp, "quelor.op"); !ok || v != string(trace.OpRenew) {
		t.Fatalf("span op %q", v)
	}
	if v, ok := spanAttr(sp, "quelor.lease_id"); !ok || v != lease.ID {
		t.Fatalf("span lease %q", v)
	}

	// 4+3 held, 3 remain. 4 must not fit; 3 must.
	if k := acquireVerdict(t, e, work("overflow", "h2", map[string]int64{"slots": 4}, "")); k == quelor.Admit {
		t.Fatal("delta was not reserved")
	}
	if k := acquireVerdict(t, e, work("fits", "h2", map[string]int64{"slots": 3}, "")); k != quelor.Admit {
		t.Fatalf("remaining capacity: %s", k)
	}

	// Empty delta only slides the expiry.
	clk.t = clk.t.Add(5 * time.Second)
	ext, err := e.RenewLease(context.Background(), quelor.RenewRequest{LeaseID: lease.ID, HolderID: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	if ext.Lease.Renewals != 2 || ext.Lease.Declared["slots"] != 7 || !ext.Lease.ExpiresAt.Equal(clk.t.Add(30*time.Second)) {
		t.Fatalf("empty renew %+v", ext.Lease)
	}
}

func TestRenewRefusalLeavesLease(t *testing.T) {
	wake := time.Date(2026, 12, 1, 0, 0, 0, 0, time.UTC)
	kinds := []quelor.VerdictKind{quelor.Defer, quelor.Shed, quelor.Deny}
	for _, kind := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			clk := baseClock()
			d := decider.Func(func(_ context.Context, req decider.Request) (decider.Response, error) {
				if req.Op == decider.OpRenew {
					return decider.Response{
						Status:      decider.StatusAnswered,
						Mode:        decider.ModeEnforce,
						Verdict:     quelor.Verdict{Kind: kind, Reason: "pause", WakeAt: &wake, Stamps: []string{"ruleset-moved"}},
						DecisionRef: "d-refuse",
						DeciderID:   "test",
					}, nil
				}
				return admitResponse("dec-acquire"), nil
			})
			e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
				manifest("slots", resource.ModelLease, 10, resource.UnknownHoldCursor),
			}, d, nil)
			lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 4}, ""), "")
			clk.t = clk.t.Add(time.Minute)
			got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
				LeaseID:  lease.ID,
				HolderID: "h1",
				Delta:    map[string]int64{"slots": 2},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.Verdict.Kind != quelor.Defer || !got.Verdict.Hibernate {
				t.Fatalf("verdict %+v", got.Verdict)
			}
			if got.Verdict.Reason != "renew-refused:pause" {
				t.Fatalf("reason %q", got.Verdict.Reason)
			}
			if got.Verdict.WakeAt == nil || !got.Verdict.WakeAt.Equal(wake) {
				t.Fatalf("wake %+v", got.Verdict.WakeAt)
			}
			if !settleHasStamp(got.Verdict.Stamps, "ruleset-moved") {
				t.Fatalf("stamps %v", got.Verdict.Stamps)
			}
			if got.Lease == nil || got.Lease.Renewals != 0 || got.Lease.Declared["slots"] != 4 || !got.Lease.ExpiresAt.Equal(lease.ExpiresAt) {
				t.Fatalf("lease changed: %+v", got.Lease)
			}
			if e.Stats().RenewRefused != 1 || e.Stats().Renews != 0 {
				t.Fatalf("stats %+v", e.Stats())
			}
			rec, ok := findTrace(tw.Records(), trace.OpRenew)
			if !ok || rec.Verdict == nil || !rec.Verdict.Hibernate || rec.DecisionRef != "d-refuse" {
				t.Fatalf("trace %+v", rec)
			}
			// Delta was not reserved: 6 of the remaining 6 still fit.
			if k := acquireVerdict(t, e, work("probe", "h2", map[string]int64{"slots": 6}, "")); k != quelor.Admit {
				t.Fatalf("refusal reserved capacity: %s", k)
			}
		})
	}
}

func TestRenewNonEnforceDoesNotRefuse(t *testing.T) {
	for _, mode := range []decider.Mode{decider.ModeLogOnly, decider.ModeAdvise} {
		t.Run(string(mode), func(t *testing.T) {
			clk := baseClock()
			d := decider.Func(func(_ context.Context, req decider.Request) (decider.Response, error) {
				if req.Op == decider.OpRenew {
					return decider.Response{
						Status:      decider.StatusAnswered,
						Mode:        mode,
						Verdict:     quelor.Verdict{Kind: quelor.Deny, Reason: "nope"},
						DecisionRef: "d-log",
						DeciderID:   "test",
					}, nil
				}
				return admitResponse("dec-acquire"), nil
			})
			e, _, _ := newTestEngine(t, clk, []resource.Manifest{
				manifest("slots", resource.ModelLease, 8, resource.UnknownHoldCursor),
			}, d, nil)
			lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 1}, ""), "")
			got, err := e.RenewLease(context.Background(), quelor.RenewRequest{LeaseID: lease.ID, HolderID: "h1"})
			if err != nil {
				t.Fatal(err)
			}
			if got.Verdict.Kind != quelor.Admit || got.Verdict.Hibernate || got.Lease.Renewals != 1 {
				t.Fatalf("result %+v", got)
			}
		})
	}
}

func TestRenewWrongHolderUnknownAndAuth(t *testing.T) {
	clk := baseClock()
	var calls atomic.Int32
	d := decider.Func(func(_ context.Context, req decider.Request) (decider.Response, error) {
		calls.Add(1)
		return admitResponse("dec-" + string(req.Op)), nil
	})
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 4, resource.UnknownHoldCursor),
	}, d, func(cfg *Config) {
		cfg.SharedToken = "sekret"
	})
	lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 1}, ""), "sekret")
	afterAcquire := calls.Load()

	_, err := e.RenewLease(context.Background(), quelor.RenewRequest{LeaseID: lease.ID, HolderID: "h2", Token: "sekret"})
	if !errors.Is(err, ErrForbidden) {
		t.Fatalf("holder err %v", err)
	}
	_, err = e.RenewLease(context.Background(), quelor.RenewRequest{LeaseID: "nope", HolderID: "h1", Token: "sekret"})
	if !errors.Is(err, ErrUnknownLease) {
		t.Fatalf("unknown err %v", err)
	}
	_, err = e.RenewLease(context.Background(), quelor.RenewRequest{LeaseID: lease.ID, HolderID: "h1"})
	if !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("auth err %v", err)
	}
	if calls.Load() != afterAcquire {
		t.Fatalf("decider calls %d -> %d", afterAcquire, calls.Load())
	}
	if e.Stats().Renews != 0 && e.Stats().RenewRefused != 0 {
		t.Fatalf("stats %+v", e.Stats())
	}
}

func TestRenewUnreachable(t *testing.T) {
	t.Run("hard budget delta refuses including operator directive", func(t *testing.T) {
		for _, pri := range []quelor.Priority{quelor.PriorityChange, quelor.PriorityOperatorDirective} {
			clk := baseClock()
			e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
				manifest("usd", resource.ModelHardBudget, 100, resource.UnknownDeny),
			}, unreachableOnRenew(decider.StatusUnavailable), nil)
			lease := acquireLease(t, e, work("u-"+string(pri), "h1", map[string]int64{"usd": 40}, pri), "")
			got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
				LeaseID:  lease.ID,
				HolderID: "h1",
				Delta:    map[string]int64{"usd": 10},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.Verdict.Reason != "renew-refused:bound-unreachable" || !got.Verdict.Hibernate || got.Verdict.Kind != quelor.Defer {
				t.Fatalf("verdict %+v", got.Verdict)
			}
			if !settleHasStamp(got.Verdict.Stamps, "bound-unreachable") {
				t.Fatalf("stamps %v", got.Verdict.Stamps)
			}
			if got.Lease.Renewals != 0 || got.Lease.Declared["usd"] != 40 || !got.Lease.ExpiresAt.Equal(lease.ExpiresAt) {
				t.Fatalf("lease changed %+v", got.Lease)
			}
			if e.Stats().BoundUnreachable != 1 || e.Stats().RenewRefused != 1 || e.Stats().Renews != 0 {
				t.Fatalf("stats %+v", e.Stats())
			}
			rec, ok := findTrace(tw.Records(), trace.OpRenew)
			if !ok || rec.DeciderStatus != trace.DeciderStatusUnavailable || !settleHasStamp(rec.Stamps, "bound-unreachable") {
				t.Fatalf("trace %+v", rec)
			}
			if k := acquireVerdict(t, e, work("probe-"+string(pri), "h2", map[string]int64{"usd": 60}, "")); k != quelor.Admit {
				t.Fatalf("refused renew consumed budget: %s", k)
			}
		}
	})

	t.Run("killed status is unreachable", func(t *testing.T) {
		clk := baseClock()
		e, _, _ := newTestEngine(t, clk, []resource.Manifest{
			manifest("usd", resource.ModelHardBudget, 20, resource.UnknownDeny),
		}, unreachableOnRenew(decider.StatusKilled), nil)
		lease := acquireLease(t, e, work("u", "h1", map[string]int64{"usd": 5}, ""), "")
		got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
			LeaseID: lease.ID, HolderID: "h1", Delta: map[string]int64{"usd": 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if !got.Verdict.Hibernate || got.Verdict.Reason != "renew-refused:bound-unreachable" {
			t.Fatalf("verdict %+v", got.Verdict)
		}
	})

	t.Run("empty delta on hard budget extends", func(t *testing.T) {
		clk := baseClock()
		e, _, _ := newTestEngine(t, clk, []resource.Manifest{
			manifest("usd", resource.ModelHardBudget, 50, resource.UnknownDeny),
		}, unreachableOnRenew(decider.StatusUnavailable), nil)
		lease := acquireLease(t, e, work("u", "h1", map[string]int64{"usd": 10}, ""), "")
		clk.t = clk.t.Add(8 * time.Second)
		got, err := e.RenewLease(context.Background(), quelor.RenewRequest{LeaseID: lease.ID, HolderID: "h1"})
		if err != nil {
			t.Fatal(err)
		}
		if got.Verdict.Kind != quelor.Admit || !settleHasStamp(got.Verdict.Stamps, "bound-unreachable") {
			t.Fatalf("verdict %+v", got.Verdict)
		}
		if got.Lease.Renewals != 1 || got.Lease.Declared["usd"] != 10 || !got.Lease.ExpiresAt.Equal(clk.t.Add(30*time.Second)) {
			t.Fatalf("lease %+v", got.Lease)
		}
		if e.Stats().Renews != 1 || e.Stats().RenewRefused != 0 || e.Stats().BoundUnreachable != 1 {
			t.Fatalf("stats %+v", e.Stats())
		}
	})

	policies := []resource.UnknownPolicy{resource.UnknownDeferShort, resource.UnknownAssumeBusy, resource.UnknownDeny}
	for _, p := range policies {
		t.Run(fmt.Sprint(p), func(t *testing.T) {
			clk := baseClock()
			e, _, _ := newTestEngine(t, clk, []resource.Manifest{
				manifest("slots", resource.ModelLease, 10, p),
			}, unreachableOnRenew(decider.StatusUnavailable), nil)
			lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 1}, quelor.PriorityChange), "")
			got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
				LeaseID: lease.ID, HolderID: "h1", Delta: map[string]int64{"slots": 1},
			})
			if err != nil {
				t.Fatal(err)
			}
			if got.Verdict.Reason != "renew-refused:bound-unreachable" || !got.Verdict.Hibernate {
				t.Fatalf("verdict %+v", got.Verdict)
			}
			if got.Lease.Declared["slots"] != 1 || got.Lease.Renewals != 0 {
				t.Fatalf("lease %+v", got.Lease)
			}
		})
	}

	t.Run("operator directive overrides unknown policy", func(t *testing.T) {
		clk := baseClock()
		e, _, _ := newTestEngine(t, clk, []resource.Manifest{
			manifest("slots", resource.ModelLease, 10, resource.UnknownDeny),
		}, unreachableOnRenew(decider.StatusUnavailable), nil)
		lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 2}, quelor.PriorityOperatorDirective), "")
		got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
			LeaseID: lease.ID, HolderID: "h1", Delta: map[string]int64{"slots": 2},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Verdict.Kind != quelor.Admit || got.Lease.Declared["slots"] != 4 || got.Lease.Renewals != 1 {
			t.Fatalf("result %+v lease %+v", got.Verdict, got.Lease)
		}
		if !settleHasStamp(got.Verdict.Stamps, "bound-unreachable") {
			t.Fatalf("stamps %v", got.Verdict.Stamps)
		}
	})

	t.Run("hold cursor continues", func(t *testing.T) {
		clk := baseClock()
		e, _, _ := newTestEngine(t, clk, []resource.Manifest{
			manifest("slots", resource.ModelLease, 10, resource.UnknownHoldCursor),
		}, unreachableOnRenew(decider.StatusUnavailable), nil)
		lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 2}, quelor.PriorityMaintenance), "")
		got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
			LeaseID: lease.ID, HolderID: "h1", Delta: map[string]int64{"slots": 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Verdict.Kind != quelor.Admit || got.Lease.Declared["slots"] != 3 {
			t.Fatalf("result %+v %+v", got.Verdict, got.Lease)
		}
	})
}

func unreachableOnRenew(status decider.Status) decider.Decider {
	return decider.Func(func(_ context.Context, req decider.Request) (decider.Response, error) {
		if req.Op == decider.OpRenew {
			return decider.Response{Status: status, DeciderID: "down"}, errors.New("down")
		}
		return admitResponse("dec-acquire"), nil
	})
}

func TestRenewQuotaAndAtomicReserve(t *testing.T) {
	t.Run("holder quota", func(t *testing.T) {
		clk := baseClock()
		e, _, _ := newTestEngine(t, clk, []resource.Manifest{
			manifest("cpu", resource.ModelLease, 100, resource.UnknownHoldCursor),
		}, nil, func(cfg *Config) {
			cfg.HolderQuotas = map[string]Quota{
				"h1": {MaxUnits: map[string]int64{"cpu": 5}},
			}
		})
		lease := acquireLease(t, e, work("u", "h1", map[string]int64{"cpu": 4}, ""), "")
		got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
			LeaseID: lease.ID, HolderID: "h1", Delta: map[string]int64{"cpu": 2},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Verdict.Reason != "renew-refused:holder-quota-exceeded" || !got.Verdict.Hibernate {
			t.Fatalf("verdict %+v", got.Verdict)
		}
		if got.Lease.Declared["cpu"] != 4 || got.Lease.Renewals != 0 {
			t.Fatalf("lease %+v", got.Lease)
		}
		// Quota is per holder. Another holder still sees the unreserved remainder.
		if k := acquireVerdict(t, e, work("probe", "h2", map[string]int64{"cpu": 96}, "")); k != quelor.Admit {
			t.Fatalf("quota refusal reserved capacity: %s", k)
		}
	})

	t.Run("zero max units is unlimited", func(t *testing.T) {
		clk := baseClock()
		e, _, _ := newTestEngine(t, clk, []resource.Manifest{
			manifest("cpu", resource.ModelLease, 10, resource.UnknownHoldCursor),
		}, nil, func(cfg *Config) {
			cfg.HolderQuotas = map[string]Quota{
				"h1": {MaxUnits: map[string]int64{"cpu": 0}},
			}
		})
		lease := acquireLease(t, e, work("u", "h1", map[string]int64{"cpu": 3}, ""), "")
		got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
			LeaseID: lease.ID, HolderID: "h1", Delta: map[string]int64{"cpu": 2},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Verdict.Kind != quelor.Admit || got.Lease.Declared["cpu"] != 5 {
			t.Fatalf("result %+v %+v", got.Verdict, got.Lease)
		}
	})

	t.Run("insufficient rolls back earlier resources", func(t *testing.T) {
		clk := baseClock()
		e, _, _ := newTestEngine(t, clk, []resource.Manifest{
			manifest("cpu", resource.ModelLease, 5, resource.UnknownHoldCursor),
			manifest("gpu", resource.ModelLease, 1, resource.UnknownHoldCursor),
		}, nil, nil)
		lease := acquireLease(t, e, work("u", "h1", map[string]int64{"cpu": 1}, ""), "")
		_ = acquireLease(t, e, work("fill-gpu", "h2", map[string]int64{"gpu": 1}, ""), "")
		got, err := e.RenewLease(context.Background(), quelor.RenewRequest{
			LeaseID:  lease.ID,
			HolderID: "h1",
			Delta:    map[string]int64{"cpu": 1, "gpu": 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.Verdict.Reason != "renew-refused:insufficient:gpu" || !got.Verdict.Hibernate {
			t.Fatalf("verdict %+v", got.Verdict)
		}
		if got.Lease.Declared["cpu"] != 1 || got.Lease.Declared["gpu"] != 0 || got.Lease.Renewals != 0 {
			t.Fatalf("lease %+v", got.Lease)
		}
		// cpu still has 4 free. A stuck delta of 1 would leave only 3.
		if k := acquireVerdict(t, e, work("cpu-probe", "h3", map[string]int64{"cpu": 4}, "")); k != quelor.Admit {
			t.Fatalf("cpu delta was not rolled back: %s", k)
		}
	})
}

func TestRenewSettleSpans(t *testing.T) {
	clk := baseClock()
	e, _, sr := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 2, resource.UnknownHoldCursor),
	}, nil, nil)
	lease := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 1}, ""), "")
	if _, err := e.RenewLease(context.Background(), quelor.RenewRequest{LeaseID: lease.ID, HolderID: "h1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: lease.ID, HolderID: "h1"}); err != nil {
		t.Fatal(err)
	}
	if spanByName(sr, "quelor.renew") == nil {
		t.Fatal("missing quelor.renew")
	}
	sp := spanByName(sr, "quelor.settle")
	if sp == nil {
		t.Fatal("missing quelor.settle")
	}
	if v, ok := spanAttr(sp, "quelor.lease_id"); !ok || v != lease.ID {
		t.Fatalf("settle span lease %q", v)
	}
	if v, ok := spanAttr(sp, "quelor.unit_id"); !ok || v != "u" {
		t.Fatalf("settle span unit %q", v)
	}
}

func TestStoreSettledFIFO(t *testing.T) {
	e := &Engine{}
	for i := 0; i < 50000; i++ {
		e.storeSettledLocked(quelor.SettleResult{LeaseID: fmt.Sprintf("L%d", i), Declared: map[string]int64{"slots": int64(i)}})
	}
	if len(e.settled) != 50000 {
		t.Fatalf("len at cap = %d", len(e.settled))
	}
	if _, ok := e.settled["L0"]; !ok {
		t.Fatal("oldest dropped before the cap")
	}
	e.storeSettledLocked(quelor.SettleResult{LeaseID: "L50000"})
	if len(e.settled) != 50000 {
		t.Fatalf("len after overflow = %d", len(e.settled))
	}
	if _, ok := e.settled["L0"]; ok {
		t.Fatal("oldest settlement kept")
	}
	if _, ok := e.settled["L1"]; !ok {
		t.Fatal("second oldest dropped")
	}
	if _, ok := e.settled["L50000"]; !ok {
		t.Fatal("newest settlement missing")
	}
	if got := e.settled["L1"].Declared["slots"]; got != 1 {
		t.Fatalf("stored declared mutated: %d", got)
	}
}
