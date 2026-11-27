package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/classify"
	"github.com/coherera/quelor/pkg/quelor"
	"github.com/coherera/quelor/pkg/resource"
	"github.com/coherera/quelor/pkg/trace"
)

func TestAcquireTracesDecisionAndHash(t *testing.T) {
	clk := baseClock()
	e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 2, resource.UnknownHoldCursor),
	}, nil, nil)
	env := work("u", "h1", map[string]int64{"slots": 1}, "")
	lease := acquireLease(t, e, env, "")
	rec, ok := findTrace(tw.Records(), trace.OpAcquire)
	if !ok {
		t.Fatal("missing acquire trace")
	}
	if rec.DecisionRef != lease.DecisionRef || rec.DecisionRef != "dec-acquire" {
		t.Fatalf("decision ref %q lease %q", rec.DecisionRef, lease.DecisionRef)
	}
	if rec.EnvelopeHash != trace.HashEnvelope(env) || rec.LeaseID != lease.ID {
		t.Fatalf("trace %+v", rec)
	}
	if rec.Verdict == nil || rec.Verdict.Kind != quelor.Admit {
		t.Fatalf("verdict %+v", rec.Verdict)
	}
	if !lease.ExpiresAt.Equal(clk.t.Add(30 * time.Second)) {
		t.Fatalf("expiry %s", lease.ExpiresAt)
	}
}

func TestAcquireRefusalIsTypedAndTraced(t *testing.T) {
	clk := baseClock()
	e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 1, resource.UnknownHoldCursor),
	}, nil, nil)
	acquireLease(t, e, work("a", "h1", map[string]int64{"slots": 1}, ""), "")
	res, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{Envelope: work("b", "h2", map[string]int64{"slots": 1}, "")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Verdict.Kind != quelor.Defer || res.Lease != nil || res.Verdict.WakeToken == "" || res.Verdict.WakeAt == nil || res.DecisionRef == "" {
		t.Fatalf("result %+v", res)
	}
	if n := countTrace(tw.Records(), trace.OpAcquire); n != 2 {
		t.Fatalf("acquire traces %d", n)
	}
	for _, bad := range []quelor.Envelope{
		work("c", "h3", map[string]int64{"nope": 1}, ""),
		work("d", "h3", map[string]int64{"slots": 0}, ""),
	} {
		got, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{Envelope: bad})
		if err != nil || got.Verdict.Kind != quelor.Deny || got.Lease != nil {
			t.Fatalf("%s: %+v %v", bad.UnitID, got, err)
		}
	}
}

func TestAcquireReplayDoesNotDoubleReserve(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 2, resource.UnknownHoldCursor),
	}, nil, nil)
	env := work("u", "h1", map[string]int64{"slots": 2}, "")
	first := acquireLease(t, e, env, "")
	again := acquireLease(t, e, env, "")
	if first.ID != again.ID {
		t.Fatalf("replay minted a new lease %s vs %s", first.ID, again.ID)
	}
	other := work("u", "h2", map[string]int64{"slots": 2}, "")
	if k := acquireVerdict(t, e, other); k != quelor.Deny {
		t.Fatalf("other holder on a held unit: %s", k)
	}
}

func TestAcquireCoalescing(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 8, resource.UnknownHoldCursor),
	}, nil, nil)
	mk := func(unit string, gen int64, pol quelor.CoalescePolicy) quelor.Envelope {
		env := work(unit, "h1", map[string]int64{"slots": 1}, "")
		env.CoalesceKey, env.Generation, env.CoalescePolicy = "pr-7", gen, pol
		return env
	}
	old := acquireLease(t, e, mk("run-1", 1, ""), "")
	if k := acquireVerdict(t, e, mk("run-0", 0, "")); k != quelor.Shed {
		t.Fatalf("older generation: %s", k)
	}
	newer := acquireLease(t, e, mk("run-2", 2, ""), "")
	e.mu.Lock()
	superseded := e.leases[old.ID].superseded
	pointsAtNew := e.byKey["pr-7"] == newer.ID
	e.mu.Unlock()
	if !superseded || !pointsAtNew {
		t.Fatalf("superseded=%v key->new=%v", superseded, pointsAtNew)
	}
	if k := acquireVerdict(t, e, mk("run-3", 3, quelor.CoalesceDropNew)); k != quelor.Shed {
		t.Fatalf("DROP_NEW: %s", k)
	}
}

func TestAcquireTicketsHoldRoomForBetterRank(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 4, resource.UnknownHoldCursor),
	}, nil, nil)
	big := acquireLease(t, e, work("big", "h1", map[string]int64{"slots": 3}, ""), "")
	// A release-critical unit needing 3 loses, and holds a ticket.
	if k := acquireVerdict(t, e, work("crit", "h2", map[string]int64{"slots": 3}, quelor.PriorityReleaseCritical)); k != quelor.Defer {
		t.Fatalf("crit: %s", k)
	}
	// A maintenance unit fits the 1 free slot but would eat the room crit needs.
	if k := acquireVerdict(t, e, work("maint", "h3", map[string]int64{"slots": 1}, quelor.PriorityMaintenance)); k != quelor.Defer {
		t.Fatalf("maintenance barged past a ticket: %s", k)
	}
	if _, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: big.ID, HolderID: "h1"}); err != nil {
		t.Fatal(err)
	}
	if k := acquireVerdict(t, e, work("crit", "h2", map[string]int64{"slots": 3}, quelor.PriorityReleaseCritical)); k != quelor.Admit {
		t.Fatalf("crit after release: %s", k)
	}
}

// PROVES-RED: a run cancelled because a newer generation superseded it is not
// a failure. Before the fix SettleLease classified the cancellation as flaky
// and traced Failed=true, which a retry budget would count.
func TestSettleSupersededRunIsNotAFailure(t *testing.T) {
	cancelled := &quelor.ExecutionOutcome{ExitCode: 1, Log: "Error: The operation was canceled."}
	setup := func(t *testing.T) (*Engine, *traceSink) {
		clk := baseClock()
		e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
			manifest("slots", resource.ModelLease, 8, resource.UnknownHoldCursor),
		}, nil, nil)
		return e, &traceSink{tw}
	}
	mk := func(unit string, gen int64) quelor.Envelope {
		env := work(unit, "h1", map[string]int64{"slots": 1}, "")
		env.CoalesceKey, env.Generation = "pr-9", gen
		return env
	}
	settle := func(t *testing.T, e *Engine, l quelor.Lease) quelor.SettleResult {
		t.Helper()
		res, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: l.ID, HolderID: "h1", Outcome: cancelled})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}

	t.Run("superseded run is not counted", func(t *testing.T) {
		e, sink := setup(t)
		old := acquireLease(t, e, mk("run-1", 1), "")
		acquireLease(t, e, mk("run-2", 2), "")
		res := settle(t, e, old)
		if res.Class != "" {
			t.Fatalf("superseded cancellation classed %q", res.Class)
		}
		rec, ok := sink.settleFor(old.ID)
		if !ok || rec.Outcome == nil || rec.Outcome.Failed || rec.Outcome.Class != "" {
			t.Fatalf("trace outcome %+v", rec.Outcome)
		}
		if len(rec.RuleIDs) != 1 || rec.RuleIDs[0] != "superseded" {
			t.Fatalf("rule ids %v", rec.RuleIDs)
		}
	})

	t.Run("same cancellation on a live run still counts", func(t *testing.T) {
		e, sink := setup(t)
		live := acquireLease(t, e, mk("run-1", 1), "")
		res := settle(t, e, live)
		if res.Class != quelor.Flaky {
			t.Fatalf("live cancellation classed %q", res.Class)
		}
		rec, _ := sink.settleFor(live.ID)
		if rec.Outcome == nil || !rec.Outcome.Failed {
			t.Fatalf("trace outcome %+v", rec.Outcome)
		}
	})
}

type traceSink struct{ w *trace.MemoryWriter }

func (s *traceSink) settleFor(leaseID string) (trace.Record, bool) {
	for _, r := range s.w.Records() {
		if r.Op == trace.OpSettle && r.LeaseID == leaseID {
			return r, true
		}
	}
	return trace.Record{}, false
}

// PROVES-RED: a superseded lease is superseded regardless of whether the settle
// carries an Outcome. Before the fix the branch ran only when Outcome != nil, so
// a settle without an outcome was indistinguishable from a success.
func TestSettleSupersededWithoutOutcome(t *testing.T) {
	clk := baseClock()
	e, tw, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 8, resource.UnknownHoldCursor),
	}, nil, nil)
	sink := &traceSink{tw}
	mk := func(unit string, gen int64) quelor.Envelope {
		env := work(unit, "h1", map[string]int64{"slots": 1}, "")
		env.CoalesceKey, env.Generation = "pr-9", gen
		return env
	}
	old := acquireLease(t, e, mk("run-1", 1), "")
	live := acquireLease(t, e, mk("run-2", 2), "")

	res, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: old.ID, HolderID: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.RuleID != classify.RuleSuperseded || res.Class != "" {
		t.Fatalf("superseded no-outcome settle: rule %q class %q", res.RuleID, res.Class)
	}
	rec, ok := sink.settleFor(old.ID)
	if !ok || len(rec.RuleIDs) != 1 || rec.RuleIDs[0] != classify.RuleSuperseded {
		t.Fatalf("trace rule ids %+v", rec.RuleIDs)
	}

	// A live lease settled without an outcome carries no rule id.
	res, err = e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: live.ID, HolderID: "h1"})
	if err != nil {
		t.Fatal(err)
	}
	if res.RuleID != "" {
		t.Fatalf("live no-outcome settle rule %q", res.RuleID)
	}
}

func TestSettleResultRuleID(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("slots", resource.ModelLease, 8, resource.UnknownHoldCursor),
	}, nil, nil)
	l := acquireLease(t, e, work("u", "h1", map[string]int64{"slots": 1}, ""), "")
	res, err := e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: l.ID, HolderID: "h1", Outcome: &quelor.ExecutionOutcome{}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RuleID != classify.RuleSuccess {
		t.Fatalf("success rule id %q", res.RuleID)
	}
	l = acquireLease(t, e, work("u2", "h1", map[string]int64{"slots": 1}, ""), "")
	res, err = e.SettleLease(context.Background(), quelor.SettleRequest{LeaseID: l.ID, HolderID: "h1", Outcome: &quelor.ExecutionOutcome{ExitCode: 1, HTTPStatus: 429}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RuleID != "gha-rate-limit" || res.Class != quelor.ResourceExhausted {
		t.Fatalf("429 settle %q %q", res.RuleID, res.Class)
	}
}

func TestAuthenticateRefusesUnlistedHolderWhenHolderTokensSet(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("cpu", resource.ModelLease, 20, resource.UnknownHoldCursor),
	}, nil, func(cfg *Config) {
		cfg.SharedToken = "shared"
		cfg.HolderTokens = map[string]string{"alice": "aaa"}
	})
	cases := []struct {
		holder, token string
		want          bool
	}{
		{"alice", "aaa", true},
		{"alice", "shared", false},
		{"mallory", "shared", false},
		{"", "shared", false},
	}
	for _, c := range cases {
		if got := e.Authenticate(c.holder, c.token); got != c.want {
			t.Errorf("Authenticate(%q,%q)=%v want %v", c.holder, c.token, got, c.want)
		}
	}
}

func TestAuthenticateSharedTokenOnly(t *testing.T) {
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, []resource.Manifest{
		manifest("cpu", resource.ModelLease, 20, resource.UnknownHoldCursor),
	}, nil, func(cfg *Config) { cfg.SharedToken = "shared" })
	if !e.Authenticate("anyone", "shared") || e.Authenticate("anyone", "nope") || e.Authenticate("anyone", "") {
		t.Fatal("shared token handling changed")
	}
}

func TestModelCreditHardBudgetDeniesUnknownReading(t *testing.T) {
	reg := resource.NewRegistry(nil)
	manifest := resource.Manifest{
		ID:            "model_spend_lifetime",
		CapacityModel: resource.ModelHardBudget,
		Capacity:      50000,
		UnknownPolicy: resource.UnknownDeny,
	}
	reader := resource.FuncReader(func(context.Context) (quelor.Reading, error) {
		return quelor.Reading{Known: false}, errors.New("provider reading unavailable")
	})
	if err := reg.Register(manifest, reader); err != nil {
		t.Fatalf("register hard budget: %v", err)
	}

	e, err := New(Config{Registry: reg, Decider: admitAll()})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	defer func() {
		if err := e.Close(); err != nil {
			t.Errorf("close engine: %v", err)
		}
	}()

	result, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{
		Envelope: work("model-credit-unknown-reading", "ci-smoke", map[string]int64{
			"model_spend_lifetime": 1000,
		}, quelor.PriorityChange),
	})
	if err != nil {
		t.Fatalf("acquire with unknown hard-budget reading: %v", err)
	}
	if result.Verdict.Kind != quelor.Deny || result.Verdict.Reason != "unknown-reading:model_spend_lifetime" {
		t.Fatalf("unknown hard-budget reading was not denied: %+v", result.Verdict)
	}
	if result.Lease != nil {
		t.Fatalf("denied acquire returned a lease: %+v", result.Lease)
	}
}
