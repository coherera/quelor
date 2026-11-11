package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/quelor"
	"github.com/coherera/quelor/pkg/resource"
	"github.com/coherera/quelor/pkg/trace"
)

func TestAdvanceDueBoundaries(t *testing.T) {
	base := time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC)
	kinds := []string{"lease", "ticket", "waiter"}
	cases := []struct {
		name string
		at   time.Duration
		want bool
	}{
		{name: "past", at: -time.Millisecond, want: true},
		{name: "exact", at: 0, want: true},
		{name: "future", at: time.Millisecond, want: false},
	}
	for _, kind := range kinds {
		for _, tc := range cases {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				e, _, mt := newAdvEngine(t, base, time.Hour, nil, slotsManifest(4))
				deadline := base.Add(tc.at)
				armDeadline(e, kind, deadline)
				evs := e.Advance(base)

				got := filterKind(evs, kindEvent[kind])
				if tc.want && len(got) != 1 {
					t.Fatalf("events %+v", evs)
				}
				if !tc.want && len(got) != 0 {
					t.Fatalf("unexpected %+v", got)
				}
				if tc.want {
					ev := got[0]
					if ev.Token != kindToken[kind] || !ev.At.Equal(base) || ev.Resource != kindResource[kind] {
						t.Fatalf("event %+v", ev)
					}
					again := filterKind(e.Advance(base), kindEvent[kind])
					if len(again) != 0 {
						t.Fatalf("not idempotent: %+v", again)
					}
				}
				assertArmed(t, e, kind, !tc.want)

				expires := opCount(mt.snapshot(), trace.OpExpire)
				wantExpires := 0
				if tc.want && kind == "lease" {
					wantExpires = 1
				}
				if expires != wantExpires {
					t.Fatalf("expire traces %d", expires)
				}
				if wantExpires == 1 {
					rec := expireRecs(mt.snapshot())[0]
					if rec.DecisionRef != "dec-L" || rec.LeaseID != "L" {
						t.Fatalf("trace %+v", rec)
					}
					if len(rec.Stamps) != 1 || rec.Stamps[0] != "lease-expired" {
						t.Fatalf("stamps %#v", rec.Stamps)
					}
				}
			})
		}
	}
}

func TestAdvanceOrdersTimerEvents(t *testing.T) {
	base := time.Date(2026, 10, 31, 1, 0, 0, 0, time.UTC)
	e, _, mt := newAdvEngine(t, base, time.Hour, nil, slotsManifest(4))
	locked(e, func() {
		e.res["slots"].take(1, base)
		e.leases["L1"] = leased("L1", "u1", "dec-1", base, map[string]int64{"slots": 1}, e.res["slots"].windowID())
		e.byUnit["u1"] = "L1"
		e.holders["h"] = &holderUse{leases: 1, units: map[string]int64{"slots": 1}}

		e.res["slots"].take(1, base)
		later := base.Add(time.Hour)
		e.leases["L2"] = leased("L2", "u2", "dec-2", later, map[string]int64{"slots": 1}, e.res["slots"].windowID())
		e.byUnit["u2"] = "L2"
		h2 := &holderUse{leases: 1, units: map[string]int64{"slots": 1}}
		e.holders["h2"] = h2
		e.leases["L2"].lease.HolderID = "h2"
		e.leases["L2"].env.HolderID = "h2"

		e.tickets["m"] = []*ticket{{unitID: "U9", need: 1, rank: 2, since: base, expires: base}}
		e.tickets["a"] = []*ticket{{unitID: "U0", need: 1, rank: 2, since: base, expires: base.Add(time.Minute)}}
		e.waiters["W9"] = &waiter{token: "W9", resource: "slots", need: 1, wakeAt: base}
		e.waiters["later"] = &waiter{token: "later", resource: "slots", need: 1, wakeAt: base.Add(time.Minute)}
	})

	evs := e.Advance(base)
	var seq []string
	for _, ev := range evs {
		switch ev.Kind {
		case "lease-expired", "ticket-expired", "wake":
			seq = append(seq, ev.Kind+"/"+ev.Token)
		}
	}
	want := []string{"lease-expired/L1", "ticket-expired/U9", "wake/W9"}
	if len(seq) != len(want) {
		t.Fatalf("seq %v events %+v", seq, evs)
	}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("seq %v", seq)
		}
	}
	locked(e, func() {
		if _, ok := e.leases["L1"]; ok {
			t.Fatal("L1 still leased")
		}
		if _, ok := e.leases["L2"]; !ok {
			t.Fatal("L2 dropped")
		}
		if _, ok := e.tickets["m"]; ok {
			t.Fatal("due ticket kept")
		}
		if len(e.tickets["a"]) != 1 {
			t.Fatal("future ticket dropped")
		}
		if _, ok := e.waiters["W9"]; ok {
			t.Fatal("woken waiter kept")
		}
		if _, ok := e.waiters["later"]; !ok {
			t.Fatal("future waiter dropped")
		}
	})
	if e.Stats().Expired != 1 {
		t.Fatalf("expired %d", e.Stats().Expired)
	}
	recs := expireRecs(mt.snapshot())
	if len(recs) != 1 || recs[0].LeaseID != "L1" || recs[0].DecisionRef != "dec-1" {
		t.Fatalf("traces %+v", recs)
	}
	again := filterKinds(e.Advance(base), "lease-expired", "ticket-expired", "wake")
	if len(again) != 0 {
		t.Fatalf("second pass %+v", again)
	}
}

func TestAdvanceIdempotent(t *testing.T) {
	base := time.Date(2026, 10, 31, 2, 0, 0, 0, time.UTC)
	e, _, mt := newAdvEngine(t, base, time.Hour, nil, slotsManifest(2))
	if evs := e.Advance(base); countKind(evs, "ticket-expired")+countKind(evs, "lease-expired")+countKind(evs, "wake") != 0 {
		t.Fatalf("spurious %+v", evs)
	}
	locked(e, func() {
		e.tickets["slots"] = []*ticket{{unitID: "u", need: 1, rank: 2, since: base, expires: base}}
	})
	evs := e.Advance(base)
	if countKind(evs, "ticket-expired") != 1 {
		t.Fatalf("events %+v", evs)
	}
	again := e.Advance(base)
	if len(again) != 0 {
		t.Fatalf("idempotent, got %+v", again)
	}
	if opCount(mt.snapshot(), trace.OpExpire) != 0 {
		t.Fatal("ticket expiry wrote a trace")
	}
	e.kickTimers()
}

func TestAdvanceSubscribe(t *testing.T) {
	base := time.Date(2026, 10, 31, 3, 0, 0, 0, time.UTC)
	e, _, _ := newAdvEngine(t, base, time.Hour, nil, slotsManifest(2))
	locked(e, func() {
		e.tickets["b"] = []*ticket{{unitID: "ub", need: 1, rank: 1, since: base, expires: base}}
		e.tickets["a"] = []*ticket{{unitID: "ua", need: 2, rank: 1, since: base, expires: base}}
	})
	sub := e.Subscribe()
	evs := e.Advance(base)
	if len(evs) == 0 {
		t.Fatal("expected events")
	}
	got := make([]Event, 0, len(evs))
	for i := 0; i < len(evs); i++ {
		select {
		case ev := <-sub:
			got = append(got, ev)
		default:
			t.Fatalf("subscriber missing event %d of %d", i, len(evs))
		}
	}
	select {
	case ev := <-sub:
		t.Fatalf("extra subscriber event %+v", ev)
	default:
	}
	if len(got) != len(evs) {
		t.Fatalf("got %d want %d", len(got), len(evs))
	}
	for i := range evs {
		if got[i].Kind != evs[i].Kind || got[i].Token != evs[i].Token || got[i].Resource != evs[i].Resource || !got[i].At.Equal(evs[i].At) {
			t.Fatalf("sub[%d] %+v want %+v", i, got[i], evs[i])
		}
	}
	quiet := e.Subscribe()
	if extra := e.Advance(base); len(extra) != 0 {
		t.Fatalf("idempotent %+v", extra)
	}
	select {
	case ev := <-quiet:
		t.Fatalf("unexpected %+v", ev)
	default:
	}
}

func TestNextDue(t *testing.T) {
	base := time.Date(2026, 10, 31, 4, 0, 0, 0, time.UTC)
	cases := []struct {
		name                string
		lease, ticket, wake bool
		leaseAt, ticketAt   time.Duration
		wakeAt              time.Duration
	}{
		{name: "none"},
		{name: "lease only", lease: true, leaseAt: 5 * time.Second},
		{name: "ticket earliest", lease: true, ticket: true, wake: true, leaseAt: 5 * time.Second, ticketAt: time.Second, wakeAt: 3 * time.Second},
		{name: "wake earliest", lease: true, ticket: true, wake: true, leaseAt: 4 * time.Second, ticketAt: 3 * time.Second, wakeAt: 2 * time.Second},
		{name: "lease earliest", lease: true, ticket: true, wake: true, leaseAt: time.Second, ticketAt: 2 * time.Second, wakeAt: 3 * time.Second},
		{name: "tie", lease: true, ticket: true, wake: true, leaseAt: 3 * time.Second, ticketAt: 3 * time.Second, wakeAt: 3 * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, _, _ := newAdvEngine(t, base, time.Hour, nil, slotsManifest(2))
			var want time.Time
			wantOK := false
			consider := func(at time.Time) {
				if !wantOK || at.Before(want) {
					want = at
					wantOK = true
				}
			}
			locked(e, func() {
				for _, st := range e.res {
					if at, ok := st.nextTimer(); ok {
						consider(at)
					}
				}
				if tc.lease {
					at := base.Add(tc.leaseAt)
					e.leases["L"] = &leaseRec{lease: quelor.Lease{ID: "L", ExpiresAt: at}}
					consider(at)
				}
				if tc.ticket {
					at := base.Add(tc.ticketAt)
					e.tickets["slots"] = []*ticket{{unitID: "u", expires: at, since: base}}
					consider(at)
				}
				if tc.wake {
					at := base.Add(tc.wakeAt)
					e.waiters["w"] = &waiter{token: "w", resource: "slots", wakeAt: at}
					consider(at)
				}
			})
			due, ok := e.nextDue()
			if ok != wantOK {
				t.Fatalf("ok %v want %v (due %s)", ok, wantOK, due)
			}
			if ok && !due.Equal(want) {
				t.Fatalf("due %s want %s", due, want)
			}
		})
	}
}

func TestLeaseExpirySettleDuplicate(t *testing.T) {
	base := time.Date(2026, 10, 31, 5, 0, 0, 0, time.UTC)
	ttl := time.Minute
	e, clk, mt := newAdvEngine(t, base, ttl, nil, slotsManifest(1))
	fill := mustAcquire(t, e, envOf("job-1", "slots", 1))
	if fill.Verdict.Kind != quelor.Admit || fill.Lease == nil {
		t.Fatalf("admit: %+v err-lease", fill.Verdict)
	}
	lease := fill.Lease
	earlyAt := lease.ExpiresAt.Add(-time.Nanosecond)
	clk.Set(earlyAt)
	if n := countKind(e.Advance(earlyAt), "lease-expired"); n != 0 {
		t.Fatalf("early expiries %d", n)
	}
	if e.Stats().ActiveLeases != 1 || e.Stats().Expired != 0 {
		t.Fatalf("stats %+v", e.Stats())
	}

	when := lease.ExpiresAt
	clk.Set(when)
	evs := e.Advance(when)
	gotEv, ok := findEvent(evs, "lease-expired", lease.ID)
	if !ok || gotEv.Resource != "" || !gotEv.At.Equal(when) {
		t.Fatalf("events %+v", evs)
	}
	if e.Stats().Expired != 1 || e.Stats().ActiveLeases != 0 {
		t.Fatalf("stats %+v", e.Stats())
	}
	if n := countKind(e.Advance(when), "lease-expired"); n != 0 {
		t.Fatal("expiry repeated")
	}

	recs := expireRecs(mt.snapshot())
	if len(recs) != 1 {
		t.Fatalf("expire traces %+v", recs)
	}
	rec := recs[0]
	if rec.Op != trace.OpExpire || rec.DecisionRef != lease.DecisionRef || rec.LeaseID != lease.ID {
		t.Fatalf("trace ids %+v", rec)
	}
	if rec.HolderID != lease.HolderID || rec.TenantID != lease.TenantID || rec.Cell != "cell-test" {
		t.Fatalf("trace identity %+v", rec)
	}
	if len(rec.Stamps) != 1 || rec.Stamps[0] != "lease-expired" {
		t.Fatalf("stamps %#v", rec.Stamps)
	}
	if rec.Envelope == nil || rec.Envelope.UnitID != "job-1" || rec.EnvelopeHash == "" {
		t.Fatalf("envelope %+v hash %q", rec.Envelope, rec.EnvelopeHash)
	}
	if rec.Declared["slots"] != 1 || len(rec.Declared) == 0 || len(rec.Declared) != len(rec.Consumed) {
		t.Fatalf("amounts declared %#v consumed %#v", rec.Declared, rec.Consumed)
	}
	for k, v := range rec.Declared {
		if rec.Consumed[k] != v {
			t.Fatalf("consumed %#v declared %#v", rec.Consumed, rec.Declared)
		}
	}
	ts, err := time.Parse(time.RFC3339Nano, rec.TS)
	if err != nil {
		t.Fatal(err)
	}
	if !ts.Equal(when.UTC()) {
		t.Fatalf("ts %s want %s", ts, when.UTC())
	}

	ctx := context.Background()
	req := quelor.SettleRequest{LeaseID: lease.ID, HolderID: lease.HolderID}
	first, err := e.SettleLease(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Duplicate {
		t.Fatalf("settle %+v", first)
	}
	second, err := e.SettleLease(ctx, req)
	if err != nil || !second.Duplicate {
		t.Fatalf("second %+v err %v", second, err)
	}
	if e.Stats().DuplicateSettles < 1 {
		t.Fatalf("dup stat %d", e.Stats().DuplicateSettles)
	}

	again := mustAcquire(t, e, envOf("job-2", "slots", 1))
	if again.Verdict.Kind != quelor.Admit || again.Lease == nil {
		t.Fatalf("capacity not reclaimed: %+v", again.Verdict)
	}
}

func TestExpireTraceFailureStillReclaims(t *testing.T) {
	base := time.Date(2026, 10, 31, 6, 0, 0, 0, time.UTC)
	mt := &memTrace{err: errors.New("trace down")}
	e, _, _ := newAdvEngine(t, base, time.Hour, mt, slotsManifest(1))
	locked(e, func() {
		if !e.res["slots"].take(1, base) {
			t.Fatal("take")
		}
		e.leases["L"] = leased("L", "u", "dec-err", base, map[string]int64{"slots": 1}, e.res["slots"].windowID())
		e.byUnit["u"] = "L"
		e.holders["h"] = &holderUse{leases: 1, units: map[string]int64{"slots": 1}}
	})
	evs := e.Advance(base)
	if _, ok := findEvent(evs, "lease-expired", "L"); !ok {
		t.Fatalf("events %+v", evs)
	}
	if e.Stats().Expired != 1 || e.Stats().TraceErrors != 1 || e.Stats().ActiveLeases != 0 {
		t.Fatalf("stats %+v", e.Stats())
	}
	if got := availOf(e, "slots", base); got != 1 {
		t.Fatalf("avail %d", got)
	}
	if len(mt.snapshot()) != 0 {
		t.Fatal("failed trace was stored")
	}
}

func TestWindowResetWakesDeferred(t *testing.T) {
	base := time.Date(2026, 10, 31, 7, 0, 0, 0, time.UTC)
	window := time.Second
	e, clk, mt := newAdvEngine(t, base, time.Hour, nil, windowManifest(window))
	fill := mustAcquire(t, e, envOf("fill", "win", 1))
	if fill.Verdict.Kind != quelor.Admit {
		t.Fatalf("fill: %+v", fill.Verdict)
	}
	deferred := mustAcquire(t, e, envOf("wait", "win", 1))
	if deferred.Verdict.Kind != quelor.Defer || deferred.Verdict.WakeToken == "" {
		t.Fatalf("defer: %+v", deferred.Verdict)
	}
	before := opCount(mt.snapshot(), trace.OpExpire)
	target := base.Add(window)
	if deferred.Verdict.WakeAt != nil && deferred.Verdict.WakeAt.After(target) {
		target = *deferred.Verdict.WakeAt
	}
	clk.Set(target)
	evs := e.Advance(target)
	reset, ok := findEvent(evs, "window-reset", "")
	if !ok {
		t.Fatalf("events at %s wake %v: %+v", target, deferred.Verdict.WakeAt, evs)
	}
	if reset.Resource != "" && reset.Resource != "win" {
		t.Fatalf("window-reset resource %q", reset.Resource)
	}
	wake, ok := findEvent(evs, "wake", deferred.Verdict.WakeToken)
	if !ok || !wake.At.Equal(target) {
		t.Fatalf("wake %+v events %+v", wake, evs)
	}
	if wake.Resource != "" && wake.Resource != "win" {
		t.Fatalf("wake resource %q", wake.Resource)
	}
	if opCount(mt.snapshot(), trace.OpExpire) != before {
		t.Fatal("window advance traced expiry")
	}
	if n := countKind(e.Advance(target), "wake") + countKind(e.Advance(target), "window-reset"); n != 0 {
		// second Advance is the idempotent one; the expression above calls it twice.
		t.Fatal("window events repeated")
	}
	// The double call above already consumed the idempotent pass. A third call
	// must stay quiet too.
	if extra := e.Advance(target); countKind(extra, "wake")+countKind(extra, "window-reset") != 0 {
		t.Fatalf("third pass %+v", extra)
	}
	again := mustAcquire(t, e, envOf("wait", "win", 1))
	if again.Verdict.Kind != quelor.Admit {
		t.Fatalf("re-acquire: %+v", again.Verdict)
	}
	if e.Stats().AcquireAdmit < 2 || e.Stats().AcquireDefer < 1 {
		t.Fatalf("stats %+v", e.Stats())
	}
}

func TestCircuitOpenToHalfOpenProbe(t *testing.T) {
	base := time.Date(2026, 10, 31, 8, 0, 0, 0, time.UTC)
	e, clk, _ := newAdvEngine(t, base, time.Hour, nil, circuitManifest(time.Second))
	locked(e, func() {
		e.res["cb"].tripOpen(base)
	})
	blocked := mustAcquire(t, e, envOf("blocked", "cb", 1))
	if blocked.Verdict.Kind == quelor.Admit {
		t.Fatalf("admitted while open: %+v", blocked.Verdict)
	}
	var due time.Time
	var ok bool
	locked(e, func() {
		due, ok = e.res["cb"].nextTimer()
	})
	if !ok {
		t.Fatal("open circuit has no half-open timer")
	}
	if due.After(base) {
		early := e.Advance(due.Add(-time.Millisecond))
		if countKind(early, "half-open") != 0 {
			t.Fatalf("early %+v", early)
		}
	}
	clk.Set(due)
	evs := e.Advance(due)
	if countKind(evs, "half-open") != 1 {
		t.Fatalf("events %+v", evs)
	}
	half, _ := findEvent(evs, "half-open", "")
	if half.Resource != "" && half.Resource != "cb" {
		t.Fatalf("half-open resource %q", half.Resource)
	}
	if n := countKind(e.Advance(due), "half-open"); n != 0 {
		t.Fatal("half-open repeated")
	}
	probe := mustAcquire(t, e, envOf("probe-1", "cb", 1))
	if probe.Verdict.Kind != quelor.Admit || probe.Lease == nil {
		t.Fatalf("probe: %+v", probe.Verdict)
	}
	second := mustAcquire(t, e, envOf("probe-2", "cb", 1))
	if second.Verdict.Kind == quelor.Admit {
		t.Fatal("second probe admitted")
	}
	third := mustAcquire(t, e, envOf("probe-3", "cb", 1))
	if third.Verdict.Kind == quelor.Admit {
		t.Fatal("third probe admitted")
	}
}

// An expired probe lease must not close the circuit. The probe never settled,
// so the breaker re-opens until a later settle or observation proves health.
func TestCircuitProbeLeaseExpireReopens(t *testing.T) {
	base := time.Date(2026, 10, 31, 9, 0, 0, 0, time.UTC)
	leaseTTL := 5 * time.Second
	openFor := time.Second
	e, clk, _ := newAdvEngine(t, base, leaseTTL, nil, circuitManifest(openFor))
	locked(e, func() {
		e.res["cb"].tripOpen(base)
	})

	var due time.Time
	var ok bool
	locked(e, func() {
		due, ok = e.res["cb"].nextTimer()
	})
	if !ok {
		t.Fatal("open circuit has no half-open timer")
	}
	clk.Set(due)
	if countKind(e.Advance(due), "half-open") != 1 {
		t.Fatal("expected half-open")
	}

	probe := mustAcquire(t, e, envOf("probe-expire", "cb", 1))
	if probe.Verdict.Kind != quelor.Admit || probe.Lease == nil {
		t.Fatalf("probe: %+v", probe.Verdict)
	}
	if probe.Lease.ExpiresAt.IsZero() {
		t.Fatal("probe lease missing expiry")
	}

	expireAt := probe.Lease.ExpiresAt
	sub := e.Subscribe()
	clk.Set(expireAt)
	evs := e.Advance(expireAt)
	if countKind(evs, "lease-expired") != 1 {
		t.Fatalf("expected lease-expired, got %+v", evs)
	}

	// Probe events from releaseLocked are published to subscribers; the
	// Advance return only carries the lease-expired timer event.
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
		t.Fatal("expired probe closed the circuit")
	}
	if !sawOpen {
		t.Fatal("expired probe did not re-open the circuit")
	}

	var circ circuitMode
	var inFlight bool
	locked(e, func() {
		s := e.res["cb"]
		circ = s.circ
		inFlight = s.probeInFlight
	})
	if circ != modeOpen {
		t.Fatalf("circuit mode = %v, want open", circ)
	}
	if inFlight {
		t.Fatal("probe slot still held after expiry")
	}

	blocked := mustAcquire(t, e, envOf("after-expire", "cb", 1))
	if blocked.Verdict.Kind == quelor.Admit {
		t.Fatalf("admitted while re-opened after probe expiry: %+v", blocked.Verdict)
	}

	// After open_for, a fresh probe slot must be available again.
	reopenDue := expireAt.Add(openFor)
	clk.Set(reopenDue)
	if countKind(e.Advance(reopenDue), "half-open") != 1 {
		t.Fatal("expected half-open after re-open cooldown")
	}
	retry := mustAcquire(t, e, envOf("probe-retry", "cb", 1))
	if retry.Verdict.Kind != quelor.Admit || retry.Lease == nil {
		t.Fatalf("retry probe: %+v", retry.Verdict)
	}
}

func TestRun(t *testing.T) {
	t.Run("cancel", func(t *testing.T) {
		e := newRealEngine(t, time.Hour, slotsManifest(4))
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- e.Run(ctx) }()
		time.Sleep(10 * time.Millisecond)
		e.kickTimers()
		waitRun(t, cancel, errCh)
	})
	t.Run("due timer", func(t *testing.T) {
		e := newRealEngine(t, 40*time.Millisecond, slotsManifest(4))
		sub := e.Subscribe()
		got := mustAcquire(t, e, envOf("run-job", "slots", 1))
		if got.Verdict.Kind != quelor.Admit || got.Lease == nil {
			t.Fatalf("admit %+v", got.Verdict)
		}
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- e.Run(ctx) }()

		deadline := time.Now().Add(2 * time.Second)
		for e.Stats().Expired < 1 || e.Stats().ActiveLeases != 0 {
			if time.Now().After(deadline) {
				t.Fatalf("timer not processed: %+v", e.Stats())
			}
			time.Sleep(10 * time.Millisecond)
		}
		saw := false
		for {
			select {
			case ev := <-sub:
				if ev.Kind == "lease-expired" && ev.Token == got.Lease.ID && ev.Resource == "" {
					saw = true
				}
			default:
				goto drained
			}
		}
	drained:
		if !saw {
			t.Fatal("subscriber missed lease-expired")
		}
		waitRun(t, cancel, errCh)
	})
}

func waitRun(t *testing.T, cancel context.CancelFunc, errCh <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit")
	}
}

var kindEvent = map[string]string{
	"lease":  "lease-expired",
	"ticket": "ticket-expired",
	"waiter": "wake",
}

var kindToken = map[string]string{
	"lease":  "L",
	"ticket": "u",
	"waiter": "tok",
}

var kindResource = map[string]string{
	"lease":  "",
	"ticket": "slots",
	"waiter": "slots",
}

func armDeadline(e *Engine, kind string, at time.Time) {
	locked(e, func() {
		switch kind {
		case "lease":
			st := e.res["slots"]
			st.take(1, at.Add(-time.Minute))
			e.leases["L"] = leased("L", "u", "dec-L", at, map[string]int64{"slots": 1}, st.windowID())
			e.byUnit["u"] = "L"
			e.holders["h"] = &holderUse{leases: 1, units: map[string]int64{"slots": 1}}
		case "ticket":
			e.tickets["slots"] = []*ticket{{
				unitID: "u", need: 1, rank: 2, since: at.Add(-time.Minute), expires: at,
			}}
		case "waiter":
			e.waiters["tok"] = &waiter{token: "tok", resource: "slots", need: 1, wakeAt: at}
		}
	})
}

func assertArmed(t *testing.T, e *Engine, kind string, still bool) {
	t.Helper()
	locked(e, func() {
		var ok bool
		switch kind {
		case "lease":
			_, ok = e.leases["L"]
		case "ticket":
			ok = len(e.tickets["slots"]) == 1
		case "waiter":
			_, ok = e.waiters["tok"]
		}
		if ok != still {
			t.Fatalf("armed %v want %v", ok, still)
		}
	})
}

func leased(id, unit, dec string, exp time.Time, declared map[string]int64, win uint64) *leaseRec {
	return &leaseRec{
		lease: quelor.Lease{
			ID: id, UnitID: unit, TenantID: "ten", HolderID: "h",
			Declared: copyAmounts(declared), GrantedAt: exp.Add(-time.Minute),
			ExpiresAt: exp, DecisionRef: dec,
		},
		env: quelor.Envelope{
			UnitID: unit, TenantID: "ten", HolderID: "h",
			Demands: copyAmounts(declared), Priority: quelor.PriorityChange,
		},
		windows: map[string]uint64{"slots": win},
		probe:   map[string]bool{},
	}
}

func availOf(e *Engine, id string, now time.Time) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.res[id].avail(now)
}

func countKind(evs []Event, kind string) int {
	n := 0
	for _, ev := range evs {
		if ev.Kind == kind {
			n++
		}
	}
	return n
}

func filterKind(evs []Event, kind string) []Event {
	var out []Event
	for _, ev := range evs {
		if ev.Kind == kind {
			out = append(out, ev)
		}
	}
	return out
}

func filterKinds(evs []Event, kinds ...string) []Event {
	ok := map[string]bool{}
	for _, k := range kinds {
		ok[k] = true
	}
	var out []Event
	for _, ev := range evs {
		if ok[ev.Kind] {
			out = append(out, ev)
		}
	}
	return out
}

func findEvent(evs []Event, kind, token string) (Event, bool) {
	for _, ev := range evs {
		if ev.Kind == kind && (token == "" || ev.Token == token) {
			return ev, true
		}
	}
	return Event{}, false
}

func opCount(recs []trace.Record, op trace.Op) int {
	n := 0
	for _, rec := range recs {
		if rec.Op == op {
			n++
		}
	}
	return n
}

func expireRecs(recs []trace.Record) []trace.Record {
	var out []trace.Record
	for _, rec := range recs {
		if rec.Op == trace.OpExpire {
			out = append(out, rec)
		}
	}
	return out
}

func envOf(unit, res string, n int64) quelor.Envelope {
	return quelor.Envelope{
		UnitID:   unit,
		TenantID: "tenant-a",
		HolderID: "holder-a",
		Demands:  map[string]int64{res: n},
		Priority: quelor.PriorityChange,
	}
}

func mustAcquire(t *testing.T, e *Engine, env quelor.Envelope) quelor.AcquireResult {
	t.Helper()
	res, err := e.AcquireLease(context.Background(), quelor.AcquireRequest{Envelope: env})
	if err != nil {
		t.Fatalf("acquire %s: %v", env.UnitID, err)
	}
	return res
}

func locked(e *Engine, fn func()) {
	e.mu.Lock()
	defer e.mu.Unlock()
	fn()
}

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

type memTrace struct {
	mu   sync.Mutex
	recs []trace.Record
	err  error
}

func (m *memTrace) Write(r trace.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.recs = append(m.recs, r)
	return nil
}

func (m *memTrace) Close() error { return nil }

func (m *memTrace) snapshot() []trace.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]trace.Record, len(m.recs))
	copy(out, m.recs)
	return out
}

func slotsManifest(n int64) resource.Manifest {
	return resource.Manifest{
		ID:            "slots",
		CapacityModel: resource.ModelLease,
		Capacity:      n,
		UnknownPolicy: resource.UnknownDeny,
	}
}

func windowManifest(window time.Duration) resource.Manifest {
	return resource.Manifest{
		ID:            "win",
		CapacityModel: resource.ModelWindowBucket,
		Capacity:      1,
		Window:        resource.Duration(window),
		UnknownPolicy: resource.UnknownDeny,
	}
}

func circuitManifest(cooldown time.Duration) resource.Manifest {
	return resource.Manifest{
		ID:            "cb",
		CapacityModel: resource.ModelCircuit,
		Capacity:      1,
		OpenFor:       resource.Duration(cooldown),
		UnknownPolicy: resource.UnknownDeny,
	}
}

func mustReg(t *testing.T, ms ...resource.Manifest) *resource.Registry {
	t.Helper()
	r := resource.NewRegistry(nil)
	for _, m := range ms {
		m.WithDefaults()
		if err := r.Register(m, nil); err != nil {
			t.Fatalf("add %s: %v", m.ID, err)
		}
	}
	return r
}

func newAdvEngine(t *testing.T, now time.Time, ttl time.Duration, tr trace.Writer, ms ...resource.Manifest) (*Engine, *clock, *memTrace) {
	t.Helper()
	mt, _ := tr.(*memTrace)
	if tr == nil {
		mt = &memTrace{}
		tr = mt
	}
	if ttl <= 0 {
		ttl = time.Hour
	}
	clk := &clock{t: now}
	e, err := New(Config{
		Registry: mustReg(t, ms...),
		Clock:    clk.Now,
		Trace:    tr,
		LeaseTTL: ttl,
		Cell:     "cell-test",
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e, clk, mt
}

func newRealEngine(t *testing.T, ttl time.Duration, ms ...resource.Manifest) *Engine {
	t.Helper()
	e, err := New(Config{
		Registry: mustReg(t, ms...),
		LeaseTTL: ttl,
		Cell:     "cell-run",
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}
