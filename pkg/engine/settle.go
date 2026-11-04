package engine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/coherera/quelor/pkg/classify"
	"github.com/coherera/quelor/pkg/decider"
	"github.com/coherera/quelor/pkg/quelor"
	"github.com/coherera/quelor/pkg/resource"
	"github.com/coherera/quelor/pkg/trace"
)

const settleLogExcerptBytes = 512

// RenewLease extends an active lease and optionally reserves a further quantum.
// The decider is consulted without e.mu held. A refusal checkpoints the holder
// (Hibernate) and leaves the lease and its expiry untouched.
func (e *Engine) RenewLease(ctx context.Context, req quelor.RenewRequest) (quelor.RenewResult, error) {
	start := e.now()
	ctx, span := e.tracer.Start(ctx, "quelor.renew")
	defer span.End()
	span.SetAttributes(
		attribute.String("quelor.op", string(trace.OpRenew)),
		attribute.String("quelor.lease_id", req.LeaseID),
		attribute.String("quelor.holder_id", req.HolderID),
	)

	if !e.authenticate(req.HolderID, req.Token) {
		span.RecordError(ErrUnauthenticated)
		return quelor.RenewResult{}, ErrUnauthenticated
	}

	e.mu.Lock()
	rec, ok := e.leases[req.LeaseID]
	if !ok {
		e.mu.Unlock()
		span.RecordError(ErrUnknownLease)
		return quelor.RenewResult{}, ErrUnknownLease
	}
	if rec.lease.HolderID != req.HolderID {
		e.mu.Unlock()
		span.RecordError(ErrForbidden)
		return quelor.RenewResult{}, ErrForbidden
	}
	env := rec.env
	env.Demands = settleCloneI64(rec.env.Demands)
	leaseSnap := settleCloneLease(rec.lease)
	e.mu.Unlock()

	span.SetAttributes(
		attribute.String("quelor.unit_id", env.UnitID),
		attribute.String("quelor.tenant_id", env.TenantID),
	)

	readings, external := e.settleReadings(ctx, settleUnionKeys(leaseSnap.Declared, req.Delta), start)
	traceID := ""
	if sc := span.SpanContext(); sc.HasTraceID() {
		traceID = sc.TraceID().String()
	}
	resp, derr := e.consult(ctx, decider.Request{
		Op:         decider.OpRenew,
		Envelope:   env,
		Lease:      &leaseSnap,
		SpendDelta: settleCloneI64(req.Delta),
		Readings:   readings,
		Meta: decider.CallMeta{
			TraceID: traceID,
			Now:     start.UTC().Format(time.RFC3339Nano),
		},
	})

	e.mu.Lock()
	defer e.mu.Unlock()

	rec, ok = e.leases[req.LeaseID]
	if !ok {
		span.RecordError(ErrUnknownLease)
		return quelor.RenewResult{}, ErrUnknownLease
	}
	if rec.lease.HolderID != req.HolderID {
		span.RecordError(ErrForbidden)
		return quelor.RenewResult{}, ErrForbidden
	}

	now := e.now()
	lat := float64(now.Sub(start)) / float64(time.Millisecond)
	stamps := append([]string{}, resp.Verdict.Stamps...)

	var (
		refuse string
		wake   *time.Time
	)
	if unreachable(resp, derr) {
		// Spend never fails open: a hard budget in the delta refuses even for
		// an operator directive. Unknown-policy refusal yields to that priority.
		e.st.boundUnreachable.Add(1)
		if !settleHasStamp(stamps, "bound-unreachable") {
			stamps = append(stamps, "bound-unreachable")
		}
		if e.renewUnreachableMustRefuse(rec.env, req.Delta) {
			refuse = "bound-unreachable"
		}
	} else if resp.Enforced() && resp.Verdict.Kind != quelor.Admit {
		refuse = resp.Verdict.Reason
		if refuse == "" {
			refuse = resp.DecisionRef
		}
		if refuse == "" {
			refuse = string(resp.Verdict.Kind)
		}
		wake = resp.Verdict.WakeAt
	}

	if refuse != "" {
		out := e.finishRenewRefusal(rec, refuse, stamps, wake, resp, readings, traceID, lat, now)
		span.SetAttributes(attribute.String("quelor.verdict", string(out.Verdict.Kind)))
		return out, nil
	}
	if reason := e.renewReadingReason(rec.env, req.Delta, readings, external); reason != "" {
		out := e.finishRenewRefusal(rec, reason, stamps, nil, resp, readings, traceID, lat, now)
		span.SetAttributes(attribute.String("quelor.verdict", string(out.Verdict.Kind)))
		return out, nil
	}
	if reason := e.renewQuotaReason(rec, req.Delta); reason != "" {
		out := e.finishRenewRefusal(rec, reason, stamps, nil, resp, readings, traceID, lat, now)
		span.SetAttributes(attribute.String("quelor.verdict", string(out.Verdict.Kind)))
		return out, nil
	}
	if reason, reserved := e.renewReserve(rec, req.Delta, now); !reserved {
		out := e.finishRenewRefusal(rec, reason, stamps, nil, resp, readings, traceID, lat, now)
		span.SetAttributes(attribute.String("quelor.verdict", string(out.Verdict.Kind)))
		return out, nil
	}

	rec.lease.ExpiresAt = now.Add(e.cfg.LeaseTTL)
	rec.lease.Renewals++
	e.st.renews.Add(1)

	verdict := quelor.Verdict{Kind: quelor.Admit, Stamps: settleNonEmpty(stamps)}
	leaseOut := settleCloneLease(rec.lease)
	dref := resp.DecisionRef
	if dref == "" {
		dref = randHex(16)
	}
	e.writeRenewTrace(rec, leaseOut, verdict, dref, resp, readings, traceID, lat)
	span.SetAttributes(attribute.String("quelor.verdict", string(verdict.Kind)))
	return quelor.RenewResult{Verdict: verdict, Lease: &leaseOut, DecisionRef: dref}, nil
}

// SettleLease releases an active lease, classifies an optional outcome, and is
// idempotent: a second call returns the stored result with Duplicate set and
// does not touch capacity again.
func (e *Engine) SettleLease(ctx context.Context, req quelor.SettleRequest) (quelor.SettleResult, error) {
	start := e.now()
	ctx, span := e.tracer.Start(ctx, "quelor.settle")
	defer span.End()
	span.SetAttributes(
		attribute.String("quelor.op", string(trace.OpSettle)),
		attribute.String("quelor.lease_id", req.LeaseID),
		attribute.String("quelor.holder_id", req.HolderID),
	)
	_ = ctx

	if !e.authenticate(req.HolderID, req.Token) {
		span.RecordError(ErrUnauthenticated)
		return quelor.SettleResult{}, ErrUnauthenticated
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()

	if rec, ok := e.leases[req.LeaseID]; ok {
		if rec.lease.HolderID != req.HolderID {
			span.RecordError(ErrForbidden)
			return quelor.SettleResult{}, ErrForbidden
		}
		span.SetAttributes(
			attribute.String("quelor.unit_id", rec.env.UnitID),
			attribute.String("quelor.tenant_id", rec.lease.TenantID),
		)
		done := e.settleActiveLocked(rec, req, now)
		e.storeSettledLocked(done.result)
		e.settleNoteEvents(done.evs)
		e.emit(done.evs...)
		e.notifyCapacityLocked(now)
		e.st.settles.Add(1)
		e.writeSettleTrace(rec, done, req, start)
		return done.result, nil
	}

	// Holder is checked only while the lease is active. A stored settlement is
	// returned as-is (aside from Duplicate) with no capacity or trace effects.
	if stored, ok := e.settled[req.LeaseID]; ok {
		e.st.dupSettles.Add(1)
		stored.Duplicate = true
		stored.Declared = settleCloneI64(stored.Declared)
		stored.Consumed = settleCloneI64(stored.Consumed)
		stored.Delta = settleCloneI64(stored.Delta)
		return stored, nil
	}
	span.RecordError(ErrUnknownLease)
	return quelor.SettleResult{}, ErrUnknownLease
}

// Observe records one upstream status against a known resource, updates AIMD or
// circuit state, and wakes waiters that can now run.
func (e *Engine) Observe(ctx context.Context, o quelor.Observation) error {
	_ = ctx
	now := e.now()
	e.mu.Lock()
	defer e.mu.Unlock()

	s := e.res[o.Resource]
	if s == nil {
		return fmt.Errorf("engine: unknown resource %q", o.Resource)
	}
	evs := s.observe(o.HTTPStatus, o.RetryAfter, now)
	rd := quelor.Reading{
		Resource:   o.Resource,
		Available:  s.avail(now),
		Capacity:   s.effective(),
		Known:      true,
		ObservedAt: now,
	}
	e.settleNoteEvents(evs)
	e.emit(evs...)
	e.notifyCapacityLocked(now)
	e.writeTrace(trace.Record{
		Op:       trace.OpObserve,
		Readings: []quelor.Reading{rd},
		Outcome: &trace.Outcome{
			HTTPStatus: o.HTTPStatus,
			Failed:     o.HTTPStatus >= 400,
		},
	})
	return nil
}

// releaseLocked returns reserved capacity for rec. The caller holds e.mu.
// Nil consumed means the reservation was fully used. The lease is dropped from
// the active indexes. Settled history and traces are the caller's job.
// Probe closure for reason "settle" is owned by SettleLease; every other reason
// settles an in-flight circuit probe as success.
func (e *Engine) releaseLocked(rec *leaseRec, consumed map[string]int64, now time.Time, reason string) quelor.SettleResult {
	declared := settleCloneI64(rec.lease.Declared)
	if declared == nil {
		declared = map[string]int64{}
	}
	cons := make(map[string]int64, len(declared))
	delta := make(map[string]int64, len(declared))
	var probeEvs []Event

	for _, id := range settleSortedKeys(declared) {
		decl := declared[id]
		c := decl
		if consumed != nil {
			c = consumed[id]
			if c < 0 {
				c = 0
			}
			if c > decl {
				c = decl
			}
		}
		cons[id] = c
		delta[id] = decl - c

		s := e.res[id]
		if s == nil {
			continue
		}
		var win uint64
		if rec.windows != nil {
			win = rec.windows[id]
		}
		s.giveBack(decl, c, win, now)
		if reason != "settle" && rec.probe != nil && rec.probe[id] {
			probeEvs = append(probeEvs, s.probeSettled(true, now)...)
		}
	}
	if len(probeEvs) > 0 {
		e.settleNoteEvents(probeEvs)
		e.emit(probeEvs...)
	}

	if h := e.holders[rec.lease.HolderID]; h != nil {
		h.leases--
		if h.leases < 0 {
			h.leases = 0
		}
		for id, n := range declared {
			if h.units == nil {
				break
			}
			h.units[id] -= n
			if h.units[id] <= 0 {
				delete(h.units, id)
			}
		}
		if h.leases == 0 && len(h.units) == 0 {
			delete(e.holders, rec.lease.HolderID)
		}
	}

	delete(e.leases, rec.lease.ID)
	if id := rec.lease.UnitID; id != "" && e.byUnit[id] == rec.lease.ID {
		delete(e.byUnit, id)
	}
	if id := rec.env.UnitID; id != "" && id != rec.lease.UnitID && e.byUnit[id] == rec.lease.ID {
		delete(e.byUnit, id)
	}
	// A superseded lease must not clear a coalesce key that already points at
	// the replacement unit.
	if key := rec.env.CoalesceKey; key != "" && e.byKey[key] == rec.lease.ID {
		delete(e.byKey, key)
	}

	return quelor.SettleResult{
		LeaseID:  rec.lease.ID,
		Declared: declared,
		Consumed: cons,
		Delta:    delta,
	}
}

// notifyCapacityLocked wakes every waiter whose resource can now cover its need.
// The caller holds e.mu. Waiters that are still short, including those whose
// wakeAt is in the future, stay queued.
func (e *Engine) notifyCapacityLocked(now time.Time) {
	if len(e.waiters) == 0 {
		return
	}
	evs := make([]Event, 0)
	for token, w := range e.waiters {
		if w == nil {
			delete(e.waiters, token)
			continue
		}
		s := e.res[w.resource]
		if s == nil || s.avail(now) < w.need {
			continue
		}
		tok := w.token
		if tok == "" {
			tok = token
		}
		evs = append(evs, Event{
			Kind:     "wake",
			Resource: w.resource,
			At:       now,
			Token:    tok,
		})
		delete(e.waiters, token)
	}
	if len(evs) == 0 {
		return
	}
	sort.Slice(evs, func(i, j int) bool { return evs[i].Token < evs[j].Token })
	e.emit(evs...)
}

// storeSettledLocked remembers a settlement for idempotent replay and drops the
// oldest entries once the history exceeds 50000.
func (e *Engine) storeSettledLocked(res quelor.SettleResult) {
	if e.settled == nil {
		e.settled = make(map[string]quelor.SettleResult)
	}
	cp := res
	cp.Declared = settleCloneI64(res.Declared)
	cp.Consumed = settleCloneI64(res.Consumed)
	cp.Delta = settleCloneI64(res.Delta)
	cp.Duplicate = false
	if _, exists := e.settled[cp.LeaseID]; !exists {
		e.settledQ = append(e.settledQ, cp.LeaseID)
	}
	e.settled[cp.LeaseID] = cp
	for len(e.settled) > 50000 && len(e.settledQ) > 0 {
		old := e.settledQ[0]
		e.settledQ = e.settledQ[1:]
		delete(e.settled, old)
	}
}

type settleExec struct {
	result quelor.SettleResult
	evs    []Event
	failed bool
	ruleID string
}

func (e *Engine) settleActiveLocked(rec *leaseRec, req quelor.SettleRequest, now time.Time) settleExec {
	var evs []Event
	if len(req.Observations) > 0 {
		for _, o := range req.Observations {
			s := e.res[o.Resource]
			if s == nil {
				continue
			}
			evs = append(evs, s.observe(o.HTTPStatus, o.RetryAfter, now)...)
		}
	} else if req.Outcome != nil && req.Outcome.HTTPStatus != 0 {
		for _, id := range settleSortedKeys(rec.lease.Declared) {
			s := e.res[id]
			if s == nil {
				continue
			}
			evs = append(evs, s.observe(req.Outcome.HTTPStatus, 0, now)...)
		}
	}

	var done settleExec
	probeOK := true
	if req.Outcome != nil && e.cfg.Classifier != nil {
		cr := e.cfg.Classifier.Classify(*req.Outcome)
		done.result.Class = cr.Class
		done.failed = cr.Failed
		done.ruleID = cr.RuleID
		if cr.Failed && cr.Class == quelor.DependencyUnavailable {
			probeOK = false
		}
	}
	if rec.superseded {
		// A run replaced by a newer generation was cancelled by policy,
		// not by a defect, whether or not an outcome or classifier is
		// present. Report it as neither a failure nor a class so a retry
		// budget reading Class or the trace does not count it, and mark it
		// with RuleID so it differs from a success over the wire.
		done.result.Class = ""
		done.failed = false
		done.ruleID = classify.RuleSuperseded
	}
	if rec.probe != nil {
		pids := make([]string, 0, len(rec.probe))
		for id, on := range rec.probe {
			if on {
				pids = append(pids, id)
			}
		}
		sort.Strings(pids)
		for _, id := range pids {
			s := e.res[id]
			if s == nil {
				continue
			}
			evs = append(evs, s.probeSettled(probeOK, now)...)
		}
	}

	result := e.releaseLocked(rec, req.Consumed, now, "settle")
	result.Class = done.result.Class
	result.RuleID = done.ruleID
	done.result = result
	done.evs = evs
	return done
}

func (e *Engine) finishRenewRefusal(rec *leaseRec, reason string, stamps []string, wake *time.Time, resp decider.Response, readings []quelor.Reading, traceID string, lat float64, now time.Time) quelor.RenewResult {
	e.st.renewRefused.Add(1)
	hint := e.renewWakeHint(rec, now)
	if wake != nil {
		hint = *wake
	}
	verdict := quelor.Verdict{
		Kind:      quelor.Defer,
		Hibernate: true,
		Reason:    "renew-refused:" + reason,
		WakeAt:    &hint,
		Stamps:    settleNonEmpty(stamps),
	}
	out := settleCloneLease(rec.lease)
	dref := resp.DecisionRef
	if dref == "" {
		dref = randHex(16)
	}
	e.writeRenewTrace(rec, out, verdict, dref, resp, readings, traceID, lat)
	return quelor.RenewResult{Verdict: verdict, Lease: &out, DecisionRef: dref}
}

func (e *Engine) renewUnreachableMustRefuse(env quelor.Envelope, delta map[string]int64) bool {
	policyRefuse := false
	for _, id := range settlePositiveKeys(delta) {
		m, ok := e.cfg.Registry.Manifest(id)
		if !ok {
			continue
		}
		if m.CapacityModel == resource.ModelHardBudget {
			return true
		}
		switch m.UnknownPolicy {
		case resource.UnknownDeferShort, resource.UnknownAssumeBusy, resource.UnknownDeny:
			policyRefuse = true
		}
	}
	if env.Priority == quelor.PriorityOperatorDirective {
		return false
	}
	return policyRefuse
}

// renewReadingReason applies acquire's reading checks to the positive keys of
// a renew delta. Unknown and stale readings follow the resource's unknown
// policy (hold_cursor lets the delta continue). An external reading below the
// requested delta refuses with insufficient:<id>. Keys with no manifest are
// left to renewReserve.
func (e *Engine) renewReadingReason(env quelor.Envelope, delta map[string]int64, readings []quelor.Reading, external map[string]bool) string {
	for _, id := range settlePositiveKeys(delta) {
		m, ok := e.cfg.Registry.Manifest(id)
		if !ok {
			continue
		}
		rd := readingFor(readings, id)
		if rd.Usable() {
			if external[id] && rd.Available < delta[id] {
				return "insufficient:" + id
			}
			continue
		}
		if m.UnknownPolicy.Verdict() != quelor.Admit {
			return "unknown-reading:" + id
		}
	}
	return ""
}

func (e *Engine) renewQuotaReason(rec *leaseRec, delta map[string]int64) string {
	q := e.quotaFor(rec.lease.HolderID)
	if len(q.MaxUnits) == 0 {
		return ""
	}
	h := e.holders[rec.lease.HolderID]
	declared := rec.lease.Declared
	for _, id := range settlePositiveKeys(delta) {
		limit, ok := q.MaxUnits[id]
		if !ok || limit <= 0 {
			continue
		}
		used := int64(0)
		if declared != nil {
			used = declared[id]
		}
		if h != nil && h.units != nil && h.units[id] > used {
			used = h.units[id]
		}
		if used+delta[id] > limit {
			return "holder-quota-exceeded"
		}
	}
	return ""
}

func (e *Engine) renewReserve(rec *leaseRec, delta map[string]int64, now time.Time) (string, bool) {
	keys := settlePositiveKeys(delta)
	if len(keys) == 0 {
		return "", true
	}
	type took struct {
		id  string
		n   int64
		win uint64
	}
	held := make([]took, 0, len(keys))
	rollback := func() {
		for _, h := range held {
			s := e.res[h.id]
			if s == nil {
				continue
			}
			s.giveBack(h.n, 0, h.win, now)
		}
	}
	for _, id := range keys {
		n := delta[id]
		s := e.res[id]
		if s == nil || !s.addReservation(n, now) {
			rollback()
			return "insufficient:" + id, false
		}
		held = append(held, took{id: id, n: n, win: s.windowID()})
	}

	if rec.lease.Declared == nil {
		rec.lease.Declared = map[string]int64{}
	}
	if rec.windows == nil {
		rec.windows = map[string]uint64{}
	}
	h := e.holders[rec.lease.HolderID]
	if h == nil {
		h = &holderUse{units: map[string]int64{}}
		e.holders[rec.lease.HolderID] = h
	}
	if h.units == nil {
		h.units = map[string]int64{}
	}
	for _, t := range held {
		rec.lease.Declared[t.id] += t.n
		if _, exists := rec.windows[t.id]; !exists {
			rec.windows[t.id] = t.win
		}
		h.units[t.id] += t.n
	}
	return "", true
}

func (e *Engine) renewWakeHint(rec *leaseRec, now time.Time) time.Time {
	var hint time.Time
	for _, id := range settlePositiveKeys(rec.lease.Declared) {
		s := e.res[id]
		if s == nil {
			continue
		}
		need := rec.lease.Declared[id]
		if need <= 0 {
			need = 1
		}
		w := s.wakeHint(need, now)
		if hint.IsZero() || w.After(hint) {
			hint = w
		}
	}
	if hint.IsZero() {
		return now.Add(e.cfg.AgingStep)
	}
	return hint
}

func (e *Engine) writeRenewTrace(rec *leaseRec, lease quelor.Lease, v quelor.Verdict, dref string, resp decider.Response, readings []quelor.Reading, traceID string, lat float64) {
	env := rec.env
	env.Demands = settleCloneI64(rec.env.Demands)
	vv := v
	e.writeTrace(trace.Record{
		Op:            trace.OpRenew,
		DecisionRef:   dref,
		TraceID:       traceID,
		EnvelopeHash:  trace.HashEnvelope(env),
		Envelope:      &env,
		HolderID:      lease.HolderID,
		TenantID:      lease.TenantID,
		LeaseID:       lease.ID,
		Verdict:       &vv,
		Readings:      readings,
		RulesetID:     resp.RulesetID,
		RulesetHash:   resp.RulesetHash,
		DeciderID:     resp.DeciderID,
		RuleIDs:       append([]string{}, resp.RuleIDs...),
		Provenance:    trace.Provenance{Step: resp.Provenance.Step, Tier: resp.Provenance.Tier, EvidenceClass: resp.Provenance.EvidenceClass},
		DeciderStatus: settleTraceStatus(resp.Status),
		Mode:          settleTraceMode(resp.Mode),
		Stamps:        append([]string{}, vv.Stamps...),
		Declared:      settleCloneI64(lease.Declared),
		LatencyMS:     lat,
	})
}

func (e *Engine) writeSettleTrace(rec *leaseRec, done settleExec, req quelor.SettleRequest, start time.Time) {
	env := rec.env
	env.Demands = settleCloneI64(rec.env.Demands)
	var outcome *trace.Outcome
	var ruleIDs []string
	if req.Outcome != nil {
		outcome = &trace.Outcome{
			Class:      done.result.Class,
			ExitCode:   req.Outcome.ExitCode,
			HTTPStatus: req.Outcome.HTTPStatus,
			LogExcerpt: settleExcerpt(req.Outcome.Log),
			Failed:     done.failed,
		}
	}
	if done.ruleID != "" {
		ruleIDs = []string{done.ruleID}
	}
	e.writeTrace(trace.Record{
		Op:            trace.OpSettle,
		DecisionRef:   rec.lease.DecisionRef,
		EnvelopeHash:  trace.HashEnvelope(env),
		Envelope:      &env,
		HolderID:      rec.lease.HolderID,
		TenantID:      rec.lease.TenantID,
		LeaseID:       rec.lease.ID,
		DeciderStatus: trace.DeciderStatusNone,
		RuleIDs:       ruleIDs,
		Declared:      settleCloneI64(done.result.Declared),
		Consumed:      settleCloneI64(done.result.Consumed),
		Outcome:       outcome,
		LatencyMS:     float64(e.now().Sub(start)) / float64(time.Millisecond),
	})
}

// settleReadings reads ids for a renew. external marks resources whose
// reading came from a registered reader rather than engine state.
func (e *Engine) settleReadings(ctx context.Context, ids []string, now time.Time) ([]quelor.Reading, map[string]bool) {
	external := map[string]bool{}
	if len(ids) == 0 {
		return nil, external
	}
	out := make([]quelor.Reading, 0, len(ids))
	for _, id := range ids {
		rd, err := e.cfg.Registry.Read(ctx, id)
		switch {
		case err == nil:
			if rd.Resource == "" {
				rd.Resource = id
			}
			external[id] = true
			out = append(out, rd)
		case errors.Is(err, resource.ErrNoReader):
			e.mu.Lock()
			s := e.res[id]
			var avail, eff int64
			known := false
			if s != nil {
				avail = s.avail(now)
				eff = s.effective()
				known = true
			}
			e.mu.Unlock()
			out = append(out, quelor.Reading{
				Resource:   id,
				Available:  avail,
				Capacity:   eff,
				Known:      known,
				ObservedAt: now,
			})
		default:
			if !errors.Is(err, resource.ErrNotFound) {
				external[id] = true
			}
			out = append(out, quelor.Reading{Resource: id, Known: false, ObservedAt: now})
		}
	}
	return out, external
}

func (e *Engine) settleNoteEvents(evs []Event) {
	var n int64
	for _, ev := range evs {
		if ev.Kind == "aimd-decrease" {
			n++
		}
	}
	if n > 0 {
		e.st.aimdDec.Add(n)
	}
}

func settleCloneI64(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func settleCloneLease(l quelor.Lease) quelor.Lease {
	l.Declared = settleCloneI64(l.Declared)
	return l
}

func settleSortedKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func settlePositiveKeys(m map[string]int64) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if v > 0 {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

func settleUnionKeys(a, b map[string]int64) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	for k := range a {
		seen[k] = struct{}{}
	}
	for k := range b {
		seen[k] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func settleNonEmpty(ss []string) []string {
	if len(ss) == 0 {
		return nil
	}
	return ss
}

func settleHasStamp(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}

func settleExcerpt(s string) string {
	if len(s) <= settleLogExcerptBytes {
		return s
	}
	return s[:settleLogExcerptBytes]
}

func settleTraceStatus(s decider.Status) trace.DeciderStatus {
	switch s {
	case decider.StatusAnswered:
		return trace.DeciderStatusAnswered
	case decider.StatusAbstained:
		return trace.DeciderStatusAbstained
	case decider.StatusUnavailable:
		return trace.DeciderStatusUnavailable
	case decider.StatusKilled:
		return trace.DeciderStatusKilled
	default:
		return trace.DeciderStatusNone
	}
}

func settleTraceMode(m decider.Mode) trace.Mode {
	if m == "" {
		m = decider.ModeEnforce
	}
	return trace.Mode(m)
}
