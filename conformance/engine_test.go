package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/decider"
	"github.com/coherera/quelor/pkg/decider/reference"
	"github.com/coherera/quelor/pkg/engine"
	"github.com/coherera/quelor/pkg/quelor"
	"github.com/coherera/quelor/pkg/resource"
)

func TestEngineConformance(t *testing.T) {
	report := RunEngine(t, newTestEngine)
	if report.Suite != suiteEngine || report.Version != Version {
		t.Fatalf("report identity %+v", report)
	}
	if report.Passed+report.Failed != len(report.Cases) {
		t.Fatalf("tally passed=%d failed=%d cases=%d", report.Passed, report.Failed, len(report.Cases))
	}
	if report.Failed > 0 {
		t.Fatalf("engine conformance failed %d: %s", report.Failed, failedList(report))
	}
}

func TestEngineConformance_PROVES_RED(t *testing.T) {
	rec := &recordingReporter{}
	report := runEngine(rec, func(*testing.T, Spec) quelor.LeaseAPI {
		return failOpenAPI{}
	})
	if report.Failed == 0 {
		t.Fatalf("PROVES-RED: fail-open engine was not detected (%d passed)", report.Passed)
	}
	mustFail := []string{
		"verdicts/defer",
		"verdicts/deny",
		"verdicts/shed",
		"unknown-is-not-available/defer_short",
		"unknown-is-not-available/deny",
		"fail-static/decider-unreachable",
		"fail-static/never-fail-open-on-spend",
		"idempotent-settle",
		"meta-admission/holder-quota",
		"meta-admission/authenticated-holder",
		"atomic-multi-resource",
		"renew-refusal-hibernate",
	}
	failed := map[string]bool{}
	for _, c := range report.Cases {
		if !c.Passed {
			failed[c.Name] = true
		}
	}
	for _, name := range mustFail {
		if !failed[name] {
			t.Errorf("PROVES-RED: %s did not fail", name)
		}
	}
}

func TestReportWriteJSON(t *testing.T) {
	cases := []struct {
		name string
		rep  Report
	}{
		{
			name: "empty",
			rep:  Report{Suite: "engine", Version: "1", Cases: []CaseResult{}},
		},
		{
			name: "mixed",
			rep: Report{
				Suite:   "decider",
				Version: "1",
				Passed:  1,
				Failed:  1,
				Cases: []CaseResult{
					{Name: "decider-id-set", Passed: true},
					{Name: "ruleset-moved", Passed: false},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tc.rep.WriteJSON(&buf); err != nil {
				t.Fatalf("write: %v", err)
			}
			var got Report
			if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
				t.Fatalf("read: %v", err)
			}
			if !reflect.DeepEqual(got, tc.rep) {
				t.Fatalf("got %+v", got)
			}
		})
	}
	var rep Report
	if err := rep.WriteJSON(nil); err == nil {
		t.Fatal("nil writer returned nil error")
	}
}

func newTestEngine(t *testing.T, spec Spec) quelor.LeaseAPI {
	t.Helper()
	now := spec.Now
	if now == nil {
		frozen := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
		now = func() time.Time { return frozen }
	}
	reg := resource.NewRegistry(now)
	for _, rs := range spec.Resources {
		model := resource.ModelLease
		if rs.Spend {
			model = resource.ModelHardBudget
		}
		var m resource.Manifest
		m.ID = rs.ID
		m.Capacity = rs.Capacity
		m.CapacityModel = model
		m.WithDefaults()
		m.ID = rs.ID
		m.Capacity = rs.Capacity
		m.CapacityModel = model
		if rs.Stale {
			m.FreshnessTTL = resource.Duration(time.Millisecond)
		}
		if p, ok := mapUnknownPolicy(rs.UnknownPolicy); ok {
			m.UnknownPolicy = p
		}
		// Some cases register deliberately invalid manifests and assert the
		// engine refuses them later, so a registration error is expected here.
		_ = reg.Register(m, resource.NewStaticReader(staticReading(rs, now())))
	}
	maxLeases := spec.HolderMaxLeases
	if maxLeases <= 0 {
		maxLeases = 1 << 20
	}
	var d decider.Decider = reference.Default()
	if spec.DeciderUnavailable {
		d = decider.Func(func(context.Context, decider.Request) (decider.Response, error) {
			return decider.Response{Status: decider.StatusUnavailable}, errors.New("down")
		})
	}
	var cfg engine.Config
	cfg.Registry = reg
	cfg.Decider = d
	cfg.Clock = now
	cfg.SharedToken = spec.SharedToken
	cfg.DefaultQuota = engine.Quota{MaxLeases: maxLeases}
	cfg.LeaseTTL = 45 * time.Second
	e, err := engine.New(cfg)
	if err != nil {
		t.Fatalf("engine: %v", err)
	}
	return e
}

func staticReading(rs ResourceSpec, now time.Time) quelor.Reading {
	if rs.Unknown {
		return quelor.Reading{Resource: rs.ID, Known: false}
	}
	var rd quelor.Reading
	rd.Resource = rs.ID
	rd.Available = rs.Capacity
	rd.Capacity = rs.Capacity
	rd.Known = true
	rd.ObservedAt = now
	if rs.Stale {
		rd.Stale = true
		rd.ObservedAt = now.Add(-time.Hour)
		rd.AgeMS = int64(time.Hour / time.Millisecond)
	}
	return rd
}

func mapUnknownPolicy(s string) (resource.UnknownPolicy, bool) {
	switch s {
	case "defer_short":
		return resource.UnknownDeferShort, true
	case "deny":
		return resource.UnknownDeny, true
	case "hold_cursor":
		return resource.UnknownHoldCursor, true
	default:
		var zero resource.UnknownPolicy
		return zero, false
	}
}

// failOpenAPI admits every acquire and never dedups settle.
type failOpenAPI struct{}

func (failOpenAPI) AcquireLease(_ context.Context, r quelor.AcquireRequest) (quelor.AcquireResult, error) {
	var lease quelor.Lease
	lease.ID = "fail-open"
	lease.UnitID = r.Envelope.UnitID
	lease.TenantID = r.Envelope.TenantID
	lease.HolderID = r.Envelope.HolderID
	lease.Declared = r.Envelope.Demands
	lease.GrantedAt = time.Unix(0, 0).UTC()
	lease.ExpiresAt = lease.GrantedAt
	var out quelor.AcquireResult
	out.Verdict.Kind = quelor.Admit
	out.Verdict.Reason = "fail-open"
	out.Lease = &lease
	out.DecisionRef = "fail-open"
	return out, nil
}

func (failOpenAPI) RenewLease(_ context.Context, r quelor.RenewRequest) (quelor.RenewResult, error) {
	var out quelor.RenewResult
	out.Verdict.Kind = quelor.Admit
	out.Lease = &quelor.Lease{ID: r.LeaseID, HolderID: r.HolderID}
	out.DecisionRef = "fail-open"
	return out, nil
}

func (failOpenAPI) SettleLease(_ context.Context, r quelor.SettleRequest) (quelor.SettleResult, error) {
	var out quelor.SettleResult
	out.LeaseID = r.LeaseID
	out.Consumed = r.Consumed
	out.Duplicate = false
	return out, nil
}

func failedList(rep Report) string {
	var b bytes.Buffer
	for _, c := range rep.Cases {
		if c.Passed {
			continue
		}
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		b.WriteString(c.Name)
	}
	return b.String()
}

var (
	_ quelor.LeaseAPI = failOpenAPI{}
	_ reporter        = tAdapter{}
	_ reporter        = (*recordingReporter)(nil)
	_ reporter        = (*captureReporter)(nil)
)
