package decider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/coherera/quelor/pkg/quelor"
)

func TestUnavailable(t *testing.T) {
	cases := []struct {
		name   string
		id     string
		reason string
	}{
		{name: "reason", id: "decider-a", reason: "circuit-open"},
		{name: "empty", id: "", reason: ""},
		{name: "stamp-reason", id: "decider-b", reason: "ruleset-moved"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Unavailable(tc.id, tc.reason)
			if got.Status != StatusUnavailable {
				t.Fatalf("status %q", got.Status)
			}
			if got.DeciderID != tc.id {
				t.Fatalf("id %q", got.DeciderID)
			}
			if got.Verdict.Reason != tc.reason {
				t.Fatalf("reason %q", got.Verdict.Reason)
			}
			if got.DeciderAPIVersion != APIVersion {
				t.Fatalf("version %q", got.DeciderAPIVersion)
			}
			if got.Enforced() {
				t.Fatal("unavailable response was enforced")
			}
			if got.Verdict.Kind != "" {
				t.Fatalf("kind %q", got.Verdict.Kind)
			}
		})
	}
}

func TestStaticDecisionRef(t *testing.T) {
	cases := []struct {
		name string
		op   Op
		unit string
		kind quelor.VerdictKind
	}{
		{name: "acquire-admit", op: OpAcquire, unit: "unit-1", kind: quelor.Admit},
		{name: "renew-defer", op: OpRenew, unit: "unit-1", kind: quelor.Defer},
		{name: "empty-unit-deny", op: OpAcquire, unit: "", kind: quelor.Deny},
		{name: "shed", op: OpAcquire, unit: "unit-1", kind: quelor.Shed},
		{name: "unknown-kind", op: OpRenew, unit: "other", kind: quelor.VerdictKind("nope")},
	}
	seen := map[string]string{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			verdict := quelor.Verdict{Kind: tc.kind, Reason: "because"}
			d := Static(verdict, "static-id")
			req := Request{Op: tc.op, Envelope: quelor.Envelope{UnitID: tc.unit}, Meta: CallMeta{TraceID: "tr"}}
			resp, err := d.Decide(context.Background(), req)
			if err != nil {
				t.Fatalf("decide: %v", err)
			}
			sum := sha256.Sum256([]byte(string(tc.op) + tc.unit + string(tc.kind)))
			wantRef := hex.EncodeToString(sum[:])[:16]
			if resp.DecisionRef != wantRef {
				t.Fatalf("ref %q want %q", resp.DecisionRef, wantRef)
			}
			if len(resp.DecisionRef) != 16 {
				t.Fatalf("ref len %d", len(resp.DecisionRef))
			}
			for _, c := range resp.DecisionRef {
				if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
					t.Fatalf("non-hex ref %q", resp.DecisionRef)
				}
			}
			again, err := d.Decide(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}
			if again.DecisionRef != resp.DecisionRef {
				t.Fatalf("unstable ref %q then %q", resp.DecisionRef, again.DecisionRef)
			}
			other := req
			other.Envelope.UnitID = tc.unit + "-b"
			otherResp, err := d.Decide(context.Background(), other)
			if err != nil {
				t.Fatal(err)
			}
			if otherResp.DecisionRef == resp.DecisionRef {
				t.Fatal("different unit produced the same ref")
			}
			if resp.Status != StatusAnswered || resp.Mode != ModeEnforce || !resp.Enforced() {
				t.Fatalf("status/mode %#v", resp)
			}
			if resp.DeciderAPIVersion != APIVersion || resp.DeciderID != "static-id" || resp.TraceID != "tr" {
				t.Fatalf("identity %#v", resp)
			}
			if resp.Verdict.Kind != tc.kind || resp.Verdict.Reason != "because" {
				t.Fatalf("verdict %#v", resp.Verdict)
			}
			key := string(tc.op) + "\x00" + tc.unit + "\x00" + string(tc.kind)
			if prev, ok := seen[resp.DecisionRef]; ok && prev != key {
				t.Fatalf("ref collision %q for %q and %q", resp.DecisionRef, prev, key)
			}
			seen[resp.DecisionRef] = key
		})
	}
}

func TestStaticIgnoresCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d := Static(quelor.Verdict{Kind: quelor.Admit}, "s")
	resp, err := d.Decide(ctx, Request{Op: OpAcquire, Envelope: quelor.Envelope{UnitID: "u"}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Verdict.Kind != quelor.Admit || resp.Status != StatusAnswered {
		t.Fatalf("%#v", resp)
	}
}
