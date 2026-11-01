package reference

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/decider"
	"github.com/coherera/quelor/pkg/quelor"
)

const fixedNow = "2026-10-31T00:00:00Z"

func mustParse(t *testing.T, y string) *Ruleset {
	t.Helper()
	rs, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return rs
}

func mustDecider(t *testing.T, y string) (*Decider, *Ruleset) {
	t.Helper()
	rs := mustParse(t, y)
	return New(rs), rs
}

func fixedNowTime(t *testing.T) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339Nano, fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

func baseReq() decider.Request {
	return decider.Request{
		DeciderAPIVersion: decider.APIVersion,
		Op:                decider.OpAcquire,
		Envelope: quelor.Envelope{
			UnitID:   "unit-1",
			TenantID: "tenant-1",
			HolderID: "holder-1",
		},
		Meta: decider.CallMeta{TraceID: "trace-1", Now: fixedNow},
	}
}

func assertIdentity(t *testing.T, resp decider.Response, trace string) {
	t.Helper()
	if resp.DeciderAPIVersion != decider.APIVersion {
		t.Fatalf("api version %q", resp.DeciderAPIVersion)
	}
	if resp.DeciderID != deciderID {
		t.Fatalf("decider id %q", resp.DeciderID)
	}
	if resp.TraceID != trace {
		t.Fatalf("trace %q", resp.TraceID)
	}
	if resp.Provenance != refProvenance {
		t.Fatalf("provenance %+v", resp.Provenance)
	}
	if resp.LatencyMS < 0 {
		t.Fatalf("latency %v", resp.LatencyMS)
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "bad cel",
			yaml: `
id: x
rules:
  - id: bad
    when: "tenant_id =="
    verdict: DENY
`,
			want: "cel",
		},
		{
			name: "non-bool string",
			yaml: `
id: x
rules:
  - id: bad
    when: "tenant_id"
    verdict: DENY
`,
			want: "bool",
		},
		{
			name: "non-bool int",
			yaml: `
id: x
rules:
  - id: bad
    when: "1 + 2"
    verdict: DENY
`,
			want: "bool",
		},
		{
			name: "empty when",
			yaml: `
id: x
rules:
  - id: bad
    when: ""
    verdict: DENY
`,
			want: "cel",
		},
		{
			name: "duplicate id",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: ADMIT
  - id: a
    when: "false"
    verdict: DENY
`,
			want: "duplicate",
		},
		{
			name: "empty id",
			yaml: `
id: x
rules:
  - id: ""
    when: "true"
    verdict: DENY
`,
			want: "empty",
		},
		{
			name: "bad verdict",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: REJECT
`,
			want: "verdict",
		},
		{
			name: "lowercase verdict",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: deny
`,
			want: "verdict",
		},
		{
			name: "unknown top-level field",
			yaml: `
id: x
extra: 1
rules: []
`,
			want: "not found",
		},
		{
			name: "unknown rule field",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: DENY
    bogus: 1
`,
			want: "not found",
		},
		{
			name: "wake on deny",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: DENY
    wake_after: 30s
`,
			want: "wake_after",
		},
		{
			name: "wake on shed",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: SHED
    wake_after: 1s
`,
			want: "wake_after",
		},
		{
			name: "bad duration",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: DEFER
    wake_after: yesterday
`,
			want: "wake_after",
		},
		{
			name: "negative wake",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: DEFER
    wake_after: -5s
`,
			want: "negative",
		},
		{
			name: "bad op",
			yaml: `
id: x
rules:
  - id: a
    when: "true"
    verdict: DENY
    ops: [explode]
`,
			want: "invalid op",
		},
		{
			name: "bad mode",
			yaml: `
id: x
mode: yolo
rules: []
`,
			want: "invalid mode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestParseOK(t *testing.T) {
	rs := mustParse(t, `
id: default-v1
rules:
  - id: deny-banned-tenant
    when: "tenant_id == 'acme-banned'"
    verdict: DENY
    reason: tenant suspended
    ops: [acquire]
  - id: cool-off
    when: "true"
    verdict: DEFER
    reason: later
    wake_after: 30s
`)
	if rs.ID != "default-v1" {
		t.Fatalf("id %q", rs.ID)
	}
	if rs.Mode != decider.ModeEnforce {
		t.Fatalf("mode %q", rs.Mode)
	}
	if len(rs.Rules) != 2 {
		t.Fatalf("rules %d", len(rs.Rules))
	}
	ban := rs.Rules[0]
	if ban.Verdict != quelor.Deny || ban.Reason != "tenant suspended" || ban.When != "tenant_id == 'acme-banned'" {
		t.Fatalf("ban rule %+v", ban)
	}
	if len(ban.Ops) != 1 || ban.Ops[0] != decider.OpAcquire {
		t.Fatalf("ops %+v", ban.Ops)
	}
	if ban.WakeAfter != 0 {
		t.Fatalf("wake %s", ban.WakeAfter)
	}
	cool := rs.Rules[1]
	if cool.Verdict != quelor.Defer || cool.WakeAfter != 30*time.Second || len(cool.Ops) != 0 {
		t.Fatalf("cool rule %+v", cool)
	}
	if h1, h2 := rs.Hash(), rs.Hash(); h1 == "" || h1 != h2 {
		t.Fatalf("hash %q", rs.Hash())
	}
}

func TestHash(t *testing.T) {
	const multi = `
id: multi
mode: log-only
rules:
  - id: r1
    when: "true"
    verdict: ADMIT
  - id: r2
    when: "generation < 2"
    verdict: DEFER
    reason: later
    wake_after: 30s
    ops: [acquire, renew]
`
	const canon = `{"id":"multi","rules":[{"id":"r1","when":"true","verdict":"ADMIT","reason":"","wake_after":"","ops":[]},{"id":"r2","when":"generation < 2","verdict":"DEFER","reason":"later","wake_after":"30s","ops":["acquire","renew"]}]}`
	rs := mustParse(t, multi)
	sum := sha256.Sum256([]byte(canon))
	want := hex.EncodeToString(sum[:])
	if rs.Hash() != want {
		t.Fatalf("hash %s want %s", rs.Hash(), want)
	}
	if len(rs.Hash()) != 64 {
		t.Fatalf("len %d", len(rs.Hash()))
	}
	for _, c := range rs.Hash() {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Fatalf("non-hex %q", rs.Hash())
		}
	}

	formatted := mustParse(t, `
# comment
mode: enforce
rules:
  - when: "true"
    verdict: ADMIT
    id: r1
  - wake_after: 30s
    ops:
      - acquire
      - renew
    id: r2
    reason: later
    verdict: DEFER
    when: "generation < 2"
id: multi
`)
	if formatted.Hash() != rs.Hash() {
		t.Fatalf("format hash %s want %s", formatted.Hash(), rs.Hash())
	}
	if formatted.Mode != decider.ModeEnforce || rs.Mode != decider.ModeLogOnly {
		t.Fatalf("modes %s %s", formatted.Mode, rs.Mode)
	}

	changed := mustParse(t, `
id: multi
mode: log-only
rules:
  - id: r1
    when: "true"
    verdict: ADMIT
  - id: r2
    when: "generation < 3"
    verdict: DEFER
    reason: later
    wake_after: 30s
    ops: [acquire, renew]
`)
	if changed.Hash() == rs.Hash() {
		t.Fatal("changed rule kept the hash")
	}

	swapped := mustParse(t, `
id: multi
rules:
  - id: r2
    when: "generation < 2"
    verdict: DEFER
    reason: later
    wake_after: 30s
    ops: [acquire, renew]
  - id: r1
    when: "true"
    verdict: ADMIT
`)
	if swapped.Hash() == rs.Hash() {
		t.Fatal("reordered rules kept the hash")
	}

	omit := mustParse(t, "id: same\nmode: advise\n")
	empty := mustParse(t, "id: same\nrules: []\n")
	if omit.Hash() != empty.Hash() {
		t.Fatalf("empty rules %s vs %s", omit.Hash(), empty.Hash())
	}
	emptyCanon := `{"id":"same","rules":[]}`
	emptySum := sha256.Sum256([]byte(emptyCanon))
	if omit.Hash() != hex.EncodeToString(emptySum[:]) {
		t.Fatalf("empty hash %s", omit.Hash())
	}

	min := mustParse(t, `
id: dur
rules:
  - id: d
    when: "true"
    verdict: DEFER
    wake_after: 60s
`)
	alt := mustParse(t, `
id: dur
mode: advise
rules:
  - id: d
    when: "true"
    verdict: DEFER
    wake_after: 1m0s
`)
	other := mustParse(t, `
id: dur
rules:
  - id: d
    when: "true"
    verdict: DEFER
    wake_after: 61s
`)
	if min.Hash() != alt.Hash() {
		t.Fatalf("duration canon %s vs %s", min.Hash(), alt.Hash())
	}
	if other.Hash() == min.Hash() {
		t.Fatal("61s matched 60s")
	}

	bare := New(nil)
	also := New(&Ruleset{Mode: decider.ModeLogOnly})
	if bare.rs.Hash() != also.rs.Hash() {
		t.Fatalf("mode leaked into default hash %s %s", bare.rs.Hash(), also.rs.Hash())
	}
}
