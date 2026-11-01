package resource

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/quelor"
	"gopkg.in/yaml.v3"
)

func TestUnknownPolicyVerdict(t *testing.T) {
	tests := []struct {
		p    UnknownPolicy
		want quelor.VerdictKind
	}{
		{p: UnknownDeferShort, want: quelor.Defer},
		{p: UnknownAssumeBusy, want: quelor.Defer},
		{p: UnknownHoldCursor, want: quelor.Admit},
		{p: UnknownDeny, want: quelor.Deny},
		{p: "", want: quelor.Defer},
	}
	for _, tc := range tests {
		if got := tc.p.Verdict(); got != tc.want {
			t.Errorf("%q: got %s want %s", tc.p, got, tc.want)
		}
	}
}

func TestManifestValidate(t *testing.T) {
	tests := []struct {
		name string
		m    Manifest
		want string
	}{
		{name: "lease ok", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1}, want: ""},
		{name: "gcra ok", m: Manifest{ID: "a", CapacityModel: ModelGCRA, Capacity: 5, Window: Duration(time.Second)}, want: ""},
		{name: "window bucket ok", m: Manifest{ID: "a", CapacityModel: ModelWindowBucket, Capacity: 5, Window: Duration(time.Minute)}, want: ""},
		{name: "circuit zero capacity", m: Manifest{ID: "a", CapacityModel: ModelCircuit}, want: ""},
		{name: "hard budget deny", m: Manifest{ID: "a", CapacityModel: ModelHardBudget, Capacity: 3, UnknownPolicy: UnknownDeny}, want: ""},
		{name: "hard budget default policy", m: Manifest{ID: "a", CapacityModel: ModelHardBudget, Capacity: 3}, want: ""},
		{name: "hold cursor lease", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1, UnknownPolicy: UnknownHoldCursor}, want: ""},
		{name: "freshness zero", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1, FreshnessTTL: 0}, want: ""},
		{name: "zero min capacity defaults valid", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1, MinCapacity: 0}, want: ""},
		{name: "missing id", m: Manifest{CapacityModel: ModelLease, Capacity: 1}, want: "id required"},
		{name: "bad model", m: Manifest{ID: "a", CapacityModel: "nope", Capacity: 1}, want: "capacity_model"},
		{name: "empty model", m: Manifest{ID: "a", Capacity: 1}, want: "capacity_model"},
		{name: "gcra zero capacity", m: Manifest{ID: "a", CapacityModel: ModelGCRA, Capacity: 0, Window: Duration(time.Second)}, want: "capacity must be > 0"},
		{name: "lease negative capacity", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: -1}, want: "capacity must be > 0"},
		{name: "circuit negative capacity", m: Manifest{ID: "a", CapacityModel: ModelCircuit, Capacity: -2}, want: "capacity must be >= 0"},
		{name: "gcra missing window", m: Manifest{ID: "a", CapacityModel: ModelGCRA, Capacity: 1}, want: "window must be > 0"},
		{name: "window bucket zero window", m: Manifest{ID: "a", CapacityModel: ModelWindowBucket, Capacity: 1, Window: 0}, want: "window must be > 0"},
		{name: "lease negative window", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1, Window: Duration(-time.Second)}, want: "window must be >= 0"},
		{name: "negative freshness", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1, FreshnessTTL: Duration(-time.Millisecond)}, want: "freshness_ttl"},
		{name: "bad policy", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1, UnknownPolicy: "sometimes"}, want: "unknown_policy"},
		{name: "hard budget hold cursor", m: Manifest{ID: "a", CapacityModel: ModelHardBudget, Capacity: 1, UnknownPolicy: UnknownHoldCursor}, want: "hold_cursor"},
		{name: "negative min capacity", m: Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1, MinCapacity: -1}, want: "min_capacity"},
		{name: "negative failure threshold", m: Manifest{ID: "a", CapacityModel: ModelCircuit, FailureThreshold: -1}, want: "failure_threshold"},
		{name: "negative open for", m: Manifest{ID: "a", CapacityModel: ModelCircuit, OpenFor: Duration(-time.Second)}, want: "open_for"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.m.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
			if !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("not a validation error: %v", err)
			}
			if strings.Contains(tc.want, "hold_cursor") && !errors.Is(err, ErrFailOpenSpend) {
				t.Fatalf("fail-open error: %v", err)
			}
		})
	}
}

func TestValidateDoesNotMutate(t *testing.T) {
	m := Manifest{ID: "a", CapacityModel: ModelLease, Capacity: 1}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	if m.UnknownPolicy != "" || m.MinCapacity != 0 || m.FailureThreshold != 0 || m.OpenFor != 0 {
		t.Fatalf("Validate mutated %+v", m)
	}
	m.WithDefaults()
	if m.UnknownPolicy != UnknownDeferShort || m.MinCapacity != 1 || m.FailureThreshold != 3 || m.OpenFor != Duration(30*time.Second) {
		t.Fatalf("defaults %+v", m)
	}
	m.WithDefaults()
	if m.UnknownPolicy != UnknownDeferShort || m.MinCapacity != 1 || m.FailureThreshold != 3 || m.OpenFor != Duration(30*time.Second) {
		t.Fatalf("second defaults %+v", m)
	}
}

func TestWithDefaultsPreservesExplicit(t *testing.T) {
	m := Manifest{
		ID:               "a",
		CapacityModel:    ModelLease,
		Capacity:         2,
		Window:           Duration(5 * time.Second),
		FreshnessTTL:     Duration(time.Second),
		UnknownPolicy:    UnknownDeny,
		MinCapacity:      4,
		FailureThreshold: 8,
		OpenFor:          Duration(time.Minute),
	}
	m.WithDefaults()
	if m.UnknownPolicy != UnknownDeny || m.MinCapacity != 4 || m.FailureThreshold != 8 || m.OpenFor != Duration(time.Minute) {
		t.Fatalf("%+v", m)
	}
	if m.Window != Duration(5*time.Second) || m.FreshnessTTL != Duration(time.Second) {
		t.Fatalf("durations changed %+v", m)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestParseManifestFields(t *testing.T) {
	raw := strings.Join([]string{
		"id: github-api",
		"capacity_model: gcra",
		"capacity: 5000",
		"window: 1h",
		"freshness_ttl: 2s",
		"unknown_policy: assume_busy",
		"min_capacity: 10",
		"failure_threshold: 5",
		"open_for: 45s",
	}, "\n")
	m, err := ParseManifest([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	want := Manifest{
		ID:               "github-api",
		CapacityModel:    ModelGCRA,
		Capacity:         5000,
		Window:           Duration(time.Hour),
		FreshnessTTL:     Duration(2 * time.Second),
		UnknownPolicy:    UnknownAssumeBusy,
		MinCapacity:      10,
		FailureThreshold: 5,
		OpenFor:          Duration(45 * time.Second),
	}
	if m != want {
		t.Fatalf("got %+v", m)
	}
	if m.UnknownPolicy.Verdict() != quelor.Defer {
		t.Fatalf("verdict %s", m.UnknownPolicy.Verdict())
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var back Manifest
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back != m {
		t.Fatalf("json round trip %+v", back)
	}
	yb, err := yaml.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	again, err := ParseManifest(yb)
	if err != nil {
		t.Fatal(err)
	}
	if again != m {
		t.Fatalf("yaml round trip %+v", again)
	}
}

func TestParseDefaults(t *testing.T) {
	raw := strings.Join([]string{
		"id: slots",
		"capacity_model: lease",
		"capacity: 4",
		"min_capacity: 0",
		"failure_threshold: 0",
		"open_for: 0s",
	}, "\n")
	m, err := ParseManifest([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	want := Manifest{
		ID:               "slots",
		CapacityModel:    ModelLease,
		Capacity:         4,
		Window:           0,
		FreshnessTTL:     0,
		UnknownPolicy:    UnknownDeferShort,
		MinCapacity:      1,
		FailureThreshold: 3,
		OpenFor:          Duration(30 * time.Second),
	}
	if m != want {
		t.Fatalf("got %+v", m)
	}
}

func TestParseManifestsShapes(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		wantIDs []string
		wantErr string
	}{
		{name: "single", in: "id: a\ncapacity_model: lease\ncapacity: 2\n", wantIDs: []string{"a"}},
		{name: "multi", in: "id: a\ncapacity_model: lease\ncapacity: 3\n---\nid: b\ncapacity_model: window_bucket\ncapacity: 8\nwindow: 1m\n", wantIDs: []string{"a", "b"}},
		{name: "list", in: "resources:\n  - id: a\n    capacity_model: gcra\n    capacity: 10\n    window: 1s\n  - id: b\n    capacity_model: hard_budget\n    capacity: 5\n    unknown_policy: deny\n", wantIDs: []string{"a", "b"}},
		{name: "list then doc", in: "resources:\n  - id: a\n    capacity_model: lease\n    capacity: 1\n---\nid: b\ncapacity_model: circuit\n", wantIDs: []string{"a", "b"}},
		{name: "empty list", in: "resources: []\n", wantIDs: []string{}},
		{name: "unknown field", in: "id: a\ncapacity_model: lease\ncapacity: 1\nnot_a_field: 1\n", wantErr: "not found"},
		{name: "unknown in list", in: "resources:\n  - id: a\n    capacity_model: lease\n    capacity: 1\n    extra: 1\n", wantErr: "not found"},
		{name: "bad duration", in: "id: a\ncapacity_model: gcra\ncapacity: 1\nwindow: nope\n", wantErr: "duration"},
		{name: "non-scalar duration", in: "id: a\ncapacity_model: gcra\ncapacity: 1\nwindow:\n  bad: 1\n", wantErr: "duration"},
		{name: "bad model", in: "id: a\ncapacity_model: nope\ncapacity: 1\n", wantErr: "capacity_model"},
		{name: "gcra missing window", in: "id: a\ncapacity_model: gcra\ncapacity: 1\n", wantErr: "window must be > 0"},
		{name: "fail open", in: "id: a\ncapacity_model: hard_budget\ncapacity: 1\nunknown_policy: hold_cursor\n", wantErr: "hold_cursor"},
		{name: "missing id", in: "capacity_model: lease\ncapacity: 1\n", wantErr: "id required"},
		{name: "syntax", in: "id: [\n", wantErr: "yaml"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseManifests([]byte(tc.in))
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !sameIDs(idsOf(got), tc.wantIDs) {
				t.Fatalf("ids %v want %v", idsOf(got), tc.wantIDs)
			}
		})
	}
}

func TestParseManifestDocumentCount(t *testing.T) {
	one := "id: a\ncapacity_model: lease\ncapacity: 1\n---\n"
	m, err := ParseManifest([]byte(one))
	if err != nil {
		t.Fatal(err)
	}
	if m.ID != "a" || m.UnknownPolicy != UnknownDeferShort {
		t.Fatalf("%+v", m)
	}
	two := "id: a\ncapacity_model: lease\ncapacity: 1\n---\nid: b\ncapacity_model: lease\ncapacity: 1\n"
	if _, err := ParseManifest([]byte(two)); err == nil {
		t.Fatal("expected multi-doc error")
	}
	if _, err := ParseManifest(nil); err == nil {
		t.Fatal("expected empty error")
	}
}

func TestParseListDetails(t *testing.T) {
	raw := strings.Join([]string{
		"resources:",
		"  - id: a",
		"    capacity_model: gcra",
		"    capacity: 10",
		"    window: 1s",
		"    freshness_ttl: 500ms",
		"  - id: b",
		"    capacity_model: hard_budget",
		"    capacity: 5",
		"    unknown_policy: deny",
	}, "\n")
	ms, err := ParseManifests([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 {
		t.Fatalf("len %d", len(ms))
	}
	if ms[0].Window != Duration(time.Second) || ms[0].FreshnessTTL != Duration(500*time.Millisecond) {
		t.Fatalf("a %+v", ms[0])
	}
	if ms[1].UnknownPolicy != UnknownDeny || ms[1].UnknownPolicy.Verdict() != quelor.Deny {
		t.Fatalf("b %+v", ms[1])
	}
	if ms[1].MinCapacity != 1 || ms[1].OpenFor != Duration(30*time.Second) {
		t.Fatalf("b defaults %+v", ms[1])
	}
}

func TestDurationCodec(t *testing.T) {
	tests := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{in: "0s", want: 0, ok: true},
		{in: "30s", want: 30 * time.Second, ok: true},
		{in: "1h", want: time.Hour, ok: true},
		{in: "1h30m", want: time.Hour + 30*time.Minute, ok: true},
		{in: "500ms", want: 500 * time.Millisecond, ok: true},
		{in: "nope", ok: false},
		{in: "1", ok: false},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			n := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: tc.in}
			var d Duration
			err := d.UnmarshalYAML(n)
			if tc.ok {
				if err != nil {
					t.Fatalf("unmarshal: %v", err)
				}
				if time.Duration(d) != tc.want {
					t.Fatalf("got %s want %s", d, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
	v, err := Duration(time.Hour).MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.(string)
	if !ok || s != "1h0m0s" {
		t.Fatalf("yaml %v", v)
	}
	jb, err := json.Marshal(Duration(30 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if string(jb) != `"30s"` {
		t.Fatalf("json %s", jb)
	}
	var back Duration
	if err := json.Unmarshal([]byte(`"1h30m"`), &back); err != nil {
		t.Fatal(err)
	}
	if back != Duration(time.Hour+30*time.Minute) {
		t.Fatalf("json back %s", back)
	}
	if err := json.Unmarshal([]byte(`123`), &back); err == nil {
		t.Fatal("expected json number to fail")
	}
	if err := json.Unmarshal([]byte(`null`), &back); err != nil {
		t.Fatal(err)
	}
	if back != 0 {
		t.Fatalf("null %s", back)
	}
	cleared := Duration(time.Second)
	nullNode := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!null", Value: "null"}
	if err := cleared.UnmarshalYAML(nullNode); err != nil {
		t.Fatal(err)
	}
	if cleared != 0 {
		t.Fatalf("yaml null %s", cleared)
	}
}

func TestJSONManifest(t *testing.T) {
	raw := []byte(`{"id":"bud","capacity_model":"hard_budget","capacity":9,"window":"0s","freshness_ttl":"2s","unknown_policy":"deny","min_capacity":4,"failure_threshold":6,"open_for":"1m"}`)
	var m Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if err := m.Validate(); err != nil {
		t.Fatal(err)
	}
	if m.ID != "bud" || m.CapacityModel != ModelHardBudget || m.Capacity != 9 {
		t.Fatalf("%+v", m)
	}
	if m.FreshnessTTL != Duration(2*time.Second) || m.OpenFor != Duration(time.Minute) || m.UnknownPolicy != UnknownDeny {
		t.Fatalf("%+v", m)
	}
	if m.MinCapacity != 4 || m.FailureThreshold != 6 {
		t.Fatalf("%+v", m)
	}
}

func TestLoadManifestFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "resources.yaml")
	body := strings.Join([]string{
		"id: a",
		"capacity_model: lease",
		"capacity: 1",
		"---",
		"id: b",
		"capacity_model: circuit",
		"failure_threshold: 4",
		"open_for: 1m",
	}, "\n")
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	ms, err := LoadManifestFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !sameIDs(idsOf(ms), []string{"a", "b"}) {
		t.Fatalf("%v", idsOf(ms))
	}
	if ms[1].FailureThreshold != 4 || ms[1].OpenFor != Duration(time.Minute) || ms[1].Capacity != 0 {
		t.Fatalf("%+v", ms[1])
	}
	if _, err := LoadManifestFile(filepath.Join(dir, "missing.yaml")); err == nil {
		t.Fatal("expected missing file error")
	}
}

func idsOf(ms []Manifest) []string {
	out := make([]string, len(ms))
	for i := range ms {
		out[i] = ms[i].ID
	}
	return out
}

func sameIDs(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
