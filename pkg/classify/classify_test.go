package classify

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/coherera/quelor/pkg/quelor"
)

func oc(exit, http int, log string, timed bool) quelor.ExecutionOutcome {
	return quelor.ExecutionOutcome{
		ExitCode:   exit,
		HTTPStatus: http,
		Log:        log,
		TimedOut:   timed,
	}
}

func TestDefaultShape(t *testing.T) {
	rs := Default()
	if rs != Default() {
		t.Fatal("Default returned distinct rulesets")
	}
	if rs.ID != "gha-default" {
		t.Fatalf("id %q", rs.ID)
	}
	if rs.Default != quelor.Unknown {
		t.Fatalf("default %q", rs.Default)
	}
	want := []struct {
		id    string
		class quelor.OutcomeClass
		re    string
	}{
		{"gha-rate-limit", quelor.ResourceExhausted, `(?i)(RATE_LIMITED|API rate limit already exceeded|API rate limit exceeded|secondary rate limit|rate limit (hit|reached)|abuse detection|HTTP 429|Too Many Requests)`},
		{"gha-dependency", quelor.DependencyUnavailable, `(?i)(503 Service Unavailable|502 Bad Gateway|504 Gateway Time-?out|connection (timed out|refused|reset by peer)|i/o timeout|context deadline exceeded|ETIMEDOUT|ECONNRESET|ECONNREFUSED|TLS handshake timeout|dial tcp .*: (connect|i/o))`},
		{"gha-flaky", quelor.Flaky, `(?i)(runner has received a shutdown signal|lost communication with the server|hosted runner .* encountered an error|The operation was canceled|No space left on device on runner)`},
		{"gha-real-defect", quelor.RealDefect, `(?m)(^--- FAIL|^FAIL\s|panic:|AssertionError|Traceback \(most recent call last\)|error\[E\d+\]|SyntaxError|^npm ERR! code ELIFECYCLE|compilation failed|undefined: |cannot find symbol)`},
	}
	// The four original rules keep their ids, classes and relative order;
	// targeted rule changes update their expected regexes here.
	byID := map[string]int{}
	for i, r := range rs.Rules {
		byID[r.ID] = i
		if r.Note == "" {
			t.Fatalf("rule %s note empty", r.ID)
		}
		if r.re == nil && r.LogRegex != "" {
			t.Fatalf("rule %s regex not compiled", r.ID)
		}
	}
	prev := -1
	for _, w := range want {
		i, ok := byID[w.id]
		if !ok {
			t.Fatalf("rule %s missing", w.id)
		}
		if i <= prev {
			t.Fatalf("rule %s out of order", w.id)
		}
		prev = i
		r := rs.Rules[i]
		if r.Class != w.class || r.LogRegex != w.re {
			t.Fatalf("rule %s changed:\n got %s %q\nwant %s %q", w.id, r.Class, r.LogRegex, w.class, w.re)
		}
	}
	if rs.Rules[1].TimedOut != nil {
		t.Fatal("gha-dependency must not match on TimedOut alone; gha-wall-clock owns it")
	}
	if len(rs.Rules[1].HTTPStatus) != 3 || rs.Rules[1].HTTPStatus[0] != 502 || rs.Rules[1].HTTPStatus[1] != 503 || rs.Rules[1].HTTPStatus[2] != 504 {
		t.Fatalf("dependency statuses %+v", rs.Rules[1].HTTPStatus)
	}
	if len(rs.Rules[0].HTTPStatus) != 1 || rs.Rules[0].HTTPStatus[0] != 429 {
		t.Fatalf("rate statuses %+v", rs.Rules[0].HTTPStatus)
	}
}

func TestLoadEmbeddedFile(t *testing.T) {
	fromDisk, err := Load("default_gha.yaml")
	if err != nil {
		t.Fatal(err)
	}
	fromEmbed := Default()
	if fromDisk.ID != fromEmbed.ID || fromDisk.Default != fromEmbed.Default || len(fromDisk.Rules) != len(fromEmbed.Rules) {
		t.Fatalf("disk and embed differ")
	}
	for i := range fromEmbed.Rules {
		d, e := fromDisk.Rules[i], fromEmbed.Rules[i]
		if d.ID != e.ID || d.Class != e.Class || d.LogRegex != e.LogRegex {
			t.Fatalf("rule %d differs", i)
		}
		if d.re == nil {
			t.Fatalf("disk rule %s not compiled", d.ID)
		}
	}
}

func TestClassifyDefault(t *testing.T) {
	rs := Default()
	ex := quelor.ResourceExhausted
	dep := quelor.DependencyUnavailable
	flaky := quelor.Flaky
	defect := quelor.RealDefect
	unk := quelor.Unknown
	rate := "gha-rate-limit"
	gdep := "gha-dependency"
	gflaky := "gha-flaky"
	gdef := "gha-real-defect"

	tests := []struct {
		name string
		in   quelor.ExecutionOutcome
		want Result
	}{
		{"zero value success", oc(0, 0, "", false), Result{RuleID: "success"}},
		{"http 200 success", oc(0, 200, "", false), Result{RuleID: "success"}},
		{"http 399 success", oc(0, 399, "", false), Result{RuleID: "success"}},
		{"success ignores panic log", oc(0, 200, "panic: ignored on green exit", false), Result{RuleID: "success"}},
		{"success ignores fail log", oc(0, 0, "--- FAIL: TestAdd\n", false), Result{RuleID: "success"}},

		{"rate by status", oc(1, 429, "", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"rate by status exit 0", oc(0, 429, "", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"rate log api", oc(1, 0, "API rate limit exceeded", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"rate log case", oc(1, 0, "api RATE LIMIT exceeded", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"rate secondary", oc(1, 0, "secondary rate limit", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"rate hit", oc(1, 0, "rate limit hit", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"rate reached", oc(1, 0, "rate limit reached", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"abuse detection", oc(1, 0, "abuse detection mechanism", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"http 429 text", oc(1, 0, "HTTP 429", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"too many requests", oc(1, 0, "Too Many Requests", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"too many requests lower", oc(1, 0, "too many requests", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"rate masks fail", oc(1, 0, "API rate limit exceeded\n--- FAIL: TestShip\n", false), Result{Failed: true, Class: ex, RuleID: rate}},
		{"429 masks timeout and fail", oc(1, 429, "--- FAIL: TestShip\n", true), Result{Failed: true, Class: ex, RuleID: rate}},

		{"503 status", oc(1, 503, "", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"502 status", oc(0, 502, "", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"504 status", oc(7, 504, "--- FAIL: TestX\n", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"timeout flag", oc(1, 0, "", true), Result{Failed: true, Class: quelor.ResourceExhausted, RuleID: "gha-wall-clock"}},
		{"timeout exit 0", oc(0, 200, "", true), Result{Failed: true, Class: quelor.ResourceExhausted, RuleID: "gha-wall-clock"}},
		{"timeout masks fail", oc(1, 0, "--- FAIL: TestAdd (0.01s)\n", true), Result{Failed: true, Class: quelor.ResourceExhausted, RuleID: "gha-wall-clock"}},
		{"503 text", oc(1, 0, "503 Service Unavailable", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"503 text lower", oc(1, 0, "503 service unavailable", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"502 text", oc(1, 0, "502 Bad Gateway", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"504 timeout spelling", oc(1, 0, "504 Gateway Timeout", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"504 hyphen spelling", oc(1, 0, "504 Gateway Time-out", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"connection timed out", oc(1, 0, "connection timed out", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"connection refused", oc(1, 0, "connection refused", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"connection reset", oc(1, 0, "connection reset by peer", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"io timeout", oc(1, 0, "i/o timeout", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"deadline", oc(1, 0, "context deadline exceeded", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"etimedout", oc(1, 0, "read tcp: ETIMEDOUT", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"econnreset", oc(1, 0, "ECONNRESET", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"econnrefused", oc(1, 0, "dial: ECONNREFUSED", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"tls handshake", oc(1, 0, "TLS handshake timeout", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"dial connect", oc(1, 0, "dial tcp 192.0.2.10:443: connect", false), Result{Failed: true, Class: dep, RuleID: gdep}},
		{"dial io", oc(1, 0, "dial tcp 192.0.2.10:443: i/o", false), Result{Failed: true, Class: dep, RuleID: gdep}},

		{"runner shutdown", oc(1, 0, "The runner has received a shutdown signal", false), Result{Failed: true, Class: flaky, RuleID: gflaky}},
		{"lost communication", oc(1, 0, "lost communication with the server", false), Result{Failed: true, Class: flaky, RuleID: gflaky}},
		{"hosted runner", oc(1, 0, "The hosted runner GitHub Actions 42 encountered an error", false), Result{Failed: true, Class: flaky, RuleID: gflaky}},
		{"operation canceled", oc(1, 0, "The operation was canceled", false), Result{Failed: true, Class: flaky, RuleID: gflaky}},
		{"operation canceled lower", oc(1, 0, "the operation was canceled", false), Result{Failed: true, Class: flaky, RuleID: gflaky}},
		{"no space", oc(1, 0, "No space left on device on runner", false), Result{Failed: true, Class: flaky, RuleID: gflaky}},

		{"go fail block", oc(1, 0, "=== RUN   TestAdd\n--- FAIL: TestAdd (0.01s)\n    add_test.go:12: got 1 want 2\n", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"fail line", oc(1, 0, "some output\nFAIL github.com/coherera/pkg [build failed]\n", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"panic", oc(1, 0, "goroutine 1 [running]:\npanic: nil pointer dereference", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"assertion", oc(1, 0, "AssertionError: expected 1", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"traceback", oc(1, 0, "Traceback (most recent call last):\n  File \"t.py\", line 1, in <module>\n", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"rust error", oc(1, 0, "error[E0308]: mismatched types", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"syntax", oc(1, 0, "SyntaxError: unexpected token", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"npm lifecycle", oc(1, 0, "npm ERR! code ELIFECYCLE", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"npm lifecycle next line", oc(1, 0, "prelude\nnpm ERR! code ELIFECYCLE\n", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"compilation failed", oc(1, 0, "compilation failed", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"undefined", oc(1, 0, "undefined: foo", false), Result{Failed: true, Class: defect, RuleID: gdef}},
		{"cannot find symbol", oc(1, 0, "cannot find symbol", false), Result{Failed: true, Class: defect, RuleID: gdef}},

		{"unknown nonzero", oc(1, 0, "", false), Result{Failed: true, Class: unk}},
		{"unknown exit 2", oc(2, 0, "all good honestly", false), Result{Failed: true, Class: unk}},
		{"http 400", oc(0, 400, "", false), Result{Failed: true, Class: unk}},
		{"http 500", oc(1, 500, "", false), Result{Failed: true, Class: unk}},
		{"bare rate words", oc(1, 0, "rate limit", false), Result{Failed: true, Class: unk}},
		{"panic upper", oc(1, 0, "PANIC: boom", false), Result{Failed: true, Class: unk}},
		{"fail lower", oc(1, 0, "--- fail: TestAdd\n", false), Result{Failed: true, Class: unk}},
		{"failure word", oc(1, 0, "FAILURE: boom", false), Result{Failed: true, Class: unk}},
		{"fail not at line start", oc(1, 0, "oops --- FAIL: TestAdd", false), Result{Failed: true, Class: unk}},
		{"npm not at line start", oc(1, 0, "prefix npm ERR! code ELIFECYCLE", false), Result{Failed: true, Class: unk}},
		{"undefined without space", oc(1, 0, "undefined:foo", false), Result{Failed: true, Class: unk}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rs.Classify(tt.in)
			if got != tt.want {
				t.Fatalf("Classify() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestClassifyCustom(t *testing.T) {
	t.Run("or exit or status", func(t *testing.T) {
		rs := mustParse(t, `
id: or-rule
default: real-defect
rules:
  - id: either
    class: flaky
    exit_codes: [7]
    http_status: [418]
`)
		cases := []struct {
			name string
			in   quelor.ExecutionOutcome
			want Result
		}{
			{"exit", oc(7, 200, "", false), Result{Failed: true, Class: quelor.Flaky, RuleID: "either"}},
			{"status", oc(1, 418, "", false), Result{Failed: true, Class: quelor.Flaky, RuleID: "either"}},
			{"both", oc(7, 418, "", false), Result{Failed: true, Class: quelor.Flaky, RuleID: "either"}},
			{"neither", oc(1, 200, "", false), Result{Failed: true, Class: quelor.RealDefect}},
			{"status on green exit", oc(0, 418, "", false), Result{Failed: true, Class: quelor.Flaky, RuleID: "either"}},
			{"clean success", oc(0, 200, "", false), Result{RuleID: "success"}},
		}
		for _, tt := range cases {
			t.Run(tt.name, func(t *testing.T) {
				got := rs.Classify(tt.in)
				if got != tt.want {
					t.Fatalf("got %+v want %+v", got, tt.want)
				}
			})
		}
	})

	t.Run("first rule wins", func(t *testing.T) {
		rs := mustParse(t, `
id: order
default: unknown
rules:
  - id: first
    class: resource-exhausted
    log_regex: 'SAME'
  - id: second
    class: real-defect
    log_regex: 'SAME'
`)
		got := rs.Classify(oc(1, 0, "SAME", false))
		want := Result{Failed: true, Class: quelor.ResourceExhausted, RuleID: "first"}
		if got != want {
			t.Fatalf("got %+v want %+v", got, want)
		}
	})

	t.Run("case sensitivity", func(t *testing.T) {
		rs := mustParse(t, `
id: case
default: unknown
rules:
  - id: sensitive
    class: real-defect
    log_regex: 'Panic:'
  - id: insensitive
    class: flaky
    log_regex: '(?i)CANCELED'
`)
		cases := []struct {
			log  string
			want Result
		}{
			{"Panic: boom", Result{Failed: true, Class: quelor.RealDefect, RuleID: "sensitive"}},
			{"panic: boom", Result{Failed: true, Class: quelor.Unknown}},
			{"was canceled", Result{Failed: true, Class: quelor.Flaky, RuleID: "insensitive"}},
			{"was CANCELED", Result{Failed: true, Class: quelor.Flaky, RuleID: "insensitive"}},
		}
		for _, tt := range cases {
			got := rs.Classify(oc(1, 0, tt.log, false))
			if got != tt.want {
				t.Fatalf("log %q: got %+v want %+v", tt.log, got, tt.want)
			}
		}
	})

	t.Run("timed out pointer", func(t *testing.T) {
		rs := mustParse(t, `
id: clocks
default: unknown
rules:
  - id: timed
    class: dependency-unavailable
    timed_out: true
  - id: not-timed
    class: flaky
    timed_out: false
`)
		timed := rs.Classify(oc(1, 0, "", true))
		if timed != (Result{Failed: true, Class: quelor.DependencyUnavailable, RuleID: "timed"}) {
			t.Fatalf("timed %+v", timed)
		}
		notTimed := rs.Classify(oc(1, 0, "", false))
		if notTimed != (Result{Failed: true, Class: quelor.Flaky, RuleID: "not-timed"}) {
			t.Fatalf("not timed %+v", notTimed)
		}
		success := rs.Classify(oc(0, 0, "", false))
		if success != (Result{RuleID: "success"}) {
			t.Fatalf("success %+v", success)
		}
	})

	t.Run("exit codes", func(t *testing.T) {
		rs := mustParse(t, `
id: exits
default: unknown
rules:
  - id: specific
    class: real-defect
    exit_codes: [2, 9, -1]
`)
		for _, code := range []int{2, 9, -1} {
			got := rs.Classify(oc(code, 0, "", false))
			want := Result{Failed: true, Class: quelor.RealDefect, RuleID: "specific"}
			if got != want {
				t.Fatalf("exit %d: got %+v", code, got)
			}
		}
		if got := rs.Classify(oc(3, 0, "", false)); got != (Result{Failed: true, Class: quelor.Unknown}) {
			t.Fatalf("exit 3: %+v", got)
		}
		if got := rs.Classify(oc(0, 0, "", false)); got.RuleID != "success" || got.Failed {
			t.Fatalf("exit 0: %+v", got)
		}
	})

	t.Run("http status zero is a value", func(t *testing.T) {
		rs := mustParse(t, `
id: zero
default: flaky
rules:
  - id: status-zero
    class: unknown
    http_status: [0]
`)
		got := rs.Classify(oc(1, 0, "", false))
		if got != (Result{Failed: true, Class: quelor.Unknown, RuleID: "status-zero"}) {
			t.Fatalf("status 0: %+v", got)
		}
		other := rs.Classify(oc(1, 1, "", false))
		if other != (Result{Failed: true, Class: quelor.Flaky}) {
			t.Fatalf("status 1: %+v", other)
		}
		if got := rs.Classify(oc(0, 0, "", false)); got.RuleID != "success" {
			t.Fatalf("clean: %+v", got)
		}
	})

	t.Run("empty rule list uses default", func(t *testing.T) {
		rs := mustParse(t, "id: empty\ndefault: flaky\nrules: []\n")
		got := rs.Classify(oc(4, 0, "panic: still no rules", false))
		if got != (Result{Failed: true, Class: quelor.Flaky}) {
			t.Fatalf("%+v", got)
		}
	})

	t.Run("note round trip", func(t *testing.T) {
		rs := mustParse(t, `
id: noted
default: unknown
rules:
  - id: a
    class: flaky
    exit_codes: [4]
    note: runner flake
`)
		if rs.ID != "noted" || rs.Rules[0].Note != "runner flake" || rs.Rules[0].ID != "a" {
			t.Fatalf("%+v", rs)
		}
	})
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "bad class",
			yaml: "id: bad\ndefault: unknown\nrules:\n  - id: c\n    class: banana\n    exit_codes: [1]\n",
			want: "banana",
		},
		{
			name: "bad class case",
			yaml: "id: bad\ndefault: unknown\nrules:\n  - id: c\n    class: Flaky\n    exit_codes: [1]\n",
			want: "Flaky",
		},
		{
			name: "bad default",
			yaml: "id: bad\ndefault: banana\nrules: []\n",
			want: "banana",
		},
		{
			name: "bad regex",
			yaml: "id: bad\ndefault: unknown\nrules:\n  - id: re\n    class: flaky\n    log_regex: '('\n",
			want: "log_regex",
		},
		{
			name: "no conditions",
			yaml: "id: bad\ndefault: unknown\nrules:\n  - id: bare\n    class: flaky\n    note: nothing here\n",
			want: "no conditions",
		},
		{
			name: "empty condition lists",
			yaml: "id: bad\ndefault: unknown\nrules:\n  - id: bare\n    class: flaky\n    exit_codes: []\n    http_status: []\n    log_regex: ''\n",
			want: "no conditions",
		},
		{
			name: "unknown field",
			yaml: "id: bad\ndefault: unknown\nnope: true\n",
			want: "not found",
		},
		{
			name: "unknown rule field",
			yaml: "id: bad\ndefault: unknown\nrules:\n  - id: c\n    class: flaky\n    exit_code: [1]\n",
			want: "not found",
		},
		{
			name: "empty",
			yaml: "",
			want: "empty document",
		},
		{
			name: "whitespace",
			yaml: " \n\t\n",
			want: "empty document",
		},
		{
			name: "malformed",
			yaml: ":\n  - [\n",
			want: "parse yaml",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs, err := Parse([]byte(tt.yaml))
			if err == nil {
				t.Fatalf("expected error, got ruleset %+v", rs)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tt.want)
			}
		})
	}
}

func TestLoad(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	body := []byte("id: t\ndefault: unknown\nrules:\n  - id: e\n    class: flaky\n    exit_codes: [9]\n")
	if err := os.WriteFile(path, body, 0o644); err != nil {
		t.Fatal(err)
	}
	rs, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	got := rs.Classify(oc(9, 0, "", false))
	want := Result{Failed: true, Class: quelor.Flaky, RuleID: "e"}
	if got != want {
		t.Fatalf("got %+v want %+v", got, want)
	}
	if rs.Rules[0].re != nil {
		t.Fatal("exit-code rule compiled a regex")
	}
}

func TestRuleJSONRoundTrip(t *testing.T) {
	to := true
	in := Rule{
		ID:         "r1",
		Class:      quelor.Flaky,
		ExitCodes:  []int{1, 2},
		HTTPStatus: []int{429},
		LogRegex:   "abc",
		TimedOut:   &to,
		Note:       "n",
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"id", "class", "exit_codes", "http_status", "log_regex", "timed_out", "note"} {
		if _, ok := raw[k]; !ok {
			t.Fatalf("missing key %s in %s", k, b)
		}
	}
	if _, ok := raw["re"]; ok {
		t.Fatalf("compiled regex leaked into json: %s", b)
	}
	var out Rule
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.ID != in.ID || out.Class != in.Class || out.LogRegex != in.LogRegex || out.Note != in.Note {
		t.Fatalf("round trip %+v", out)
	}
	if out.TimedOut == nil || !*out.TimedOut {
		t.Fatalf("timed_out %+v", out.TimedOut)
	}
	if len(out.ExitCodes) != 2 || out.ExitCodes[0] != 1 || out.ExitCodes[1] != 2 {
		t.Fatalf("exit codes %+v", out.ExitCodes)
	}
	if len(out.HTTPStatus) != 1 || out.HTTPStatus[0] != 429 {
		t.Fatalf("http %+v", out.HTTPStatus)
	}
}

func TestClassifyConcurrent(t *testing.T) {
	rs := Default()
	type sample struct {
		in   quelor.ExecutionOutcome
		want Result
	}
	samples := []sample{
		{oc(0, 204, "panic: ignored", false), Result{RuleID: "success"}},
		{oc(1, 0, "API rate limit exceeded\n--- FAIL: TestX\n", false), Result{Failed: true, Class: quelor.ResourceExhausted, RuleID: "gha-rate-limit"}},
		{oc(1, 503, "", false), Result{Failed: true, Class: quelor.DependencyUnavailable, RuleID: "gha-dependency"}},
		{oc(1, 0, "The operation was canceled", false), Result{Failed: true, Class: quelor.Flaky, RuleID: "gha-flaky"}},
		{oc(1, 0, "--- FAIL: TestAdd\n", false), Result{Failed: true, Class: quelor.RealDefect, RuleID: "gha-real-defect"}},
		{oc(2, 0, "", false), Result{Failed: true, Class: quelor.Unknown}},
	}

	var wg sync.WaitGroup
	errCh := make(chan string, 1)
	report := func(format string, args ...any) {
		select {
		case errCh <- fmt.Sprintf(format, args...):
		default:
		}
	}
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 40; i++ {
				tt := samples[i%len(samples)]
				got := rs.Classify(tt.in)
				if got != tt.want {
					report("classify got %+v want %+v", got, tt.want)
					return
				}
			}
		}()
	}
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if Default() != rs {
				report("Default pointer changed")
				return
			}
			parsed, err := Parse(defaultGHA)
			if err != nil {
				report("parse embed: %v", err)
				return
			}
			got := parsed.Classify(samples[1].in)
			if got != samples[1].want {
				report("parsed classify got %+v", got)
			}
		}()
	}
	wg.Wait()
	select {
	case msg := <-errCh:
		t.Fatal(msg)
	default:
	}
}

func mustParse(t *testing.T, y string) *Ruleset {
	t.Helper()
	rs, err := Parse([]byte(y))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return rs
}

// PROVES-RED: before the fix gha-dependency carried timed_out: true and preceded
// gha-wall-clock, so every TimedOut outcome classed as dependency-unavailable.
func TestTimedOutWithoutDependencyEvidenceIsWallClock(t *testing.T) {
	rs := Default()
	wall := Result{Failed: true, Class: quelor.ResourceExhausted, RuleID: "gha-wall-clock"}
	if got := rs.Classify(oc(1, 0, "step output only", true)); got != wall {
		t.Fatalf("bare timeout: %+v", got)
	}
	if got := rs.Classify(oc(1, 0, "", true)); got != wall {
		t.Fatalf("empty-log timeout: %+v", got)
	}
	dep := Result{Failed: true, Class: quelor.DependencyUnavailable, RuleID: "gha-dependency"}
	if got := rs.Classify(oc(1, 0, "dial tcp 10.0.0.1:443: i/o timeout", true)); got != dep {
		t.Fatalf("timeout with dependency evidence: %+v", got)
	}
	if got := rs.Classify(oc(1, 503, "", true)); got != dep {
		t.Fatalf("timeout with 503: %+v", got)
	}
}
