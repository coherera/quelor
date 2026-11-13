package classify

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/coherera/quelor/pkg/quelor"
)

type signatureFixture struct {
	Name       string `json:"name"`
	Log        string `json:"log"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	HTTPStatus int    `json:"http_status,omitempty"`
	TimedOut   bool   `json:"timed_out,omitempty"`
	WantClass  string `json:"want_class"`
	WantRule   string `json:"want_rule"`
}

// TestGHASignatureFixtures pins the generic GitHub Actions error signatures in
// testdata/gha_signatures.json: each is a sanitized, hand-written log line of the
// shape the real platform prints, with the class and the rule id (sub-reason)
// the default ruleset must return. Rows with want_class "unknown" guard against
// over-matching: text that names no infrastructure cause must stay unknown.
func TestGHASignatureFixtures(t *testing.T) {
	b, err := os.ReadFile("testdata/gha_signatures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx []signatureFixture
	if err := json.Unmarshal(b, &fx); err != nil {
		t.Fatal(err)
	}
	if len(fx) < 30 {
		t.Fatalf("only %d fixtures", len(fx))
	}
	rs := Default()
	for _, f := range fx {
		exit := 1
		if f.ExitCode != nil {
			exit = *f.ExitCode
		}
		got := rs.Classify(quelor.ExecutionOutcome{
			ExitCode:   exit,
			HTTPStatus: f.HTTPStatus,
			Log:        f.Log,
			TimedOut:   f.TimedOut,
		})
		if string(got.Class) != f.WantClass || got.RuleID != f.WantRule {
			t.Errorf("%s: got %s/%q want %s/%q", f.Name, got.Class, got.RuleID, f.WantClass, f.WantRule)
		}
		if !got.Failed {
			t.Errorf("%s: not a failure", f.Name)
		}
	}
}
