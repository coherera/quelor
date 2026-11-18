// Package decider defines the pluggable Decider interface. The engine records
// the provenance strings a Decider returns but never interprets them.
package decider

import (
	"context"

	"github.com/coherera/quelor/pkg/decision"
	"github.com/coherera/quelor/pkg/quelor"
)

// APIVersion is sent on every call.
const APIVersion = "1"

// Op names the engine operation that triggered the decision.
type Op = decision.Op

const (
	OpAcquire Op = "acquire"
	OpRenew   Op = "renew"
)

// Status is the Decider's answer state.
type Status string

const (
	StatusAnswered    Status = "answered"
	StatusAbstained   Status = "abstained"
	StatusUnavailable Status = "unavailable"
	StatusKilled      Status = "killed"
)

// Mode is how a ruleset's verdicts are applied by the engine.
type Mode = decision.Mode

const (
	ModeLogOnly = decision.ModeLogOnly // verdict recorded, never applied
	ModeAdvise  = decision.ModeAdvise  // verdict recorded as advisory, never applied
	ModeEnforce = decision.ModeEnforce // verdict applied
)

// CallMeta carries observability context.
type CallMeta struct {
	TraceID string `json:"trace_id"`
	Now     string `json:"now,omitempty"` // RFC3339Nano, engine clock
}

// Request is the same JSON for in-process and HTTP sidecar Deciders.
type Request struct {
	DeciderAPIVersion string           `json:"decider_api_version"`
	Op                Op               `json:"op"`
	Envelope          quelor.Envelope  `json:"envelope"`
	Lease             *quelor.Lease    `json:"lease,omitempty"`
	SpendDelta        map[string]int64 `json:"spend_delta,omitempty"`
	Readings          []quelor.Reading `json:"readings"`
	PolicyProfile     string           `json:"policy_profile,omitempty"`
	ExpectRulesetHash string           `json:"expect_ruleset_hash,omitempty"`
	Meta              CallMeta         `json:"call_meta"`
}

// Provenance is decider-defined and opaque to the engine.
type Provenance = decision.Provenance

// Response is a Decider's answer.
type Response struct {
	DeciderAPIVersion string           `json:"decider_api_version"`
	Status            Status           `json:"status"`
	Mode              Mode             `json:"mode,omitempty"` // empty means enforce
	Verdict           quelor.Verdict   `json:"verdict"`
	DecisionRef       string           `json:"decision_ref"`
	RulesetID         string           `json:"ruleset_id,omitempty"`
	RulesetHash       string           `json:"ruleset_hash,omitempty"`
	RuleIDs           []string         `json:"rule_ids,omitempty"`
	Readings          []quelor.Reading `json:"readings,omitempty"`
	Abstains          []string         `json:"abstains,omitempty"`
	Provenance        Provenance       `json:"provenance"`
	TraceID           string           `json:"trace_id,omitempty"`
	LatencyMS         float64          `json:"latency_ms"`
	DeciderID         string           `json:"decider_id"`
}

// Enforced reports whether the engine should apply this response's verdict.
func (r Response) Enforced() bool {
	return r.Status == StatusAnswered && (r.Mode == "" || r.Mode == ModeEnforce)
}

// Decider decides one request. Implementations must be safe for concurrent use
// and must never be a harder dependency than the work they protect.
type Decider interface {
	Decide(ctx context.Context, req Request) (Response, error)
}

// Func adapts a function to Decider.
type Func func(ctx context.Context, req Request) (Response, error)

func (f Func) Decide(ctx context.Context, req Request) (Response, error) { return f(ctx, req) }
