package engine

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/classify"
	"github.com/coherera/quelor/pkg/quelor"
	"github.com/coherera/quelor/pkg/resource"
)

func TestRegistryCircuitExampleLifecycle(t *testing.T) {
	ms, err := resource.LoadManifestFile(filepath.Join("..", "..", "examples", "manifests", "registry-circuit.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	clk := baseClock()
	e, _, _ := newTestEngine(t, clk, ms, nil, func(c *Config) { c.Classifier = classify.Default() })
	ctx := context.Background()
	next := 0
	acquire := func(want quelor.VerdictKind) *quelor.Lease {
		t.Helper()
		next++
		got, err := e.AcquireLease(ctx, quelor.AcquireRequest{
			Envelope: work(fmt.Sprintf("registry-%d", next), "h1", map[string]int64{"maven_central": 1}, ""),
		})
		if err != nil || got.Verdict.Kind != want {
			t.Fatalf("acquire: %+v, %v; want %s", got, err, want)
		}
		if (got.Lease != nil) != (want == quelor.Admit) {
			t.Fatalf("unexpected lease: %+v", got)
		}
		return got.Lease
	}
	settle := func(lease *quelor.Lease, status int) {
		t.Helper()
		req := quelor.SettleRequest{LeaseID: lease.ID, HolderID: "h1"}
		if status == 0 {
			req.Outcome = &quelor.ExecutionOutcome{ExitCode: 1, Log: "npm error code E429"}
		} else {
			req.Observations = []quelor.Observation{{Resource: "maven_central", HTTPStatus: status}}
		}
		if _, err := e.SettleLease(ctx, req); err != nil {
			t.Fatal(err)
		}
	}
	// Log classification alone supplies no HTTP observation and cannot trip it.
	for i := 0; i < 3; i++ {
		settle(acquire(quelor.Admit), 0)
	}
	// A bare 403 is not a throttling observation for this breaker.
	for i := 0; i < 3; i++ {
		settle(acquire(quelor.Admit), 403)
	}
	for i := 0; i < 3; i++ {
		settle(acquire(quelor.Admit), 429)
	}
	acquire(quelor.Defer)
	clk.t = clk.t.Add(time.Minute)
	e.Advance(clk.t)
	probe := acquire(quelor.Admit)
	acquire(quelor.Defer) // Only one half-open probe may run.
	settle(probe, 429)
	acquire(quelor.Defer)
	clk.t = clk.t.Add(time.Minute)
	e.Advance(clk.t)
	settle(acquire(quelor.Admit), 200)
	settle(acquire(quelor.Admit), 200)
}
