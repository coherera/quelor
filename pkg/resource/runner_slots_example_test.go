package resource

import (
	"path/filepath"
	"testing"
)

func TestRunnerSlotsExample(t *testing.T) {
	ms, err := LoadManifestFile(filepath.Join("..", "..", "examples", "manifests", "runner-slots.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"gha_hosted_jobs": 20, "gha_macos_jobs": 5, "gpu_runner_slots": 2}
	if len(ms) != len(want) {
		t.Fatalf("got %d resources, want %d", len(ms), len(want))
	}
	for _, m := range ms {
		capacity, ok := want[m.ID]
		if !ok || m.CapacityModel != ModelLease || m.Capacity != capacity {
			t.Fatalf("unexpected runner resource: %+v", m)
		}
		delete(want, m.ID)
	}
}
