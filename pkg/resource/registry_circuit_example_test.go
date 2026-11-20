package resource

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRegistryCircuitExample(t *testing.T) {
	ms, err := LoadManifestFile(filepath.Join("..", "..", "examples", "manifests", "registry-circuit.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 {
		t.Fatalf("got %d resources, want 1", len(ms))
	}
	m := ms[0]
	if m.ID != "maven_central" || m.CapacityModel != ModelCircuit || m.FailureThreshold != 3 || m.OpenFor != Duration(time.Minute) {
		t.Fatalf("unexpected registry circuit: %+v", m)
	}
}
