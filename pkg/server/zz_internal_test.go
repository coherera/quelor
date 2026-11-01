package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHolderTokensFileOverridesMapInternal(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "holders.yaml")
	if err := os.WriteFile(p, []byte("alice: file-token\nbob: file-bob\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		HolderTokens:     map[string]string{"alice": "map-token", "carol": "map-carol"},
		HolderTokensFile: p,
	}
	got := cfg.holderTokens()
	if got["alice"] != "file-token" {
		t.Fatalf("file did not override map: %v", got)
	}
	if got["carol"] != "map-carol" {
		t.Fatalf("map entry missing: %v", got)
	}
	if got["bob"] != "file-bob" {
		t.Fatalf("file entry missing: %v", got)
	}
}
