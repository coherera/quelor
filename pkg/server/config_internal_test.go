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
	got, err := cfg.holderTokens()
	if err != nil {
		t.Fatal(err)
	}
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

func TestHolderTokensFileMissingInternal(t *testing.T) {
	cfg := Config{HolderTokensFile: filepath.Join(t.TempDir(), "does-not-exist.yaml")}
	if _, err := cfg.holderTokens(); err == nil {
		t.Fatal("expected an error for a missing holder_tokens_file")
	}
}

func TestHolderTokensFileBadYAMLInternal(t *testing.T) {
	p := filepath.Join(t.TempDir(), "holders.yaml")
	if err := os.WriteFile(p, []byte("alice: [unclosed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := Config{HolderTokensFile: p}
	if _, err := cfg.holderTokens(); err == nil {
		t.Fatal("expected an error for invalid holder_tokens_file YAML")
	}
}

func TestHolderTokensEnvUnsetInternal(t *testing.T) {
	const name = "QUELOR_TEST_HOLDER_TOKENS_UNSET"
	os.Unsetenv(name)
	cfg := Config{HolderTokensEnv: name}
	if _, err := cfg.holderTokens(); err == nil {
		t.Fatal("expected an error for an unset holder_tokens_env")
	}
}

func TestHolderTokensEnvNoEntriesInternal(t *testing.T) {
	const name = "QUELOR_TEST_HOLDER_TOKENS_EMPTY"
	t.Setenv(name, "{}")
	cfg := Config{HolderTokensEnv: name}
	if _, err := cfg.holderTokens(); err == nil {
		t.Fatal("expected an error for a holder_tokens_env with no entries")
	}
}
