package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/admit"
	"github.com/coherera/quelor/pkg/version"
)

func execCLI(t *testing.T, args []string, stdin string, env func(string) string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	if env == nil {
		env = func(string) string { return "" }
	}
	code := run(args, strings.NewReader(stdin), &out, &errb, env)
	return code, out.String(), errb.String()
}

func githubEnv(token, output string) func(string) string {
	return func(k string) string {
		switch k {
		case "GITHUB_TOKEN":
			return token
		case "GITHUB_OUTPUT":
			return output
		default:
			return ""
		}
	}
}

func rateServer(t *testing.T, status int, remaining int64, body string) *httptest.Server {
	t.Helper()
	reset := time.Date(2026, 10, 31, 15, 0, 0, 0, time.UTC).Unix()
	if body == "" {
		payload := map[string]any{
			"resources": map[string]any{
				"core": map[string]any{
					"limit": 5000, "remaining": remaining, "reset": reset, "used": int64(5000) - remaining,
				},
			},
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		body = string(raw)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/rate_limit" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestAdmitEndToEnd(t *testing.T) {
	cases := []struct {
		name      string
		remaining int64
		optional  bool
		wantExit  int
		verdict   string
	}{
		{"admit", 4950, false, 0, "admit"},
		{"defer", 10, true, 42, "defer"},
		{"degraded", 10, false, 43, "degraded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := rateServer(t, http.StatusOK, tc.remaining, "")
			dir := t.TempDir()
			outPath := filepath.Join(dir, "github_output")
			args := []string{"admit", "--resource", "github_api", "--need", "50", "--api-url", srv.URL + "/"}
			if tc.optional {
				args = append(args, "--optional")
			}
			code, stdout, stderr := execCLI(t, args, "", githubEnv("ghs_SUPERSECRET", outPath))
			if code != tc.wantExit {
				t.Fatalf("exit %d stderr %s stdout %s", code, stderr, stdout)
			}
			wantLine := fmt.Sprintf("%s github_api need=50 remaining=%d reset=2026-10-31T15:00:00Z", tc.verdict, tc.remaining)
			if strings.TrimSpace(stdout) != wantLine {
				t.Fatalf("stdout %q", stdout)
			}
			body, err := os.ReadFile(outPath)
			if err != nil {
				t.Fatal(err)
			}
			wantFile := fmt.Sprintf("verdict=%s\nremaining=%d\n", tc.verdict, tc.remaining)
			if string(body) != wantFile {
				t.Fatalf("GITHUB_OUTPUT %q", body)
			}
			if strings.Contains(stdout+stderr+string(body), "ghs_SUPERSECRET") {
				t.Fatal("token leaked")
			}
		})
	}
}

func TestAdmitJSONDeferReason(t *testing.T) {
	srv := rateServer(t, http.StatusOK, 10, "")
	dir := t.TempDir()
	outPath := filepath.Join(dir, "github_output")
	args := []string{"admit", "--resource", "github_api", "--need", "50", "--optional", "--json", "--api-url", srv.URL}
	code, stdout, stderr := execCLI(t, args, "", githubEnv("tok", outPath))
	if code != 42 {
		t.Fatalf("exit %d stderr %s", code, stderr)
	}
	var got admit.Result
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("json %s: %v", stdout, err)
	}
	if got.Verdict != "defer" || got.Exit != 42 || got.Remaining != 10 || got.Need != 50 || got.Resource != "github_api" {
		t.Fatalf("result %+v", got)
	}
	if !strings.Contains(got.Reason, "2026-10-31T15:00:00Z") || !strings.Contains(got.Reason, "seconds until reset") {
		t.Fatalf("reason %q", got.Reason)
	}
	body, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "verdict=defer\nremaining=10\n" {
		t.Fatalf("GITHUB_OUTPUT %q", body)
	}
}

func TestAdmitMissingTokenAndHTTPError(t *testing.T) {
	t.Run("missing token", func(t *testing.T) {
		dir := t.TempDir()
		outPath := filepath.Join(dir, "github_output")
		args := []string{"admit", "--resource", "github_api", "--need", "50"}
		code, stdout, stderr := execCLI(t, args, "", githubEnv("", outPath))
		if code != 43 {
			t.Fatalf("exit %d stderr %s stdout %s", code, stderr, stdout)
		}
		if strings.TrimSpace(stdout) != "degraded github_api need=50 remaining=0 reset=" {
			t.Fatalf("stdout %q", stdout)
		}
		body, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "verdict=degraded\nremaining=\n" {
			t.Fatalf("GITHUB_OUTPUT %q", body)
		}
	})

	t.Run("non-200", func(t *testing.T) {
		token := "ghs_SUPERSECRET"
		srv := rateServer(t, http.StatusInternalServerError, 0, `{"token":"`+token+`"}`)
		dir := t.TempDir()
		outPath := filepath.Join(dir, "github_output")
		args := []string{"admit", "--resource", "github_api", "--need", "50", "--json", "--api-url", srv.URL}
		code, stdout, stderr := execCLI(t, args, "", githubEnv(token, outPath))
		if code != 43 {
			t.Fatalf("exit %d stderr %s stdout %s", code, stderr, stdout)
		}
		if strings.Contains(stdout+stderr, token) {
			t.Fatalf("token leaked stdout=%s stderr=%s", stdout, stderr)
		}
		var got admit.Result
		if err := json.Unmarshal([]byte(stdout), &got); err != nil {
			t.Fatal(err)
		}
		if got.Verdict != "degraded" || got.Exit != 43 {
			t.Fatalf("result %+v", got)
		}
		body, err := os.ReadFile(outPath)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != "verdict=degraded\nremaining=\n" {
			t.Fatalf("GITHUB_OUTPUT %q", body)
		}
	})
}

func TestAdmitAppendsGitHubOutput(t *testing.T) {
	srv := rateServer(t, http.StatusOK, 4950, "")
	dir := t.TempDir()
	outPath := filepath.Join(dir, "github_output")
	if err := os.WriteFile(outPath, []byte("pre=1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	args := []string{"admit", "--resource", "github_api", "--need", "50", "--api-url", srv.URL}
	code, stdout, stderr := execCLI(t, args, "", githubEnv("tok", outPath))
	if code != 0 {
		t.Fatalf("exit %d stderr %s stdout %s", code, stderr, stdout)
	}
	body, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "pre=1\nverdict=admit\nremaining=4950\n" {
		t.Fatalf("GITHUB_OUTPUT %q", body)
	}
}

const rateLimitLog = "API rate limit exceeded\nYou have exceeded a secondary rate limit\nrate limit\nrate-limit\nToo Many Requests\nHTTP 429\n"

func assertResourceExhausted(t *testing.T, code int, stdout, stderr string) {
	t.Helper()
	if code != 0 {
		t.Fatalf("exit %d stderr %s stdout %s", code, stderr, stdout)
	}
	got := strings.TrimSpace(stdout)
	id := strings.TrimPrefix(got, "resource-exhausted rule=")
	if id == got || id == "" {
		t.Fatalf("stdout %q stderr %s", stdout, stderr)
	}
}

func TestClassify(t *testing.T) {
	t.Run("stdin rate limit", func(t *testing.T) {
		args := []string{"classify", "--exit-code", "1", "--http-status", "429"}
		code, stdout, stderr := execCLI(t, args, rateLimitLog, nil)
		assertResourceExhausted(t, code, stdout, stderr)
	})

	t.Run("log file dash", func(t *testing.T) {
		args := []string{"classify", "--exit-code", "1", "--http-status", "429", "--log-file", "-"}
		code, stdout, stderr := execCLI(t, args, rateLimitLog, nil)
		assertResourceExhausted(t, code, stdout, stderr)
	})

	t.Run("log file path", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "unit.log")
		if err := os.WriteFile(path, []byte(rateLimitLog), 0o644); err != nil {
			t.Fatal(err)
		}
		args := []string{"classify", "--exit-code", "1", "--http-status", "429", "--log-file", path}
		code, stdout, stderr := execCLI(t, args, "", nil)
		assertResourceExhausted(t, code, stdout, stderr)
	})

	t.Run("missing log", func(t *testing.T) {
		args := []string{"classify", "--log-file", filepath.Join(t.TempDir(), "missing.log")}
		code, stdout, stderr := execCLI(t, args, "", nil)
		if code != 2 || stdout != "" || stderr == "" {
			t.Fatalf("exit %d stdout %q stderr %q", code, stdout, stderr)
		}
	})

	t.Run("missing rules", func(t *testing.T) {
		args := []string{"classify", "--rules", filepath.Join(t.TempDir(), "missing.yml")}
		code, _, stderr := execCLI(t, args, "", nil)
		if code != 2 || stderr == "" {
			t.Fatalf("exit %d stderr %q", code, stderr)
		}
	})
}

func TestReplayAndVersion(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "t.jsonl")
	deciderPath := filepath.Join(dir, "ref.yaml")
	if err := os.WriteFile(tracePath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(deciderPath, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Run("replay text", func(t *testing.T) {
		args := []string{"replay", "--trace", tracePath, "--decider", deciderPath}
		code, stdout, stderr := execCLI(t, args, "", nil)
		if code != 0 {
			t.Fatalf("exit %d stderr %s stdout %s", code, stderr, stdout)
		}
	})

	t.Run("replay json", func(t *testing.T) {
		args := []string{"replay", "--trace", tracePath, "--decider", deciderPath, "--json"}
		code, stdout, stderr := execCLI(t, args, "", nil)
		if code != 0 {
			t.Fatalf("exit %d stderr %s stdout %s", code, stderr, stdout)
		}
		if !json.Valid([]byte(stdout)) {
			t.Fatalf("stdout not json %q", stdout)
		}
	})

	t.Run("replay missing trace", func(t *testing.T) {
		args := []string{"replay", "--trace", filepath.Join(dir, "missing.jsonl"), "--decider", deciderPath}
		code, _, stderr := execCLI(t, args, "", nil)
		if code != 1 || stderr == "" {
			t.Fatalf("exit %d stderr %q", code, stderr)
		}
	})

	t.Run("version", func(t *testing.T) {
		code, stdout, stderr := execCLI(t, []string{"version"}, "", nil)
		if code != 0 || stderr != "" {
			t.Fatalf("exit %d stderr %q stdout %q", code, stderr, stdout)
		}
		if strings.TrimSpace(stdout) != strings.TrimSpace(version.String()) {
			t.Fatalf("stdout %q version %q", stdout, version.String())
		}
	})
}

func TestUsage(t *testing.T) {
	cases := []struct {
		name string
		args []string
	}{
		{"no args", nil},
		{"unknown", []string{"frobnicate"}},
		{"help", []string{"--help"}},
		{"admit need zero", []string{"admit", "--resource", "github_api", "--need", "0"}},
		{"admit need negative", []string{"admit", "--resource", "github_api", "--need", "-3"}},
		{"admit missing need", []string{"admit", "--resource", "github_api"}},
		{"admit missing resource", []string{"admit", "--need", "1"}},
		{"admit extra", []string{"admit", "--resource", "github_api", "--need", "1", "nope"}},
		{"classify bad flag", []string{"classify", "--exit-code", "nope"}},
		{"replay missing flags", []string{"replay"}},
		{"replay missing decider", []string{"replay", "--trace", "t.jsonl"}},
		{"version extra", []string{"version", "now"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, stdout, stderr := execCLI(t, tc.args, "", nil)
			if code != 2 {
				t.Fatalf("exit %d stdout %q stderr %q", code, stdout, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout %q", stdout)
			}
			if stderr == "" {
				t.Fatal("empty stderr")
			}
		})
	}
}

func TestMCPFlagsAndSeparator(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		var errb bytes.Buffer
		opts, err := parseMCPArgs([]string{"--", "my-mcp-server"}, &errb)
		if err != nil {
			t.Fatalf("parseMCPArgs: %v", err)
		}
		if opts.rateCount != 30 || opts.rateWindow != time.Minute {
			t.Fatalf("rate = %d/%v, want 30/1m", opts.rateCount, opts.rateWindow)
		}
		if opts.maxConcurrent != 4 || opts.coalesce || opts.tracePath != "" {
			t.Fatalf("unexpected defaults: %+v", opts)
		}
		if opts.cmd != "my-mcp-server" || len(opts.cmdArgs) != 0 {
			t.Fatalf("cmd = %q %v, want my-mcp-server []", opts.cmd, opts.cmdArgs)
		}
	})

	t.Run("all flags", func(t *testing.T) {
		var errb bytes.Buffer
		args := []string{
			"--rate", "12/30s",
			"--max-concurrent", "2",
			"--coalesce",
			"--trace", "/tmp/mcp.jsonl",
			"--",
			"npx", "-y", "@modelcontextprotocol/server-github",
		}
		opts, err := parseMCPArgs(args, &errb)
		if err != nil {
			t.Fatalf("parseMCPArgs: %v", err)
		}
		if opts.rateCount != 12 || opts.rateWindow != 30*time.Second {
			t.Fatalf("rate = %d/%v, want 12/30s", opts.rateCount, opts.rateWindow)
		}
		if opts.maxConcurrent != 2 || !opts.coalesce || opts.tracePath != "/tmp/mcp.jsonl" {
			t.Fatalf("unexpected opts: %+v", opts)
		}
		if opts.cmd != "npx" || len(opts.cmdArgs) != 2 || opts.cmdArgs[0] != "-y" || opts.cmdArgs[1] != "@modelcontextprotocol/server-github" {
			t.Fatalf("cmd = %q %v", opts.cmd, opts.cmdArgs)
		}
	})

	t.Run("missing separator", func(t *testing.T) {
		code, stdout, stderr := execCLI(t, []string{"mcp", "--rate", "10/1m", "my-mcp-server"}, "", nil)
		if code != exitUsage {
			t.Fatalf("exit = %d, want %d (stderr=%q)", code, exitUsage, stderr)
		}
		if stdout != "" {
			t.Fatalf("stdout = %q, want empty", stdout)
		}
		if !strings.Contains(stderr, "--") {
			t.Fatalf("stderr = %q, want mention of --", stderr)
		}
	})

	t.Run("missing command after separator", func(t *testing.T) {
		code, stdout, stderr := execCLI(t, []string{"mcp", "--rate", "10/1m", "--"}, "", nil)
		if code != exitUsage || stdout != "" || stderr == "" {
			t.Fatalf("exit=%d stdout=%q stderr=%q", code, stdout, stderr)
		}
	})
}
