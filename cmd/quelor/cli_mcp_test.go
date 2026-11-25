package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/trace"
)

// TestMCPHelperProcess is the child server the mcp tests spawn: the test binary
// re-executed with QUELOR_TEST_MCP_CHILD set. It is a no-op otherwise.
func TestMCPHelperProcess(t *testing.T) {
	switch os.Getenv("QUELOR_TEST_MCP_CHILD") {
	case "exit3":
		os.Exit(3)
	case "echo":
		// Answer every tools/call with an ok result until stdin closes.
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
			var req struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			if json.Unmarshal(sc.Bytes(), &req) != nil || req.Method != "tools/call" {
				continue
			}
			os.Stdout.WriteString(`{"jsonrpc":"2.0","id":` + string(req.ID) + `,"result":{"content":[{"type":"text","text":"ok"}]}}` + "\n")
		}
		os.Exit(0)
	case "block":
		// Hold until stdin closes or the proxy kills us.
		sc := bufio.NewScanner(os.Stdin)
		for sc.Scan() {
		}
		os.Exit(0)
	}
}

func mcpChildArgs(t *testing.T, mode string, flags ...string) []string {
	t.Helper()
	t.Setenv("QUELOR_TEST_MCP_CHILD", mode)
	args := append([]string{"mcp"}, flags...)
	return append(args, "--", os.Args[0], "-test.run=^TestMCPHelperProcess$")
}

func TestMCPExitCodePropagates(t *testing.T) {
	code, stdout, stderr := execCLI(t, mcpChildArgs(t, "exit3"), "", nil)
	if code != 3 {
		t.Fatalf("exit = %d, want 3 (stdout=%q stderr=%q)", code, stdout, stderr)
	}
}

func TestMCPTraceFileIsReadableByTraceReader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mcp.jsonl")
	call := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"search_code","arguments":{"q":"x"}}}` + "\n"

	code, stdout, stderr := execCLI(t, mcpChildArgs(t, "echo", "--trace", path), call, nil)
	if code != 0 {
		t.Fatalf("exit = %d (stderr=%q)", code, stderr)
	}
	if !strings.Contains(stdout, `"text":"ok"`) {
		t.Fatalf("stdout = %q, want the child's reply", stdout)
	}
	if !strings.Contains(stderr, `[quelor] ADMIT id=1 tool="search_code" reason=ok`) {
		t.Fatalf("stderr = %q, want the ADMIT line", stderr)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	recs, err := trace.ReadAll(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("trace.ReadAll: %v", err)
	}
	var ops []trace.Op
	for _, rec := range recs {
		if err := trace.Validate(rec); err != nil {
			t.Fatalf("invalid trace record: %v", err)
		}
		ops = append(ops, rec.Op)
	}
	if len(ops) != 2 || ops[0] != trace.OpAcquire || ops[1] != trace.OpSettle {
		t.Fatalf("trace ops = %v, want [acquire settle]", ops)
	}
}

func TestMCPCallTimeoutFlag(t *testing.T) {
	var errb bytes.Buffer
	opts, err := parseMCPArgs([]string{"--", "srv"}, &errb)
	if err != nil {
		t.Fatal(err)
	}
	if opts.callTimeout != 10*time.Minute {
		t.Fatalf("default call timeout = %v, want 10m", opts.callTimeout)
	}
	opts, err = parseMCPArgs([]string{"--call-timeout", "45s", "--", "srv"}, &errb)
	if err != nil || opts.callTimeout != 45*time.Second {
		t.Fatalf("call timeout = %v, err = %v, want 45s", opts.callTimeout, err)
	}
	if _, err := parseMCPArgs([]string{"--call-timeout", "0s", "--", "srv"}, &errb); err == nil {
		t.Fatal("--call-timeout 0s must be rejected")
	}
}
