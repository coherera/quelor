//go:build unix

package main

import (
	"bytes"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestMCPSIGTERMStopsTheChildAndExitsNonZero sends this very process a SIGTERM
// while the proxy runs a child. Without the signal handler the default action
// would kill the test binary; with it the proxy stops the child and returns.
func TestMCPSIGTERMStopsTheChildAndExitsNonZero(t *testing.T) {
	pr, pw := io.Pipe()
	defer pw.Close()

	var out, errb bytes.Buffer
	done := make(chan int, 1)
	args := mcpChildArgs(t, "block")
	go func() {
		done <- run(args, pr, &out, &errb, func(string) string { return "" })
	}()

	// The handler is installed before the child starts; give both a moment.
	time.Sleep(500 * time.Millisecond)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}

	select {
	case code := <-done:
		if code == 0 {
			t.Fatalf("exit = 0 after SIGTERM, want non-zero (stderr=%q)", errb.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("proxy did not stop after SIGTERM (stderr=%q)", strings.TrimSpace(errb.String()))
	}
}
