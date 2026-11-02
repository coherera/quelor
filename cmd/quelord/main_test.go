package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coherera/quelor/pkg/version"
)

func TestRunVersion(t *testing.T) {
	code, stdout, stderr := runWithOutput(t, []string{"-version"})
	if code != 0 {
		t.Fatalf("exit %d stderr %q", code, stderr)
	}
	if stdout != version.String()+"\n" {
		t.Fatalf("stdout %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("stderr %q", stderr)
	}

	// -version exits before the config file is opened.
	missing := filepath.Join(t.TempDir(), "missing.yaml")
	code, stdout, stderr = runWithOutput(t, []string{"-version", "-config", missing})
	if code != 0 {
		t.Fatalf("exit %d stderr %q", code, stderr)
	}
	if stdout != version.String()+"\n" {
		t.Fatalf("stdout %q", stdout)
	}
	if stderr != "" {
		t.Fatalf("stderr %q", stderr)
	}
}

func TestRunMissingConfig(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "no flags", args: nil},
		{name: "empty", args: []string{"-config", ""}},
		{name: "blank", args: []string{"-config", " \t "}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runWithOutput(t, tt.args)
			if code != 2 {
				t.Fatalf("exit %d stdout %q stderr %q", code, stdout, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout %q", stdout)
			}
			if stderr != "quelord: -config is required\n" {
				t.Fatalf("stderr %q", stderr)
			}
		})
	}
}

func TestRunFlagError(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "unknown flag", args: []string{"-not-a-flag"}, want: "flag provided but not defined"},
		{name: "config missing value", args: []string{"-config"}, want: "flag needs an argument"},
		{name: "help", args: []string{"-h"}, want: "Usage of"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runWithOutput(t, tt.args)
			if code != 2 {
				t.Fatalf("exit %d stdout %q stderr %q", code, stdout, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout %q", stdout)
			}
			if !strings.Contains(stderr, tt.want) {
				t.Fatalf("stderr %q", stderr)
			}
		})
	}
}

func TestRunConfigErrors(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.yaml")

	blocker := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	unreadable := filepath.Join(blocker, "quelord.yaml")

	invalidPath := filepath.Join(dir, "invalid.yaml")
	if err := os.WriteFile(invalidPath, []byte("listen: [\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	unknownPath := filepath.Join(dir, "unknown.yaml")
	if err := os.WriteFile(unknownPath, []byte("nope: true\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// A bad limit must not start the listener.
	badTTL := filepath.Join(dir, "ttl.yaml")
	if err := os.WriteFile(badTTL, []byte("listen: 127.0.0.1:0\nlease_ttl: nope\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "missing", path: missing, want: "quelord: config:"},
		{name: "unreadable", path: unreadable, want: "quelord: config:"},
		{name: "invalid yaml", path: invalidPath, want: "quelord: config:"},
		{name: "unknown field", path: unknownPath, want: "quelord: config:"},
		{name: "bad lease ttl", path: badTTL, want: "quelord: engine:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, stdout, stderr := runWithOutput(t, []string{"-config", tt.path})
			if code != 1 {
				t.Fatalf("exit %d stdout %q stderr %q", code, stdout, stderr)
			}
			if stdout != "" {
				t.Fatalf("stdout %q", stdout)
			}
			if !strings.Contains(stderr, tt.want) {
				t.Fatalf("stderr %q", stderr)
			}
			if tt.name == "bad lease ttl" && !strings.Contains(stderr, "lease_ttl") {
				t.Fatalf("stderr %q", stderr)
			}
		})
	}
}

func TestRunListenAndShutdown(t *testing.T) {
	ctx, cancelCtx := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelCtx()

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "quelord.yaml")
	tracePath := filepath.Join(dir, "trace.jsonl")
	doc := fmt.Sprintf("listen: 127.0.0.1:0\ntrace_path: %s\nresources:\n  - id: runner_slots\n    capacity_model: lease\n    capacity: 2\n", strconv.Quote(tracePath))
	if err := os.WriteFile(cfgPath, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}

	oldOut, oldErr := os.Stdout, os.Stderr
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		_ = outR.Close()
		_ = outW.Close()
		t.Fatal(err)
	}
	os.Stdout = outW
	os.Stderr = errW

	origNotify := notifyContext
	ready := make(chan context.CancelFunc, 1)
	notifyContext = func(parent context.Context, _ ...os.Signal) (context.Context, context.CancelFunc) {
		cctx, cancel := context.WithCancel(parent)
		ready <- cancel
		return cctx, cancel
	}

	var stop context.CancelFunc
	defer func() {
		if stop != nil {
			stop()
		} else {
			select {
			case c := <-ready:
				c()
			default:
			}
		}
		notifyContext = origNotify
		os.Stdout = oldOut
		os.Stderr = oldErr
		_ = outW.Close()
		_ = errW.Close()
		_ = outR.Close()
		_ = errR.Close()
	}()

	lines := make(chan string, 4)
	outDone := make(chan struct{})
	var outBuf bytes.Buffer
	go func() {
		defer close(outDone)
		sc := bufio.NewScanner(outR)
		for sc.Scan() {
			outBuf.WriteString(sc.Text())
			outBuf.WriteByte('\n')
			lines <- sc.Text()
		}
	}()
	errDone := make(chan struct{})
	var errBuf bytes.Buffer
	go func() {
		defer close(errDone)
		_, _ = io.Copy(&errBuf, errR)
	}()

	codeCh := make(chan int, 1)
	go func() {
		codeCh <- run([]string{"-config", cfgPath})
	}()

	var listenLine string
	select {
	case listenLine = <-lines:
	case code := <-codeCh:
		t.Fatalf("exited %d before listen; stderr so far not drained", code)
	case <-ctx.Done():
		t.Fatal("timed out waiting for listen")
	}
	select {
	case stop = <-ready:
	case code := <-codeCh:
		t.Fatalf("exited %d before signal hook", code)
	case <-ctx.Done():
		t.Fatal("timed out waiting for signal hook")
	}

	addr, n, err := parseListenLine(listenLine)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("resources %d", n)
	}
	if !strings.HasPrefix(addr, "127.0.0.1:") {
		t.Fatalf("addr %s", addr)
	}

	client := &http.Client{Timeout: time.Second}
	resp, err := getHealth(ctx, client, "http://"+addr+"/healthz")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	var body struct {
		Status  string `json:"status"`
		Version string `json:"version"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&body)
	_ = resp.Body.Close()
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if resp.StatusCode != http.StatusOK || body.Status != "ok" || body.Version != version.String() {
		t.Fatalf("healthz %d %+v", resp.StatusCode, body)
	}

	stop()
	var code int
	select {
	case code = <-codeCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for shutdown")
	}

	_ = outW.Close()
	_ = errW.Close()
	select {
	case <-outDone:
	case <-ctx.Done():
		t.Fatal("timed out reading stdout")
	}
	select {
	case <-errDone:
	case <-ctx.Done():
		t.Fatal("timed out reading stderr")
	}

	if code != 0 {
		t.Fatalf("exit %d stdout %q stderr %q", code, outBuf.String(), errBuf.String())
	}
	if strings.TrimSpace(outBuf.String()) != listenLine {
		t.Fatalf("stdout %q", outBuf.String())
	}
	if errBuf.Len() != 0 {
		t.Fatalf("stderr %q", errBuf.String())
	}
	if _, err := client.Get("http://" + addr + "/healthz"); err == nil {
		t.Fatal("listener still serving after shutdown")
	}
	st, err := os.Stat(tracePath)
	if err != nil {
		t.Fatal(err)
	}
	if st.IsDir() {
		t.Fatal("trace path is a directory")
	}
}

func parseListenLine(line string) (string, int, error) {
	rest, ok := strings.CutPrefix(line, "quelord listening on ")
	if !ok {
		return "", 0, fmt.Errorf("listen line %q", line)
	}
	addr, res, ok := strings.Cut(rest, " ")
	if !ok || !strings.HasPrefix(res, "resources=") || addr == "" {
		return "", 0, fmt.Errorf("listen line %q", line)
	}
	n, err := strconv.Atoi(strings.TrimPrefix(res, "resources="))
	if err != nil {
		return "", 0, fmt.Errorf("listen line %q", line)
	}
	return addr, n, nil
}

func getHealth(ctx context.Context, client *http.Client, url string) (*http.Response, error) {
	var last error
	for {
		resp, err := client.Get(url)
		if err == nil {
			return resp, nil
		}
		last = err
		select {
		case <-ctx.Done():
			return nil, last
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func runWithOutput(t *testing.T, args []string) (code int, stdout, stderr string) {
	t.Helper()
	oldOut, oldErr := os.Stdout, os.Stderr
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		_ = outR.Close()
		_ = outW.Close()
		t.Fatal(err)
	}
	os.Stdout = outW
	os.Stderr = errW
	defer func() {
		os.Stdout = oldOut
		os.Stderr = oldErr
		_ = outW.Close()
		_ = errW.Close()
		out, oerr := io.ReadAll(outR)
		errb, eerr := io.ReadAll(errR)
		_ = outR.Close()
		_ = errR.Close()
		stdout = string(out)
		stderr = string(errb)
		if oerr != nil {
			t.Fatalf("read stdout: %v", oerr)
		}
		if eerr != nil {
			t.Fatalf("read stderr: %v", eerr)
		}
	}()
	return run(args), "", ""
}
