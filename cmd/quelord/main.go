package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coherera/quelor/pkg/server"
	"github.com/coherera/quelor/pkg/version"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// notifyContext reports when the process should shut down.
// The default watches SIGINT and SIGTERM. Tests replace it so a listener
// can stop without a real signal.
var notifyContext = signal.NotifyContext

func run(args []string) (code int) {
	fs := flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	configPath := fs.String("config", "", "path to config file")
	showVersion := fs.Bool("version", false, "print version and exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	if *showVersion {
		fmt.Println(version.String())
		return 0
	}
	if strings.TrimSpace(*configPath) == "" {
		fmt.Fprintln(os.Stderr, "quelord: -config is required")
		return 2
	}

	cfg, err := server.LoadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "quelord: config: %v\n", err)
		return 1
	}
	eng, err := cfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "quelord: engine: %v\n", err)
		return 1
	}
	n, err := cfg.CountResources()
	if err != nil {
		_ = eng.Close()
		fmt.Fprintf(os.Stderr, "quelord: resources: %v\n", err)
		return 1
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		if err := eng.Run(runCtx); err != nil && runCtx.Err() == nil {
			fmt.Fprintf(os.Stderr, "quelord: engine: %v\n", err)
		}
	}()
	defer func() {
		runCancel()
		<-runDone
		if cerr := eng.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "quelord: close: %v\n", cerr)
			if code == 0 {
				code = 1
			}
		}
	}()

	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		fmt.Fprintf(os.Stderr, "quelord: listen: %v\n", err)
		return 1
	}
	srv := &http.Server{
		Handler:           server.New(eng, server.Options{}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- srv.Serve(ln)
	}()

	fmt.Printf("quelord listening on %s resources=%d\n", ln.Addr().String(), n)

	sigCtx, stop := notifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-sigCtx.Done():
		if err := shutdownHTTP(srv); err != nil {
			fmt.Fprintf(os.Stderr, "quelord: shutdown: %v\n", err)
			return 1
		}
		// Shutdown makes Serve return ErrServerClosed. That is a clean stop.
		if err := <-serveDone; err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "quelord: serve:", err)
		}
		return 0
	case err := <-serveDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "quelord: serve: %v\n", err)
			return 1
		}
		return 0
	}
}

func shutdownHTTP(srv *http.Server) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(ctx)
}
