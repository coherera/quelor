package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"testing"

	"github.com/coherera/quelor/bench"
)

func TestRunJSONOutput(t *testing.T) {
	want := bench.Report{
		Throughput: 12.5,
		Scenarios: []bench.ScenarioResult{{Name: "sample", Units: 1}},
	}
	var gotOptions bench.Options
	var out bytes.Buffer

	err := runWith([]string{"-units", "1", "-json"}, &out, func(opt bench.Options) bench.Report {
		gotOptions = opt
		return want
	})
	if err != nil {
		t.Fatalf("runWith() error = %v", err)
	}
	if gotOptions.Units != 1 {
		t.Fatalf("Units = %d, want 1", gotOptions.Units)
	}
	if gotOptions.Seed != 1 || gotOptions.LatencySamples != 20000 {
		t.Fatalf("unexpected benchmark options: %+v", gotOptions)
	}

	var got bench.Report
	if err := json.Unmarshal(out.Bytes(), &got); err != nil {
		t.Fatalf("JSON output is invalid: %v\n%s", err, out.String())
	}
	if got.Throughput != want.Throughput || len(got.Scenarios) != 1 || got.Scenarios[0].Name != "sample" {
		t.Fatalf("JSON output = %+v, want %+v", got, want)
	}
}

func TestRunSmallUnits(t *testing.T) {
	var out bytes.Buffer
	called := false

	err := runWith([]string{"-units", "1"}, &out, func(opt bench.Options) bench.Report {
		called = true
		if opt.Units != 1 {
			t.Fatalf("Units = %d, want 1", opt.Units)
		}
		return bench.Report{}
	})
	if err != nil {
		t.Fatalf("runWith() error = %v", err)
	}
	if !called {
		t.Fatal("benchmark runner was not called")
	}
	if out.Len() == 0 {
		t.Fatal("expected text report output")
	}
}

func TestRunRejectsUnknownFlag(t *testing.T) {
	err := runWith([]string{"-unknown"}, &bytes.Buffer{}, func(bench.Options) bench.Report {
		t.Fatal("benchmark runner should not be called for invalid flags")
		return bench.Report{}
	})
	if err == nil {
		t.Fatal("expected an error for unknown flag")
	}
}

func TestRunHelpDoesNotRunBenchmark(t *testing.T) {
	called := false
	err := runWith([]string{"-h"}, &bytes.Buffer{}, func(bench.Options) bench.Report {
		called = true
		return bench.Report{}
	})
	if err != flag.ErrHelp {
		t.Fatalf("runWith() error = %v, want %v", err, flag.ErrHelp)
	}
	if called {
		t.Fatal("benchmark runner should not be called for help")
	}
}
