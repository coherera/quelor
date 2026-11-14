// Command quelor-bench prints a Quelor admission-control scenario report.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/coherera/quelor/bench"
)

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		if err == flag.ErrHelp {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func run(args []string, out io.Writer) error {
	return runWith(args, out, bench.RunAll)
}

func runWith(args []string, out io.Writer, runAll func(bench.Options) bench.Report) error {
	flags := flag.NewFlagSet("quelor-bench", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	units := flags.Int("units", 20000, "units each of 3 holders submits in the shared-token-bucket scenario")
	asJSON := flags.Bool("json", false, "print the report as JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}

	rep := runAll(bench.Options{
		Units:          *units,
		Seed:           1,
		LatencySamples: 20000,
	})
	if *asJSON {
		_, err := fmt.Fprint(out, rep.JSON())
		return err
	}
	_, err := fmt.Fprint(out, rep.String())
	return err
}
