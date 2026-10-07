package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/andrii2g/chandy-lamport-snapshot/internal/render"
	"github.com/andrii2g/chandy-lamport-snapshot/internal/sim"
	"github.com/andrii2g/chandy-lamport-snapshot/internal/verify"
)

func main() {
	if err := execute(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "snapshot:", err)
		os.Exit(1)
	}
}

func execute(args []string, w io.Writer) error {
	if len(args) == 0 || args[0] != "run" {
		return fmt.Errorf("usage: snapshot run --scenario scenarios/marker-window.json --algorithm both --out out/marker-window")
	}
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(w)
	scenario := flags.String("scenario", "scenarios/marker-window.json", "scenario JSON path")
	algorithm := flags.String("algorithm", "both", "both, naive, or chandy-lamport")
	out := flags.String("out", "out/marker-window", "output directory")
	seed := flags.Int64("seed", 0, "override the scenario random seed")
	if err := flags.Parse(args[1:]); err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	file, err := os.Open(*scenario)
	if err != nil {
		return err
	}
	config, err := sim.Decode(file)
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	flags.Visit(func(f *flag.Flag) {
		if f.Name == "seed" {
			config.Seed = *seed
		}
	})
	run, err := sim.Execute(config, *algorithm)
	if err != nil {
		return err
	}
	report := verify.Analyze(run)
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	for _, artifact := range []struct {
		name  string
		value any
	}{{"trace.json", run}, {"report.json", report}} {
		data, err := json.MarshalIndent(artifact.value, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(*out, artifact.name), append(data, '\n'), 0644); err != nil {
			return err
		}
	}
	svg, err := os.Create(filepath.Join(*out, "diagram.svg"))
	if err != nil {
		return err
	}
	err = render.SVG(svg, run, report)
	closeErr = svg.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	fmt.Fprintf(w, "%s (seed %d)\n", config.Name, config.Seed)
	m := run.Metrics
	fmt.Fprintf(w, "Applications: %d sent / %d received; skipped: %d\nMarkers: %d sent / %d received\nLive conservation: %t; trace valid: %t\n", m.ApplicationSent, m.ApplicationReceived, m.TransfersSkipped, m.MarkersSent, m.MarkersReceived, report.LiveConservation, report.TraceValid)
	failed := !report.LiveConservation || !report.TraceValid
	for _, c := range report.Checks {
		fmt.Fprintf(w, "\n%s: total %s/%s; consistent cut: %t; exact channels: %t; complete: %t\n", c.Algorithm, c.RecordedTotal, c.InitialTotal, c.ConsistentCut, c.ExactChannels, c.Complete)
		fmt.Fprintf(w, "  channel state: %d entries / %s tokens; latency: %d ticks\n", c.ChannelEntries, c.ChannelAmount, c.Latency)
		b := run.Bookkeeping[c.Algorithm]
		fmt.Fprintf(w, "  bookkeeping: %d local copies, %d message copies, %d receive checks, %d channel flags, %d peak retained entries\n", b.LocalCopies, b.MessageCopies, b.ReceiveChecks, b.ChannelFlags, b.PeakEntries)
		for _, witness := range c.Witnesses {
			fmt.Fprintln(w, "  -", witness)
		}
		if c.Algorithm == "chandy-lamport" && !c.Valid {
			failed = true
		}
	}
	fmt.Fprintf(w, "\nNaive causal inconsistencies: %d; naive conservation failures: %d\nArtifacts: %s\n", report.NaiveInconsistent, report.NaiveConservationFailures, *out)
	if failed {
		return fmt.Errorf("verification failed; inspect report.json")
	}
	return nil
}
