package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/andrii2g/chandy-lamport-snapshot/internal/verify"
)

func TestCLIArtifactsAndExpectedNaiveFailure(t *testing.T) {
	out := t.TempDir()
	var stdout bytes.Buffer
	if err := execute([]string{"run", "--scenario", "../../scenarios/double-count.json", "--out", out, "--seed", "0"}, &stdout); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"trace.json", "report.json", "diagram.svg"} {
		info, err := os.Stat(filepath.Join(out, name))
		if err != nil || info.Size() == 0 {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	data, err := os.ReadFile(filepath.Join(out, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report verify.Report
	if err := json.Unmarshal(data, &report); err != nil {
		t.Fatal(err)
	}
	if report.Seed != 0 || report.NaiveInconsistent != 1 || !report.Checks[1].Valid {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestCLIRejectsBadArguments(t *testing.T) {
	for _, args := range [][]string{{}, {"unknown"}, {"run", "--wat"}, {"run", "extra"}, {"run", "--scenario", "missing.json"}, {"run", "--scenario", "../../scenarios/quiet.json", "--algorithm", "invalid"}} {
		if err := execute(args, &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted %v", args)
		}
	}
}
