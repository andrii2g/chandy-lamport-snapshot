package sim_test

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/andrii2g/chandy-lamport-snapshot/internal/sim"
	"github.com/andrii2g/chandy-lamport-snapshot/internal/verify"
)

func fixture(t testing.TB, name string) sim.Config {
	t.Helper()
	f, err := os.Open("../../scenarios/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	c, err := sim.Decode(f)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func execute(t testing.TB, c sim.Config) sim.Run {
	t.Helper()
	r, err := sim.Execute(c, "both")
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func assertCorrect(t testing.TB, r sim.Run) {
	t.Helper()
	report := verify.Analyze(r)
	if !report.LiveConservation || !report.TraceValid {
		t.Fatalf("invalid live execution: %+v", report)
	}
	cl := report.Checks[1]
	if !cl.Valid {
		t.Fatalf("seed %d: invalid marker snapshot: %+v", r.Config.Seed, cl)
	}
	if r.Metrics.MarkersSent != len(r.Config.Channels) || r.Metrics.MarkersReceived != len(r.Config.Channels) {
		t.Fatal("expected one marker per directed channel")
	}
	// Compare complete wire histories: markers share FIFO with application messages.
	sent, received := map[string][]string{}, map[string][]string{}
	for _, v := range r.Trace {
		key := v.From + "->" + v.To
		switch v.Kind {
		case "send", "marker_send":
			sent[key] = append(sent[key], v.MessageID)
		case "receive", "marker_receive":
			received[key] = append(received[key], v.MessageID)
		}
	}
	if !reflect.DeepEqual(sent, received) {
		t.Fatalf("FIFO violation: sent %v; received %v", sent, received)
	}
}

func TestScenarios(t *testing.T) {
	cases := []struct {
		name, total string
		consistent  bool
		entries     int
	}{
		{"quiet", "300", true, 0}, {"in-flight", "290", true, 1}, {"double-count", "310", false, 1},
		{"missing-transfer", "290", true, 1}, {"marker-window", "290", true, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := execute(t, fixture(t, tc.name))
			assertCorrect(t, r)
			report := verify.Analyze(r)
			if n := report.Checks[0]; n.RecordedTotal != tc.total || n.ConsistentCut != tc.consistent {
				t.Fatalf("unexpected naive result %+v", n)
			}
			if report.Checks[1].ChannelEntries != tc.entries {
				t.Fatalf("unexpected channel entries %+v", report.Checks[1])
			}
		})
	}
	assertCorrect(t, execute(t, fixture(t, "seeded-traffic")))
}

func TestNumericCancellationDoesNotProveConsistency(t *testing.T) {
	c := fixture(t, "quiet")
	for i := range c.Channels {
		c.Channels[i].Delay = 1
	}
	c.Snapshot = sim.SnapshotConfig{At: 4, Initiator: "C", Reads: []sim.Read{{Process: "A", At: 4}, {Process: "B", At: 10}, {Process: "C", At: 4}}}
	c.Transfers = []sim.Transfer{{At: 6, From: "A", To: "B", Amount: 10}, {At: 8, From: "B", To: "A", Amount: 10}}
	r := execute(t, c)
	assertCorrect(t, r)
	n := verify.Analyze(r).Checks[0]
	if !n.Conserved || n.ConsistentCut || n.ExactChannels || n.Valid {
		t.Fatalf("must reject numerically balanced but inconsistent snapshot: %+v", n)
	}
}

func randomConfig(seed int64) sim.Config {
	rng := rand.New(rand.NewSource(seed))
	n := 3 + rng.Intn(2)
	c := sim.Config{Name: "generated", Seed: seed, Snapshot: sim.SnapshotConfig{At: int64(rng.Intn(60)), Initiator: "A"}}
	for i := 0; i < n; i++ {
		c.Processes = append(c.Processes, sim.Process{ID: string(rune('A' + i)), Balance: 100})
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				c.Channels = append(c.Channels, sim.Link{From: c.Processes[i].ID, To: c.Processes[j].ID, Delay: int64(1 + rng.Intn(10)), Jitter: int64(rng.Intn(20))})
			}
		}
	}
	for i := 0; i < 200; i++ {
		l := c.Channels[rng.Intn(len(c.Channels))]
		c.Transfers = append(c.Transfers, sim.Transfer{At: int64(rng.Intn(100)), From: l.From, To: l.To, Amount: int64(1 + rng.Intn(50))})
	}
	return c
}

func TestSeededPropertiesAndReplay(t *testing.T) {
	for seed := int64(0); seed < 200; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			c := randomConfig(seed)
			r := execute(t, c)
			assertCorrect(t, r)
			a, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			b, err := json.Marshal(execute(t, c))
			if err != nil {
				t.Fatal(err)
			}
			if string(a) != string(b) {
				t.Fatal("identical inputs must produce byte-identical traces and snapshots")
			}
		})
	}
}

func TestMarkerInsertionDoesNotChangeApplicationHistory(t *testing.T) {
	c := randomConfig(71)
	both := execute(t, c)
	naive, err := sim.Execute(c, "naive")
	if err != nil {
		t.Fatal(err)
	}
	application := func(r sim.Run) []sim.Event {
		var events []sim.Event
		for _, v := range r.Trace {
			if v.Kind == "send" || v.Kind == "receive" || v.Kind == "transfer_skipped" {
				v.Index = 0
				events = append(events, v)
			}
		}
		return events
	}
	if !reflect.DeepEqual(application(both), application(naive)) {
		t.Fatal("marker insertion altered application history")
	}
}

func TestVerifierRejectsCorruption(t *testing.T) {
	t.Run("wrong-message-same-value", func(t *testing.T) {
		r := execute(t, fixture(t, "marker-window"))
		r.Snapshots[1].Channels["A->B"][0].ID = "invented"
		c := verify.Analyze(r).Checks[1]
		if c.Valid || c.ExactChannels || !c.Conserved {
			t.Fatalf("wrong ID must fail exact check: %+v", c)
		}
	})
	t.Run("wrong-local-same-total", func(t *testing.T) {
		r := execute(t, fixture(t, "quiet"))
		s := &r.Snapshots[1]
		a, b := s.Locals["A"], s.Locals["B"]
		a.Balance++
		b.Balance--
		s.Locals["A"], s.Locals["B"] = a, b
		c := verify.Analyze(r).Checks[1]
		if c.Valid || c.LocalStateValid || !c.Conserved {
			t.Fatalf("wrong local balances must fail: %+v", c)
		}
	})
	t.Run("incomplete", func(t *testing.T) {
		r := execute(t, fixture(t, "quiet"))
		r.Snapshots[1].Complete = false
		if verify.Analyze(r).Checks[1].Valid {
			t.Fatal("incomplete snapshot accepted")
		}
	})
}

func TestInvalidConfiguration(t *testing.T) {
	for name, change := range map[string]func(*sim.Config){
		"zero-delay":        func(c *sim.Config) { c.Channels[0].Delay = 0 },
		"negative-jitter":   func(c *sim.Config) { c.Channels[0].Jitter = -1 },
		"disconnected":      func(c *sim.Config) { c.Channels = c.Channels[:2] },
		"duplicate-process": func(c *sim.Config) { c.Processes[1].ID = "A" },
		"duplicate-channel": func(c *sim.Config) { c.Channels = append(c.Channels, c.Channels[0]) },
		"negative-balance":  func(c *sim.Config) { c.Processes[0].Balance = -1 },
		"incomplete-reads":  func(c *sim.Config) { c.Snapshot.Reads = []sim.Read{{Process: "A", At: 6}} },
		"read-before-start": func(c *sim.Config) {
			c.Snapshot.Reads = []sim.Read{{Process: "A", At: 1}, {Process: "B", At: 6}, {Process: "C", At: 6}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := fixture(t, "quiet")
			change(&c)
			if _, err := sim.Execute(c, "both"); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestInsufficientFundsSkipsTransfer(t *testing.T) {
	c := fixture(t, "quiet")
	c.Transfers = []sim.Transfer{{At: 1, From: "A", To: "B", Amount: 101}}
	r := execute(t, c)
	assertCorrect(t, r)
	if r.Metrics.TransfersSkipped != 1 || r.Metrics.ApplicationSent != 0 {
		t.Fatal("overspending was not rejected")
	}
}

func FuzzSnapshot(f *testing.F) {
	for _, seed := range []int64{0, 1, 42, 999} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed int64) { assertCorrect(t, execute(t, randomConfig(seed))) })
}

func BenchmarkSnapshot(b *testing.B) {
	c := randomConfig(42)
	for _, algorithm := range []string{"naive", "chandy-lamport", "both"} {
		b.Run(algorithm, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := sim.Execute(c, algorithm); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
