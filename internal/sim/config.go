package sim

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/andrii2g/chandy-lamport-snapshot/internal/snapshot"
)

// Limits bound trace and diagram size, integer arithmetic, and accidental workloads.
const MaxTick int64 = 1_000_000_000
const MaxTransfers = 10_000

type Process struct {
	ID      string `json:"id"`
	Balance int64  `json:"balance"`
}
type Link struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Delay  int64  `json:"delay"`
	Jitter int64  `json:"jitter,omitempty"`
}

func (l Link) Channel() snapshot.Channel { return snapshot.Channel{From: l.From, To: l.To} }

type Transfer struct {
	At     int64  `json:"at"`
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int64  `json:"amount"`
}
type Read struct {
	Process string `json:"process"`
	At      int64  `json:"at"`
}
type SnapshotConfig struct {
	At        int64  `json:"at"`
	Initiator string `json:"initiator"`
	Reads     []Read `json:"naive_reads"`
}
type Config struct {
	Name      string         `json:"name"`
	Seed      int64          `json:"seed"`
	Processes []Process      `json:"processes"`
	Channels  []Link         `json:"channels"`
	Transfers []Transfer     `json:"transfers"`
	Snapshot  SnapshotConfig `json:"snapshot"`
}

func Decode(reader io.Reader) (Config, error) {
	var c Config
	d := json.NewDecoder(reader)
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("expected one JSON object")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if len(c.Processes) < 3 || len(c.Processes) > 4 {
		return fmt.Errorf("use 3 or 4 processes")
	}
	ids := map[string]bool{}
	var total int64
	for _, p := range c.Processes {
		if p.ID == "" || strings.Contains(p.ID, "->") || len(p.ID) > 32 || ids[p.ID] {
			return fmt.Errorf("invalid or duplicate process ID %q", p.ID)
		}
		if p.Balance < 0 || p.Balance > math.MaxInt64-total {
			return fmt.Errorf("invalid balance or total overflow")
		}
		ids[p.ID] = true
		total += p.Balance
	}
	links := map[string]bool{}
	for _, l := range c.Channels {
		key := l.Channel().Key()
		if !ids[l.From] || !ids[l.To] || l.From == l.To || links[key] {
			return fmt.Errorf("invalid or duplicate channel %s", key)
		}
		if l.Delay < 1 || l.Delay > MaxTick || l.Jitter < 0 || l.Jitter > MaxTick {
			return fmt.Errorf("channel %s requires positive delay and nonnegative jitter <= %d", key, MaxTick)
		}
		links[key] = true
	}
	for start := range ids {
		reached := map[string]bool{start: true}
		for i := 0; i < len(ids); i++ {
			for _, l := range c.Channels {
				if reached[l.From] {
					reached[l.To] = true
				}
			}
		}
		if len(reached) != len(ids) {
			return fmt.Errorf("channels must form a strongly connected graph")
		}
	}
	if len(c.Transfers) > MaxTransfers {
		return fmt.Errorf("at most %d transfers are allowed", MaxTransfers)
	}
	for _, t := range c.Transfers {
		if t.At < 0 || t.At > MaxTick || t.Amount <= 0 || t.Amount > total || !links[(snapshot.Channel{From: t.From, To: t.To}).Key()] {
			return fmt.Errorf("invalid transfer %+v", t)
		}
	}
	if !ids[c.Snapshot.Initiator] || c.Snapshot.At < 0 || c.Snapshot.At > MaxTick {
		return fmt.Errorf("invalid snapshot initiator or time")
	}
	reads := map[string]bool{}
	for _, r := range c.Snapshot.Reads {
		if !ids[r.Process] || reads[r.Process] || r.At < c.Snapshot.At || r.At > MaxTick {
			return fmt.Errorf("invalid or duplicate naive read %+v", r)
		}
		reads[r.Process] = true
	}
	if len(reads) != 0 && len(reads) != len(ids) {
		return fmt.Errorf("naive_reads must specify every process, or be omitted for simultaneous reads")
	}
	return nil
}
