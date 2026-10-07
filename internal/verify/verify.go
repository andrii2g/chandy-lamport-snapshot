// Package verify reconstructs state from the trace, independently of the marker
// recorder. A correct sum alone is not proof of a consistent snapshot.
package verify

import (
	"fmt"
	"math/big"
	"reflect"
	"sort"

	"github.com/andrii2g/chandy-lamport-snapshot/internal/sim"
	"github.com/andrii2g/chandy-lamport-snapshot/internal/snapshot"
)

type Check struct {
	Algorithm       string   `json:"algorithm"`
	Complete        bool     `json:"complete"`
	LocalStateValid bool     `json:"local_state_valid"`
	Conserved       bool     `json:"conserved"`
	ConsistentCut   bool     `json:"consistent_cut"`
	ExactChannels   bool     `json:"exact_channels"`
	Valid           bool     `json:"valid"`
	InitialTotal    string   `json:"initial_total"`
	RecordedTotal   string   `json:"recorded_total"`
	ChannelEntries  int      `json:"channel_state_entries"`
	ChannelAmount   string   `json:"channel_state_amount"`
	Latency         int64    `json:"completion_latency_ticks"`
	Witnesses       []string `json:"witnesses"`
}

type Report struct {
	Scenario                  string                          `json:"scenario"`
	Seed                      int64                           `json:"seed"`
	Metrics                   sim.Metrics                     `json:"metrics"`
	Bookkeeping               map[string]snapshot.Bookkeeping `json:"bookkeeping"`
	LiveConservation          bool                            `json:"live_conservation"`
	TraceValid                bool                            `json:"trace_valid"`
	TraceWitnesses            []string                        `json:"trace_witnesses"`
	NaiveInconsistent         int                             `json:"inconsistent_naive_snapshots"`
	NaiveConservationFailures int                             `json:"naive_conservation_failures"`
	Checks                    []Check                         `json:"checks"`
	Snapshots                 []snapshot.Result               `json:"snapshots"`
}

func Analyze(run sim.Run) Report {
	r := Report{Scenario: run.Config.Name, Seed: run.Config.Seed, Metrics: run.Metrics, Bookkeeping: run.Bookkeeping, LiveConservation: true, TraceValid: true, TraceWitnesses: []string{}, Checks: []Check{}, Snapshots: run.Snapshots}
	balances := map[string]int64{}
	var initial int64
	for _, p := range run.Config.Processes {
		balances[p.ID] = p.Balance
		initial += p.Balance
	}
	outstanding := map[string]sim.Event{}
	seen := map[string]bool{}
	var inFlight int64
	for i, v := range run.Trace {
		if v.Index != i+1 || (i > 0 && v.Tick < run.Trace[i-1].Tick) {
			r.TraceWitnesses = append(r.TraceWitnesses, fmt.Sprintf("invalid event ordering at %d", v.Index))
		}
		switch v.Kind {
		case "send":
			if seen[v.MessageID] || v.Amount <= 0 || balances[v.From] < v.Amount {
				r.TraceWitnesses = append(r.TraceWitnesses, "invalid send "+v.MessageID)
			}
			seen[v.MessageID] = true
			outstanding[v.MessageID] = v
			balances[v.From] -= v.Amount
			inFlight += v.Amount
		case "receive":
			s, ok := outstanding[v.MessageID]
			if !ok || s.From != v.From || s.To != v.To || s.Amount != v.Amount {
				r.TraceWitnesses = append(r.TraceWitnesses, "unmatched receive "+v.MessageID)
			}
			delete(outstanding, v.MessageID)
			balances[v.To] += v.Amount
			inFlight -= v.Amount
		}
		var total int64 = inFlight
		for _, balance := range balances {
			total += balance
		}
		if total != initial {
			r.LiveConservation = false
		}
		if v.Process != "" && v.Balance != balances[v.Process] {
			r.TraceWitnesses = append(r.TraceWitnesses, fmt.Sprintf("event %d has incorrect local balance", v.Index))
		}
	}
	if len(outstanding) != 0 {
		r.TraceWitnesses = append(r.TraceWitnesses, "undelivered application messages after event queue drained")
	}
	r.TraceValid = len(r.TraceWitnesses) == 0
	for _, s := range run.Snapshots {
		c := check(run, s, initial)
		r.Checks = append(r.Checks, c)
		if s.Algorithm == "naive" {
			if !c.ConsistentCut {
				r.NaiveInconsistent++
			}
			if !c.Conserved {
				r.NaiveConservationFailures++
			}
		}
	}
	return r
}

func check(run sim.Run, s snapshot.Result, initial int64) Check {
	c := Check{Algorithm: s.Algorithm, Complete: s.Complete && len(s.Locals) == len(run.Config.Processes), LocalStateValid: true, ConsistentCut: true, ExactChannels: true, InitialTotal: fmt.Sprint(initial), Latency: s.Completed - s.Started, Witnesses: []string{}}
	// Reconstruct completion rather than trusting the protocol's finish flag.
	finished := s.Started
	for _, local := range s.Locals {
		if local.Tick > finished {
			finished = local.Tick
		}
		if local.Tick < s.Started {
			c.Complete = false
		}
	}
	if s.Algorithm == "chandy-lamport" {
		for _, link := range run.Config.Channels {
			count := 0
			for _, event := range run.Trace {
				if event.Kind == "marker_receive" && event.From == link.From && event.To == link.To {
					count++
					if event.Tick > finished {
						finished = event.Tick
					}
					local, ok := s.Locals[link.To]
					// A first marker immediately precedes its local record.
					if !ok || event.Index < local.Cut-1 {
						c.Complete = false
					}
				}
			}
			if count != 1 {
				c.Complete = false
				c.Witnesses = append(c.Witnesses, "missing or duplicate closing marker on "+link.Channel().Key())
			}
		}
	}
	if s.Completed != finished {
		c.Complete = false
		c.Witnesses = append(c.Witnesses, "completion tick does not match trace")
	}
	// big.Int keeps deliberately invalid naive sums representable even near int64 limits.
	total := new(big.Int)
	channelAmount := new(big.Int)
	for _, p := range run.Config.Processes {
		l, ok := s.Locals[p.ID]
		if !ok {
			c.LocalStateValid = false
			c.Witnesses = append(c.Witnesses, "missing local state for "+p.ID)
			continue
		}
		total.Add(total, big.NewInt(l.Balance))
		balance := p.Balance
		for _, v := range run.Trace {
			if v.Index >= l.Cut {
				break
			}
			if v.Kind == "send" && v.From == p.ID {
				balance -= v.Amount
			}
			if v.Kind == "receive" && v.To == p.ID {
				balance += v.Amount
			}
		}
		boundaryOK := l.Cut > 0 && l.Cut <= len(run.Trace)
		if boundaryOK {
			v := run.Trace[l.Cut-1]
			boundaryOK = v.Kind == "record" && v.Process == p.ID && v.Algorithm == s.Algorithm && v.Tick == l.Tick
		}
		if balance != l.Balance || !boundaryOK {
			c.LocalStateValid = false
			c.Witnesses = append(c.Witnesses, "local state does not match trace boundary for "+p.ID)
		}
	}
	sends := map[string]sim.Event{}
	receives := map[string]sim.Event{}
	for _, v := range run.Trace {
		if v.Kind == "send" {
			sends[v.MessageID] = v
		}
		if v.Kind == "receive" {
			receives[v.MessageID] = v
		}
	}
	expected := map[string][]snapshot.Message{}
	for _, l := range run.Config.Channels {
		expected[l.Channel().Key()] = []snapshot.Message{}
	}
	// Iterate the trace, rather than maps, to preserve FIFO order and stable witnesses.
	for _, v := range run.Trace {
		if v.Kind != "send" {
			continue
		}
		sender, hasSender := s.Locals[v.From]
		receiver, hasReceiver := s.Locals[v.To]
		sent := hasSender && v.Index < sender.Cut
		recv, exists := receives[v.MessageID]
		received := exists && hasReceiver && recv.Index < receiver.Cut
		if received && !sent {
			c.ConsistentCut = false
			c.Witnesses = append(c.Witnesses, fmt.Sprintf("%s: receive at %s is included but send at %s is excluded", v.MessageID, v.To, v.From))
		}
		if sent && !received {
			key := (snapshot.Channel{From: v.From, To: v.To}).Key()
			expected[key] = append(expected[key], snapshot.Message{ID: v.MessageID, From: v.From, To: v.To, Amount: v.Amount})
		}
	}
	for _, v := range run.Trace {
		if v.Kind == "receive" {
			if _, ok := sends[v.MessageID]; !ok {
				c.ConsistentCut = false
				c.Witnesses = append(c.Witnesses, "receive without any send: "+v.MessageID)
			}
		}
	}
	keys := make([]string, 0, len(s.Channels))
	for key := range s.Channels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		for _, m := range s.Channels[key] {
			c.ChannelEntries++
			channelAmount.Add(channelAmount, big.NewInt(m.Amount))
		}
	}
	for _, l := range run.Config.Channels {
		key := l.Channel().Key()
		if !reflect.DeepEqual(expected[key], s.Channels[key]) {
			c.ExactChannels = false
			c.Witnesses = append(c.Witnesses, fmt.Sprintf("channel %s: expected %v; recorded %v", key, messageIDs(expected[key]), messageIDs(s.Channels[key])))
		}
	}
	for _, key := range keys {
		if _, ok := expected[key]; !ok {
			c.ExactChannels = false
			c.Witnesses = append(c.Witnesses, "unknown channel "+key)
		}
	}
	total.Add(total, channelAmount)
	c.RecordedTotal = total.String()
	c.ChannelAmount = channelAmount.String()
	c.Conserved = total.Cmp(big.NewInt(initial)) == 0
	if !c.Conserved {
		c.Witnesses = append(c.Witnesses, fmt.Sprintf("recorded total %s differs from initial total %d", total, initial))
	}
	if !c.Complete {
		c.Witnesses = append(c.Witnesses, "snapshot did not complete")
	}
	c.Valid = c.Complete && c.LocalStateValid && c.Conserved && c.ConsistentCut && c.ExactChannels
	return c
}

func messageIDs(messages []snapshot.Message) []string {
	ids := []string{}
	for _, m := range messages {
		ids = append(ids, m.ID)
	}
	return ids
}
