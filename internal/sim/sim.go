// Package sim supplies deterministic scheduling and FIFO delivery. Simulation
// ticks are never used by the snapshot protocol to decide what to record.
package sim

import (
	"container/heap"
	"fmt"
	"math/rand"

	"github.com/andrii2g/chandy-lamport-snapshot/internal/snapshot"
)

type Event struct {
	Index     int    `json:"index"`
	Tick      int64  `json:"tick"`
	Kind      string `json:"kind"`
	Process   string `json:"process,omitempty"`
	From      string `json:"from,omitempty"`
	To        string `json:"to,omitempty"`
	MessageID string `json:"message_id,omitempty"`
	Amount    int64  `json:"amount,omitempty"`
	Balance   int64  `json:"balance"`
	Algorithm string `json:"algorithm,omitempty"`
}

type Metrics struct {
	ApplicationSent     int   `json:"application_messages_sent"`
	ApplicationReceived int   `json:"application_messages_received"`
	TransfersSkipped    int   `json:"transfers_skipped_insufficient_funds"`
	MarkersSent         int   `json:"marker_messages_sent"`
	MarkersReceived     int   `json:"marker_messages_received"`
	FinalTick           int64 `json:"final_tick"`
}

type Run struct {
	Config      Config                          `json:"config"`
	Trace       []Event                         `json:"events"`
	Snapshots   []snapshot.Result               `json:"snapshots"`
	Metrics     Metrics                         `json:"metrics"`
	Bookkeeping map[string]snapshot.Bookkeeping `json:"bookkeeping"`
}

type scheduled struct {
	tick     int64
	sequence int
	kind     string
	process  string
	message  snapshot.Message
	transfer Transfer
}
type queue []scheduled

func (q queue) Len() int { return len(q) }
func (q queue) Less(i, j int) bool {
	if q[i].tick != q[j].tick {
		return q[i].tick < q[j].tick
	}
	return q[i].sequence < q[j].sequence
}
func (q queue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *queue) Push(x any)   { *q = append(*q, x.(scheduled)) }
func (q *queue) Pop() any     { old := *q; x := old[len(old)-1]; *q = old[:len(old)-1]; return x }

type engine struct {
	run             Run
	queue           queue
	sequence        int
	now             int64
	balances        map[string]int64
	links           map[string]Link
	lastDelivery    map[string]int64
	random          *rand.Rand
	recorder        *snapshot.Recorder
	naive           *snapshot.Result
	nextApplication int
	nextMarker      int
}

func Execute(c Config, algorithm string) (Run, error) {
	if err := c.Validate(); err != nil {
		return Run{}, err
	}
	if algorithm != "both" && algorithm != "naive" && algorithm != "chandy-lamport" {
		return Run{}, fmt.Errorf("algorithm must be both, naive, or chandy-lamport")
	}
	e := &engine{run: Run{Config: c, Trace: []Event{}, Snapshots: []snapshot.Result{}, Bookkeeping: map[string]snapshot.Bookkeeping{}}, balances: map[string]int64{}, links: map[string]Link{}, lastDelivery: map[string]int64{}, random: rand.New(rand.NewSource(c.Seed))}
	var ids []string
	var channels []snapshot.Channel
	for _, p := range c.Processes {
		ids = append(ids, p.ID)
		e.balances[p.ID] = p.Balance
	}
	for _, l := range c.Channels {
		channels = append(channels, l.Channel())
		e.links[l.Channel().Key()] = l
	}
	// Configured transfers are inserted before snapshot actions at equal ticks.
	for _, t := range c.Transfers {
		e.schedule(scheduled{tick: t.At, kind: "transfer", transfer: t})
	}
	if algorithm != "naive" {
		e.recorder = snapshot.New(ids, channels, c.Snapshot.At)
		e.schedule(scheduled{tick: c.Snapshot.At, kind: "start", process: c.Snapshot.Initiator})
	}
	if algorithm != "chandy-lamport" {
		n := snapshot.NewResult("naive", c.Snapshot.At, channels)
		e.naive = &n
		reads := c.Snapshot.Reads
		if len(reads) == 0 {
			for _, p := range c.Processes {
				reads = append(reads, Read{Process: p.ID, At: c.Snapshot.At})
			}
		}
		for _, r := range reads {
			e.schedule(scheduled{tick: r.At, kind: "read", process: r.Process})
		}
	}
	for e.queue.Len() > 0 {
		s := heap.Pop(&e.queue).(scheduled)
		e.now = s.tick
		switch s.kind {
		case "transfer":
			e.transfer(s.transfer)
		case "application":
			if e.recorder != nil {
				e.recorder.Application(s.message)
			}
			e.balances[s.message.To] += s.message.Amount
			e.run.Metrics.ApplicationReceived++
			e.logMessage("receive", s.message)
		case "start":
			if e.recorder.Start(s.process, e.local(s.process)) {
				e.record(s.process, "chandy-lamport")
				e.markers(s.process)
			}
		case "marker":
			e.run.Metrics.MarkersReceived++
			e.logMessage("marker_receive", s.message)
			c := snapshot.Channel{From: s.message.From, To: s.message.To}
			if e.recorder.Marker(c, e.local(c.To), e.now) {
				e.record(c.To, "chandy-lamport")
				e.markers(c.To)
			}
		case "read":
			e.naive.Locals[s.process] = e.local(s.process)
			e.record(s.process, "naive")
			if len(e.naive.Locals) == len(c.Processes) {
				e.naive.Complete = true
				e.naive.Completed = e.now
			}
		}
	}
	e.run.Metrics.FinalTick = e.now
	if e.naive != nil {
		e.run.Snapshots = append(e.run.Snapshots, *e.naive)
		e.run.Bookkeeping["naive"] = snapshot.Bookkeeping{LocalCopies: len(e.naive.Locals), PeakEntries: len(e.naive.Locals)}
	}
	if e.recorder != nil {
		e.run.Snapshots = append(e.run.Snapshots, e.recorder.Result)
		e.run.Bookkeeping["chandy-lamport"] = e.recorder.Stats
	}
	return e.run, nil
}

func (e *engine) schedule(s scheduled) { e.sequence++; s.sequence = e.sequence; heap.Push(&e.queue, s) }
func (e *engine) append(v Event) {
	v.Index = len(e.run.Trace) + 1
	v.Tick = e.now
	e.run.Trace = append(e.run.Trace, v)
}
func (e *engine) local(p string) snapshot.Local {
	return snapshot.Local{Balance: e.balances[p], Cut: len(e.run.Trace) + 1, Tick: e.now}
}
func (e *engine) record(p, algorithm string) {
	e.append(Event{Kind: "record", Process: p, Balance: e.balances[p], Algorithm: algorithm})
}
func (e *engine) logMessage(kind string, m snapshot.Message) {
	p := m.From
	if kind == "receive" || kind == "marker_receive" {
		p = m.To
	}
	e.append(Event{Kind: kind, Process: p, From: m.From, To: m.To, MessageID: m.ID, Amount: m.Amount, Balance: e.balances[p]})
}
func (e *engine) transfer(t Transfer) {
	if e.balances[t.From] < t.Amount {
		e.run.Metrics.TransfersSkipped++
		e.append(Event{Kind: "transfer_skipped", Process: t.From, From: t.From, To: t.To, Amount: t.Amount, Balance: e.balances[t.From]})
		return
	}
	e.nextApplication++
	m := snapshot.Message{ID: fmt.Sprintf("a%04d", e.nextApplication), From: t.From, To: t.To, Amount: t.Amount}
	e.balances[t.From] -= t.Amount
	e.run.Metrics.ApplicationSent++
	e.logMessage("send", m)
	e.deliver(m, false)
}
func (e *engine) markers(p string) {
	// Configuration order is stable; never iterate a map to schedule work.
	for _, l := range e.run.Config.Channels {
		if l.From == p {
			e.nextMarker++
			m := snapshot.Message{ID: fmt.Sprintf("m%04d", e.nextMarker), From: l.From, To: l.To}
			e.run.Metrics.MarkersSent++
			e.logMessage("marker_send", m)
			e.deliver(m, true)
		}
	}
}
func (e *engine) deliver(m snapshot.Message, marker bool) {
	key := (snapshot.Channel{From: m.From, To: m.To}).Key()
	l := e.links[key]
	delay := l.Delay
	// Markers use base delay, leaving the application RNG stream independent.
	if !marker && l.Jitter > 0 {
		delay += e.random.Int63n(l.Jitter + 1)
	}
	at := e.now + delay
	if at < e.lastDelivery[key] {
		at = e.lastDelivery[key]
	}
	e.lastDelivery[key] = at
	kind := "application"
	if marker {
		kind = "marker"
	}
	e.schedule(scheduled{tick: at, kind: kind, message: m})
}
