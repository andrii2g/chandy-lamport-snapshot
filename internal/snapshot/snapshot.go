// Package snapshot implements recording rules without access to simulated time
// scheduling, transport queues, or any other process's live state.
package snapshot

type Channel struct {
	From string `json:"from"`
	To   string `json:"to"`
}

func (c Channel) Key() string { return c.From + "->" + c.To }

type Message struct {
	ID     string `json:"id"`
	From   string `json:"from"`
	To     string `json:"to"`
	Amount int64  `json:"amount"`
}

type Local struct {
	Balance int64 `json:"balance"`
	// Cut is an observer-assigned trace boundary, not an algorithm clock.
	Cut  int   `json:"cut_event"`
	Tick int64 `json:"recorded_tick"`
}

type Result struct {
	Algorithm string               `json:"algorithm"`
	Started   int64                `json:"started_tick"`
	Completed int64                `json:"completed_tick"`
	Complete  bool                 `json:"complete"`
	Locals    map[string]Local     `json:"locals"`
	Channels  map[string][]Message `json:"channels"`
}

func NewResult(algorithm string, started int64, channels []Channel) Result {
	r := Result{Algorithm: algorithm, Started: started, Locals: map[string]Local{}, Channels: map[string][]Message{}}
	for _, c := range channels {
		r.Channels[c.Key()] = []Message{}
	}
	return r
}

type Bookkeeping struct {
	LocalCopies    int `json:"local_state_copies"`
	MessageCopies  int `json:"channel_message_copies"`
	ReceiveChecks  int `json:"application_receive_checks"`
	ChannelsOpened int `json:"channel_recordings_opened"`
	ChannelsClosed int `json:"channel_recordings_closed"`
	PeakEntries    int `json:"peak_retained_state_and_message_entries"`
	ChannelFlags   int `json:"channel_flags"`
}

// Recorder implements one snapshot. The driver must send outgoing markers
// immediately when Start or Marker returns true, before any next application send.
type Recorder struct {
	Result       Result
	Stats        Bookkeeping
	incoming     map[string][]Channel
	open         map[string]bool
	done         map[string]bool
	processCount int
}

func New(processes []string, channels []Channel, started int64) *Recorder {
	r := &Recorder{Result: NewResult("chandy-lamport", started, channels), incoming: map[string][]Channel{}, open: map[string]bool{}, done: map[string]bool{}, processCount: len(processes)}
	for _, c := range channels {
		r.incoming[c.To] = append(r.incoming[c.To], c)
	}
	r.Stats.ChannelFlags = len(channels)
	return r
}

func (r *Recorder) Start(process string, local Local) bool {
	if _, ok := r.Result.Locals[process]; ok {
		return false
	}
	r.Result.Locals[process] = local
	r.Stats.LocalCopies++
	for _, c := range r.incoming[process] {
		r.open[c.Key()] = true
		r.Stats.ChannelsOpened++
	}
	r.peak()
	return true
}

func (r *Recorder) Marker(c Channel, local Local, tick int64) bool {
	first := r.Start(c.To, local)
	if r.open[c.Key()] {
		r.open[c.Key()] = false
		r.Stats.ChannelsClosed++
	}
	finished := true
	for _, in := range r.incoming[c.To] {
		if r.open[in.Key()] {
			finished = false
		}
	}
	if finished {
		r.done[c.To] = true
	}
	if len(r.done) == r.processCount {
		r.Result.Complete = true
		r.Result.Completed = tick
	}
	return first
}

func (r *Recorder) Application(m Message) {
	r.Stats.ReceiveChecks++
	key := (Channel{From: m.From, To: m.To}).Key()
	if r.open[key] {
		r.Result.Channels[key] = append(r.Result.Channels[key], m)
		r.Stats.MessageCopies++
		r.peak()
	}
}

func (r *Recorder) peak() {
	n := r.Stats.LocalCopies + r.Stats.MessageCopies
	if n > r.Stats.PeakEntries {
		r.Stats.PeakEntries = n
	}
}
