# chandy-lamport-snapshot

A deterministic Go simulator showing how a distributed system records a globally
consistent state while transfers continue. Compare naïve balance reads with a
hand-written Chandy–Lamport marker implementation, then inspect the evidence in
an SVG space-time diagram and machine-readable reports.

The default demonstration starts with 300 tokens. Naïve reads report **290**.
Chandy–Lamport records **290 in process balances + 10 in channel state = 300**.

## Run

Requires Go 1.24 or newer. Uses only the standard library; no installation,
network services, or third-party modules are needed.

```sh
go run ./cmd/snapshot run
go run ./cmd/snapshot run --scenario scenarios/double-count.json --out out/double-count
go run ./cmd/snapshot run --scenario scenarios/seeded-traffic.json --seed 99 --out out/seed99
```

| Flag | Default | Meaning |
| --- | --- | --- |
| `--scenario` | `scenarios/marker-window.json` | Input scenario |
| `--algorithm` | `both` | `both`, `naive`, or `chandy-lamport` |
| `--out` | `out/marker-window` | Artifact directory; existing output files are replaced |
| `--seed` | Scenario seed | Override seeded application-message jitter, including with `0` |

Open `out/marker-window/diagram.svg` in a browser. The output directory contains:

- `trace.json`: configuration, complete ordered event trace, snapshots, and counters.
- `report.json`: independent verification, witnesses, snapshots, and metrics.
- `diagram.svg`: one panel per strategy, using the same execution when `both` is selected.

Expected naïve failures return exit status 0: they are the experiment's result.
Invalid input, I/O errors, an invalid live trace, or a failed Chandy–Lamport check
return a nonzero status. Verification artifacts are written before reporting an
invariant failure.

## Experiments

| Scenario | Naïve result | Lesson |
| --- | --- | --- |
| `quiet.json` | 300; valid | A quiescent system needs no channel state |
| `in-flight.json` | 290; consistent cut, incomplete state | Simultaneous balance reads omit money on the wire |
| `double-count.json` | 310; inconsistent cut | Receiver's credit is included but sender's debit is excluded |
| `missing-transfer.json` | 290; consistent cut, incomplete state | Staggered reads can omit an in-flight transfer |
| `marker-window.json` | 290; consistent cut, incomplete state | A marker reaches B through C while A's transfer is still in flight |
| `seeded-traffic.json` | Depends on seed | Four processes with overlapping transfers and FIFO jitter |

In `marker-window`, A sends 10 at tick 1. C initiates at tick 3; A and B record at
tick 5. B receives the transfer at tick 11 and records it on A→B until A's marker
arrives at tick 15. Application transfers continue at ticks 6, 8, and 9. The
snapshot completes 12 ticks after initiation, with one channel-state entry.

A **consistent cut** includes a send whenever it includes that message's receive.
It need not correspond to one instant of the actual execution. A cut with an
included send and excluded receive is valid, but the message must be in its
channel state. Therefore a consistent cut and a complete snapshot are different
checks. A test also constructs a naïve snapshot with the correct total but
opposing errors: conservation alone cannot prove consistency.

## Model and determinism

- Three or four processes, reliable directed channels, and a strongly connected
  graph. One snapshot is in progress per run; no failures or message loss.
- Integer balances and positive transfer amounts. Sending debits immediately;
  receiving credits immediately. An insufficient-funds send is logged as skipped.
- One event loop and a min-heap ordered by `(tick, insertion sequence)`. No
  goroutines, wall-clock sleeps, or scheduler-dependent ordering.
- Application delay is `base delay + uniform integer jitter [0, jitter]` from a
  seeded generator. Marker delay is the base delay. Both message kinds share the
  FIFO constraint: `delivery = max(send tick + delay, previous delivery)`.
- Equal-tick deliveries retain send order. Channels model latency without an
  additional bandwidth/service-time limit. Markers do not consume application
  random draws or shift application deliveries in this model.
- Initially, transfers are scheduled in JSON order, followed by initiation and
  naïve reads. A delivery created later sorts after already-scheduled actions at
  the same tick. Within one handler, local recording and outgoing marker sends
  are atomic with respect to other events.
- The engine and observer know ticks and trace indices. The protocol only uses
  local recording status, incoming messages, and channel identity. Tick/index
  metadata is attached for reporting; no protocol decision relies on it.
- Cuts refer to **application** events. Markers are control traffic, not money
  and not messages that belong in the recorded application channel state.

Identical configuration and seed produce byte-identical output. Arrays determine
all scheduling order; map iteration never schedules events.

## Marker rules

1. The initiator copies its state and starts recording incoming channels. It
   sends a marker on every outgoing channel before any further application send.
2. The first incoming marker makes an unrecorded process copy its state, mark
   that channel empty, record other incoming channels, and send its own markers.
3. Application messages received on an open recording channel are copied into
   its channel state **and still delivered normally**.
4. A later marker closes its incoming channel. A process finishes when every
   incoming channel has closed. The observer finishes when all processes finish.

Every directed channel carries exactly one marker. An observer assembles the
results without modeled network traffic. Distributed collection, storage I/O,
crash recovery, concurrent snapshots, and non-FIFO transport are outside this MVP.

## Verification and metrics

The verifier reconstructs balances and message histories from the trace. It
checks live conservation, local state at every cut, causal consistency, and
exact channel contents (IDs, endpoints, amounts, and FIFO order). Completion is
checked against local records and closing marker arrivals. The verifier does
not consult the recorder's open-channel flags or transport queue.

The report includes application sends/deliveries, skipped transfers, marker
sends/deliveries, channel-state entries and token sum, completion latency in
simulated ticks, naïve causal inconsistencies, and naïve conservation failures.
Counts of naïve failures are per run (zero or one); aggregate `report.json`
files for a multi-run rate. Global completion latency excludes result collection.

Bookkeeping counters are logical operations: local copies, message copies,
application receive checks, opened/closed channel recordings, allocated channel
flags, and peak retained local-state-plus-message entries. Receive checks count
all application deliveries when the marker recorder is enabled. Channel flags
are counted separately from retained entries. Results remain retained through
the end of the run; these counters are not byte-accurate heap estimates.

Totals are decimal strings in JSON so even an invalid naïve sum larger than
`int64` is represented exactly. Go benchmarks report actual allocation counts
and bytes for full simulation runs, including trace construction. They do not
isolate recorder memory or include verification/rendering.

## Diagram

Processes are vertical lanes; event order increases downward. Tick labels show
actual simulated time. Vertical spacing is by **event order**, not elapsed
duration, so same-tick events and marker ordering remain visible.

- Solid arrows: application transfers, labeled with ID and amount.
- Dashed teal arrows: markers.
- Purple connected points: local recording boundaries and saved balances.
- Amber arrows: messages retained in the selected snapshot's channel state.
- Red arrows: included receives with excluded sends.

Arrow tooltips show endpoints and send/receive ticks. In the paired diagram,
both panels include the same marker traffic; only the selected cut and its
recorded state differ. The SVG is standalone, with escaped labels and no scripts.

## Scenario format

Use the committed fixtures as complete examples. `processes` defines IDs and
initial balances; `channels` defines directed endpoints, positive `delay`, and
optional nonnegative `jitter`; `transfers` defines `at`, `from`, `to`, and `amount`.
The `snapshot` object defines `at`, `initiator`, and optionally `naive_reads`:

```json
"snapshot": {
  "at": 2,
  "initiator": "C",
  "naive_reads": [
    {"process": "A", "at": 2},
    {"process": "B", "at": 8},
    {"process": "C", "at": 3}
  ]
}
```

Omit `naive_reads` for simultaneous observer reads at initiation. Otherwise
specify every process exactly once, at or after initiation. Unknown fields,
duplicate IDs/channels, disconnected graphs, negative balances, invalid times,
and total overflow are rejected. Input times/delays/jitter are bounded to
1 billion ticks, and scenarios to 10,000 transfers. Large traces produce tall
SVGs; the short fixtures are intended for visual teaching.

## Develop and test

```sh
go test ./...
go vet ./...
go test ./internal/sim -run=^$ -fuzz=FuzzSnapshot -fuzztime=10s
go test ./internal/sim -run=^$ -bench=BenchmarkSnapshot -benchmem
```

On shells that treat `^` specially, quote the `-run=^$` argument. CI additionally
runs the race detector on Linux and uploads the default demonstration artifacts.
Tests cover all fixtures, 200 generated workloads with replay, FIFO across
both message kinds, conservation, causal checks, deliberately corrupted snapshot
state, CLI errors/artifacts, and escaped valid SVG XML.

| Package | Responsibility |
| --- | --- |
| `cmd/snapshot` | CLI and artifact output |
| `internal/sim` | Configuration validation, scheduling, transport, live balances, trace |
| `internal/snapshot` | Snapshot data and marker recording rules |
| `internal/verify` | Independent trace-based checks and reports |
| `internal/render` | Standalone SVG generation |

The model follows Chandy and Lamport's 1985 paper,
[Distributed Snapshots: Determining Global States of a Distributed System](https://www.microsoft.com/en-us/research/publication/distributed-snapshots-determining-global-states-of-a-distributed-system/).
