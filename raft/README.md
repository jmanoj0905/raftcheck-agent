# raft: a Raft core with a JSONL trace log

This folder holds a Raft (a way for several computers to agree on one ordered log of commands) written in Go. It is the system that the agent later checks for bugs. Everything it does is written to a trace log, one JSON line per state change.

## Core and wrapper (decision D1)

The Raft is split in two:

- **`core/`** is a plain state machine. It has no goroutines (Go's lightweight threads), no clock, no randomness and no input or output. You give it one event with `Step(event)`, and it returns an `Output`. Because it is deterministic (same input, same output), a replay of a run is exact.
- **`node/`** is the wrapper. It adds goroutines, real timers, an in-memory network (`MemNetwork`) and an in-memory saved-state store, so it can run MIT-style tests and, later, Docker mode.
- **`internal/testkit/`** is the simulator. It calls the core directly and drives it with a seeded random walk.

The simulator needs the core to be deterministic, so the core can never read the time or start a goroutine.

## The four events and the Output order

The core accepts four events: `ElectionTimeout`, `HeartbeatTimeout`, `Receive` (a message arrived) and `Propose` (a client gives the leader a command).

Each `Step` returns, in this order:

1. **persist**: the state that must survive a crash (term, vote, log), if it changed;
2. **messages**: the messages to send;
3. **apply**: newly committed entries to hand to the state machine;
4. **records**: the log records of the step.

The order matters: a node must save before it sends, or a crash could make it forget a vote it already told someone about.

## The record format

One JSON object per line. The fields, the 18 event names and the rules are in [`tracelog/schema.json`](tracelog/schema.json), which is the single source of truth (Go and Python both use it). Some details that are easy to miss:

- **Records come after their own change.** Inside one step, a follower records `advance_commit` before `recv_append_entries`, and a leader records `recv_append_reply` before `advance_commit`. Each record is written after the change it describes.
- **Reply messages get a message id (`MsgID`) but no `send_*` record.** Only `RequestVote` and `AppendEntries` requests get `send_request_vote` and `send_append_entries`. A reply is linked through the record of the request it answers instead: every `recv_request_vote` and `recv_append_entries` sends exactly one reply (rejections too) and carries its id as `reply_msg_id`. So the leader's `recv_append_reply {msg_id: "2-9"}` matches the follower's `recv_append_entries {reply_msg_id: "2-9"}`.
- **The counters are saved separately (decision D6).** `seq` (the record counter) and the message counter are not part of Raft's persistent state. `Restore` restores them whenever the counters are non-zero, even if there is no saved state, so a `seq` is never used twice.
- **`FileSink` appends.** It opens the file for appending, so delete the file if you want a fresh run.

## The in-memory network

`MemNetwork` gives every node an epoch (a life number). It goes up when the node is disconnected or restarted. A message is stamped with the sender's and receiver's epochs, so a restarted node never sees messages from its old life. `Disconnect` also drops messages that are already in flight.

## Catch-up speed (decision D5)

When a follower is behind, the leader steps `nextIndex` back by one each time a reply says "no match". So catch-up takes a number of round trips proportional to how far behind the follower is (linear in the lag). The Figure 8 unreliable test (`TestFigure8Unreliable3C`) therefore runs 50 rounds (30 under `-short`), not 1000.

## The random walk

`internal/testkit` picks one action per turn with these weights (out of 100): deliver 58, heartbeat 15, propose 10, drop 5, duplicate 5, timeout 2, crash 2.5, restart 2.5. The plan first had timeout 10 and deliver 50, but then a timeout knocked the leader out before it could replicate, and 92 of 100 five-node seeds never committed anything. The user changed it on 2026-11-11, and `TestWalkCoverage` fails if the walk ever goes weak again.

## How to run

All commands run from this `raft/` folder.

```bash
go build ./... && go vet ./... && staticcheck ./... && gofmt -l .   # static checks
go test -short ./...                                                # fast tests
go test -race ./...                                                 # full tests
RAFT_SOAK=1 go test -race -run Soak ./internal/testkit/ -timeout 6h # soak: 20000 seeds each for 3 and 5 nodes
go test -race -count=100 ./tests/... -timeout 12h                   # the 100x run, overnight
```

A flaky test is a bug. Find its cause from the trace log and never loosen a timeout.

## Reading a trace with `raftlog`

```bash
go run ./cmd/raftlog trace.jsonl          # from a file
cat trace.jsonl | go run ./cmd/raftlog    # or from stdin
```

It prints one line per record, with detail keys sorted:

```
[n2 t5 LEADER] become_leader votes=[1 2]
```

That is node 2, term 5, role LEADER, event `become_leader`. A bad line stops the tool with an error naming the line number.
