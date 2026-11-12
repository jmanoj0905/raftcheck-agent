# Spec 1: Raft Implementation + Structured Logging

Status: Draft v1 · 2026-11-11 · Author: Manoj J · Updated 2026-11-12 to match what got built (final review)
Parent: [design.md](../design.md), component A
Depends on: nothing · Needed by: Spec 2 (runner), Spec 3 (testbed), Spec 5 (checker)

This spec says **exactly what to build** for your Raft and its log lines, and **how we know it's done**. It is not code. After you approve it, it becomes an implementation plan.

---

## 0. Decisions (all approved 2026-11-11)

These are choices I made while writing this spec. Each one has a reason. If you disagree with any, tell me and I'll rewrite that part.

| # | Decision | Why | What you give up |
|---|---|---|---|
| D1 | Split Raft into a **core** (pure logic, no threads, no timers, no network) and a **wrapper** (threads, real timers, network) | The simulator in Spec 2 must replay a bad sequence **exactly**. That's only possible if nothing happens "by itself" inside Raft. Each core step also lines up 1:1 with a TLA+ action. etcd's Raft uses the same design. | It looks less like a typical MIT student solution, so it doesn't follow the MIT code shape line by line (the interface is the same). |
| D2 | **No snapshots** (MIT Lab 3D) and no cluster membership changes | Snapshots roughly double the size of the TLA+ model and the translation work. They can be added later as their own spec. | Bugs that only happen with snapshots are out of scope for v1. |
| D3 | Planted bugs (Spec 3) will be **logic bugs, not threading bugs** | Follows from D1: the core has no threads. Threading bugs (e.g. holding a lock during a network call) can't be replayed exactly and aren't modelled in TLA+ anyway. Most mistakes in the Students' Guide to Raft are logic bugs. | Threading bugs are out of scope. |
| D4 | We write **our own tests in MIT style**. MIT's official test files are never committed to the public repo | The licence for redistributing MIT's lab files is unclear. You may still run MIT's official tests privately on your laptop as an extra check. | Some extra work writing tests. |
| D5 | Log replication uses the **simple "step back by one"** method when logs disagree, not the faster "jump back by term" trick | Simpler model and simpler translation. The fast trick can be added later behind an on/off option. | Slower catch-up for a lagging follower, which doesn't matter in tests with 3–5 nodes and short logs. |
| D6 | *(added while planning, 2026-11-11)* The record counter (`seq`) and the message counter are **not** part of Raft's persistent state. They are kept as separate `Counters`, saved by the wrapper or runner after every step, and passed back into `Restore`. | Otherwise every heartbeat would change "persistent" state and need a save. That would hide the "send before persist" bug in Spec 3 and make the `persist` log lines meaningless. | None for Raft. Counters are bookkeeping, like a log file that survives a crash. |

---

## 1. Glossary (new words in this spec)

| Term | Meaning |
|---|---|
| Git / repo | Git is the tool that tracks every change to the code. A repo (repository) is one project's folder plus its full change history. |
| Commit | One saved change in git, with a message saying what changed. |
| Branch | A separate line of work in git. You work on a branch, then merge it into `main` when it's ready. |
| `.gitignore` | A file listing what git should never track (build output, secrets, big files). |
| `.gitattributes` | A file telling git how to treat files, e.g. always use Unix line endings. |
| CI | Continuous Integration: GitHub runs the tests automatically on every push and shows a ✓ or ✗. |
| GitHub Actions | GitHub's CI service. It's configured with a YAML file in `.github/workflows/`. |
| Go | The programming language Raft is written in. |
| Goroutine | Go's lightweight thread: a piece of code running at the same time as others. |
| Mutex | Mutual exclusion lock: makes sure only one goroutine changes some data at a time. |
| State machine (in code) | Code that holds some state and changes it only when you hand it an "event". It does nothing on its own. |
| Deterministic | Same input → exactly the same output, every time. |
| Core | The pure Raft logic of this spec (D1). Deterministic. |
| Wrapper | The code around the core that adds goroutines, real timers and a network. |
| Event | One thing that happens to a node: a message arrives, a timer fires, a client sends a command. |
| Output (of a step) | What the core says should happen after an event: save this, send these messages, apply these commands, record these log lines. |
| Persist / persistent state | Saving data so it survives a crash. Raft must persist `currentTerm`, `votedFor` and `log`. |
| Volatile state | Data lost on crash: role, `commitIndex`, `lastApplied`, leader bookkeeping. |
| Index | Position of an entry in the log. Starts at 1, as in the Raft paper. |
| `commitIndex` | Highest log index known to be committed. |
| `lastApplied` | Highest log index already handed to the application. |
| `nextIndex` / `matchIndex` | Per-follower numbers the leader keeps: next entry to send, and highest entry known to be copied. |
| Heartbeat | An empty AppendEntries message the leader sends so followers know it's alive. |
| JSON Schema | A file that describes exactly which fields a JSON record must have. Python (Spec 5) uses it to validate log lines. |
| Hash | A short fingerprint of some data. Same data → same hash. |
| Figure 8 | The scenario in Figure 8 of the Raft paper: a leader must not commit an old-term entry just by counting copies. A classic bug source. |
| Epoch | A "life number" the in-memory network keeps per node. It goes up when the node is disconnected or restarted, so messages from an older life can be thrown away. |
| Lexicographic order | Dictionary order for text: strings are compared character by character, so `"10"` comes before `"2"`. |

---

## 2. Scope

**In scope:**
1. Leader election (MIT Lab 3A).
2. Log replication (MIT Lab 3B).
3. Persistence and crash/restart (MIT Lab 3C).
4. Structured JSONL log lines for every state change (section 6).
5. A wrapper with an MIT-style interface (section 5).
6. Tests (section 8).
7. Git repository, GitHub repo, licence and CI (section 9). **This is the first task of the plan**, done before any Raft code.

**Out of scope (v1):** snapshots / log compaction (D2), membership changes, PreVote, leader leases, read-only query optimisations, batching/pipelining, fast log backtracking (D5), a key/value store on top (MIT Lab 4), real network transport (Spec 8 adds it).

---

## 3. Package layout

```
raft/
  go.mod                  module: github.com/jmanoj0905/raftcheck-agent/raft  (Go 1.27)
  core/                   pure Raft logic (section 4) — no goroutines, no time, no I/O
    types.go              Message, Entry, Event, Output, Role, PersistentState
    core.go               Node struct, New, Restore, Step
    election.go           timeouts, RequestVote handling, becoming leader
    replication.go        Propose, AppendEntries handling, commit rule
    log.go                log helpers (1-based indexing, term lookups)
  tracelog/               log-line types + JSON encoding (section 6)
    record.go
    schema.json           JSON Schema for one record (shared with Python)
  node/                   wrapper (section 5): goroutines, real timers, MIT-style API
    node.go
    transport.go          Transport interface + in-memory implementation for tests
    persister.go          Persister interface + in-memory implementation
  internal/testkit/       test-only helpers: tiny deterministic driver + invariant checks
  tests/                  MIT-style scenario tests (section 8.3)
```

**Rule for the core:** it never imports `time`, `math/rand`, `sync`, `net` or `os`, and never starts a goroutine. An automated test checks this (section 8.1, test C0).

---

## 4. The core (deterministic Raft logic)

### 4.1 State held per node

| Field | Kind | Notes |
|---|---|---|
| `id` | fixed | 1..N |
| `peers` | fixed | all ids, **always kept sorted** (Go map order is random, so we never loop over a map) |
| `currentTerm` | persistent | starts at 0 |
| `votedFor` | persistent | 0 = nobody |
| `log` | persistent | list of `Entry{Term, Command}`, index 1 = first entry |
| `role` | volatile | Follower / Candidate / Leader |
| `commitIndex`, `lastApplied` | volatile | start at 0 |
| `votesGranted` | volatile | set of ids, candidates only |
| `nextIndex`, `matchIndex` | volatile | leaders only, indexed by peer id |
| `leaderId` | volatile | who this node thinks the leader is (0 = unknown) |

`Command` is a short **string** (e.g. `"set x 5"`), so log lines stay readable.

### 4.2 Events (inputs)

The core changes **only** in response to one of these events:

| Event | Meaning |
|---|---|
| `ElectionTimeout{}` | This node's election timer fired. Ignored by a leader. |
| `HeartbeatTimeout{}` | Leader only: time to send AppendEntries to everyone. Ignored by others. |
| `Receive{Msg}` | A message arrived from another node. |
| `Propose{Command}` | A client asks to add a command. Only a leader accepts it. |

Crash and restart are **not** core events. A crash means throwing the core away. A restart means building a new core with `Restore(id, peers, savedState, counters)` (section 4.6).

### 4.3 Messages

| Type | Fields |
|---|---|
| `RequestVote` | term, candidateId, lastLogIndex, lastLogTerm |
| `RequestVoteReply` | term, voteGranted |
| `AppendEntries` | term, leaderId, prevLogIndex, prevLogTerm, entries[], leaderCommit |
| `AppendEntriesReply` | term, success, matchIndex (highest index the follower now matches; used only when success), reqPrevLogIndex (the `prevLogIndex` of the request being answered, so a duplicate failure reply can't step `nextIndex` back twice) |

Every message also has `From`, `To`, and a `MsgID`. (`candidateId` and `leaderId` are simply `From`.) The `MsgID` is `"<from>-<per-node counter>"` (e.g. `"2-17"`) and comes from the message counter in `Counters` (D6), so IDs stay unique across restarts. The runner (Spec 2) uses `MsgID` to say "deliver message 2-17".

Replies (`RequestVoteReply`, `AppendEntriesReply`) get a `MsgID` too, but no `send_*` record. Instead, the `recv_request_vote` or `recv_append_entries` record of the request being answered carries the reply's id as `reply_msg_id` (section 6.3). Every such record sends exactly one reply, rejections included.

### 4.4 Output of each step

```
Step(event) → Output{
    Persist  *PersistentState   // nil = nothing changed; otherwise save this BEFORE sending
    Messages []Message          // in a fixed, deterministic order (sorted by To)
    Apply    []Applied          // Applied{Index, Term, Command}: newly committed entries, in order
    Records  []tracelog.Record  // the log lines for this step (section 6)
    ResetElectionTimer bool     // true = restart this node's election timer (rule 6 in 4.5)
    Proposed *ProposeResult     // set only for a Propose event: (index, term, isLeader) for Start()
}
```

**Ordering rule the wrapper and the runner must follow:** persist first, then send messages, then apply. That's what Raft needs to be correct. Spec 3 can plant a "send before persist" bug by changing this order in a variant.

### 4.5 Behaviour rules

These are exactly the rules in Figure 2 of the Raft paper, with the choices below made explicit:

1. **Any message with a higher term** → set `currentTerm` to it, set `votedFor = 0` and `leaderId = 0`, become follower, and *then* handle the message. All of these changes happen together (like the TLA+ `UpdateTerm` action), and only then are `update_term` and (if the role changed) `become_follower` recorded. So both snapshots show a follower in the new term, never a leader or candidate of a term the node did not win or start.
2. **Messages with a lower term** → reply with the current term and `false` (for requests), or ignore (for replies).
3. **ElectionTimeout** (follower or candidate) → term + 1, vote for self, become candidate, send RequestVote to every peer. With 1 node, it becomes leader straight away.
4. **Granting a vote**: only if `votedFor` is 0 or the candidate, **and** the candidate's log is at least as up-to-date (compare last term first, then last index).
5. **Becoming leader**: on a majority of votes in the *current* term. Set `nextIndex = lastIndex + 1` and `matchIndex = 0` for each peer, and send AppendEntries (heartbeats) to all peers straight away.
6. **The core never "resets a timer" itself.** It sets a flag in the output (`ResetElectionTimer: true`) when Raft says to: on granting a vote, or on accepting AppendEntries from the current leader. The wrapper or runner acts on that flag. Spec 3 can plant the classic "didn't reset the timer" bug by removing the flag.
7. **AppendEntries on a follower**: reject if `prevLogIndex` is missing or has a different term. Otherwise delete any **conflicting** entries (same index, different term) and the entries after them, append any new entries, and set `commitIndex = min(leaderCommit, index of last new entry)`. **Never** cut off entries that match (a classic bug).
8. **Leader on reply**: on success, update `matchIndex` and `nextIndex`. On failure (same term), decrease `nextIndex` by 1 (D5) and resend.
9. **Commit rule (leader)**: move `commitIndex` to N if a majority have `matchIndex ≥ N` **and** `log[N].term == currentTerm`. The second condition is the Figure 8 rule.
10. **Propose** on a leader: append `{currentTerm, command}`, send AppendEntries to all peers, return `(index, term, true)`. On a non-leader: return `(0, currentTerm, false)`, change no state, and write one `propose_rejected` record.
11. **Apply**: whenever `commitIndex > lastApplied`, output entries `lastApplied+1 … commitIndex` and advance `lastApplied`.

### 4.6 Crash and restart

- `PersistentState = {currentTerm, votedFor, log}`. `Counters = {lastSeq, lastMsg}` (D6), read with `node.Counters()` after each step.
- `Restore(id, peers, state, counters)` builds a follower with that state and returns it with an `Output` holding the `restart` record. All volatile state is reset (`commitIndex = 0`, `lastApplied = 0`).
- The caller decides when to restore. The wrapper's `Make` restores whenever there is saved state **or** the saved counters are non-zero, even with no saved state: a node killed before it ever saved still has counters (its `crash` record bumped `seq`), and starting it fresh would reuse `seq` numbers.
- After a restart, already-committed entries are applied again from index 1 once `commitIndex` catches up. This matches MIT's behaviour, and the test kit handles it.

---

## 5. The wrapper (MIT-style interface)

The wrapper makes the core usable like a normal Raft library. Spec 8 (Docker) uses it, and so do the scenario tests.

```go
func Make(id int, peers []int, t Transport, p Persister, applyCh chan ApplyMsg, cfg Config) *Node
func (n *Node) Start(command string) (index int, term int, isLeader bool)
func (n *Node) GetState() (term int, isLeader bool)
func (n *Node) Kill()
```

- One goroutine per node owns the core. Every event (timer, incoming message, `Start` call) goes through **one channel**, so the core is only ever touched by one goroutine and needs no mutex inside it.
- Election timeout: random in the half-open range `[ElectionMin, ElectionMax)` (at least `ElectionMin`, always below `ElectionMax`; default 300–600 ms). Heartbeat: every `HeartbeatInterval` (default 100 ms). All values are in `Config`, and the random source can be seeded.
- On a restart (see 4.6), `Make` first throws away what the old life left behind. If the transport also implements the optional `Restarter` interface, `Make` calls `Restart(id)`, which clears the inbox and every message still in flight to or from the old life. Otherwise it only drains the inbox, so a late message may still arrive.
- Follows the output ordering rule (persist → send → apply) and the `ResetElectionTimer` flag.
- Writes every `Record` from the output to a `RecordSink` (a file, or memory in tests).

**Interfaces:**

```go
type Transport interface {
    Send(m core.Message)              // best effort; may drop
    Inbox(id int) <-chan core.Message
}
type Persister interface {
    Save(id int, s core.PersistentState)
    Load(id int) (core.PersistentState, bool)
    SaveCounters(id int, c core.Counters)   // after every step (D6)
    LoadCounters(id int) core.Counters
}
type Restarter interface {                  // optional, for transports
    Restart(id int)                         // forget everything sent to or by id's old life
}
```

In-memory implementations ship with this spec. The in-memory transport (`MemNetwork`) supports, for tests only: drop rate, extra delay, reordering, and disconnecting a node. It gives every node an **epoch**, which goes up on `Disconnect` and on `Restart`. Each message is stamped with the sender's and receiver's epochs when it is sent, and is delivered only if both are unchanged. So `Disconnect` drops the messages already in flight for good (reconnecting does not bring them back), and a restarted node never sees a message from its old life. `MemNetwork` is a `Restarter`. Spec 2's simulator does **not** use the wrapper. It calls `core.Step` directly.

---

## 6. Structured log lines (the contract with Specs 2, 5 and 10)

### 6.1 Format

- One JSON record per line (JSONL), UTF-8.
- Produced by the **core** inside `Output.Records`, so log lines are deterministic as well.
- One record for **each state change**, written **after** the change. Several records can come from one step (e.g. `update_term` then `become_follower` then `recv_request_vote`).
- In the higher-term rule (4.5 rule 1), the whole step-down happens first, so the `update_term` and `become_follower` snapshots both show the state **after** it: a follower in the new term with no vote and no leader.
- **Order within one step:**
  1. the handler's records, in the order the changes happen;
  2. `persist`, after all handler records (only if term, vote or log changed);
  3. `send_request_vote` / `send_append_entries`, one per request, sorted by `to` (replies have no `send_*` record);
  4. `apply_entry`, one per newly committed entry.

  Because each record comes after its own change, a follower writes `advance_commit` **before** `recv_append_entries`, and a leader writes `recv_append_reply` **before** `advance_commit`.

### 6.2 Fields in every record

| Field | Type | Meaning |
|---|---|---|
| `v` | int | Schema version. Starts at `1`. |
| `node` | int | Node id. |
| `seq` | int | Per-node counter, +1 per record, kept in `Counters` (D6) so it never repeats across restarts. |
| `event` | string | One of the names in 6.3. |
| `term` | int | `currentTerm` **after** the change. |
| `details` | object | Event-specific fields (6.3). |
| `state` | object | Snapshot after the change (6.4). |

The **runner adds** `step` (global step number) and `cause` (the event or message id that caused this step) when it writes records to disk (Spec 2). The core leaves these out.

### 6.3 Event types

| `event` | When | `details` |
|---|---|---|
| `update_term` | term increased because of a higher-term message | `old_term`, `from` |
| `become_follower` | role changed to follower | `old_role` |
| `become_candidate` | election started | `old_role` |
| `become_leader` | won election | `votes` (sorted list of ids) |
| `send_request_vote` | RequestVote sent | `to`, `msg_id`, `last_log_index`, `last_log_term` |
| `recv_request_vote` | RequestVote handled | `from`, `msg_id`, `granted`, `reason` (`ok`/`stale_term`/`already_voted`/`log_not_up_to_date`), `reply_msg_id` (the `MsgID` of the reply this step sends) |
| `recv_vote_reply` | vote reply handled | `from`, `msg_id`, `granted` |
| `send_append_entries` | AppendEntries sent | `to`, `msg_id`, `prev_log_index`, `prev_log_term`, `n_entries`, `leader_commit` |
| `recv_append_entries` | AppendEntries handled | `from`, `msg_id`, `success`, `reason` (`ok`/`stale_term`/`log_mismatch`), `reply_msg_id` (the `MsgID` of the reply this step sends) |
| `recv_append_reply` | reply handled by leader | `from`, `msg_id`, `success`, `match_index` |
| `append_entry` | entry added to own log | `index`, `entry_term`, `command` |
| `truncate_log` | conflicting entries removed | `from_index`, `removed` (count) |
| `advance_commit` | `commitIndex` moved | `old`, `new` |
| `apply_entry` | entry handed to app | `index`, `entry_term`, `command` |
| `persist` | output contained persistent state | `log_len` |
| `restart` | node rebuilt from saved state | `restored_term`, `restored_log_len` |
| `propose_rejected` | `Propose` on a non-leader | `command` |

Replies get no `send_*` record. To follow a reply, match the `msg_id` in the leader's or candidate's `recv_append_reply` / `recv_vote_reply` with the `reply_msg_id` in the other node's `recv_append_entries` / `recv_request_vote`. Every one of these two `recv_*` records sends exactly one reply, including `stale_term` and `log_mismatch` rejections.

A `crash` record is written by the runner or wrapper, not the core, because the core is just thrown away. Format: same fields, `event: "crash"`, `details: {}`, and `state` as last known.

### 6.4 `state` snapshot

```json
{
  "role": "leader",
  "current_term": 5,
  "voted_for": 2,
  "log": [[1, "set x 1"], [3, "set y 2"]],
  "commit_index": 1,
  "last_applied": 1,
  "leader_id": 2,
  "next_index":  {"1": 3, "3": 2},
  "match_index": {"1": 2, "3": 0}
}
```

- `log` lists the **full** log as `[term, command]` pairs. Logs in testing are short (≤ ~10 entries), and the conformance checker needs the whole thing.
- `next_index` / `match_index` are present only on leaders. Keys are sorted in lexicographic order (`"10"` before `"2"`), because they are JSON strings. A leader of a 1-node cluster has no other peers, so its maps are empty and are left out (empty maps are dropped by `omitempty`, Go's "leave out if empty" option).
- JSON keys are written in a fixed order, so the same run gives **byte-identical** files.

### 6.5 Example (node 2 wins an election in term 5)

```json
{"v":1,"node":2,"seq":40,"event":"become_candidate","term":5,"details":{"old_role":"follower"},"state":{...}}
{"v":1,"node":2,"seq":41,"event":"persist","term":5,"details":{"log_len":2},"state":{...}}
{"v":1,"node":2,"seq":42,"event":"send_request_vote","term":5,"details":{"to":1,"msg_id":"2-17","last_log_index":2,"last_log_term":3},"state":{...}}
{"v":1,"node":2,"seq":43,"event":"send_request_vote","term":5,"details":{"to":3,"msg_id":"2-18","last_log_index":2,"last_log_term":3},"state":{...}}
{"v":1,"node":2,"seq":44,"event":"recv_vote_reply","term":5,"details":{"from":1,"msg_id":"1-9","granted":true},"state":{...}}
{"v":1,"node":2,"seq":45,"event":"become_leader","term":5,"details":{"votes":[1,2]},"state":{...}}
```

### 6.6 JSON Schema

`raft/tracelog/schema.json` describes 6.2–6.4 exactly. A Go test checks that every record the tests produce validates against it. Spec 5's Python checker validates against the same file, so both languages agree.

---

## 7. Mapping to the future TLA+ model (why the design looks like this)

| Core event or output | Future TLA+ action / runner action |
|---|---|
| `ElectionTimeout` | `Timeout(i)` |
| `HeartbeatTimeout` | `SendHeartbeats(i)` |
| `Receive{RequestVote}` | `HandleRequestVote(i, m)` |
| `Receive{AppendEntries}` | `HandleAppendEntries(i, m)` |
| `Receive{...Reply}` | `HandleVoteReply` / `HandleAppendReply` |
| `Propose` | `ClientRequest(i, v)` |
| restart via `Restore` | `Restart(i)` |
| message not delivered | `DropMessage(m)` |

This table is a **guide** for Spec 4's prompt, not something Spec 1 builds. It shows that every core step lines up with one model step, which is the whole reason for D1.

---

## 8. Tests (how we know it's correct)

### 8.1 Core unit tests (deterministic, no goroutines)

| ID | Test |
|---|---|
| C0 | The core package imports none of `time`, `math/rand`, `sync`, `net`, `os` (checked with `go list`). |
| C1 | Election timeout on a follower → candidate, term+1, votes for self, RequestVote to every peer, persist present. |
| C2 | A vote is granted only once per term. A second candidate in the same term is refused with `already_voted`. |
| C3 | A vote is refused when the candidate's log is less up-to-date (both "last term lower" and "same term, shorter" cases). |
| C4 | A higher-term message of each kind steps a leader or candidate down to follower and clears `votedFor`. |
| C5 | AppendEntries with a matching prefix never truncates. With a conflict, it truncates from the conflict point. |
| C6 | Figure 8: the leader does **not** commit an old-term entry that's on a majority until an entry from its own term is too. |
| C7 | `Propose` on a non-leader changes nothing and emits `propose_rejected`. |
| C8 | Output ordering: `Persist` is present whenever term, vote or log changed in that step. |
| C9 | `Restore` gives a follower with the saved state and fresh volatile state, and `seq` and message IDs continue from the saved `Counters` (no repeated IDs). |
| C10 | **Determinism:** the same sequence of 10,000 random events (seeded) gives byte-identical record output across 3 runs. |

### 8.2 Randomised safety tests (test kit, no goroutines)

The test kit is a tiny deterministic driver used **only for tests**. It is not Spec 2's runner. It picks random events with a seed: timeouts, deliver/drop/duplicate/reorder messages, proposals, crash/restart. After **every** step it checks the four safety rules on the cluster's state:

1. At most one leader per term.
2. Log matching.
3. Leader completeness.
4. State machine safety (no two nodes apply different commands at the same index).

Rules 1 and 3 are checked from the records as they appear: each `become_leader` record is checked against earlier leaders of that term (rule 1) and against everything committed so far (rule 3), and each `advance_commit` record adds the entries it commits. Leader completeness is checked only for leaders of terms **later** than the term an entry was committed in, as in the Raft paper (§5.4: "present in the logs of the leaders for all higher-numbered terms"). A slow candidate of an older term can still win after a newer term committed something.

It also checks every record's snapshot: a record showing a leader in term T must come from the node that recorded `become_leader` for T, and a record showing a candidate in term T must come after that node's `become_candidate` in T. Each reply sent must be named by exactly one `reply_msg_id` in the same step.

**Messages across restarts:** in the test kit, a crash does not remove messages already in flight. Messages sent to or by a node's old life can still be delivered after it restarts. This is on purpose (a real network can deliver late messages), and it is unlike the wrapper's `MemNetwork`, which drops them (section 5).

**Random-walk weights** (out of 100, decided by the user on 2026-11-11): deliver 58, heartbeat 15, propose 10, drop 5, duplicate 5, timeout 2, crash 2.5, restart 2.5. The first plan had timeout 10 and deliver 50, but then a timeout knocked the leader out before it could replicate, and 92 of 100 five-node seeds never committed anything. `TestWalkCoverage` fails if the walk goes weak again (too few seeds commit, or no commits or log truncations happen at all).

Runs: 3 and 5 nodes, 1,000 seeds × 2,000 steps each, in CI (Continuous Integration: tests run automatically on every push) with fewer seeds, and a longer "soak" run by hand. On failure it prints the seed and the event list, so the failure can be replayed.

### 8.3 Scenario tests through the wrapper (MIT style, real goroutines and timers)

Our own versions of the MIT Lab 3A–3C scenarios (D4): initial election; re-election after the leader disconnects; many elections with random disconnects; basic agreement; agreement with a follower down; no agreement without a majority; concurrent `Start` calls; a disconnected leader rejoins; leader backs up quickly over incorrect follower logs; RPC (Remote Procedure Call) count is reasonable; persistence 1–3; Figure 8 (reliable and unreliable network); unreliable agreement; churn.

All must pass **with Go's race detector on** (`go test -race`), and **100 times in a row** without a single failure (flaky tests count as failing).

Figure 8 on an unreliable network runs **50 rounds** (30 under `-short`), not MIT's 1,000 (decided by the user on 2026-11-11). Because of D5 (step back by one), a lagging follower catches up one entry per round trip, so 1,000 rounds would take far too long.

### 8.4 Log-line tests

- Every record from 8.1–8.3 validates against `schema.json`.
- After **every** test kit action, for **every** live node, the node's real state equals the `state` in the last record that node wrote, whether or not it wrote one in this action. This also catches a state change with no record after it (6.1).
- Spot checks: each event type in 6.3 appears at least once across the test suite.

---

## 9. Repository setup (first task of the plan)

Listed here, but **done first**, before any Raft code. These decisions came from you (2026-11-11).

### 9.1 Where the repo lives

- `raftcheck-agent/` becomes **its own git repo**, with `main` as the default branch.
- It stays inside the `CSE-Projects` folder, and the **parent repo ignores it**: one line `raftcheck-agent/` is added to `CSE-Projects/.gitignore`, the same way `My-Projects/FleetFlow/` is handled today. That change is a separate commit in the parent repo.
- Before `git init`, check that the parent repo has no files from this folder already tracked. If it does, remove them from the parent's tracking only (`git rm --cached`), never from disk.

### 9.2 Author and commits

- **You are the only author.** Commits use your existing identity: `Manoj J` with your GitHub noreply email. **No `Co-Authored-By` lines** (including Claude's), and no "generated with" footers, in commits or pull requests.
- Commit messages are **casual, with a bit of detail**, like a progress note. One line, lowercase is fine. For example:
  - `set up repo with licence, gitignore and ci`
  - `got leader election working in the core, stale terms not handled yet`
  - `fixed vote being granted twice in the same term, added test for it`
  - `figure 8 test passes now, commit rule checks the term`
- Commit small and often: each commit should build and leave the tests passing, so any commit can be checked out safely.

### 9.3 Branches

- **One branch per implementation plan**, named `spec<N>/<short-name>`, e.g. `spec1/repo-setup`, `spec1/raft-core`.
- Merge into `main` only when that plan's tests pass locally and on CI. Merge with a merge commit (`git merge --no-ff`), so each plan shows up as one group in the history. Then delete the branch.
- Never rewrite history on `main` (no force-push).

### 9.4 Files in the first commit

| File | Contents |
|---|---|
| `LICENSE` | Standard MIT License text, `Copyright (c) 2026 Manoj J`. |
| `README.md` | Short for now: one-paragraph summary, "status: design phase", links to `docs/design.md` and `docs/specs/`. Grows with each spec. |
| `.gitignore` | Go build output and coverage files; Python `__pycache__/`, `.venv/`, `.pytest_cache/`; `node_modules/` and dashboard build output; `runs/` (agent output); `.env` and `*.env` (API keys); `.DS_Store`; editor folders (`.idea/`, `.vscode/`); `raft/mit-official/` (MIT's official lab files, kept local only, D4); TLC output (`states/`, `*.tlc.out`). |
| `.gitattributes` | `* text=auto eol=lf`, so line endings are always Unix-style. |
| `CLAUDE.md` | The existing project guide. |
| `docs/` | `design.md` and `specs/` as they are now. |

### 9.5 GitHub

- Create **`jmanoj0905/raftcheck-agent` as a public repo now**, with the description: *"An agent that finds bugs in Raft implementations by checking an AI-written TLA+ model against the real running system."*
- Push `main` after the first commit. The design and specs are public from day one.
- Add the topics: `raft`, `tla-plus`, `distributed-systems`, `llm-agents`, `model-checking`, `fault-injection`.

### 9.6 CI (GitHub Actions)

`.github/workflows/ci.yml` runs on every push and every pull request:

1. Set up Go 1.27, with the module cache saved between runs.
2. `go build ./...`
3. `go vet ./...`
4. `staticcheck ./...`
5. `go test -race -short ./...`: the fast tests only.

**Slow tests use `-short`:** the randomised safety tests (8.2) run fewer seeds under `-short`, and the 100× scenario repeat (8.3) and the long soak run are skipped. Those are run by hand before merging a branch: `go test -race -count=100 ./tests/...`.

The README shows the CI status badge (the small ✓/✗ image).

---

## 10. Done when (acceptance criteria)

1. All tests in section 8 pass, including race-detector runs and 100× repeats of the scenario tests.
2. The core has no forbidden imports (C0).
3. Determinism test C10 passes.
4. `schema.json` exists, and every record in the test suite validates against it.
5. `go vet` and `staticcheck` (a stricter Go code checker) report nothing.
6. A short `raft/README.md` explains the core/wrapper split, the event list and the log format, in plain language.
7. The repo exists as in section 9: its own git repo ignored by the parent, public on GitHub, MIT licence, `.gitignore`/`.gitattributes` in place, and every commit authored only by you.
8. CI passes on `main`, and the badge shows ✓ in the README.
9. **Optional private check:** your Raft passes MIT's official Lab 3A–3C tests locally through a small adapter. The adapter and MIT's files are never committed (D4).

---

## 11. Open questions (small, can be settled during planning)

1. Should `Command` stay a string, or be `[]byte` (raw bytes) with a readable form only in log lines? Recommendation: string. It's simpler, and only tests use commands.
2. Should the Go side also emit a human-readable one-line log (e.g. `[n2 t5 LEADER] won election votes=[1 2]`) for debugging? Recommendation: yes, generated from the same records, as a tool rather than a second logger.
