# Spec 1 (Raft + Structured Logging) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Set up the project's own git repo, then build a deterministic Raft core in Go, plus an MIT-style wrapper and JSONL (JSON Lines) log records. Prove it correct with unit, randomised-safety and scenario tests.

**Architecture:** The `core` package is a pure state machine. `Step(event)` returns an `Output` (persist → messages → apply → records), and the core has no goroutines, time, randomness or I/O (input/output). The `tracelog` package defines the record format and the JSON Schema. `internal/testkit` drives cores deterministically for randomised safety tests. The `node` package wraps a core with goroutines, real timers and in-memory transport and persister for the MIT-style scenario tests in `tests/`.

**Tech Stack:** Go 1.27, standard library, `github.com/santhosh-tekuri/jsonschema/v6` (schema validation, test-side only), staticcheck, GitHub Actions.

**Spec:** `docs/specs/01-raft-and-logging.md`. Read it alongside this plan. Section numbers like "§4.5" below refer to the spec.

## Global Constraints

- Go module: `github.com/jmanoj0905/raftcheck-agent/raft`, in `raft/go.mod`, with `go 1.27`.
- `raft/core` must not import `time`, `math/rand`, `math/rand/v2`, `crypto/rand`, `sync`, `sync/atomic`, `net` (or any `net/...`), or `os`. It must contain no `go` statements and must never `range` over a map: loop over the sorted `peers` slice instead.
- The same events must give **byte-identical** JSONL output.
- Defaults: `ElectionMin` 300 ms, `ElectionMax` 600 ms, `HeartbeatInterval` 100 ms.
- Log indexes start at 1. `votedFor`/`leaderId` 0 means nobody.
- Persistent state is `{CurrentTerm, VotedFor, Log}` only. Counters are separate (D6).
- MIT's official lab files are never committed (`raft/mit-official/` is git-ignored).
- **Git:**
  - Author is `Manoj J` with the existing noreply email. No `Co-Authored-By` lines and no "generated with" footers.
  - Commit messages are casual one-liners with some detail, e.g. `got leader election working in the core, stale terms not handled yet`.
  - All work after Task 1 happens on branch `spec1/raft-and-logging`, merged into `main` with `git merge --no-ff`. Never force-push `main`.
- Only one third-party library: `santhosh-tekuri/jsonschema/v6`, used in `internal/schemacheck` only.

## Review Focus

1. **Duplicate or reordered AppendEntries replies.** A duplicated failure reply must not step `nextIndex` back twice, an old success reply must not lower `matchIndex`, and `nextIndex` must never go below 1. Task 5 tests this (`TestDuplicateFailureReplyDecrementsOnce`, `TestStaleSuccessReplyNeverLowersMatch`).
2. **A vote reply from an earlier term arrives after a new election starts.** It must not be counted towards the new term. Task 4 tests this (`TestOldTermVoteReplyNotCounted`).
3. **A heartbeat whose `leaderCommit` is larger than the follower's matched prefix.** `commitIndex` must become `min(leaderCommit, prevLogIndex+len(entries))`, never the follower's own last index, because the follower may still hold unchecked entries from an older term. Task 5 tests this (`TestCommitCappedAtLastNewEntry`).
4. **A message delivered to the wrong node, or from an id not in `peers`.** That's a runner bug: the core must panic with a clear message instead of handling it silently. Task 4 tests this (`TestMisaddressedMessagePanics`).
5. **`Kill()` while the application isn't reading `applyCh`.** `Kill` must still return promptly, and no `Save` may happen after it returns. Task 8 tests this (`TestKillWithBlockedApplyCh`).

---

## File Structure

```
raftcheck-agent/
  LICENSE  README.md  .gitignore  .gitattributes  CLAUDE.md      (Task 1)
  .github/workflows/ci.yml                                       (Task 2)
  raft/
    go.mod, go.sum                                               (Task 2)
    README.md                                                    (Task 11)
    tracelog/record.go        Record, State, LogPair, event-name constants, Marshal
    tracelog/schema.json      JSON Schema (draft 2020-12) for one record
    tracelog/schema.go        //go:embed schema.json → var SchemaJSON []byte
    internal/schemacheck/     ValidateLine / ValidateJSONL (only user of jsonschema lib)
    core/types.go             Role, Entry, MsgType, Message, Event types, PersistentState, Counters, Applied, ProposeResult, Output
    core/core.go              Node, New, Restore, Step dispatch, State(), Counters(), record/persist/send/apply helpers
    core/log.go               1-based log helpers
    core/election.go          ElectionTimeout, RequestVote, vote replies, becomeLeader, higher-term rule
    core/replication.go       HeartbeatTimeout, Propose, AppendEntries, replies, commit rule, apply
    internal/testkit/         Cluster driver, invariants, random walk
    node/                     persister.go, transport.go, sink.go, node.go
    tests/                    harness_test.go + scenario tests
    cmd/raftlog/main.go       JSONL → one-line human-readable log (spec open question 2)
```

---

### Task 1: Repository bootstrap (on `main`)

**Files:**
- Modify: `/Users/manojj/Documents/CSE-Projects/.gitignore` (parent repo, add one line)
- Create: `LICENSE`, `README.md`, `.gitignore`, `.gitattributes`. `CLAUDE.md` and `docs/` already exist.

**Interfaces:** Produces the git repo at `raftcheck-agent/`, its GitHub remote, and branch `spec1/raft-and-logging`.

- [ ] **Step 1: Check that the parent repo tracks nothing here and has a clean `.gitignore`**

Run: `git -C /Users/manojj/Documents/CSE-Projects ls-files raftcheck-agent; git -C /Users/manojj/Documents/CSE-Projects diff --stat -- .gitignore`
Expected: both print nothing. If either prints something, stop and ask the user (§9.1).

- [ ] **Step 2: Ignore this folder in the parent and commit only that file**

Append `raftcheck-agent/` to the parent `.gitignore`, next to the existing `My-Projects/FleetFlow/` line.
Run: `git -C /Users/manojj/Documents/CSE-Projects commit -m "ignore raftcheck-agent, it has its own repo now" -- .gitignore`
Expected: `1 file changed, 1 insertion(+)`. Nothing else from the parent's dirty tree is committed.

- [ ] **Step 3: Initialise the repo**

Run: `git init -b main && git rev-parse --show-toplevel && git config user.email`
Expected: the top level is `/Users/manojj/Documents/CSE-Projects/raftcheck-agent`, and the email is the `...@users.noreply.github.com` address.

- [ ] **Step 4: Write the first-commit files** with exactly the contents listed in §9.4:
  - `LICENSE`: standard MIT text, `Copyright (c) 2026 Manoj J`.
  - `README.md`: one-paragraph summary from design.md §2, "Status: design phase", links to `docs/design.md` and `docs/specs/`.
  - `.gitignore`: every entry listed in §9.4, plus `*.test`, `*.out`, `coverage.*`, `raft/bin/`.
  - `.gitattributes`: `* text=auto eol=lf`.

- [ ] **Step 5: Check that nothing secret or unwanted is staged**

Run: `git add -A && git status --short`
Expected: only `.gitattributes .gitignore CLAUDE.md LICENSE README.md docs/...`.

- [ ] **Step 6: Commit, create the public GitHub repo, push, add topics**

```bash
git commit -m "set up repo with licence, gitignore, design doc and spec 1"
gh repo create jmanoj0905/raftcheck-agent --public --source=. --remote=origin --push \
  --description "An agent that finds bugs in Raft implementations by checking an AI-written TLA+ model against the real running system."
gh repo edit jmanoj0905/raftcheck-agent --add-topic raft,tla-plus,distributed-systems,llm-agents,model-checking,fault-injection
```
Expected: `gh repo view jmanoj0905/raftcheck-agent --json visibility` gives `"PUBLIC"`, and `git log --format='%an %ae%n%b'` shows only `Manoj J` with no trailers.

- [ ] **Step 7: Create the work branch**

Run: `git switch -c spec1/raft-and-logging`

---

### Task 2: Go module, record format, JSON Schema, CI

**Files:**
- Create: `raft/go.mod`, `raft/tracelog/record.go`, `raft/tracelog/schema.json`, `raft/tracelog/schema.go`, `raft/internal/schemacheck/schemacheck.go`, `.github/workflows/ci.yml`
- Test: `raft/tracelog/record_test.go`, `raft/internal/schemacheck/schemacheck_test.go`

**Interfaces — Produces:**

```go
package tracelog
const Version = 1
// Event names, exactly as in spec §6.3, plus EvCrash = "crash":
const (EvUpdateTerm = "update_term"; EvBecomeFollower = "become_follower"; EvBecomeCandidate = "become_candidate";
  EvBecomeLeader = "become_leader"; EvSendRequestVote = "send_request_vote"; EvRecvRequestVote = "recv_request_vote";
  EvRecvVoteReply = "recv_vote_reply"; EvSendAppendEntries = "send_append_entries"; EvRecvAppendEntries = "recv_append_entries";
  EvRecvAppendReply = "recv_append_reply"; EvAppendEntry = "append_entry"; EvTruncateLog = "truncate_log";
  EvAdvanceCommit = "advance_commit"; EvApplyEntry = "apply_entry"; EvPersist = "persist"; EvRestart = "restart";
  EvProposeRejected = "propose_rejected"; EvCrash = "crash")
var AllEvents []string // the 18 names above, in this order

type LogPair struct{ Term int; Command string }   // marshals as [term, "command"]
type State struct {
  Role        string         `json:"role"`          // "follower" | "candidate" | "leader"
  CurrentTerm int            `json:"current_term"`
  VotedFor    int            `json:"voted_for"`
  Log         []LogPair      `json:"log"`           // never null: empty log → []
  CommitIndex int            `json:"commit_index"`
  LastApplied int            `json:"last_applied"`
  LeaderID    int            `json:"leader_id"`
  NextIndex   map[string]int `json:"next_index,omitempty"`  // leaders only
  MatchIndex  map[string]int `json:"match_index,omitempty"` // leaders only
}
type Record struct {
  V       int            `json:"v"`
  Node    int            `json:"node"`
  Seq     int64          `json:"seq"`
  Event   string         `json:"event"`
  Term    int            `json:"term"`
  Details map[string]any `json:"details"`          // never null: no details → {}
  State   State          `json:"state"`
  Step    *int64         `json:"step,omitempty"`   // filled by the Spec 2 runner, never by the core
  Cause   string         `json:"cause,omitempty"`  // filled by the Spec 2 runner
}
func Marshal(r Record) ([]byte, error)          // one line, no trailing newline; field order = struct order
func AppendJSONL(dst []byte, rs []Record) []byte // each record + "\n"; panics on marshal error (types are closed)

//go:embed schema.json
var SchemaJSON []byte

package schemacheck
func ValidateLine(line []byte) error
func ValidateJSONL(data []byte) error   // error names the 1-based line number
```

- [ ] **Step 1: Create the module**

Run: `mkdir raft && cd raft && go mod init github.com/jmanoj0905/raftcheck-agent/raft && go mod edit -go=1.27`

- [ ] **Step 2: Write failing record tests** in `raft/tracelog/record_test.go`
  - `TestMarshalGolden`: a leader record with `Log: [{1,"set x 1"},{3,"set y 2"}]`, `NextIndex {"1":3,"3":2}` and `Details {"votes":[]int{1,2}}` marshals to an exact golden string whose keys appear in the order `v,node,seq,event,term,details,state`, with `"log":[[1,"set x 1"],[3,"set y 2"]]`.
  - `TestEmptyLogAndDetails`: a record with nil `Log` and nil `Details` contains `"log":[]` and `"details":{}`, and never `null`.
  - `TestFollowerOmitsIndexMaps`: a follower record has no `next_index` or `match_index` key.
  - `TestLogPairRoundTrip`: unmarshalling `[[2,"a"]]` into `[]LogPair` gives `{2,"a"}`.
  - `TestAllEventsCount`: `len(AllEvents) == 18`, with no duplicates.

- [ ] **Step 3: Run them and confirm they fail**

Run: `cd raft && go test ./tracelog/`
Expected: FAIL (undefined: Record).

- [ ] **Step 4: Implement `record.go`**

`LogPair` gets `MarshalJSON` (outputs `[term,command]`) and `UnmarshalJSON`. `Marshal` replaces a nil `Log` with an empty slice and a nil `Details` with an empty map before encoding, and uses `json.Marshal`, which sorts map keys.

- [ ] **Step 5: Write `schema.json`**

JSON Schema draft 2020-12, covering spec §6.2–6.4 exactly:
- **Top level:** `required [v,node,seq,event,term,details,state]`. Optional `step` (integer ≥ 1) and `cause` (string). `additionalProperties: false`. Field rules: `v` is `const: 1`, `node` ≥ 1, `seq` ≥ 1, `term` ≥ 0, and `event` is an `enum` of the 18 names.
- **`state`:**
  - `required [role,current_term,voted_for,log,commit_index,last_applied,leader_id]` and `additionalProperties: false`.
  - `role` is an enum of `follower`, `candidate`, `leader`.
  - Each `log` item uses `prefixItems [{type:integer,minimum:1},{type:string}]`, with `minItems: 2`, `maxItems: 2` and `items: false`.
  - `next_index` and `match_index` are objects with `propertyNames {pattern:"^[1-9][0-9]*$"}` and integer values ≥ 0.
- **Per-event `details`:** one `allOf` entry per event, of the form `if {properties:{event:{const:X}}} then {properties:{details:{required:[...], additionalProperties:false, properties:{...}}}}`. The field names come from the §6.3 table:
  - `msg_id` matches `"^[1-9][0-9]*-[1-9][0-9]*$"`.
  - `granted` and `success` are booleans.
  - `reason` enums: `ok`/`stale_term`/`already_voted`/`log_not_up_to_date` for `recv_request_vote`, and `ok`/`stale_term`/`log_mismatch` for `recv_append_entries`.
  - `votes` is an array of integers ≥ 1.
  - `crash` requires empty `details` (`maxProperties: 0`).

`schema.go` embeds it.

- [ ] **Step 6: Write failing schemacheck tests** in `raft/internal/schemacheck/schemacheck_test.go`
  - `TestGoldenRecordValid`: the Task 2 golden record passes `ValidateLine`.
  - `TestRejects`: each of these fails `ValidateLine`: `seq` missing; `"event":"vote"`; `recv_request_vote` with `reason:"nope"`; a `log` item `[1]`; an extra top-level key `"x":1`; `crash` with non-empty details; `msg_id:"17"`.
  - `TestValidateJSONLLineNumber`: two valid lines plus a bad third line gives an error containing `line 3`.

- [ ] **Step 7: Implement schemacheck and add the dependency**

Run: `cd raft && go get github.com/santhosh-tekuri/jsonschema/v6`
Compile `tracelog.SchemaJSON` once (`sync.Once` is fine here, since this isn't the core).

- [ ] **Step 8: Run the tests**

Run: `cd raft && go test ./tracelog/ ./internal/schemacheck/`
Expected: PASS.

- [ ] **Step 9: Write `.github/workflows/ci.yml`** (§9.6)
- Triggers: `on: [push, pull_request]`.
- One job on `ubuntu-latest`, with `defaults.run.working-directory: raft`.
- Actions: use the latest major versions of `actions/checkout` and `actions/setup-go`, checked with `gh api repos/actions/checkout/releases/latest --jq .tag_name` and the same for `setup-go`. Configure setup-go with `go-version-file: raft/go.mod` and `cache-dependency-path: raft/go.sum`.
- Steps, in order: `go build ./...`, `go vet ./...`, `go install honnef.co/go/tools/cmd/staticcheck@latest && staticcheck ./...`, `go test -race -short ./...`.

- [ ] **Step 10: Add the CI badge to the root `README.md`, then commit and push**

```bash
git add -A && git commit -m "added the log record format and json schema, plus ci on github"
git push -u origin spec1/raft-and-logging
```
Expected: `gh run watch` finishes with success.

---

### Task 3: Core types, `New`, state snapshot, import guard (C0)

**Files:**
- Create: `raft/core/types.go`, `raft/core/core.go`, `raft/core/log.go`
- Test: `raft/core/guard_test.go`, `raft/core/core_test.go`

**Interfaces — Consumes:** `tracelog.Record`, `tracelog.State`, `tracelog.LogPair`, event constants.
**Produces:**

```go
package core
type Role int; const (Follower Role = iota; Candidate; Leader); func (r Role) String() string // "follower"...
type Entry struct{ Term int; Command string }
type MsgType int; const (MsgRequestVote MsgType = iota + 1; MsgRequestVoteReply; MsgAppendEntries; MsgAppendEntriesReply)
type Message struct {
  Type MsgType; From, To int; MsgID string; Term int
  LastLogIndex, LastLogTerm int                              // RequestVote
  VoteGranted bool                                           // RequestVoteReply
  PrevLogIndex, PrevLogTerm int; Entries []Entry; LeaderCommit int // AppendEntries
  Success bool; MatchIndex int; ReqPrevLogIndex int          // AppendEntriesReply
}
type Event interface{ isEvent() }
type ElectionTimeout struct{}; type HeartbeatTimeout struct{}
type Receive struct{ Msg Message }; type Propose struct{ Command string }
type PersistentState struct{ CurrentTerm, VotedFor int; Log []Entry }
type Counters struct{ LastSeq, LastMsg int64 }               // D6; both start at 0, first record seq=1, first msg "<id>-1"
type Applied struct{ Index, Term int; Command string }
type ProposeResult struct{ Index, Term int; IsLeader bool }
type Output struct {
  Persist *PersistentState; Messages []Message; Apply []Applied
  Records []tracelog.Record; ResetElectionTimer bool; Proposed *ProposeResult // non-nil only for Propose
}
func New(id int, peers []int) *Node                           // peers includes id; copied + sorted; panics if id ∉ peers or duplicates
func (n *Node) ID() int
func (n *Node) State() tracelog.State                         // deep copy; maps only when Leader
func (n *Node) Counters() Counters
func (n *Node) Step(ev Event) Output                          // dispatch; real handlers arrive in Tasks 4–5
```

Unexported helpers in `core.go` that later tasks use:
- `n.record(event string, details map[string]any)` appends a record with the next seq and a fresh `State()`.
- `n.setTerm(t)`, `n.setVote(id)`, `n.appendEntry(e)` and `n.truncateFrom(i)` are the **only** ways persistent fields change, and each sets `n.dirty`.
- `n.send(m Message)` queues an outgoing message.
- `n.finish() Output` builds the output in this order: (1) if dirty, set `Persist` (a deep copy) and add a `persist` record (`log_len`). (2) Stable-sort queued messages by `To`, assign MsgIDs, and add one `send_*` record per message. (3) While `commitIndex > lastApplied`, increment `lastApplied`, add an `Applied` entry and an `apply_entry` record.
- `log.go`: `lastIndex()`, `lastTerm()`, `termAt(i) int` (0 for i == 0), `entriesFrom(i) []Entry` (a copy).

- [ ] **Step 1: Write the failing guard test** in `guard_test.go`
  - `TestCoreImportsAndGoroutines`: parse every non-`_test.go` file in the package dir with `go/parser`. Fail if any import path is in, or starts with, the Global Constraints list. Use `ast.Inspect` and fail on any `*ast.GoStmt`.

- [ ] **Step 2: Write the failing core tests** in `core_test.go`
  - `TestNewSortsPeers`: `New(2, []int{3,1,2})`, and the leader's `State` maps (later) use keys `"1","3"`. For now assert `State()` is `{Role:"follower", CurrentTerm:0, VotedFor:0, Log:[], CommitIndex:0, LastApplied:0, LeaderID:0}`, with nil maps.
  - `TestNewPanics`: `New(4, []int{1,2,3})` and `New(1, []int{1,1,2})` panic.
  - `TestStateIsCopy`: mutating the returned `Log` slice doesn't change a later `State()`.

- [ ] **Step 3: Run and confirm they fail**

Run: `cd raft && go test ./core/`
Expected: FAIL to compile.

- [ ] **Step 4: Implement `types.go`, `core.go` and `log.go`** as described in the Interfaces block. For now, `Step` returns `n.finish()` for every event.

- [ ] **Step 5: Run the tests and staticcheck**

Run: `cd raft && go test ./core/ && go vet ./... && staticcheck ./...`
Expected: PASS, no findings.

- [ ] **Step 6: Commit**

`git commit -am "core types and node skeleton, plus a test that stops the core importing time/sync/os"`

---

### Task 4: Elections in the core

**Files:**
- Create: `raft/core/election.go`
- Test: `raft/core/election_test.go`

**Interfaces:** Consumes the Task 3 helpers. Produces the handling of `ElectionTimeout`, `Receive{RequestVote}`, `Receive{RequestVoteReply}`, the higher-term rule `n.observeTerm(term, from int)` (used by Task 5 too), and `n.becomeLeader()`.

The behaviour follows spec §4.5 rules 1–5. These choices aren't fixed by the spec, so the plan fixes them:
- **Higher-term rule:** a higher term in any message adds `update_term {old_term, from}`. If the node wasn't already a follower, it also adds `become_follower {old_role}`, then `leaderId = 0` and `votedFor = 0`. The message is handled after that.
- **ElectionTimeout on a leader:** returns an empty `Output` with no records.
- **ElectionTimeout on a follower or candidate:** adds `become_candidate {old_role}` (also candidate → candidate). The node votes for itself and resets `leaderId` to 0. It sends RequestVote to each peer except itself, in sorted order. If there's only 1 peer, `becomeLeader` runs in the same step.
- **`recv_request_vote`** is recorded for every RequestVote, including stale ones. A reply is always sent. Reasons are checked in this order: `stale_term`, then `already_voted`, then `log_not_up_to_date`, then `ok`. A granted vote sets `ResetElectionTimer`.
- **`recv_vote_reply`** is recorded for every reply after the higher-term rule. It counts only if the node is a candidate, `reply.Term == currentTerm` and the vote is granted.
- **`becomeLeader`:** adds `become_leader {votes: sorted ids}`, then sets `leaderId = id`, `nextIndex[p] = lastIndex+1` and `matchIndex[p] = 0` for each other peer. It sends AppendEntries to each peer using the Task 5 builder `n.appendEntriesFor(p)`. Until Task 5 exists, write the builder here: `prevLogIndex = nextIndex[p]-1`, `entries = entriesFrom(nextIndex[p])`, `leaderCommit = commitIndex`.
- **Misaddressed messages:** `Receive` with `Msg.To != id`, or `Msg.From` not in peers, panics with `fmt.Sprintf("core: node %d got message %s for node %d from %d", ...)`.

- [ ] **Step 1: Write the failing tests** in `election_test.go`. Use a small helper `stepAll(n, evs...)`, a `recordEvents(out) []string` helper, and message builders.
  - `TestC1ElectionTimeout`: 3-node follower → `Role` candidate, term 1, `VotedFor` 1. `Persist` is non-nil with `{1,1,[]}`. Messages are RequestVote to 2 then 3, with MsgIDs `"1-1"`, `"1-2"`. The record events are exactly `[become_candidate, persist, send_request_vote, send_request_vote]`.
  - `TestSingleNodeBecomesLeaderImmediately`: `New(1,[]int{1})` on timeout gives the events `[become_candidate, become_leader, persist]` and `Role` leader.
  - `TestC2VoteOncePerTerm`: a follower grants a vote to candidate 2 (term 1). Its reply has `VoteGranted` true and `ResetElectionTimer` true. Candidate 3 asking in term 1 gets `reason already_voted`, not granted, and no timer reset. A repeated request from 2 in term 1 is granted again (idempotent).
  - `TestC3LogUpToDate`: the voter's log is `[{1},{2}]`. A candidate with lastLogTerm 1, lastLogIndex 5 gets `log_not_up_to_date`. A candidate with lastLogTerm 2, lastLogIndex 1 also gets `log_not_up_to_date`. A candidate with lastLogTerm 2, lastLogIndex 2 is granted.
  - `TestStaleRequestVoteRejected`: a node in term 3 gets RequestVote term 2 → reply term 3, not granted, `reason stale_term`, `Persist` nil.
  - `TestC4HigherTermStepsDown_Vote`: a leader in term 2 gets a RequestVoteReply with term 5 → events start `[update_term, become_follower, recv_vote_reply]`. Then `CurrentTerm` is 5, `VotedFor` 0, `Role` follower, `LeaderID` 0, and `Persist` is set.
  - `TestWinsElectionWithMajority`: in a 3-node cluster, the candidate gets one granted reply → `become_leader {votes:[1,2]}`, `NextIndex {"2":1,"3":1}`, `MatchIndex {"2":0,"3":0}`, and AppendEntries to 2 and 3.
  - `TestOldTermVoteReplyNotCounted`: the candidate is in term 2. A granted reply carrying term 1 is recorded but not counted. The node stays a candidate.
  - `TestMisaddressedMessagePanics`: node 1 receiving `To: 2` panics, and so does `From: 9`.
  - `TestLeaderIgnoresElectionTimeout`: the output is empty (`len(Records) == 0`, `Persist == nil`).

- [ ] **Step 2: Run them and confirm they fail**

Run: `cd raft && go test ./core/ -run 'C1|C2|C3|C4|Single|Stale|Wins|OldTerm|Misaddressed|LeaderIgnores'`
Expected: FAIL.

- [ ] **Step 3: Implement `election.go`**, following the rules above.

- [ ] **Step 4: Run all core tests**

Run: `cd raft && go test ./core/`
Expected: PASS.

- [ ] **Step 5: Commit**

`git commit -am "got leader election working in the core, votes once per term and checks logs are up to date"`

---

### Task 5: Log replication, commit and apply in the core

**Files:**
- Create: `raft/core/replication.go` (move `appendEntriesFor` here)
- Test: `raft/core/replication_test.go`

**Interfaces:** Consumes `observeTerm`, `becomeLeader` and the helpers. Produces the handling of `HeartbeatTimeout`, `Propose`, `Receive{AppendEntries}` and `Receive{AppendEntriesReply}`, plus `n.advanceCommit()`.

Behaviour follows §4.5 rules 6–11. These choices are fixed here:
- **Every AppendEntries carries entries from `nextIndex[p]`**, including heartbeats. That way a dropped message gets repaired by the next heartbeat.
- **AppendEntries with term ≥ current:** after the higher-term rule, a candidate in the same term adds `become_follower`. Then `leaderId = From` and `ResetElectionTimer = true`, even if the log check then fails.
- **Follower log handling:** for each entry, either skip it (same term already present), or truncate (`truncate_log {from_index, removed}`) and then append (`append_entry {index, entry_term, command}`).
- **Follower commit:** if `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, prevLogIndex+len(entries))`, but only if that's larger than the current value, and add `advance_commit {old,new}`.
- **Follower reply:** `MatchIndex = prevLogIndex + len(entries)` on success, and `ReqPrevLogIndex = prevLogIndex` always.
- **`recv_append_entries`** is recorded after the log changes, with `reason ok`, `stale_term` or `log_mismatch`.
- **Reply on the leader:**
  - A reply is handled only if the node is still leader and `reply.Term == currentTerm`.
  - On success: `matchIndex = max(matchIndex, reply.MatchIndex)` and `nextIndex = max(nextIndex, matchIndex+1)`, then `advanceCommit`.
  - On failure: only if `nextIndex[p] == ReqPrevLogIndex+1` and `nextIndex[p] > 1`, decrease `nextIndex[p]` by one and send `appendEntriesFor(p)` immediately.
  - `recv_append_reply {from, msg_id, success, match_index}` is always recorded.
- **`advanceCommit`:** check N from `lastIndex` down to `commitIndex+1`. Commit the first N where `log[N].Term == currentTerm` and a majority, counting the leader itself, has `matchIndex ≥ N`.
- **Propose on a leader:** adds `append_entry`, then `advanceCommit` (a 1-node cluster commits immediately), then AppendEntries to each peer. It returns `Proposed {lastIndex, term, true}`.
- **Propose on a non-leader:** adds `propose_rejected {command}` and returns `Proposed {0, currentTerm, false}` with no other change.
- **HeartbeatTimeout on a non-leader:** returns an empty output.

- [ ] **Step 1: Write the failing tests** in `replication_test.go`
  - `TestProposeOnLeader`: a 3-node leader in term 1 proposes `"set x 1"` → `Proposed {1,1,true}`. Events are `[append_entry, persist, send_append_entries, send_append_entries]`. Each AppendEntries has `PrevLogIndex 0` and `Entries [{1,"set x 1"}]`.
  - `TestC7ProposeOnFollower`: the `State()` is unchanged, `Persist` is nil, events are `[propose_rejected]` and `Proposed.IsLeader` is false.
  - `TestHeartbeatCarriesMissingEntries`: after a dropped AppendEntries, a HeartbeatTimeout resends `Entries` starting at `nextIndex`.
  - `TestC5MatchingPrefixNeverTruncates`: the follower log is `[{1,a},{1,b},{1,c}]`. An AppendEntries with prev 1/1 and entries `[{1,b}]` → log unchanged, no `truncate_log`, `Persist` nil, reply `MatchIndex 2`.
  - `TestC5ConflictTruncates`: the follower log is `[{1,a},{1,b},{1,c}]`. An AppendEntries with term 2, prev 1/1 and entries `[{2,x}]` → log `[{1,a},{2,x}]`. Events contain `truncate_log {from_index:2, removed:2}`, then `append_entry {index:2}`.
  - `TestLogMismatchRejected`: when `prevLogIndex` is beyond the end, or its term differs → `Success` false, `reason log_mismatch`, `ReqPrevLogIndex` equal to the request's, and `ResetElectionTimer` still true.
  - `TestCommitCappedAtLastNewEntry`: the follower log is `[{1,a},{1,b},{1,c}]`. A heartbeat with prev 1/1, no entries, `leaderCommit 3` → `commitIndex` 1, not 3.
  - `TestFollowerApplies`: after `commitIndex` advances to 2, `Apply` is `[{1,..},{2,..}]`. The events end with two `apply_entry` records, and `LastApplied` is 2.
  - `TestC6Figure8`: in a 3-node leader in term 3 with log `[{2,old}]`, both peers report `MatchIndex 1` → `commitIndex` stays 0. After a proposal in term 3 and peer 2 reporting `MatchIndex 2` → `commitIndex` 2, and `Apply` contains indexes 1 and 2.
  - `TestDuplicateFailureReplyDecrementsOnce`: `nextIndex[2]` is 3. Two identical failure replies with `ReqPrevLogIndex 2` → `nextIndex[2]` is 2, and only one extra AppendEntries is sent.
  - `TestNextIndexNeverBelowOne`: a failure reply with `ReqPrevLogIndex 0` leaves `nextIndex` at 1.
  - `TestStaleSuccessReplyNeverLowersMatch`: `matchIndex[2]` is 3, and a late success with `MatchIndex 1` leaves it at 3.
  - `TestCandidateStepsDownOnSameTermAppend`: a candidate in term 2 gets AppendEntries in term 2 → `become_follower`, `LeaderID` set to the sender.
  - `TestC4HigherTermStepsDown_Append`: a leader gets an AppendEntriesReply with a higher term → follower, `VotedFor` 0.
  - `TestC8PersistWheneverDurableChanged`: table-driven over every handler above. Compare `PersistentState` before and after. `Persist != nil` must hold exactly when they differ, and the `persist` record must come before every `send_*` record.

- [ ] **Step 2: Run them and confirm they fail**

Run: `cd raft && go test ./core/`
Expected: FAIL on the new tests.

- [ ] **Step 3: Implement `replication.go`**, following the rules above.

- [ ] **Step 4: Run all tests**

Run: `cd raft && go test ./core/ && staticcheck ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

`git commit -am "log replication, commit rule and apply in the core, figure 8 test passes now"`

---

### Task 6: `Restore` and counters (C9)

**Files:**
- Modify: `raft/core/core.go`
- Test: `raft/core/restore_test.go`

**Interfaces — Produces:** `func Restore(id int, peers []int, ps PersistentState, c Counters) (*Node, Output)`. It deep-copies `ps`, builds a follower with all volatile state at zero, and continues the counters. The `Output` holds exactly one `restart {restored_term, restored_log_len}` record, with no persist and no messages.

- [ ] **Step 1: Write the failing test**
  - `TestC9Restore`: run node 1 through an election and a proposal. Save `out.Persist` and `n.Counters()` (e.g. `LastSeq 9`, `LastMsg 4`). Restore from them → `State()` has the same term, vote and log, with `Role` follower, `CommitIndex` 0, `LastApplied` 0 and `LeaderID` 0. The restart record has `Seq 10`. The next ElectionTimeout's first message ID is `"1-5"`.

- [ ] **Step 2:** Run `cd raft && go test ./core/ -run C9`. Expected: FAIL.
- [ ] **Step 3: Implement `Restore`**
- [ ] **Step 4:** Run `cd raft && go test ./core/`. Expected: PASS.
- [ ] **Step 5: Commit**

`git commit -am "restore from saved state, seq and message ids carry on after a restart"`

---

### Task 7: Test kit: deterministic cluster, safety checks, randomised runs (§8.2, C10, §8.4)

**Files:**
- Create: `raft/internal/testkit/cluster.go`, `raft/internal/testkit/invariants.go`, `raft/internal/testkit/random.go`
- Test: `raft/internal/testkit/testkit_test.go`

**Interfaces — Consumes:** `core.New`, `core.Restore`, `Node.Step`, `State()`, `Counters()`, and `schemacheck.ValidateJSONL`.
**Produces:**

```go
func NewCluster(n int) *Cluster                      // ids 1..n
func (c *Cluster) Timeout(id int) error; Heartbeat(id int) error; Propose(id int, cmd string) error
func (c *Cluster) Deliver(i int) error; Drop(i int) error; Duplicate(i int) error   // i = index into InFlight()
func (c *Cluster) Crash(id int) error; Restart(id int) error
func (c *Cluster) InFlight() []core.Message
func (c *Cluster) Records() []tracelog.Record       // all records, in step order
func (c *Cluster) JSONL() []byte
func (c *Cluster) Actions() []string                // e.g. "deliver 1-3", "crash 2"; for replaying failures
func RandomRun(nodes int, seed int64, steps int) (*Cluster, error) // math/rand/v2 PCG(seed, 0)
```

How each action works:
- Every action applies the `Output` in order: (1) store `Persist`; (2) save `Counters()`; (3) append the messages to `InFlight`; (4) collect `Apply`; (5) append the records.
- `Crash` writes a `crash` record itself, using the node's last state and `LastSeq+1`, and saves the counters. After that the node ignores actions until `Restart`.
- `Restart` calls `core.Restore` with the stored state and counters.
- `Deliver` to a crashed node acts as `Drop`.
- After every action, `checkInvariants()` runs, together with the §8.4 check that `State()` equals the `State` in that node's last record from this step, if it emitted any. The first violation is returned as an error that includes the action list.

The four invariants (§8.2):
1. **One leader per term:** a map from term to leader id, built from `become_leader` records and kept across crashes.
2. **Log matching:** for every pair of live nodes, at every index where the terms are equal, the earlier prefixes are equal.
3. **Leader completeness:** every `advance_commit` adds `log[old+1..new]` to a committed map, and any conflict with an existing entry is itself a violation. Every `become_leader` node's log must contain every committed entry.
4. **State machine safety:** a global map from index to applied entry. Any differing apply is a violation (a restarted node re-applying the same entry is fine).

`RandomRun` action weights:

| Action | Weight |
|---|---|
| deliver | 50 |
| heartbeat (on a random leader, if any) | 15 |
| timeout | 10 |
| propose (`"c<k>"`, k increasing, to a random node) | 10 |
| drop | 5 |
| duplicate | 5 |
| crash (only if fewer than n nodes are down) | 2.5 |
| restart (only if any node is down) | 2.5 |

If more than 200 messages are in flight, use only deliver and drop. Actions that aren't possible in the current state are skipped and re-drawn.

- [ ] **Step 1: Write the failing tests** in `testkit_test.go`
  - `TestHandScriptedElectionAndCommit`: a 3-node cluster. Node 1 times out, all messages are delivered, node 1 proposes `"a"`, everything is delivered, then node 1 sends a heartbeat and everything is delivered → every node's `State().CommitIndex` is 1, and no invariant error.
  - `TestInvariantCatchesTwoLeaders`: feed `checkInvariants` two crafted `become_leader` records for term 3 from nodes 1 and 2 → the error mentions `election safety` and `term 3`.
  - `TestInvariantCatchesLogMismatch`, `TestInvariantCatchesLostCommit`, `TestInvariantCatchesDifferentApply`: one crafted-state case per invariant, each expecting its named error.
  - `TestRandomSafety`: for nodes in {3, 5}, use seeds 1..1000 (1..50 under `testing.Short()`), 2,000 steps each, with `t.Parallel()` sub-tests. On error, `t.Fatalf` prints `seed=%d nodes=%d` and the last 50 actions.
  - `TestRandomSoak`: skipped unless `RAFT_SOAK=1`. Seeds 1..20000, 5,000 steps.
  - `TestC10Determinism`: `RandomRun(5, 42, 10000)` three times → the `sha256` of `JSONL()` is identical.
  - `TestRecordsMatchSchema`: for seeds 1..20 with 3 nodes, `schemacheck.ValidateJSONL(c.JSONL())` returns nil.
  - `TestEveryEventTypeAppears`: across seeds 1..200 with 3 nodes, the set of record events covers every name in `tracelog.AllEvents`.

- [ ] **Step 2:** Run `cd raft && go test ./internal/testkit/`. Expected: FAIL to compile.
- [ ] **Step 3: Implement** `cluster.go`, `invariants.go` and `random.go`.
- [ ] **Step 4:** Run `cd raft && go test -race ./internal/testkit/ && go test -race -short ./...`. Expected: PASS. If `TestRandomSafety` fails, fix the core bug it found, add a core unit test that pins that bug, and rerun. Never weaken an invariant to make a test pass.
- [ ] **Step 5: Commit**

`git commit -am "test kit that drives the cores with random events and checks the 4 safety rules after every step"`

---

### Task 8: Wrapper (`node` package)

**Files:**
- Create: `raft/node/persister.go`, `raft/node/transport.go`, `raft/node/sink.go`, `raft/node/node.go`
- Test: `raft/node/node_test.go`

**Interfaces — Consumes:** the core API and `tracelog`.
**Produces** (spec §5, with D6):

```go
type Config struct{ ElectionMin, ElectionMax, HeartbeatInterval time.Duration; Seed uint64; Sink RecordSink }
func DefaultConfig() Config                           // 300ms, 600ms, 100ms, Seed 0, Sink nil (discard)
type ApplyMsg struct{ CommandValid bool; Command string; CommandIndex, CommandTerm int }
type Persister interface{ Save(id int, s core.PersistentState); Load(id int) (core.PersistentState, bool)
  SaveCounters(id int, c core.Counters); LoadCounters(id int) core.Counters }
func NewMemPersister() *MemPersister                  // deep-copies on Save and Load; SaveCount(id) int for tests
type Transport interface{ Send(m core.Message); Inbox(id int) <-chan core.Message }
func NewMemNetwork(ids []int, seed uint64) *MemNetwork
func (n *MemNetwork) Connect(id int); Disconnect(id int); SetDropRate(p float64); SetMaxDelay(d time.Duration)
func (n *MemNetwork) MessageCount() int64             // total Send calls
type RecordSink interface{ Write(r tracelog.Record) }
type MemorySink struct{ ... }; func (s *MemorySink) JSONL() []byte   // safe for concurrent Write
func NewFileSink(path string) (*FileSink, error); func (s *FileSink) Close() error
func Make(id int, peers []int, t Transport, p Persister, applyCh chan ApplyMsg, cfg Config) *Node
func (n *Node) Start(command string) (index, term int, isLeader bool)
func (n *Node) GetState() (term int, isLeader bool)
func (n *Node) Kill()                                  // idempotent; returns after both goroutines exit
```

How the wrapper works:
- **`Make`:** if `p.Load` finds saved state, call `core.Restore(…, p.LoadCounters(id))` and process its Output. Otherwise call `core.New`.
- **The loop goroutine** owns the core. It `select`s on the inbox, the election timer, the heartbeat ticker, a `startCh` request (with a reply channel) and `killCh`.
- **Each `Output`** is processed in this order: write records to the sink, `Save` if `Persist` is set, `SaveCounters`, `Send` each message, push `Apply` to the applier queue, reset the election timer if `ResetElectionTimer` is set or the timer just fired, and update an `atomic.Pointer[tracelog.State]` that `GetState` reads.
- **Election timeout duration:** drawn uniformly from `[ElectionMin, ElectionMax)` using `rand.New(rand.NewPCG(cfg.Seed, uint64(id)))`.
- **The applier goroutine** drains an unbounded queue into `applyCh`, using `select` with `killCh`.
- **`Kill`:** closes `killCh`. The loop writes a `crash` record (last state, `LastSeq+1`), calls `SaveCounters` and exits. `Kill` waits on a WaitGroup for both goroutines.
- **`MemNetwork`:** each inbox is a 1024-buffer channel. Delivery happens after a random delay in `[0, maxDelay)` using `time.AfterFunc`. A message is dropped if the sender or receiver is disconnected (checked both at send time and at delivery time), if it loses the drop-rate roll, or if the inbox is full. `Send` never blocks.

- [ ] **Step 1: Write the failing tests** in `node_test.go`
  - `TestThreeNodesElectLeader`: within 5 s, exactly one of 3 nodes reports `isLeader`.
  - `TestStartReplicates`: `Start("a")` on the leader → every `applyCh` delivers `{true, "a", 1, term}` within 2 s.
  - `TestPersistBeforeSend`: wrap `MemPersister` and `MemNetwork` in recording fakes that share one ordered event log. For every message sent after a step whose `Persist` was non-nil, its `Save` appears first in the log.
  - `TestKillWritesCrashRecordAndStops`: after `Kill()`, the last sink record is `crash` with seq = previous + 1. `SaveCount` doesn't change over the following 500 ms.
  - `TestKillWithBlockedApplyCh`: use an unbuffered `applyCh` that's never read, start 3 commands on a 1-node cluster, then `Kill()` → it returns within 1 s.
  - `TestRestartFromPersister`: run, `Kill`, `Make` again with the same persister → `GetState` term ≥ the old term, and the first record is `restart`.
  - `TestSinkRecordsValid`: the `MemorySink` JSONL from the tests above passes `schemacheck.ValidateJSONL`.

- [ ] **Step 2:** Run `cd raft && go test ./node/`. Expected: FAIL.
- [ ] **Step 3: Implement** the four files.
- [ ] **Step 4:** Run `cd raft && go test -race -count=20 ./node/`. Expected: PASS every time.
- [ ] **Step 5: Commit**

`git commit -am "wrapper with goroutines and real timers around the core, in-memory network and persister for tests"`

---

### Task 9: Scenario test harness + election scenarios (MIT Lab 3A style)

**Files:**
- Create: `raft/tests/harness_test.go`, `raft/tests/election_test.go`

**Interfaces — Consumes:** the whole `node` API and `schemacheck`.
**Produces** (test-only, `package tests`):

```go
func newCluster(t *testing.T, n int, reliable bool) *cluster  // registers t.Cleanup(c.cleanup); reliable=false → drop 0.1, maxDelay 27ms
func (c *cluster) checkOneLeader() int        // polls up to 10×(450–550ms); fails if two leaders share a term or none found
func (c *cluster) checkNoLeader()
func (c *cluster) checkTerms() int            // all connected nodes agree on term
func (c *cluster) nCommitted(index int) (count int, cmd string)  // fails on disagreement
func (c *cluster) one(cmd string, expected int, retry bool) int  // MIT semantics; 10 s budget
func (c *cluster) wait(index, n, startTerm int) string
func (c *cluster) crash(id int); func (c *cluster) restart(id int)  // Kill; Make with same persister
func (c *cluster) disconnect(id int); func (c *cluster) connect(id int)
func (c *cluster) setUnreliable(b bool); func (c *cluster) rpcTotal() int64
func (c *cluster) cleanup()                   // Kill all; fail on any recorded apply error; validate every node's MemorySink JSONL
```

How the harness checks applied commands:
- Each node gets its own `applyCh`, read by a goroutine.
- Within one incarnation (one run between restarts), indexes must arrive as 1, 2, 3, … with no gaps. A restarted node starts again from 1.
- A command at an index must match what every other node applied at that index.
- The first error is stored behind a mutex and reported by the next check call and by `cleanup`.
- No `t.Parallel()` in scenario tests. They run one after another, as in MIT's labs, because timing assertions depend on it.

- [ ] **Step 1: Write the election scenarios** (they will fail until the harness compiles)
  - `TestInitialElection3A`: there's a leader. After 50 ms the term is ≥ 1. After 2× the election timeout, the term is unchanged and there's still one leader.
  - `TestReElection3A`: disconnect the leader → a new leader is elected. Reconnecting the old leader doesn't change that. With 2 of 3 disconnected → `checkNoLeader`. Reconnecting one → a leader again.
  - `TestManyElections3A`: 7 nodes, 10 rounds of disconnecting 3 random nodes → `checkOneLeader` holds each time.

- [ ] **Step 2: Implement `harness_test.go`**

- [ ] **Step 3: Run them**

Run: `cd raft && go test -race -run 3A ./tests/ -count=5`
Expected: PASS, about 15 s per run.

- [ ] **Step 4: Commit**

`git commit -am "scenario test harness and the election scenarios, like mit lab 3a"`

---

### Task 10: Replication and persistence scenarios (MIT Lab 3B/3C style)

**Files:**
- Create: `raft/tests/replication_test.go`, `raft/tests/persist_test.go`

**Interfaces — Consumes:** the Task 9 harness only.

- [ ] **Step 1: Write the replication scenarios** in `replication_test.go`

Own implementations; the assertions match MIT's well-known checks.
- `TestBasicAgree3B`: for indexes 1..3, `nCommitted(i)` is 0 before `one(cmd, 3, false)` returns `i`.
- `TestFollowerFailure3B`: disconnect one follower → still commits. Disconnect the remaining follower → `Start` on the leader isn't committed after 2 s.
- `TestLeaderFailure3B`: disconnecting the leader still lets the other two commit.
- `TestFailNoAgree3B`: in a 5-node cluster, disconnect 3 → the index isn't committed in 2 s. After reconnecting, a new command commits.
- `TestConcurrentStarts3B`: 5 goroutines call `Start` → all 5 commands are committed, each exactly once.
- `TestRejoin3B`: the old leader takes uncommitted entries while partitioned, then rejoins → its extra entries are overwritten, and every node agrees.
- `TestBackup3B`: 5 nodes, with 50 uncommitted entries on the minority side and 50 committed on the majority side. After healing, everything converges within 10 s per `one`.
- `TestCount3B`: the initial election uses ≤ 30 messages on 3 nodes. Agreeing on 10 commands uses ≤ (10+1+3)×3 = 42 extra messages within 1 s. An idle leader sends ≤ 3×20 messages per second.

- [ ] **Step 2: Write the persistence scenarios** in `persist_test.go`
  - `TestPersist13C`: crash and restart all nodes, then the leader, then a follower. Each time, `one` still commits and earlier entries are still there.
  - `TestPersist23C`: 5 nodes, 5 rounds of crash/disconnect/restart patterns.
  - `TestPersist33C`: the leader takes an entry, crashes before commit, and restarts → the entry eventually commits or is overwritten consistently.
  - `TestFigure83C`: 1000 iterations of random leader crashes and restarts on 5 nodes, `one(…, 1, true)` each round, then all nodes up → one final commit on all.
  - `TestFigure8Unreliable3C`: the same with an unreliable network. Under `testing.Short()`, run 200 iterations.
  - `TestUnreliableAgree3C`: 5 nodes, 50 concurrent `one` calls on an unreliable network.
  - `TestReliableChurn3C` and `TestUnreliableChurn3C`: 5 nodes, 5 s of random crash, restart and disconnect while clients `Start`. After healing, everything that was committed is applied identically everywhere.

- [ ] **Step 3: Run them**

Run: `cd raft && go test -race ./tests/ -count=3`
Expected: PASS. The total time per run should stay under about 4 minutes. If a test is flaky, treat it as a bug and find the cause using the sink JSONL plus `cmd/raftlog` (Task 11). Never loosen a timeout to make a failure go away.

- [ ] **Step 4: Commit**

`git commit -am "replication and persistence scenarios incl figure 8 and churn, all passing with -race"`

---

### Task 11: Human-readable log tool, README, final verification, merge

**Files:**
- Create: `raft/cmd/raftlog/main.go`, `raft/cmd/raftlog/main_test.go`, `raft/README.md`
- Modify: root `README.md` (status line)

**Interfaces — Produces:** `func FormatLine(r tracelog.Record) string`, in package `main` and exported for its test, plus a CLI (command-line tool) that reads JSONL from a file argument or from stdin and prints one line per record.

- [ ] **Step 1: Write the failing test**
  - `TestFormatLine`: the `become_leader` record of node 2, term 5 with votes `[1,2]` formats as `[n2 t5 LEADER] become_leader votes=[1 2]`. The detail keys are printed sorted, `key=value` separated by spaces.

- [ ] **Step 2: Implement it, then run the test**

Run: `cd raft && go test ./cmd/raftlog/`
Expected: PASS.

- [ ] **Step 3: Write `raft/README.md`**

Use plain language, as spec §10 item 6 requires:
- the core/wrapper split and why it exists (D1);
- the four events and the Output order;
- the record format, with a link to `tracelog/schema.json`;
- how to run the fast tests, the soak run and the 100× run;
- how to use `raftlog`.

Update the root README status to "Spec 1 done: Raft core + logging".

- [ ] **Step 4: Run the full acceptance check (spec §10)**

```bash
cd raft && go build ./... && go vet ./... && staticcheck ./...
go test -race ./...
RAFT_SOAK=1 go test -race -run Soak ./internal/testkit/ -timeout 6h
go test -race -count=100 ./tests/... -timeout 12h      # overnight
```
Expected: everything passes, with zero failures across all 100 scenario runs.

- [ ] **Step 5: Commit, push, check CI**

`git commit -am "added raftlog pretty printer and raft readme, spec 1 is done"` then `git push`.
Expected: `gh run watch` gives success.

- [ ] **Step 6: Merge into main**

```bash
git switch main && git merge --no-ff spec1/raft-and-logging -m "merge spec 1: raft core, wrapper, logging and tests"
git push origin main && git branch -d spec1/raft-and-logging && git push origin --delete spec1/raft-and-logging
```
Expected: CI is green on `main` and the README badge shows passing. `git log --format='%an%n%b' main | grep -i co-authored` prints nothing.

- [ ] **Step 7 (optional, private):** run MIT's official Lab 3A–3C tests through a local adapter in `raft/mit-official/`, which is git-ignored. Then run `git status --short` and confirm nothing from `raft/mit-official/` appears.
