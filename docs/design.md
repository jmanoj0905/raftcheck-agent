# RaftCheck Agent: Design Document

Status: Draft v1 · 2026-11-07 · Author: Manoj J

This document says **what** we are building and **how the pieces fit together**. It is not code. It will be split into separate specs (section 11), and each spec will become its own implementation plan.

---

## 1. Glossary (read this first)

| Term | Full form / meaning |
|---|---|
| Raft | A set of rules (a "consensus algorithm") that lets several computers agree on one leader and keep the same data, even when some crash. |
| Node | One computer (or one simulated computer) running Raft. |
| Term | Raft's word for an election "round". Each new election starts a new term. |
| Leader | The node in charge for a term. Raft says there can be at most one per term. |
| Log entry | One command (e.g. "set x = 5") stored in a node's list of commands. |
| Committed | A log entry that a majority of nodes have stored, so it must never be lost. |
| TLA+ | Temporal Logic of Actions. A language for writing a "blueprint" (model) of how a system behaves. |
| Spec / model / blueprint | The TLA+ description of the system. In this document they all mean the same thing. |
| TLC | The TLA+ model checker. It tries every possible ordering of events in the model and reports a rule violation if it finds one. |
| SANY | The TLA+ syntax checker (checks that the spec is valid TLA+). |
| Invariant / safety rule | Something that must **never** happen, e.g. two leaders in one term. |
| Counterexample | A sequence of steps, found by TLC, that ends in a rule being broken. |
| Fault | Something going wrong on purpose: crashing a node, cutting the network between nodes, delaying messages. |
| Fault schedule | An exact, step-by-step list of faults and events to replay on the real system. |
| Trace | The recorded list of everything the real system did during one run (from its log lines). |
| Conformance | The share of real-system steps that the model agrees with (a percentage). |
| Divergence point | The first step where the model and the real system disagree. |
| LLM | Large Language Model (e.g. Gemini, Llama). The "brain" of the agent. |
| API | Application Programming Interface: how our code sends requests to an LLM service. |
| Tool calling | The LLM asks to run a tool (e.g. "run TLC on this spec"), our code runs it and sends back the result. |
| Agent | Our Python program: the LLM plus the tools plus the loop that connects them. |
| Ablation | Switching off one part of the system to measure how much it matters. |
| Baseline | A simpler method we compare against to prove the agent is better. |
| Jepsen | A well-known tool that tests distributed systems by injecting random faults. |
| SysMoBench | A public benchmark that scores how well AI-written TLA+ specs match real code. |
| etcd | A widely used database written in Go. Its Raft library is part of SysMoBench. |
| MIT 6.5840 | MIT's Distributed Systems course. Its public lab template defines the Raft interface we follow. |
| RPC | Remote Procedure Call: one node asking another node to run a function (Raft has two: RequestVote and AppendEntries). |
| Docker / container | A tool to run small isolated "computers" on one laptop. |
| tc / netem | Linux traffic control / network emulator: tools that drop, delay or block network packets. |
| JSONL | JSON Lines: a text file with one JSON (JavaScript Object Notation) record per line. |
| FastAPI | A Python framework for building web servers. |
| WebSocket | A connection that lets a web page receive live updates from a server. |
| React | A JavaScript library for building web pages (our dashboard). |
| GRPO | Group Relative Policy Optimization: a reinforcement-learning method for training LLMs (stretch goal only). |

---

## 2. Goal

**One sentence:** build an agent that finds real bugs in Raft implementations by checking an AI-written TLA+ model against the real running system, and fixing whichever of the two is wrong.

**Why:** this is a resume and placement project for AI-engineer and SDE (Software Development Engineer) roles. Success means:

1. A working, public, open-source tool.
2. **Measured** results: bugs found, comparisons with simpler methods, and a public SysMoBench score.
3. A demo video and a live dashboard that recruiters and peers can understand in two minutes.

**Non-goals:**
- A research paper. We may write one later, but no design decision is made for it.
- Supporting non-Raft protocols (Paxos, ZooKeeper) in v1.
- Liveness rules ("something good eventually happens"). v1 checks safety rules only.
- Paid AI models. Everything runs on free tiers.
- Raft snapshots, membership changes and threading bugs (Spec 1, D2/D3).

---

## 3. Constraints

| Constraint | Effect on design |
|---|---|
| Laptop: Apple M1, 8 GB RAM (Random Access Memory), about 33 GB free disk | The simulated mode is the main workhorse. Docker mode runs at most 3–5 small containers. No local LLM while the cluster is running. |
| No money | Free LLM tiers only (e.g. Google Gemini, Groq, OpenRouter free models). There are daily and per-minute limits, so the agent must **cache** LLM answers, **resume** interrupted runs, and respect rate limits. |
| Free models are weaker than the paid models used in published results | We report numbers honestly per model. We compare 2–3 free models, which turns a weakness into a result. |
| No deadline: quality over speed | Nothing is cut to save time. Each spec is finished properly (tested and measured) before anything that depends on it starts. |
| Fair measurement | The agent must never see the planted bug list, the bug-free code or the git history. |

---

## 4. The big picture

```
                ┌──────────────────────────── AGENT (Python) ───────────────────────────┐
                │                                                                        │
 Raft source ──►│ 1. MODEL     LLM reads code + sample traces → writes TLA+ spec          │
 code + logs    │      │                                                                 │
                │      ▼                                                                 │
                │ 2. CHECK     SANY (syntax) → TLC (all orderings) → counterexample?      │
                │      │  yes                                    │ no                    │
                │      ▼                                         ▼                       │
                │ 3. REPLAY    translate counterexample     run random real executions,  │
                │              → fault schedule → run it    check them against the model │
                │      │                                         │ (conformance check)   │
                │      ▼                                         ▼                       │
                │ 4. DECIDE    real system broke the rule? ──► CONFIRMED BUG (+ repro)    │
                │              real system disagreed with model? ──► MODEL WRONG:         │
                │                 send divergence point to LLM, fix the spec, go to 2     │
                └────────────────────────────────────────────────────────────────────────┘
                         │ every step saved                       ▲ fault schedules
                         ▼                                        │ and results
                   Run store (files + SQLite) ──► Dashboard   Cluster runner
                                                  (React)     (simulated or Docker)
```

Main components:

| # | Component | Language | One-line job |
|---|---|---|---|
| A | Raft implementation | Go | Your own Raft, built on the MIT 6.5840 interface, which prints structured log lines. |
| B | Cluster runner | Go | Runs N Raft nodes and lets an outside controller decide exactly which message and timeout happens next. Two modes: simulated and Docker. |
| C | Testbed | Go + scripts | Copies of your Raft, each with one planted bug, plus a registry of what each bug is (hidden from the agent). |
| D | Agent | Python | The model → check → replay → decide loop. |
| E | Trace and conformance checker | Python | Reads real traces and decides (a) did a safety rule break and (b) does the model accept this trace. |
| F | Run store and tracing | Python | Saves every LLM call, tool call, spec version, schedule and verdict. |
| G | Evaluation harness | Python | Runs the agent and the baselines over the testbed and produces result tables. |
| H | SysMoBench adapter | Python | Runs our agent on SysMoBench's etcd Raft task and reports its official metrics. |
| I | Dashboard | React + FastAPI | Live and replayable visual of the loop for the demo video. |
| J | Fine-tuning (stretch) | Python | GRPO training using "agreement with reality" as the reward. |

---

## 5. Component designs

### A. Raft implementation (Go)

- Follows the MIT 6.5840 Lab 3 interface: `Make`, `Start`, `GetState`, a persister (saved state that survives crashes), and an apply channel (how committed commands reach the application).
- Written by you. MIT's public template is used only for the interface and test structure. No course solutions are copied.
- **Split into a core and a wrapper** (Spec 1, decision D1):
  - The **core** is pure Raft logic: no threads, no timers, no network, no randomness. It changes only when handed an event (a message arrived, a timeout fired, a client command). It returns what to save, which messages to send, what to apply, and the log lines. Each core step lines up 1:1 with a TLA+ action.
  - The **wrapper** adds goroutines (Go threads), real randomised timers and a `Transport` (network) interface. It gives the MIT-style `Make`/`Start`/`GetState` interface.
  - The simulator (B) drives the core directly, so replays are exact. Docker mode (B) uses the wrapper.
- **No snapshots** (MIT Lab 3D) and no membership changes in v1 (D2). On a mismatch, the leader steps back by one entry at a time (D5).
- We write our own MIT-style tests. MIT's official test files are never committed (D4).
- Must pass MIT-style tests (elections, log agreement, persistence) before any bugs are planted.

**Log lines (designed once, by you, in a fixed format).** One JSONL record for every important state change:

```
{"seq": 42, "node": 2, "term": 5, "event": "become_leader", "state": {...}}
```

The final list of event types and fields is in Spec 1 §6. The log lines are produced by the core, so they are deterministic too.

Each record includes a **snapshot of the node's key variables** (role, currentTerm, votedFor, the full log, commitIndex, lastApplied, and nextIndex/matchIndex on leaders). That way the trace checker can compare real state with model state step by step.

### B. Cluster runner (Go)

The key idea is that **an outside controller decides what happens next**. Nothing happens "by itself".

**Simulated mode (built first):**
- All N nodes run inside one Go process.
- Messages go into a queue instead of being delivered straight away. There are no real timers: "node 2's election timer fires" is just an event the controller chooses to hand to node 2's core.
- A controller decides each step: deliver message X, drop message X, fire node 2's election timeout, crash node 1, restart node 1, cut the network between {1,2} and {3}, heal it.
- The same seed and the same schedule give **exactly the same run**. This is what makes replaying a TLC counterexample reliable.
- The controller is driven from Python through a simple local API: a small JSON-over-stdin/stdout or HTTP (Hypertext Transfer Protocol) server, chosen in Spec 2.
- Also supports **random mode**: a seeded random choice at each step. This is used for conformance checks and for the random-faults baseline.

**Docker mode (built later):**
- Each node is its own container. Real network between them.
- Faults use tc/netem and/or toxiproxy (a proxy that can cut or slow connections), plus `docker pause/kill/start`.
- Exact ordering cannot be forced, so replay is **best effort**. The runner repeats a schedule K times and reports how often the bug appears (e.g. "reproduced 7/10 runs").
- Purpose: show the bug on "real" networking for the demo and for credibility. It is not the main measurement tool.

**Fault schedule format (shared by both modes):**

```yaml
cluster: {nodes: 3, seed: 1234}
steps:
  - {action: timeout,   node: 1}
  - {action: deliver,   type: RequestVote, from: 1, to: 2}
  - {action: partition, groups: [[1], [2, 3]]}
  - {action: crash,     node: 1, when: after_event, event: append_entry}
  - {action: restart,   node: 1}
expect_violation: election_safety
```

### C. Testbed (planted bugs)

- 20–30 bugs, each taken from **real common student mistakes** (sources: MIT's "Students' Guide to Raft" and similar lists). Examples: not resetting the election timer on granting a vote, voting twice in one term, committing entries from a previous term by counting replicas, not truncating a conflicting log, forgetting to persist before replying.
- **Logic bugs only, not threading bugs** (Spec 1, D3). The core has no threads, and threading bugs can't be replayed exactly or modelled in TLA+.
- One bug per copy of the code (one "variant"). Each variant has a hidden metadata file with the bug id, a description, which safety rule it can break, and whether MIT's tests catch it.
- Some bugs will be **unreachable or harmless** in practice. That is fine and gets recorded, because it tests whether the agent reports false alarms.
- **Isolation:** the agent receives a clean copy of the variant source code only. No `.git` folder, no metadata, no comments that give the bug away.

### D. Agent loop (Python)

Plain Python with the LLM's built-in tool calling. No agent framework.

**LLM provider layer:** one small interface (`complete(messages, tools) → response`) with adapters for 2–3 free providers. Most offer OpenAI-compatible APIs, so one adapter may cover several. It includes:
- rate-limit handling (wait and retry),
- a **response cache** keyed by the exact prompt, so re-runs cost nothing,
- token and call counting for the evaluation.

**Tools the LLM can call:**

| Tool | What it does |
|---|---|
| `read_file`, `list_files` | Read the target's source code (read-only, inside the sandboxed copy). |
| `get_sample_traces` | Get a few random real traces, so the model can be informed by real behaviour. |
| `write_spec` | Save a new version of the `.tla` and `.cfg` (TLC config) files. |
| `check_syntax` | Run SANY and return errors. |
| `run_tlc` | Run TLC with limits (number of nodes, max term, max log length, time limit) and return "no violation" or a counterexample. |
| `translate_counterexample` | Turn a TLC counterexample into a fault schedule (see below). |
| `run_schedule` | Run a schedule on the cluster runner and return the trace and a verdict. |
| `check_conformance` | Check K random real traces against the spec. Returns a conformance % and the first divergence point. |

**The loop (state machine):**

```
START → MODEL → SYNTAX_OK? ─no→ fix (max 5 tries) ─→ give up: INCONCLUSIVE
                  │yes
                  ▼
                 TLC → violation found? ─yes→ TRANSLATE → REPLAY → real rule broken?
                  │no                                         ├─yes→ CONFIRMED_BUG ✔
                  │                                           └─no → MODEL_WRONG
                  ▼                                                     │
          CONFORMANCE CHECK → high enough (≥ threshold)? ─yes→ NO_BUG_FOUND
                  │no                                                    │
                  └──────────── MODEL_WRONG: send divergence point ◄─────┘
                                to LLM, revise spec, back to TLC
```

- **Stop conditions:** confirmed bug, or a budget used up (max iterations, max LLM calls, max wall-clock time per target; the numbers are set in Spec 4).
- **Final verdicts:** `CONFIRMED_BUG` (with a minimal repro schedule), `NO_BUG_FOUND` (with the final conformance %), `INCONCLUSIVE` (with the reason).
- **Minimising the repro:** after a confirmed bug, try removing steps one at a time and keep each removal if the bug still happens. This gives the shortest reproduction.

**Translating a counterexample to a schedule (the hardest part).** It works because we designed the model's actions and the runner's controller to **line up one-to-one**:
- The agent is told the **action vocabulary** the runner supports (timeout, deliver, drop, crash, restart, partition, heal, client request).
- The prompt asks the LLM to name its TLA+ actions after these, so most translation is a simple lookup done by code, not by the LLM.
- The LLM is only asked to translate when a model step does not map directly.
- During replay, the runner checks after each step that the real state matches the counterexample's state. The first mismatch is the divergence point.

**Deciding "bug" vs "model wrong" never depends on the LLM's opinion.** A bug is confirmed only when the **real trace** breaks a safety rule, as checked by component E's independent checker. This means confirmed bugs can never be false alarms.

### E. Trace and conformance checker (Python)

Two separate jobs:

1. **Real-system safety checker.** Reads a real trace and checks the four rules directly. It is hand-written code and does not use the LLM:
   - *Election safety:* at most one leader per term.
   - *Log matching:* if two logs have the same index and term, all earlier entries match.
   - *Leader completeness:* a committed entry is in every later leader's log.
   - *State machine safety:* no two nodes apply different commands at the same index.
2. **Model conformance checker.** Checks whether the spec can produce a given real trace. Approach: turn the real trace into a sequence of expected states, then let TLC check whether the spec can follow those states step by step (the standard "trace validation" technique, which SysMoBench also uses). Output: conformance % and the first step the spec rejects.

### F. Run store and tracing (Python)

- One folder per run: `runs/<run_id>/` with every spec version, every TLC output, every schedule, every trace, and an `events.jsonl` of every LLM call and tool call (inputs, outputs, tokens, time taken).
- A small SQLite database (a single-file database) indexes runs for the evaluation harness and the dashboard.
- Runs can be **resumed** after a crash or after hitting the free-tier limit.

### G. Evaluation harness (Python)

Runs every method over every testbed variant and makes tables and charts.

**Methods compared:**

| Method | Description |
|---|---|
| Full agent | Everything on. |
| Random faults (Jepsen-style) | Same runner, random schedules, same time budget. |
| Blueprint-only agent | Writes the spec and runs TLC, but never runs the real system. Every TLC violation counts as a "bug" report, which measures false alarms. |
| MIT-style tests | Your normal Raft test suite. Does it catch the bug? |
| Ablations | Full agent with one part switched off: no sample traces, no conformance check, no divergence feedback, no repro minimisation. |

Each method is also run with **2–3 free LLMs**.

**Metrics:**
- Bugs found / bugs planted (detection rate), per method and per model.
- False alarms (reported bugs that are not real).
- Time, LLM calls and tokens to the first confirmed bug.
- Final conformance % of the spec.
- Repro length before and after minimising.
- Bugs found that MIT-style tests miss (a headline number for the demo).

Each run is repeated with several seeds, and averages are reported with their spread.

### H. SysMoBench adapter (Python)

- Runs our agent on SysMoBench's **etcd Raft** task (and optionally its other tasks) using SysMoBench's own instrumentation and scoring.
- Reports their four levels: syntax, runtime, conformance, invariant.
- Baseline: the published scores (about 8–15% conformance on etcd Raft, using paid models) and the same free models without our real-system loop.
- **Open question for Spec 7:** whether SysMoBench's code and harness are public and runnable on our laptop. The first task in that spec is to verify this. If not, we re-create its etcd Raft setup as closely as possible and say so clearly.

### I. Dashboard (React + FastAPI)

- A **FastAPI** backend reads the run store and streams events over a **WebSocket**.
- A **React** frontend with four panels:
  1. **Cluster view:** nodes as circles (colour = role), messages as moving dots, partitions as red lines, crashes as greyed-out nodes.
  2. **Model vs reality:** two timelines side by side (the TLC counterexample and the real trace), with the divergence point highlighted.
  3. **Agent log:** each LLM thought or tool call as a card. Spec diffs are shown when the spec is revised.
  4. **Verdict banner:** "BUG CONFIRMED: two leaders in term 5", with a "download repro" button.
- **Replay mode** plays back a saved run at any speed. The demo video is recorded from replay mode, so free-tier slowness never shows on camera.

### J. Fine-tuning (stretch, decided after evaluation)

- Reward for the trained model = (spec passes SANY) + (spec passes TLC) + **(conformance % against real traces)**. The last part is the new idea.
- Data comes from the run store (component F), so logging from day one is all we need now.
- Uses free Kaggle or Colab GPUs (Graphics Processing Units), a small open model and TRL (Transformer Reinforcement Learning, a Hugging Face library). The go/no-go decision is made once the evaluation results (Spec 6) are in.

---

## 6. Data formats (decided in specs, listed here so specs agree)

| Format | Owner spec | Used by |
|---|---|---|
| Raft log line (JSONL) | Spec 1 | Runner, checker, dashboard |
| Runner control API | Spec 2 | Agent, eval harness |
| Fault schedule (YAML) | Spec 2 | Agent, runner, Docker runner |
| Bug metadata | Spec 3 | Eval harness only (hidden from agent) |
| Run store layout + `events.jsonl` | Spec 4 | Eval harness, dashboard, fine-tuning |
| Verdict record | Spec 4 | Eval harness, dashboard |

---

## 7. Project layout (planned)

```
raftcheck-agent/
  docs/            design.md, specs/, plans/
  raft/            Go: your Raft (component A)
  runner/          Go: simulated runner + Docker runner (B)
  testbed/         bug variants + hidden metadata (C)
  agent/           Python: loop, tools, LLM providers (D)
  checker/         Python: safety + conformance checking (E)
  store/           Python: run store (F)
  eval/            Python: harness, baselines, reports (G)
  sysmobench/      Python: adapter (H)
  dashboard/       React frontend + FastAPI backend (I)
  training/        stretch (J)
  runs/            output (git-ignored)
```

---

## 8. Risks and what we do about them

| Risk | Plan |
|---|---|
| Model steps don't map onto real events | Name model actions after runner actions (1:1 vocabulary). Rich state in log lines. Step-by-step state checks during replay. |
| Free LLMs write poor TLA+ | SANY/TLC error feedback loop. Give example specs of *generic* Raft in the prompt (not your code). Report per-model results honestly. |
| Free-tier rate limits slow everything down | Response cache, resumable runs, overnight batch runs, work on non-LLM parts meanwhile. |
| TLC state explosion (too many orderings) | Small limits: 3 nodes, max term 3–4, max log length 2–3, time limit. These limits are reported in the results. |
| 8 GB RAM | Simulated mode for all bulk runs. Docker for a few demos only. Never run Docker and heavy jobs at the same time. |
| SysMoBench not runnable as published | First task of Spec 7 checks this. A fallback is described in H. |
| No bug found in the real-world target | Expected and fine. Planted-bug results and SysMoBench carry the project. |
| Bias: we wrote both the bugs and the agent | Bugs come from public lists of common mistakes. Bug list is frozen before the agent is tuned. Some variants are kept aside as an untouched "final exam" set. |
| Something similar gets published | Re-check arXiv for "TLA+", "conformance", "LLM", "fault injection" when the agent loop first works end-to-end, and again before publishing. |

---

## 9. Testing approach (for our own code)

- Go: unit tests for Raft, the MIT-style test suite, and runner determinism tests (same seed and schedule → identical trace, checked by hash).
- Python: unit tests for the safety checker using hand-made traces that should pass and fail; tests for the translator; agent loop tests with a **fake LLM** that returns scripted answers, so they need no API calls.
- An end-to-end smoke test: a fake LLM plus a hand-written correct spec plus one bug variant must give `CONFIRMED_BUG`.

---

## 10. Open questions (to be settled inside the specs)

1. Runner control API: stdin/stdout JSON vs local HTTP (Spec 2).
2. Exact budget numbers: iterations, calls, time per target (Spec 4).
3. Conformance threshold for `NO_BUG_FOUND` (Spec 4/5).
4. Which 2–3 free providers and models, chosen by checking current free limits at spec time (Spec 4).
5. Which small open-source Raft library to target (Spec 9). It must have a clear license and be small enough to read.
6. Size of the held-out "final exam" bug set (Spec 3).

---

## 11. How this splits into specs

Each spec becomes one implementation plan. Arrows show "needs this spec first".

| Spec | Title | Covers components | Depends on | Done when |
|---|---|---|---|---|
| 1 | Raft implementation + structured logging | A | none | Your Raft passes MIT-style tests and prints the agreed log lines. |
| 2 | Simulated cluster runner + fault schedule format | B (simulated) | 1 | A hand-written schedule reproduces a hand-planted bug identically every time (same trace hash). |
| 3 | Testbed: planted bugs + isolation | C | 1 | 20–30 variants with hidden metadata; MIT-style test results recorded for each; "final exam" set set aside. |
| 4 | Agent core: LLM layer, tools, loop, run store | D, F | 2 | The agent confirms at least one planted bug end-to-end with a minimal repro, and the run can be resumed. |
| 5 | Trace safety checker + conformance checker + translator | E, translation part of D | 1, 2 | Checker passes all hand-made good/bad traces; a hand-written correct spec gets high conformance on real traces. |
| 6 | Evaluation harness + baselines + ablations | G | 3, 4, 5 | Full tables: all methods × 2–3 models × all variants, with several seeds each. |
| 7 | SysMoBench adapter | H | 4, 5 | Official-metric scores on etcd Raft, compared with the published baseline. |
| 8 | Docker cluster mode | B (Docker) | 2 | At least 3 confirmed bugs reproduce in Docker (reported as x/10 runs). |
| 9 | Real-world target (open-source Raft, then etcd) | — | 4, 5, 8 | Findings (or "none found") documented; any real bug reported responsibly; etcd tried only if a bug was found. |
| 10 | Dashboard (React + FastAPI) | I | 4 (run store format) | Live and replay modes work on saved runs; demo video recorded from replay mode. |
| 11 | Fine-tuning (stretch, only if go) | J | 4, 6 | Go/no-go decided from Spec 6 results; if go, trained model compared with untrained on the same tests. |

```
1 ──► 2 ──► 4 ──► 6
│     │     │ ╲
│     │     │  ► 7
│     │     ► 10
│     ► 8 ──► 9
├──► 3 ──► 6
└──► 5 ──► 6, 7, 9
```

Suggested writing order: **1 → 2 → 5 → 3 → 4 → 6**, then 7, 8, 10, 9, and 11 if it's a go.
