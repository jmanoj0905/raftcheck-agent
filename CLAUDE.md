# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

**Design phase. There is no code yet.** Do not write any code until the user explicitly says so. The workflow is:

`docs/design.md` (overall design) → `docs/specs/NN-*.md` (one spec per part) → `docs/plans/` (one implementation plan per approved spec) → code.

Every step needs the user's approval before the next one starts. **Quality over speed:** there is no timeline, and nothing should be cut to save time.

## What this project is

An agent that finds bugs in Raft implementations. It writes a TLA+ model from the source code, uses the TLC model checker to find a sequence of events that breaks a safety rule, and replays that sequence on the real system. If the real system breaks the rule, the bug is confirmed. If it doesn't, the model was wrong, and the agent fixes it using the first point where the model and the real system disagree.

Read `docs/design.md` first. Section 11 there lists all specs, the order they depend on each other, and each spec's "done when" checks.

## Architecture decisions that span multiple specs

- **Go** for the Raft and the cluster runner. **Python** for the agent, the checkers, the evaluation and the SysMoBench adapter. **React + FastAPI** for the dashboard.
- **The Raft is split into a deterministic core and a wrapper** (Spec 1, decision D1, approved).
  - The core has no goroutines, time, randomness or I/O. Each `Step(event)` returns persist → messages → apply → log records, and that order matters.
  - The simulator (Spec 2) calls the core directly, so replays are exact. The wrapper adds goroutines and timers for the MIT-style tests and Docker mode.
- **The JSONL log-line format is the shared contract** between Go and Python. It is defined in Spec 1 §6. `raft/tracelog/schema.json` will be the single source of truth.
- **A bug is confirmed only by the hand-written safety checker running on a real trace, never by the LLM's opinion.**
- **Fair evaluation:** the agent must never see planted-bug metadata, bug-free code or git history of the testbed variants.
- **Constraints:**
  - Free LLM tiers only. Compare 2–3 free models and report results honestly.
  - M1 laptop with 8 GB RAM. Simulated mode does the bulk of the work; Docker mode runs at most 3–5 containers.
- **v1 is out of scope for:** snapshots, membership changes, liveness, threading bugs, and committing MIT's official lab files to the repo.

## Writing style for docs and questions

The user asked for simple, clear language:

- Write every abbreviation in full on first use, with a short meaning in brackets, e.g. "TLC (the TLA+ model checker)".
- Each spec starts with a "Decisions to confirm" table and a glossary of new terms.
- Each spec ends with "Done when" acceptance criteria and open questions.
- When grilling or clarifying, use the AskUserQuestion tool and put the recommended option first.

## Git rules (full details in Spec 1 §9)

- The user is the **only author**. Never add `Co-Authored-By` lines (including Claude's) or "generated with" footers to commits or PRs (pull requests).
- Commit messages are casual, one line with a bit of detail, like a progress note. Example: `got leader election working in the core, stale terms not handled yet`.
- Use one branch per implementation plan, named `spec<N>/<short-name>`. Merge into `main` with `--no-ff` only after tests pass locally and on CI (Continuous Integration). Never force-push `main`.
- This folder becomes its own repo (`jmanoj0905/raftcheck-agent`, public, MIT licence). The parent repo `CSE-Projects` ignores it through its `.gitignore`. Until Spec 1's setup task runs, it is still inside the parent repo, so don't commit unless asked.
