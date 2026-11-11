# raftcheck-agent

An agent that finds real bugs in Raft implementations. It writes a TLA+ (a language for describing system behaviour) model from the source code, uses TLC (the TLA+ model checker) to find a sequence of events that breaks a safety rule, and replays that sequence on the real running system. If the real system breaks the rule too, the bug is confirmed. If it doesn't, the model was wrong, and the agent fixes the model using the first point where the model and the real system disagree.

**Status:** design phase.

- Overall design: [`docs/design.md`](docs/design.md)
- Specs, one per part: [`docs/specs/`](docs/specs/)
