package core

import (
	"fmt"
	"slices"
	"strconv"

	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// Node is one Raft node's state. It is not safe for concurrent use; the
// caller (wrapper or simulator) owns it from a single goroutine.
//
// Never range over the maps below: loop over n.peers so the order is fixed.
type Node struct {
	id    int
	peers []int // all ids including id, sorted, no duplicates

	// Persistent. Change only through setTerm, setVote, appendEntry and
	// truncateFrom, so that dirty stays correct.
	currentTerm int
	votedFor    int     // 0 = nobody
	log         []Entry // log[0] is index 1

	// Volatile.
	role         Role
	commitIndex  int
	lastApplied  int
	leaderID     int          // 0 = unknown
	votesGranted map[int]bool // candidates only; ids that granted a vote (incl. self)
	nextIndex    map[int]int  // leaders only; keyed by peer id
	matchIndex   map[int]int  // leaders only; keyed by peer id

	counters Counters

	// Per-step scratch, cleared by finish.
	dirty      bool // a persistent field changed this step
	records    []tracelog.Record
	outbox     []Message
	resetTimer bool           // becomes Output.ResetElectionTimer
	proposed   *ProposeResult // becomes Output.Proposed
}

// New builds a fresh follower in term 0. peers lists every node id including
// id; it is copied and sorted. New panics if an id is below 1, if peers has a
// duplicate, or if id is not in peers.
func New(id int, peers []int) *Node {
	return &Node{id: id, peers: checkPeers(id, peers)}
}

func checkPeers(id int, peers []int) []int {
	ps := slices.Clone(peers)
	slices.Sort(ps)
	for i, p := range ps {
		if p < 1 {
			panic(fmt.Sprintf("core: peer id %d must be at least 1", p))
		}
		if i > 0 && ps[i-1] == p {
			panic(fmt.Sprintf("core: duplicate peer id %d", p))
		}
	}
	if _, ok := slices.BinarySearch(ps, id); !ok {
		panic(fmt.Sprintf("core: node id %d is not in peers %v", id, ps))
	}
	return ps
}

// ID returns this node's id.
func (n *Node) ID() int { return n.id }

// Counters returns the record and message counters to save after each step.
func (n *Node) Counters() Counters { return n.counters }

// State returns a deep copy of the node's state as written in log records.
// Log is never nil. NextIndex and MatchIndex are set only on a leader, with a
// key for every peer except this node.
func (n *Node) State() tracelog.State {
	log := make([]tracelog.LogPair, len(n.log))
	for i, e := range n.log {
		log[i] = tracelog.LogPair{Term: e.Term, Command: e.Command}
	}
	st := tracelog.State{
		Role:        n.role.String(),
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Log:         log,
		CommitIndex: n.commitIndex,
		LastApplied: n.lastApplied,
		LeaderID:    n.leaderID,
	}
	if n.role == Leader {
		st.NextIndex = map[string]int{}
		st.MatchIndex = map[string]int{}
		for _, p := range n.peers {
			if p == n.id {
				continue
			}
			key := strconv.Itoa(p)
			st.NextIndex[key] = n.nextIndex[p]
			st.MatchIndex[key] = n.matchIndex[p]
		}
	}
	return st
}

// Step handles one event and returns what the caller must do.
func (n *Node) Step(ev Event) Output {
	switch ev := ev.(type) {
	case ElectionTimeout:
		n.electionTimeout()
	case Receive:
		n.receive(ev.Msg)
	case HeartbeatTimeout:
		n.heartbeatTimeout()
	case Propose:
		n.propose(ev.Command)
	default:
		panic(fmt.Sprintf("core: unknown event %T", ev))
	}
	return n.finish()
}

// record appends a log record for a change that has already happened. It
// takes the next seq, the current term and a fresh State() snapshot. A nil
// details is written as {}.
func (n *Node) record(event string, details map[string]any) {
	if details == nil {
		details = map[string]any{}
	}
	n.counters.LastSeq++
	n.records = append(n.records, tracelog.Record{
		V:       tracelog.Version,
		Node:    n.id,
		Seq:     n.counters.LastSeq,
		Event:   event,
		Term:    n.currentTerm,
		Details: details,
		State:   n.State(),
	})
}

// The four setters below are the only code that changes persistent state.
// Each sets dirty only when the state really changes, so Output.Persist is
// non-nil exactly when the durable state differs. None of them writes a
// record; the caller records the change (update_term, append_entry, ...).

// setTerm sets currentTerm. It does not touch votedFor.
func (n *Node) setTerm(t int) {
	if t != n.currentTerm {
		n.currentTerm = t
		n.dirty = true
	}
}

// setVote sets votedFor (0 = nobody).
func (n *Node) setVote(id int) {
	if id != n.votedFor {
		n.votedFor = id
		n.dirty = true
	}
}

// appendEntry adds e at index lastIndex()+1. It panics if e.Term < 1.
func (n *Node) appendEntry(e Entry) {
	if e.Term < 1 {
		panic(fmt.Sprintf("core: node %d appending entry with term %d", n.id, e.Term))
	}
	n.log = append(n.log, e)
	n.dirty = true
}

// truncateFrom removes entries i..lastIndex(). It panics unless
// 1 <= i <= lastIndex(), so it always removes at least one entry.
func (n *Node) truncateFrom(i int) {
	if i < 1 || i > n.lastIndex() {
		panic(fmt.Sprintf("core: node %d truncateFrom(%d) with lastIndex %d", n.id, i, n.lastIndex()))
	}
	n.log = n.log[:i-1]
	n.dirty = true
}

// send queues m. It sets m.From to this node; finish assigns MsgID. The
// caller fills in everything else, including Term.
func (n *Node) send(m Message) {
	m.From = n.id
	n.outbox = append(n.outbox, m)
}

// finish turns the step's scratch state into an Output, in this order:
//  1. If a persistent field changed: Persist (a deep copy) and a persist
//     record {log_len}.
//  2. Queued messages, stable-sorted by To, each given the next MsgID. Each
//     RequestVote / AppendEntries gets a send_* record; replies get none,
//     since spec §6.3 has no event for them.
//  3. While commitIndex > lastApplied: lastApplied++, an Applied entry and
//     an apply_entry record.
//
// It then clears the scratch state, so the next Step starts clean.
func (n *Node) finish() Output {
	var out Output
	if n.dirty {
		out.Persist = &PersistentState{
			CurrentTerm: n.currentTerm,
			VotedFor:    n.votedFor,
			Log:         slices.Clone(n.log),
		}
		if out.Persist.Log == nil {
			out.Persist.Log = []Entry{}
		}
		n.record(tracelog.EvPersist, map[string]any{"log_len": len(n.log)})
	}

	slices.SortStableFunc(n.outbox, func(a, b Message) int { return a.To - b.To })
	for i := range n.outbox {
		m := &n.outbox[i]
		n.counters.LastMsg++
		m.MsgID = strconv.Itoa(n.id) + "-" + strconv.FormatInt(n.counters.LastMsg, 10)
		switch m.Type {
		case MsgRequestVote:
			n.record(tracelog.EvSendRequestVote, map[string]any{
				"to": m.To, "msg_id": m.MsgID,
				"last_log_index": m.LastLogIndex, "last_log_term": m.LastLogTerm,
			})
		case MsgAppendEntries:
			n.record(tracelog.EvSendAppendEntries, map[string]any{
				"to": m.To, "msg_id": m.MsgID,
				"prev_log_index": m.PrevLogIndex, "prev_log_term": m.PrevLogTerm,
				"n_entries": len(m.Entries), "leader_commit": m.LeaderCommit,
			})
		}
	}
	out.Messages = n.outbox

	for n.commitIndex > n.lastApplied {
		n.lastApplied++
		e := n.log[n.lastApplied-1]
		out.Apply = append(out.Apply, Applied{Index: n.lastApplied, Term: e.Term, Command: e.Command})
		n.record(tracelog.EvApplyEntry, map[string]any{
			"index": n.lastApplied, "entry_term": e.Term, "command": e.Command,
		})
	}

	out.Records = n.records
	out.ResetElectionTimer = n.resetTimer
	out.Proposed = n.proposed
	n.dirty, n.records, n.outbox, n.resetTimer, n.proposed = false, nil, nil, false, nil
	return out
}
