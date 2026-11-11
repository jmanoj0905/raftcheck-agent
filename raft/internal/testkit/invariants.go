package testkit

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// The four Raft safety rules of spec §8.2, checked from records and node
// states only, so each can be tested with crafted input. The checker keeps
// what it has learned (leaders, committed and applied entries) across steps
// and across crashes. Maps here are only looked up, never ranged over in an
// order that matters: anything ranged over is sorted first.

// liveNode is one running node's current state.
type liveNode struct {
	ID    int
	State tracelog.State
}

// committedEntry is an entry known to be committed at some index.
type committedEntry struct {
	Entry tracelog.LogPair
	Term  int // lowest term of an advance_commit record that covered it
	Node  int // node of that record
}

type invariants struct {
	leaders   map[int]int              // term -> leader id (rule 1)
	committed map[int]committedEntry   // index -> committed entry (rule 3)
	applied   map[int]tracelog.LogPair // index -> applied entry (rule 4)
	appliedBy map[int]int              // index -> first node that applied it
}

func newInvariants() *invariants {
	return &invariants{
		leaders:   map[int]int{},
		committed: map[int]committedEntry{},
		applied:   map[int]tracelog.LogPair{},
		appliedBy: map[int]int{},
	}
}

// checkInvariants folds the records of one step into what the checker knows
// and checks the four rules; live lists the running nodes in id order. It
// returns the first violation.
func (iv *invariants) checkInvariants(recs []tracelog.Record, live []liveNode) error {
	for _, r := range recs {
		var err error
		switch r.Event {
		case tracelog.EvBecomeLeader:
			if err = iv.electionSafety(r); err == nil {
				err = iv.leaderHasCommitted(r)
			}
		case tracelog.EvAdvanceCommit:
			err = iv.addCommitted(r)
		case tracelog.EvApplyEntry:
			err = iv.stateMachineSafety(r)
		}
		if err != nil {
			return fmt.Errorf("node %d seq %d %s: %w", r.Node, r.Seq, r.Event, err)
		}
	}
	return logMatching(live)
}

// Rule 1: at most one leader per term.
func (iv *invariants) electionSafety(r tracelog.Record) error {
	if prev, ok := iv.leaders[r.Term]; ok && prev != r.Node {
		return fmt.Errorf("election safety: term %d has two leaders, nodes %d and %d", r.Term, prev, r.Node)
	}
	iv.leaders[r.Term] = r.Node
	return nil
}

// Rule 2: for every pair of live nodes, if their logs hold entries with the
// same term at an index, the logs are identical up to and including it. It
// is enough to check the highest such index for each pair.
func logMatching(live []liveNode) error {
	for a := 0; a < len(live); a++ {
		for b := a + 1; b < len(live); b++ {
			la, lb := live[a].State.Log, live[b].State.Log
			for i := min(len(la), len(lb)); i >= 1; i-- {
				if la[i-1].Term != lb[i-1].Term {
					continue
				}
				if !slices.Equal(la[:i], lb[:i]) {
					return fmt.Errorf("log matching: nodes %d and %d have term %d at index %d but different logs up to it: %v vs %v",
						live[a].ID, live[b].ID, la[i-1].Term, i, la[:i], lb[:i])
				}
				break
			}
		}
	}
	return nil
}

// Rule 3, first half: advance_commit {old, new} commits log[old+1..new] of
// that node's log. A different entry at an already committed index means a
// committed entry was lost.
func (iv *invariants) addCommitted(r tracelog.Record) error {
	oldC, err := intDetail(r, "old")
	if err != nil {
		return err
	}
	newC, err := intDetail(r, "new")
	if err != nil {
		return err
	}
	if newC > len(r.State.Log) || oldC < 0 || newC <= oldC {
		return fmt.Errorf("advance_commit %d -> %d with log length %d", oldC, newC, len(r.State.Log))
	}
	for i := oldC + 1; i <= newC; i++ {
		e := r.State.Log[i-1]
		prev, ok := iv.committed[i]
		if !ok {
			iv.committed[i] = committedEntry{Entry: e, Term: r.Term, Node: r.Node}
			continue
		}
		if prev.Entry != e {
			return fmt.Errorf("leader completeness: committed entry at index %d changed from %v (node %d, term %d) to %v",
				i, prev.Entry, prev.Node, prev.Term, e)
		}
		if r.Term < prev.Term {
			prev.Term, prev.Node = r.Term, r.Node
			iv.committed[i] = prev
		}
	}
	return nil
}

// Rule 3, second half: a node that becomes leader of term T holds every
// entry committed in a term before T (Raft paper §5.4: "present in the logs
// of the leaders for all higher-numbered terms"). A candidate of an older
// term can still win after a newer term committed something, so those
// leaders are not covered.
func (iv *invariants) leaderHasCommitted(r tracelog.Record) error {
	idxs := make([]int, 0, len(iv.committed))
	for i := range iv.committed {
		idxs = append(idxs, i)
	}
	slices.Sort(idxs)
	log := r.State.Log
	for _, i := range idxs {
		c := iv.committed[i]
		if c.Term >= r.Term {
			continue
		}
		if i > len(log) || log[i-1] != c.Entry {
			return fmt.Errorf("leader completeness: leader of term %d is missing entry %v at index %d, committed in term %d by node %d; its log is %v",
				r.Term, c.Entry, i, c.Term, c.Node, log)
		}
	}
	return nil
}

// Rule 4: no two applies at the same index differ. A restarted node applying
// the same entry again is fine.
func (iv *invariants) stateMachineSafety(r tracelog.Record) error {
	idx, err := intDetail(r, "index")
	if err != nil {
		return err
	}
	term, err := intDetail(r, "entry_term")
	if err != nil {
		return err
	}
	cmd, ok := r.Details["command"].(string)
	if !ok {
		return fmt.Errorf("apply_entry has no string command: %v", r.Details)
	}
	e := tracelog.LogPair{Term: term, Command: cmd}
	if prev, ok := iv.applied[idx]; ok {
		if prev != e {
			return fmt.Errorf("state machine safety: index %d applied as %v by node %d but as %v by node %d",
				idx, prev, iv.appliedBy[idx], e, r.Node)
		}
		return nil
	}
	iv.applied[idx] = e
	iv.appliedBy[idx] = r.Node
	return nil
}

// checkRecordedState is the §8.4 check: if node id wrote records in this
// step, the state in the last of them equals the node's real state.
func checkRecordedState(id int, st tracelog.State, recs []tracelog.Record) error {
	for i := len(recs) - 1; i >= 0; i-- {
		if recs[i].Node != id {
			continue
		}
		if !reflect.DeepEqual(recs[i].State, st) {
			return fmt.Errorf("node %d: state in last record (seq %d %s) is %+v, real state is %+v",
				id, recs[i].Seq, recs[i].Event, recs[i].State, st)
		}
		return nil
	}
	return nil
}

// intDetail reads an integer detail. The core writes Go ints; decoded JSON
// would give float64, so both are accepted.
func intDetail(r tracelog.Record, key string) (int, error) {
	switch v := r.Details[key].(type) {
	case int:
		return v, nil
	case int64:
		return int(v), nil
	case float64:
		if v == float64(int(v)) {
			return int(v), nil
		}
	}
	return 0, fmt.Errorf("%s record has no integer %q: %v", r.Event, key, r.Details)
}
