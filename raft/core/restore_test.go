package core

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jmanoj0905/raftcheck-agent/raft/internal/schemacheck"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

func TestC9Restore(t *testing.T) {
	n := leaderOfTerm(t, 1)
	out := step(t, n, Propose{Command: "set x 1"})
	saved := *out.Persist
	c := n.Counters()
	before := n.State()

	r, rout := Restore(1, []int{1, 2, 3}, saved, c)
	if err := schemacheck.ValidateJSONL(tracelog.AppendJSONL(nil, rout.Records)); err != nil {
		t.Fatalf("restart record fails the schema: %v", err)
	}
	st := r.State()
	if st.CurrentTerm != before.CurrentTerm || st.VotedFor != before.VotedFor || !reflect.DeepEqual(st.Log, before.Log) {
		t.Errorf("restored %+v, want term/vote/log of %+v", st, before)
	}
	if st.Role != "follower" || st.CommitIndex != 0 || st.LastApplied != 0 || st.LeaderID != 0 || st.NextIndex != nil {
		t.Errorf("volatile state not reset: %+v", st)
	}
	if rout.Persist != nil || len(rout.Messages) != 0 || len(rout.Apply) != 0 || len(rout.Records) != 1 {
		t.Fatalf("output = %+v, want exactly one restart record", rout)
	}
	rec := rout.Records[0]
	if rec.Event != tracelog.EvRestart || rec.Seq != c.LastSeq+1 || rec.Term != 1 ||
		!reflect.DeepEqual(rec.Details, map[string]any{"restored_term": 1, "restored_log_len": 1}) {
		t.Errorf("restart record = %+v", rec)
	}
	// Concrete numbers of this scripted run.
	if c.LastSeq != 12 || c.LastMsg != 6 || rec.Seq != 13 {
		t.Errorf("LastSeq %d LastMsg %d restart seq %d, want 12, 6, 13", c.LastSeq, c.LastMsg, rec.Seq)
	}
	next := step(t, r, ElectionTimeout{})
	if got, want := next.Messages[0].MsgID, fmt.Sprintf("1-%d", c.LastMsg+1); got != want || got != "1-7" {
		t.Errorf("first MsgID after restart = %s, want %s (1-7)", got, want)
	}
	if next.Records[len(next.Records)-1].Seq <= rec.Seq {
		t.Errorf("seq did not keep growing after restart")
	}
}

func TestRestoreCopiesLog(t *testing.T) {
	log := []Entry{{1, "a"}, {1, "b"}}
	r, _ := Restore(1, []int{1, 2, 3}, PersistentState{CurrentTerm: 1, Log: log}, Counters{})
	log[0] = Entry{9, "zzz"}
	st := r.State()
	if len(st.Log) != 2 || st.Log[0].Term != 1 || st.Log[0].Command != "a" {
		t.Errorf("caller mutation leaked into node: %+v", st.Log)
	}
}

func TestRestoreEmptyState(t *testing.T) {
	r, out := Restore(2, []int{3, 2, 1}, PersistentState{}, Counters{})
	if st := r.State(); st.Log == nil || len(st.Log) != 0 {
		t.Errorf("Log = %#v, want empty non-nil", st.Log)
	}
	if out.Records[0].Seq != 1 {
		t.Errorf("seq = %d, want 1", out.Records[0].Seq)
	}
}

func TestRestoreValidatesPeers(t *testing.T) {
	for _, peers := range [][]int{{1, 3}, {1, 2, 2}, {0, 2}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("Restore(2, %v) did not panic", peers)
				}
			}()
			Restore(2, peers, PersistentState{}, Counters{})
		}()
	}
}
