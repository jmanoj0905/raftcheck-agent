package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jmanoj0905/raftcheck-agent/raft/node"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// Tests of the harness itself (not scenarios): the apply checker catches
// what it should, and one / wait / nCommitted / crash / restart work.

// TestApplyChecker feeds made-up ApplyMsgs to check, without any nodes.
func TestApplyChecker(t *testing.T) {
	c := &cluster{logs: []map[int]applied{{}, {}}, agreed: map[int]applied{}}
	msg := func(i int, cmd string, term int) node.ApplyMsg {
		return node.ApplyMsg{CommandValid: true, Command: cmd, CommandIndex: i, CommandTerm: term}
	}
	steps := []struct {
		id      int
		m       node.ApplyMsg
		next    int
		wantErr string
	}{
		{1, msg(1, "a", 1), 1, ""},
		{1, msg(2, "b", 1), 2, ""},
		{2, msg(1, "a", 1), 1, ""},
		{2, msg(3, "c", 1), 2, "want 2"},                   // gap
		{2, msg(2, "x", 1), 2, "was applied there before"}, // other command
		{2, msg(2, "b", 2), 2, "was applied there before"}, // same command, other term
		{1, msg(1, "a", 1), 1, ""},                         // restarted node starts at 1 again
		{1, node.ApplyMsg{CommandIndex: 3}, 3, "CommandValid false"},
	}
	for i, s := range steps {
		err := c.check(s.id, s.m, s.next)
		switch {
		case s.wantErr == "" && err != nil:
			t.Errorf("step %d: unexpected error %v", i, err)
		case s.wantErr != "" && (err == nil || !strings.Contains(err.Error(), s.wantErr)):
			t.Errorf("step %d: error %v, want one containing %q", i, err, s.wantErr)
		}
	}
}

// TestHarnessCrashRestart drives a cluster through one, a crash and a
// restart, and checks the restarted node re-applies the log from 1.
func TestHarnessCrashRestart(t *testing.T) {
	c := newCluster(t, 3, true)
	if i := c.one("a", 3, false); i != 1 {
		t.Fatalf("one(a) committed at %d, want 1", i)
	}
	if got := c.wait(1, 3, -1); got != "a" {
		t.Fatalf("wait(1) = %q, want a", got)
	}
	leader := c.checkOneLeader()
	follower := leader%3 + 1
	c.crash(follower)
	if i := c.one("b", 2, true); i != 2 {
		t.Fatalf("one(b) committed at %d, want 2", i)
	}
	c.restart(follower)
	c.one("c", 3, true) // the restarted node catches up and re-applies 1..3
	if n, cmd := c.nCommitted(2); n != 3 || cmd != "b" {
		t.Fatalf("nCommitted(2) = (%d, %q), want (3, b)", n, cmd)
	}
	if len(c.running()) != 3 || c.rpcTotal() == 0 {
		t.Fatalf("running %d nodes, %d messages", len(c.running()), c.rpcTotal())
	}
	term, _ := c.live()[leader].GetState()
	if got := c.wait(2, 3, term); got != "b" && got != termMoved {
		t.Fatalf("wait(2) = %q", got)
	}
}

// TestTermChangeReport: after the leader is cut off, the report names the
// nodes that left the old term, marks the one whose timer fired, and shows
// timed records.
func TestTermChangeReport(t *testing.T) {
	c := newCluster(t, 3, true)
	leader := c.checkOneLeader()
	term := c.checkTerms()
	if r := c.termChangeReport(term); r != "" {
		t.Fatalf("report before any change: %q", r)
	}
	c.disconnect(leader)
	c.checkOneLeader()
	r := c.termChangeReport(term)
	t.Log(r)
	if !strings.Contains(r, "election timer fired: true") || !strings.Contains(r, `"event":"become_candidate"`) {
		t.Fatalf("report does not show the election:\n%s", r)
	}
	if strings.Contains(r, fmt.Sprintf("node %d left", leader)) {
		t.Fatalf("report lists the cut-off leader %d, whose term did not move:\n%s", leader, r)
	}
}

// TestCheckSink: checkSink catches a bad term in the middle of a long log
// (the part the schema does not see), a record the schema rejects, a seq
// gap and a record from another node, and leaves the sink's records alone.
func TestCheckSink(t *testing.T) {
	rec := func(seq int64, log []tracelog.LogPair) tracelog.Record {
		return tracelog.Record{
			V: tracelog.Version, Node: 1, Seq: seq, Event: tracelog.EvCrash, Term: 1,
			Details: map[string]any{},
			State:   tracelog.State{Role: "follower", CurrentTerm: 1, Log: log},
		}
	}
	long := func(badAt int) []tracelog.LogPair {
		log := make([]tracelog.LogPair, 20)
		for i := range log {
			log[i] = tracelog.LogPair{Term: 1, Command: fmt.Sprint(i)}
		}
		if badAt >= 0 {
			log[badAt].Term = 0
		}
		return log
	}
	cases := []struct {
		name    string
		recs    []tracelog.Record
		wantErr string
	}{
		{"valid", []tracelog.Record{rec(1, long(-1)), rec(2, nil)}, ""},
		{"term 0 mid-log", []tracelog.Record{rec(1, long(10))}, "log entry 11 has term 0"},
		{"term 0 at the end", []tracelog.Record{rec(1, long(19))}, "log entry 20 has term 0"},
		{"schema", []tracelog.Record{func() tracelog.Record { r := rec(1, long(-1)); r.State.Role = "boss"; return r }()}, "seq 1:"},
		{"seq gap", []tracelog.Record{rec(1, nil), rec(3, nil)}, "seq 3, want 2"},
		{"other node", []tracelog.Record{func() tracelog.Record { r := rec(1, nil); r.Node = 2; return r }()}, "record from node 2"},
	}
	for _, tc := range cases {
		var s node.MemorySink
		for _, r := range tc.recs {
			s.Write(r)
		}
		err := checkSink(1, &s)
		switch {
		case tc.wantErr == "" && err != nil:
			t.Errorf("%s: unexpected error %v", tc.name, err)
		case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
			t.Errorf("%s: error %v, want one containing %q", tc.name, err, tc.wantErr)
		}
		if got := len(s.Records()[0].State.Log); got != len(tc.recs[0].State.Log) {
			t.Errorf("%s: checkSink changed the stored log (length %d, was %d)", tc.name, got, len(tc.recs[0].State.Log))
		}
	}
}
