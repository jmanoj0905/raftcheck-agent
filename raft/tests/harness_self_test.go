package tests

import (
	"strings"
	"testing"

	"github.com/jmanoj0905/raftcheck-agent/raft/node"
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
