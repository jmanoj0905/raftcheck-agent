package testkit

import (
	"crypto/sha256"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jmanoj0905/raftcheck-agent/raft/internal/schemacheck"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// deliverAll delivers the oldest in-flight message until none are left.
func deliverAll(t *testing.T, c *Cluster) {
	t.Helper()
	for i := 0; len(c.InFlight()) > 0; i++ {
		if i > 1000 {
			t.Fatalf("messages never drained; actions: %v", c.Actions())
		}
		if err := c.Deliver(0); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHandScriptedElectionAndCommit(t *testing.T) {
	c := NewCluster(3)
	if err := c.Timeout(1); err != nil {
		t.Fatal(err)
	}
	deliverAll(t, c)
	if err := c.Propose(1, "a"); err != nil {
		t.Fatal(err)
	}
	deliverAll(t, c)
	if err := c.Heartbeat(1); err != nil {
		t.Fatal(err)
	}
	deliverAll(t, c)
	for id := 1; id <= 3; id++ {
		st := c.nodes[id-1].State()
		if st.CommitIndex != 1 || st.LastApplied != 1 {
			t.Errorf("node %d commit %d applied %d, want 1 and 1", id, st.CommitIndex, st.LastApplied)
		}
		if len(st.Log) != 1 || st.Log[0] != (tracelog.LogPair{Term: 1, Command: "a"}) {
			t.Errorf("node %d log %v, want [[1 a]]", id, st.Log)
		}
	}
	if err := schemacheck.ValidateJSONL(c.JSONL()); err != nil {
		t.Fatal(err)
	}
}

func TestClusterRejectsBadActions(t *testing.T) {
	c := NewCluster(3)
	for name, err := range map[string]error{
		"timeout 0":  c.Timeout(0),
		"timeout 4":  c.Timeout(4),
		"deliver 0":  c.Deliver(0),
		"drop -1":    c.Drop(-1),
		"duplicate":  c.Duplicate(0),
		"restart up": c.Restart(1),
	} {
		if err == nil {
			t.Errorf("%s: want an error", name)
		}
	}
	if err := c.Crash(2); err != nil {
		t.Fatal(err)
	}
	if c.Crash(2) == nil || c.Timeout(2) == nil || c.Propose(2, "x") == nil || c.Heartbeat(2) == nil {
		t.Error("actions on a crashed node must fail")
	}
	if len(c.Actions()) != 1 {
		t.Errorf("rejected actions were logged: %v", c.Actions())
	}
}

func TestCrashAndRestartContinueSeq(t *testing.T) {
	c := NewCluster(3)
	if err := c.Timeout(1); err != nil {
		t.Fatal(err)
	}
	deliverAll(t, c)
	if err := c.Crash(1); err != nil {
		t.Fatal(err)
	}
	// A message to the crashed node is dropped on delivery.
	if err := c.Timeout(2); err != nil {
		t.Fatal(err)
	}
	deliverAll(t, c)
	if err := c.Restart(1); err != nil {
		t.Fatal(err)
	}
	var seqs []int64
	var events []string
	for _, r := range c.Records() {
		if r.Node == 1 {
			seqs = append(seqs, r.Seq)
			events = append(events, r.Event)
		}
	}
	for i, s := range seqs {
		if s != int64(i+1) {
			t.Fatalf("node 1 seqs %v are not 1..n", seqs)
		}
	}
	crash := slices.Index(events, tracelog.EvCrash)
	if crash < 0 || events[crash+1] != tracelog.EvRestart || crash+2 != len(events) {
		t.Fatalf("node 1 events end %v, want ... crash, restart", events)
	}
	rec := c.Records()[slices.IndexFunc(c.Records(), func(r tracelog.Record) bool { return r.Event == tracelog.EvCrash })]
	if rec.State.Role != "leader" || rec.Term != 1 || len(rec.Details) != 0 {
		t.Errorf("crash record %+v, want leader's last state in term 1 and empty details", rec)
	}
	if !strings.Contains(strings.Join(c.Actions(), "\n"), "dropped") {
		t.Errorf("no delivery to the down node was logged as dropped: %v", c.Actions())
	}
	if err := schemacheck.ValidateJSONL(c.JSONL()); err != nil {
		t.Fatal(err)
	}
}

// rec builds a crafted record for the invariant tests.
func rec(node int, event string, term int, details map[string]any, log ...tracelog.LogPair) tracelog.Record {
	if log == nil {
		log = []tracelog.LogPair{}
	}
	return tracelog.Record{
		V: tracelog.Version, Node: node, Event: event, Term: term, Details: details,
		State: tracelog.State{Role: "follower", CurrentTerm: term, Log: log},
	}
}

func lp(term int, cmd string) tracelog.LogPair { return tracelog.LogPair{Term: term, Command: cmd} }

func wantErr(t *testing.T, err error, parts ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("no error, want one mentioning %q", parts)
	}
	for _, p := range parts {
		if !strings.Contains(err.Error(), p) {
			t.Errorf("error %q does not mention %q", err, p)
		}
	}
}

func TestInvariantCatchesTwoLeaders(t *testing.T) {
	iv := newInvariants()
	ok := rec(1, tracelog.EvBecomeLeader, 3, map[string]any{"votes": []int{1, 2}})
	if err := iv.checkInvariants([]tracelog.Record{ok}, nil); err != nil {
		t.Fatalf("one leader: %v", err)
	}
	// The same leader seen again (e.g. a re-check) is not a second leader.
	if err := iv.checkInvariants([]tracelog.Record{ok}, nil); err != nil {
		t.Fatalf("same leader again: %v", err)
	}
	two := rec(2, tracelog.EvBecomeLeader, 3, map[string]any{"votes": []int{2, 3}})
	wantErr(t, iv.checkInvariants([]tracelog.Record{two}, nil), "election safety", "term 3")
}

func TestInvariantCatchesLogMismatch(t *testing.T) {
	iv := newInvariants()
	good := []liveNode{
		{1, tracelog.State{Log: []tracelog.LogPair{lp(1, "a"), lp(2, "b")}}},
		{2, tracelog.State{Log: []tracelog.LogPair{lp(1, "a"), lp(3, "c")}}},
		{3, tracelog.State{Log: []tracelog.LogPair{lp(1, "a"), lp(2, "b"), lp(2, "d")}}},
	}
	if err := iv.checkInvariants(nil, good); err != nil {
		t.Fatalf("matching logs: %v", err)
	}
	// Same term at index 2 but different entries before it.
	bad := []liveNode{
		{1, tracelog.State{Log: []tracelog.LogPair{lp(1, "a"), lp(2, "b")}}},
		{2, tracelog.State{Log: []tracelog.LogPair{lp(1, "x"), lp(2, "b")}}},
	}
	wantErr(t, iv.checkInvariants(nil, bad), "log matching", "nodes 1 and 2", "index 2")
	// Same term at the same index but a different command.
	bad2 := []liveNode{
		{1, tracelog.State{Log: []tracelog.LogPair{lp(1, "a")}}},
		{3, tracelog.State{Log: []tracelog.LogPair{lp(1, "z")}}},
	}
	wantErr(t, newInvariants().checkInvariants(nil, bad2), "log matching", "nodes 1 and 3")
}

func TestInvariantCatchesLostCommit(t *testing.T) {
	commit := rec(1, tracelog.EvAdvanceCommit, 1, map[string]any{"old": 0, "new": 1}, lp(1, "a"))

	t.Run("leader without a committed entry", func(t *testing.T) {
		iv := newInvariants()
		if err := iv.checkInvariants([]tracelog.Record{commit}, nil); err != nil {
			t.Fatal(err)
		}
		good := rec(3, tracelog.EvBecomeLeader, 2, map[string]any{"votes": []int{1, 3}}, lp(1, "a"))
		if err := iv.checkInvariants([]tracelog.Record{good}, nil); err != nil {
			t.Fatalf("leader with the entry: %v", err)
		}
		bad := rec(2, tracelog.EvBecomeLeader, 3, map[string]any{"votes": []int{2, 3}})
		wantErr(t, iv.checkInvariants([]tracelog.Record{bad}, nil), "leader completeness", "index 1")
	})

	t.Run("lower-term leader is exempt", func(t *testing.T) {
		// Leader completeness covers leaders of later terms only (Raft paper
		// §5.4): a slow candidate can still win an older term.
		iv := newInvariants()
		c2 := rec(1, tracelog.EvAdvanceCommit, 2, map[string]any{"old": 0, "new": 1}, lp(2, "a"))
		if err := iv.checkInvariants([]tracelog.Record{c2}, nil); err != nil {
			t.Fatal(err)
		}
		old := rec(2, tracelog.EvBecomeLeader, 1, map[string]any{"votes": []int{2, 3}})
		if err := iv.checkInvariants([]tracelog.Record{old}, nil); err != nil {
			t.Fatalf("leader of an earlier term: %v", err)
		}
	})

	t.Run("committed entry overwritten", func(t *testing.T) {
		iv := newInvariants()
		if err := iv.checkInvariants([]tracelog.Record{commit}, nil); err != nil {
			t.Fatal(err)
		}
		again := rec(2, tracelog.EvAdvanceCommit, 1, map[string]any{"old": 0, "new": 1}, lp(1, "a"))
		if err := iv.checkInvariants([]tracelog.Record{again}, nil); err != nil {
			t.Fatalf("same entry committed again: %v", err)
		}
		other := rec(3, tracelog.EvAdvanceCommit, 2, map[string]any{"old": 0, "new": 1}, lp(2, "b"))
		wantErr(t, iv.checkInvariants([]tracelog.Record{other}, nil), "leader completeness", "index 1")
	})
}

func TestInvariantCatchesDifferentApply(t *testing.T) {
	iv := newInvariants()
	a := rec(1, tracelog.EvApplyEntry, 1, map[string]any{"index": 1, "entry_term": 1, "command": "a"})
	if err := iv.checkInvariants([]tracelog.Record{a, a}, nil); err != nil {
		t.Fatalf("same entry applied twice (restart): %v", err)
	}
	b := rec(2, tracelog.EvApplyEntry, 1, map[string]any{"index": 1, "entry_term": 1, "command": "b"})
	wantErr(t, iv.checkInvariants([]tracelog.Record{b}, nil), "state machine safety", "index 1")
}

func TestRecordedStateCheck(t *testing.T) {
	st := tracelog.State{Role: "leader", CurrentTerm: 2, Log: []tracelog.LogPair{}}
	r1 := rec(1, tracelog.EvBecomeCandidate, 2, nil)
	r2 := rec(1, tracelog.EvBecomeLeader, 2, nil)
	r2.State = st
	if err := checkRecordedState(1, st, []tracelog.Record{r1, r2}); err != nil {
		t.Fatal(err)
	}
	if err := checkRecordedState(1, st, nil); err != nil {
		t.Fatalf("no records: %v", err)
	}
	wantErr(t, checkRecordedState(1, st, []tracelog.Record{r2, r1}), "node 1", "state")
}

func TestRandomSafety(t *testing.T) {
	seeds := 1000
	if testing.Short() {
		seeds = 50
	}
	for _, nodes := range []int{3, 5} {
		for seed := int64(1); seed <= int64(seeds); seed++ {
			t.Run(fmt.Sprintf("n%d/seed%d", nodes, seed), func(t *testing.T) {
				t.Parallel()
				c, err := RandomRun(nodes, seed, 2000)
				if err != nil {
					acts := c.Actions()
					t.Fatalf("seed=%d nodes=%d: %v\nlast actions:\n%s",
						seed, nodes, err, strings.Join(acts[max(0, len(acts)-50):], "\n"))
				}
			})
		}
	}
}

func TestRandomSoak(t *testing.T) {
	if os.Getenv("RAFT_SOAK") != "1" {
		t.Skip("set RAFT_SOAK=1 to run")
	}
	for _, nodes := range []int{3, 5} {
		for seed := int64(1); seed <= 20000; seed++ {
			t.Run(fmt.Sprintf("n%d/seed%d", nodes, seed), func(t *testing.T) {
				t.Parallel()
				c, err := RandomRun(nodes, seed, 5000)
				if err != nil {
					acts := c.Actions()
					t.Fatalf("seed=%d nodes=%d: %v\nlast actions:\n%s",
						seed, nodes, err, strings.Join(acts[max(0, len(acts)-50):], "\n"))
				}
			})
		}
	}
}

func TestC10Determinism(t *testing.T) {
	var sums [3][32]byte
	// The runs share nothing, so they can run in parallel; t.Run returns
	// once all of them are done.
	t.Run("runs", func(t *testing.T) {
		for i := range sums {
			t.Run(fmt.Sprintf("run%d", i), func(t *testing.T) {
				t.Parallel()
				c, err := RandomRun(5, 42, 10000)
				if err != nil {
					t.Fatal(err)
				}
				sums[i] = sha256.Sum256(c.JSONL())
			})
		}
	})
	if sums[0] != sums[1] || sums[1] != sums[2] {
		t.Fatalf("JSONL differs between runs: %x %x %x", sums[0], sums[1], sums[2])
	}
}

func TestRecordsMatchSchema(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			t.Parallel() // the validator is slow under -race
			c, err := RandomRun(3, seed, 2000)
			if err != nil {
				t.Fatal(err)
			}
			if err := schemacheck.ValidateJSONL(c.JSONL()); err != nil {
				t.Fatalf("seed %d: %v", seed, err)
			}
		})
	}
}

func TestEveryEventTypeAppears(t *testing.T) {
	const seeds = 200
	var perSeed [seeds]map[string]bool
	t.Run("seeds", func(t *testing.T) { // returns once every seed is done
		for i := range perSeed {
			t.Run(fmt.Sprintf("seed%d", i+1), func(t *testing.T) {
				t.Parallel()
				c, err := RandomRun(3, int64(i+1), 2000)
				if err != nil {
					t.Fatal(err)
				}
				perSeed[i] = map[string]bool{}
				for _, r := range c.Records() {
					perSeed[i][r.Event] = true
				}
			})
		}
	})
	seen := map[string]bool{}
	for _, m := range perSeed {
		for ev := range m {
			seen[ev] = true
		}
	}
	for _, ev := range tracelog.AllEvents {
		if !seen[ev] {
			t.Errorf("event %q never appeared", ev)
		}
	}
}

// TestWalkCoverage stops the random walk from quietly going weak: most
// seeds must commit something, and at 5 nodes the runs must reach commits
// and log conflicts, or the later safety rules go untested. Measured on
// 2026-11-11 with seeds 1..50: n=3 50/50 commit, n=5 46/50.
func TestWalkCoverage(t *testing.T) {
	const seeds = 50
	for _, tc := range []struct{ nodes, minCommitting int }{{3, 45}, {5, 35}} {
		t.Run(fmt.Sprintf("n%d", tc.nodes), func(t *testing.T) {
			t.Parallel()
			committing := 0
			events := map[string]int{}
			for seed := int64(1); seed <= seeds; seed++ {
				c, err := RandomRun(tc.nodes, seed, 2000)
				if err != nil {
					t.Fatal(err)
				}
				committed := false
				for _, r := range c.Records() {
					events[r.Event]++
					committed = committed || r.Event == tracelog.EvAdvanceCommit
				}
				if committed {
					committing++
				}
			}
			t.Logf("%d of %d seeds commit; advance_commit %d, truncate_log %d",
				committing, seeds, events[tracelog.EvAdvanceCommit], events[tracelog.EvTruncateLog])
			if committing < tc.minCommitting {
				t.Errorf("only %d of %d seeds commit anything, want at least %d", committing, seeds, tc.minCommitting)
			}
			for _, ev := range []string{tracelog.EvAdvanceCommit, tracelog.EvTruncateLog} {
				if events[ev] == 0 {
					t.Errorf("no %s record in any seed", ev)
				}
			}
		})
	}
}
