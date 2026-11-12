package node

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jmanoj0905/raftcheck-agent/raft/core"
	"github.com/jmanoj0905/raftcheck-agent/raft/internal/schemacheck"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// ---------------------------------------------------------------------------
// Test cluster helper

// cluster is n wrapper nodes (ids 1..n) on one MemNetwork and one
// MemPersister, all writing to one MemorySink. Cleanup kills every node and
// checks the sink: schema-valid, and per node the seqs are 1, 2, 3, ...
type cluster struct {
	t       *testing.T
	ids     []int
	net     *MemNetwork
	p       Persister
	sink    *MemorySink
	cfg     Config
	nodes   []*Node
	applyCh []chan ApplyMsg
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i + 1
	}
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	c := &cluster{
		t:    t,
		ids:  ids,
		net:  NewMemNetwork(ids, seed),
		p:    NewMemPersister(),
		sink: &MemorySink{},
	}
	c.cfg = DefaultConfig()
	c.cfg.Seed = seed
	c.cfg.Sink = c.sink
	c.nodes = make([]*Node, n)
	c.applyCh = make([]chan ApplyMsg, n)
	t.Cleanup(c.cleanup)
	return c
}

// start makes every node, each with an applyCh of the given capacity.
func (c *cluster) start(applyCap int) {
	for _, id := range c.ids {
		c.make(id, make(chan ApplyMsg, applyCap))
	}
}

func (c *cluster) make(id int, applyCh chan ApplyMsg) *Node {
	c.applyCh[id-1] = applyCh
	c.nodes[id-1] = Make(id, c.ids, c.net, c.p, applyCh, c.cfg)
	return c.nodes[id-1]
}

func (c *cluster) cleanup() {
	for _, n := range c.nodes {
		if n != nil {
			n.Kill()
		}
	}
	checkSink(c.t, c.sink)
}

// checkSink validates the sink's JSONL and that each node's seqs run
// 1, 2, 3, ... with no gap or repeat (across restarts too).
func checkSink(t *testing.T, s *MemorySink) {
	t.Helper()
	if err := schemacheck.ValidateJSONL(s.JSONL()); err != nil {
		t.Errorf("sink JSONL invalid: %v", err)
	}
	last := map[int]int64{}
	for _, r := range s.Records() {
		if r.Seq != last[r.Node]+1 {
			t.Errorf("node %d: seq %d after %d", r.Node, r.Seq, last[r.Node])
		}
		last[r.Node] = r.Seq
	}
}

// waitLeader polls until exactly one node reports isLeader and returns it.
// Two leaders in the same term fail the test at once.
func (c *cluster) waitLeader(within time.Duration) int {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		byTerm := map[int][]int{}
		var leaders []int
		for _, id := range c.ids {
			n := c.nodes[id-1]
			if n == nil {
				continue
			}
			if term, ok := n.GetState(); ok {
				byTerm[term] = append(byTerm[term], id)
				leaders = append(leaders, id)
			}
		}
		for term, ls := range byTerm {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
		}
		if len(leaders) == 1 {
			return leaders[0]
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.t.Fatalf("no single leader within %v", within)
	return 0
}

// nodeRecords returns the sink's records for node id.
func nodeRecords(s *MemorySink, id int) []tracelog.Record {
	var out []tracelog.Record
	for _, r := range s.Records() {
		if r.Node == id {
			out = append(out, r)
		}
	}
	return out
}

func recvApply(t *testing.T, ch chan ApplyMsg, within time.Duration) ApplyMsg {
	t.Helper()
	select {
	case m := <-ch:
		return m
	case <-time.After(within):
		t.Fatalf("no ApplyMsg within %v", within)
	}
	return ApplyMsg{}
}

// waitGoroutines waits until the goroutine count is back to at most n.
func waitGoroutines(t *testing.T, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for runtime.NumGoroutine() > n {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			buf = buf[:runtime.Stack(buf, true)]
			t.Fatalf("goroutine leak: %d running, want <= %d\n%s", runtime.NumGoroutine(), n, buf)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---------------------------------------------------------------------------
// Brief tests

func TestThreeNodesElectLeader(t *testing.T) {
	c := newCluster(t, 3)
	c.start(100)
	leader := c.waitLeader(5 * time.Second)
	term, _ := c.nodes[leader-1].GetState()
	if term < 1 {
		t.Fatalf("leader %d in term %d", leader, term)
	}
	// With a healthy network the leader should stay put.
	time.Sleep(500 * time.Millisecond)
	if got := c.waitLeader(time.Second); got != leader {
		t.Logf("leader changed %d -> %d (allowed, but unusual)", leader, got)
	}
	for _, id := range c.ids {
		if tm, _ := c.nodes[id-1].GetState(); tm < term {
			t.Errorf("node %d term %d below leader term %d", id, tm, term)
		}
	}
}

func TestStartReplicates(t *testing.T) {
	c := newCluster(t, 3)
	c.start(100)
	leader := c.waitLeader(5 * time.Second)
	index, term, isLeader := c.nodes[leader-1].Start("a")
	if !isLeader || index != 1 || term < 1 {
		t.Fatalf("Start = (%d, %d, %v), want (1, >=1, true)", index, term, isLeader)
	}
	want := ApplyMsg{CommandValid: true, Command: "a", CommandIndex: 1, CommandTerm: term}
	for _, id := range c.ids {
		if got := recvApply(t, c.applyCh[id-1], 2*time.Second); got != want {
			t.Errorf("node %d applied %+v, want %+v", id, got, want)
		}
	}
	// A follower refuses.
	f := c.ids[leader%3]
	if idx, _, ok := c.nodes[f-1].Start("b"); ok || idx != 0 {
		t.Errorf("follower %d Start = (%d, _, %v), want (0, _, false)", f, idx, ok)
	}
}

// ordered is one event log shared by the recording persister and network.
type ordered struct {
	mu     sync.Mutex
	events []orderedEvent
}

type orderedEvent struct {
	save bool
	id   int                  // node that saved or sent
	ps   core.PersistentState // save only
	msg  core.Message         // send only
}

func (o *ordered) add(e orderedEvent) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
}

type recPersister struct {
	*MemPersister
	log *ordered
}

func (r recPersister) Save(id int, s core.PersistentState) {
	r.log.add(orderedEvent{save: true, id: id, ps: cloneState(s)})
	r.MemPersister.Save(id, s)
}

type recNetwork struct {
	*MemNetwork
	log *ordered
}

func (r recNetwork) Send(m core.Message) {
	r.log.add(orderedEvent{id: m.From, msg: m})
	r.MemNetwork.Send(m)
}

// checkPersistBeforeSend checks that every message is backed by what its
// sender had saved before sending it: the sender's last Save has the
// message's term; a granted vote's VotedFor is the candidate; an
// AppendEntries' prefix and entries are in the saved log.
func checkPersistBeforeSend(events []orderedEvent) error {
	saved := map[int]*core.PersistentState{}
	sends := 0
	for i, e := range events {
		if e.save {
			ps := e.ps
			saved[e.id] = &ps
			continue
		}
		sends++
		m := e.msg
		ps := saved[m.From]
		if ps == nil {
			return fmt.Errorf("event %d: node %d sent %s before any Save", i, m.From, m.MsgID)
		}
		if ps.CurrentTerm != m.Term {
			return fmt.Errorf("event %d: node %d sent %s in term %d, last saved term %d", i, m.From, m.MsgID, m.Term, ps.CurrentTerm)
		}
		switch m.Type {
		case core.MsgRequestVote:
			if ps.VotedFor != m.From {
				return fmt.Errorf("event %d: candidate %d sent %s, saved votedFor %d", i, m.From, m.MsgID, ps.VotedFor)
			}
		case core.MsgRequestVoteReply:
			if m.VoteGranted && ps.VotedFor != m.To {
				return fmt.Errorf("event %d: node %d granted %d a vote (%s), saved votedFor %d", i, m.From, m.To, m.MsgID, ps.VotedFor)
			}
		case core.MsgAppendEntries:
			end := m.PrevLogIndex + len(m.Entries)
			if end > len(ps.Log) {
				return fmt.Errorf("event %d: leader %d sent %s up to index %d, saved log has %d", i, m.From, m.MsgID, end, len(ps.Log))
			}
			if !slices.Equal(ps.Log[m.PrevLogIndex:end], m.Entries) {
				return fmt.Errorf("event %d: leader %d sent %s with entries not in its saved log", i, m.From, m.MsgID)
			}
		case core.MsgAppendEntriesReply:
			if m.Success && m.MatchIndex > len(ps.Log) {
				return fmt.Errorf("event %d: node %d acked match %d (%s), saved log has %d", i, m.From, m.MatchIndex, m.MsgID, len(ps.Log))
			}
		}
	}
	if sends == 0 {
		return fmt.Errorf("no messages sent")
	}
	return nil
}

func TestPersistBeforeSendChecker(t *testing.T) {
	// The checker itself must catch a send that comes before its save.
	ps := core.PersistentState{CurrentTerm: 1, VotedFor: 1, Log: []core.Entry{}}
	vote := core.Message{Type: core.MsgRequestVote, From: 1, To: 2, MsgID: "1-1", Term: 1}
	good := []orderedEvent{{save: true, id: 1, ps: ps}, {id: 1, msg: vote}}
	if err := checkPersistBeforeSend(good); err != nil {
		t.Fatalf("good order rejected: %v", err)
	}
	bad := []orderedEvent{{id: 1, msg: vote}, {save: true, id: 1, ps: ps}}
	if err := checkPersistBeforeSend(bad); err == nil {
		t.Fatal("send before save not caught")
	}
	ps2 := core.PersistentState{CurrentTerm: 1, VotedFor: 1, Log: []core.Entry{{Term: 1, Command: "a"}}}
	ae := core.Message{Type: core.MsgAppendEntries, From: 1, To: 2, MsgID: "1-2", Term: 1, Entries: []core.Entry{{Term: 1, Command: "a"}}}
	stale := []orderedEvent{{save: true, id: 1, ps: ps}, {id: 1, msg: ae}, {save: true, id: 1, ps: ps2}}
	if err := checkPersistBeforeSend(stale); err == nil {
		t.Fatal("entries sent before their save not caught")
	}
}

func TestPersistBeforeSend(t *testing.T) {
	log := &ordered{}
	c := newCluster(t, 3)
	rn := recNetwork{c.net, log}
	rp := recPersister{NewMemPersister(), log}
	c.p = rp
	for _, id := range c.ids {
		c.applyCh[id-1] = make(chan ApplyMsg, 100)
		c.nodes[id-1] = Make(id, c.ids, rn, rp, c.applyCh[id-1], c.cfg)
	}
	leader := c.waitLeader(5 * time.Second)
	for i := range 5 {
		c.nodes[leader-1].Start(fmt.Sprintf("c%d", i))
	}
	// Force a second election so votes and new terms are covered too.
	c.net.Disconnect(leader)
	deadline := time.Now().Add(5 * time.Second)
	for {
		l2 := 0
		for _, id := range c.ids {
			if _, ok := c.nodes[id-1].GetState(); ok && id != leader {
				l2 = id
			}
		}
		if l2 != 0 {
			c.nodes[l2-1].Start("after")
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no new leader after disconnecting the old one")
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.net.Connect(leader)
	time.Sleep(500 * time.Millisecond)
	for _, n := range c.nodes {
		n.Kill()
	}
	log.mu.Lock()
	events := slices.Clone(log.events)
	log.mu.Unlock()
	if err := checkPersistBeforeSend(events); err != nil {
		t.Fatal(err)
	}
}

func TestKillWritesCrashRecordAndStops(t *testing.T) {
	c := newCluster(t, 3)
	c.start(100)
	leader := c.waitLeader(5 * time.Second)
	c.nodes[leader-1].Start("x")
	time.Sleep(200 * time.Millisecond)

	mp := c.p.(*MemPersister)
	for _, id := range c.ids {
		n := c.nodes[id-1]
		n.Kill()
		n.Kill() // idempotent
		recs := nodeRecords(c.sink, id)
		if len(recs) < 2 {
			t.Fatalf("node %d: only %d records", id, len(recs))
		}
		crash, prev := recs[len(recs)-1], recs[len(recs)-2]
		if crash.Event != tracelog.EvCrash || crash.Seq != prev.Seq+1 {
			t.Fatalf("node %d: last record %s seq %d after seq %d, want crash seq %d", id, crash.Event, crash.Seq, prev.Seq, prev.Seq+1)
		}
		// Every state change writes a record, so the crash record repeats
		// the state of the record before it.
		if !reflect.DeepEqual(crash.State, prev.State) || crash.Term != prev.State.CurrentTerm {
			t.Errorf("node %d: crash state %+v term %d, want last state %+v", id, crash.State, crash.Term, prev.State)
		}
		if got := mp.LoadCounters(id).LastSeq; got != crash.Seq {
			t.Errorf("node %d: saved LastSeq %d, want crash seq %d", id, got, crash.Seq)
		}
	}
	saves := map[int]int{}
	for _, id := range c.ids {
		saves[id] = mp.SaveCount(id)
	}
	nrec := len(c.sink.Records())
	time.Sleep(500 * time.Millisecond)
	for _, id := range c.ids {
		if got := mp.SaveCount(id); got != saves[id] {
			t.Errorf("node %d: SaveCount %d -> %d after Kill", id, saves[id], got)
		}
	}
	if got := len(c.sink.Records()); got != nrec {
		t.Errorf("sink grew %d -> %d after Kill", nrec, got)
	}
	// A killed node refuses Start without blocking.
	if _, _, ok := c.nodes[leader-1].Start("y"); ok {
		t.Error("Start on a killed node returned isLeader")
	}
}

func TestKillWithBlockedApplyCh(t *testing.T) {
	before := runtime.NumGoroutine()
	c := newCluster(t, 1)
	n := c.make(1, make(chan ApplyMsg)) // unbuffered, never read
	// This test kills n itself, so cleanup must not call Kill again: on a
	// regression a second Kill would hang instead of letting the test fail.
	defer func() { c.nodes[0] = nil }()
	c.waitLeader(5 * time.Second)
	for i := range 3 {
		if idx, _, ok := n.Start(fmt.Sprintf("c%d", i)); !ok || idx != i+1 {
			t.Fatalf("Start %d = (%d, _, %v)", i, idx, ok)
		}
	}
	time.Sleep(50 * time.Millisecond) // let the applier block on applyCh
	done := make(chan struct{})
	go func() { n.Kill(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Kill did not return within 1s with a blocked applyCh")
	}
	recs := nodeRecords(c.sink, 1)
	if last := recs[len(recs)-1]; last.Event != tracelog.EvCrash {
		t.Errorf("last record %s, want crash", last.Event)
	}
	waitGoroutines(t, before, time.Second)
}

func TestRestartFromPersister(t *testing.T) {
	c := newCluster(t, 3)
	c.start(100)
	leader := c.waitLeader(5 * time.Second)
	c.nodes[leader-1].Start("a")
	for _, id := range c.ids {
		recvApply(t, c.applyCh[id-1], 2*time.Second)
	}
	oldTerm := map[int]int{}
	crashSeq := map[int]int64{}
	for _, id := range c.ids {
		c.nodes[id-1].Kill()
		oldTerm[id], _ = c.nodes[id-1].GetState()
		recs := nodeRecords(c.sink, id)
		crashSeq[id] = recs[len(recs)-1].Seq
	}
	nBefore := len(c.sink.Records())
	for _, id := range c.ids {
		c.make(id, make(chan ApplyMsg, 100))
	}
	for _, id := range c.ids {
		if term, _ := c.nodes[id-1].GetState(); term < oldTerm[id] {
			t.Errorf("node %d restarted in term %d, was %d", id, term, oldTerm[id])
		}
	}
	after := c.sink.Records()[nBefore:]
	for _, id := range c.ids {
		var first *tracelog.Record
		for i := range after {
			if after[i].Node == id {
				first = &after[i]
				break
			}
		}
		if first == nil || first.Event != tracelog.EvRestart || first.Seq != crashSeq[id]+1 {
			t.Fatalf("node %d: first record after restart %+v, want restart seq %d", id, first, crashSeq[id]+1)
		}
	}
	// The restarted cluster elects a leader and re-applies the old entry.
	l := c.waitLeader(5 * time.Second)
	idx, _, ok := c.nodes[l-1].Start("b")
	if !ok || idx != 2 {
		t.Fatalf("Start after restart = (%d, _, %v), want (2, _, true)", idx, ok)
	}
	for _, id := range c.ids {
		if m := recvApply(t, c.applyCh[id-1], 2*time.Second); m.Command != "a" || m.CommandIndex != 1 {
			t.Errorf("node %d re-applied %+v first, want index 1 \"a\"", id, m)
		}
		if m := recvApply(t, c.applyCh[id-1], 2*time.Second); m.Command != "b" || m.CommandIndex != 2 {
			t.Errorf("node %d applied %+v, want index 2 \"b\"", id, m)
		}
	}
}

// TestRestartNeverPersisted: a node killed before it ever saved state must
// not reuse seqs when it comes back.
func TestRestartNeverPersisted(t *testing.T) {
	c := newCluster(t, 3)
	c.cfg.ElectionMin, c.cfg.ElectionMax = time.Hour, time.Hour // never times out
	c.make(1, make(chan ApplyMsg, 10))
	c.nodes[0].Kill()
	if _, ok := c.p.Load(1); ok {
		t.Fatal("node saved state without any event")
	}
	c.make(1, make(chan ApplyMsg, 10))
	c.nodes[0].Kill()
	recs := nodeRecords(c.sink, 1)
	evs := []string{}
	for _, r := range recs {
		evs = append(evs, r.Event)
	}
	if !slices.Equal(evs, []string{tracelog.EvCrash, tracelog.EvRestart, tracelog.EvCrash}) {
		t.Errorf("events %v, want crash, restart, crash", evs)
	}
}

// inboxOnly hides MemNetwork's Restart, to test Make's fallback for
// transports that are not Restarters.
type inboxOnly struct{ net *MemNetwork }

func (o inboxOnly) Send(m core.Message)              { o.net.Send(m) }
func (o inboxOnly) Inbox(id int) <-chan core.Message { return o.net.Inbox(id) }

// TestRestartDrainsInbox: messages sent to a node's previous life are not
// handled by the restarted node, with a Restarter transport and without.
func TestRestartDrainsInbox(t *testing.T) {
	for _, restarter := range []bool{true, false} {
		t.Run(fmt.Sprintf("restarter=%v", restarter), func(t *testing.T) {
			c := newCluster(t, 3)
			c.cfg.ElectionMin, c.cfg.ElectionMax = time.Hour, time.Hour // quiet cluster
			var tr Transport = c.net
			if !restarter {
				tr = inboxOnly{c.net}
			}
			mk := func() {
				c.applyCh[0] = make(chan ApplyMsg, 10)
				c.nodes[0] = Make(1, c.ids, tr, c.p, c.applyCh[0], c.cfg)
			}
			mk()
			c.nodes[0].Kill()
			// While node 1 is down, a candidate in a far-ahead term asks for its vote.
			c.net.Send(core.Message{Type: core.MsgRequestVote, From: 2, To: 1, MsgID: "2-1", Term: 99})
			if len(c.net.Inbox(1)) != 1 {
				t.Fatalf("inbox holds %d messages before restart, want 1", len(c.net.Inbox(1)))
			}
			c.net.mu.Lock()
			ep := c.net.epoch[1]
			c.net.mu.Unlock()
			mk()
			c.net.mu.Lock()
			bumped := c.net.epoch[1] != ep
			c.net.mu.Unlock()
			if bumped != restarter {
				t.Errorf("epoch bumped = %v, want %v: Make must call Restart on a Restarter", bumped, restarter)
			}
			time.Sleep(200 * time.Millisecond) // time to (wrongly) handle it
			if term, _ := c.nodes[0].GetState(); term != 0 {
				t.Errorf("restarted node is in term %d: it handled a message sent to its previous life", term)
			}
			for _, r := range nodeRecords(c.sink, 1) {
				if r.Term == 99 {
					t.Errorf("record %s in term 99 after restart", r.Event)
				}
			}
		})
	}
}

func TestSinkRecordsValid(t *testing.T) {
	c := newCluster(t, 3)
	c.start(100)
	leader := c.waitLeader(5 * time.Second)
	c.nodes[leader-1].Start("a")
	c.nodes[c.ids[leader%3]-1].Start("rejected")
	time.Sleep(300 * time.Millisecond)
	c.nodes[leader-1].Kill()
	c.make(leader, make(chan ApplyMsg, 100))
	c.waitLeader(5 * time.Second)
	for _, n := range c.nodes {
		n.Kill()
	}
	seen := map[string]bool{}
	for _, r := range c.sink.Records() {
		seen[r.Event] = true
	}
	for _, ev := range []string{
		tracelog.EvBecomeCandidate, tracelog.EvBecomeLeader, tracelog.EvSendRequestVote,
		tracelog.EvSendAppendEntries, tracelog.EvAppendEntry, tracelog.EvApplyEntry,
		tracelog.EvPersist, tracelog.EvProposeRejected, tracelog.EvCrash, tracelog.EvRestart,
	} {
		if !seen[ev] {
			t.Errorf("no %s record", ev)
		}
	}
	if err := schemacheck.ValidateJSONL(c.sink.JSONL()); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Parts

func TestDefaultConfig(t *testing.T) {
	got := DefaultConfig()
	want := Config{ElectionMin: 300 * time.Millisecond, ElectionMax: 600 * time.Millisecond, HeartbeatInterval: 100 * time.Millisecond}
	if got != want {
		t.Errorf("DefaultConfig() = %+v, want %+v", got, want)
	}
}

func TestMemPersisterDeepCopies(t *testing.T) {
	p := NewMemPersister()
	if _, ok := p.Load(1); ok {
		t.Fatal("Load on empty persister found state")
	}
	if c := p.LoadCounters(1); c != (core.Counters{}) {
		t.Fatalf("LoadCounters on empty = %+v", c)
	}
	s := core.PersistentState{CurrentTerm: 2, VotedFor: 1, Log: []core.Entry{{Term: 1, Command: "a"}}}
	p.Save(1, s)
	s.Log[0].Command = "changed"
	got, ok := p.Load(1)
	if !ok || got.Log[0].Command != "a" {
		t.Fatalf("Save did not copy: %+v", got)
	}
	got.Log[0].Command = "changed"
	if again, _ := p.Load(1); again.Log[0].Command != "a" {
		t.Fatal("Load did not copy")
	}
	p.SaveCounters(1, core.Counters{LastSeq: 5, LastMsg: 3})
	if c := p.LoadCounters(1); c != (core.Counters{LastSeq: 5, LastMsg: 3}) {
		t.Errorf("LoadCounters = %+v", c)
	}
	if p.SaveCount(1) != 1 || p.SaveCount(2) != 0 {
		t.Errorf("SaveCount = %d, %d; want 1, 0", p.SaveCount(1), p.SaveCount(2))
	}
}

func msg(from, to int, id string) core.Message {
	return core.Message{Type: core.MsgRequestVote, From: from, To: to, MsgID: id, Term: 1}
}

func recvMsg(ch <-chan core.Message, within time.Duration) (core.Message, bool) {
	select {
	case m := <-ch:
		return m, true
	case <-time.After(within):
		return core.Message{}, false
	}
}

func TestMemNetworkDeliversAndDrops(t *testing.T) {
	net := NewMemNetwork([]int{1, 2, 3}, 1)
	net.Send(msg(1, 2, "1-1"))
	if m, ok := recvMsg(net.Inbox(2), time.Second); !ok || m.MsgID != "1-1" {
		t.Fatalf("got %+v, %v", m, ok)
	}
	net.Disconnect(2)
	net.Send(msg(1, 2, "1-2")) // receiver down
	net.Send(msg(2, 3, "2-1")) // sender down
	net.Connect(2)
	net.Send(msg(1, 3, "1-3"))
	if m, ok := recvMsg(net.Inbox(3), time.Second); !ok || m.MsgID != "1-3" {
		t.Fatalf("got %+v, %v; want 1-3 (2-1 dropped)", m, ok)
	}
	if m, ok := recvMsg(net.Inbox(2), 50*time.Millisecond); ok {
		t.Fatalf("disconnected receiver got %+v", m)
	}
	net.SetDropRate(1)
	net.Send(msg(1, 2, "1-4"))
	if m, ok := recvMsg(net.Inbox(2), 50*time.Millisecond); ok {
		t.Fatalf("drop rate 1 delivered %+v", m)
	}
	if got := net.MessageCount(); got != 5 {
		t.Errorf("MessageCount = %d, want 5", got)
	}
}

func TestMemNetworkDelayAndDisconnectInFlight(t *testing.T) {
	net := NewMemNetwork([]int{1, 2}, 7)
	net.SetMaxDelay(100 * time.Millisecond)
	for i := range 20 {
		net.Send(msg(1, 2, fmt.Sprintf("1-%d", i+1)))
	}
	got := 0
	for range 20 {
		if _, ok := recvMsg(net.Inbox(2), time.Second); ok {
			got++
		}
	}
	if got != 20 {
		t.Fatalf("delivered %d of 20 delayed messages", got)
	}

	// Disconnect after send but before delivery: dropped, even though the
	// node is reconnected before the delay ends.
	checkOldLifeDropped(t, func(net *MemNetwork) {
		net.Disconnect(2)
		net.Connect(2)
	})
}

// TestMemNetworkRestartDropsOldLife: after a crash and restart, nothing in
// flight to or from the node's previous life is delivered, even when its
// delay ends after the node is back on the network.
func TestMemNetworkRestartDropsOldLife(t *testing.T) {
	t.Run("disconnect-restart-connect", func(t *testing.T) {
		checkOldLifeDropped(t, func(net *MemNetwork) {
			net.Disconnect(2)
			net.Restart(2)
			net.Connect(2)
		})
	})
	t.Run("restart-only", func(t *testing.T) {
		checkOldLifeDropped(t, func(net *MemNetwork) { net.Restart(2) })
	})
}

// checkOldLifeDropped sends 20 delayed messages each way between nodes 1
// and 2, then runs cut (which must end the old link or life of node 2),
// and checks that no message whose delay ends after cut returned is ever
// delivered. The network's random source is swapped for a seeded one with
// a twin, so each message's delay is known exactly and no timing guess is
// needed. The delays are long (up to 400 ms) so most messages are still in
// flight after cut; if a stall makes fewer than 10 per direction late, the
// round proves too little and is run again with a new seed.
func checkOldLifeDropped(t *testing.T, cut func(net *MemNetwork)) {
	t.Helper()
	const maxDelay = 400 * time.Millisecond
	for round := range 10 {
		seed := uint64(time.Now().UnixNano())
		net := NewMemNetwork([]int{1, 2}, 0)
		net.rng = rand.New(rand.NewPCG(seed, 7))
		twin := rand.New(rand.NewPCG(seed, 7))
		net.SetMaxDelay(maxDelay)
		delays := map[string]time.Duration{}
		t0 := time.Now()
		for i := range 40 {
			from, to := 1, 2
			if i%2 == 1 {
				from, to = 2, 1
			}
			id := fmt.Sprintf("%d-%d", from, i+1)
			delays[id] = time.Duration(twin.Int64N(int64(maxDelay)))
			net.Send(msg(from, to, id))
		}
		cut(net)
		at := time.Since(t0)
		late := map[int]int{} // by receiver
		for id, d := range delays {
			if d > at {
				late[3-int(id[0]-'0')]++ // "1-x" goes to 2, "2-x" to 1
			}
		}
		if late[1] < 10 || late[2] < 10 {
			t.Logf("round %d: only %v messages still in flight after the cut (%v); trying again", round, late, at)
			continue
		}
		time.Sleep(maxDelay + 300*time.Millisecond) // let the timers fire
		for _, to := range []int{1, 2} {
			for drained := false; !drained; {
				select {
				case m := <-net.Inbox(to):
					if delays[m.MsgID] > at {
						t.Errorf("message %s (delay %v, cut ended at %v) delivered after the cut",
							m.MsgID, delays[m.MsgID], at)
					}
				default:
					drained = true
				}
			}
		}
		return
	}
	t.Fatal("10 rounds in a row had too few messages in flight at the cut")
}

func TestMemNetworkSendNeverBlocks(t *testing.T) {
	net := NewMemNetwork([]int{1, 2}, 1)
	done := make(chan struct{})
	go func() {
		for i := range 3000 { // more than the 1024 buffer
			net.Send(msg(1, 2, fmt.Sprintf("1-%d", i)))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Send blocked on a full inbox")
	}
	if n := len(net.Inbox(2)); n != 1024 {
		t.Errorf("inbox holds %d, want 1024", n)
	}
}

func TestMemNetworkCopiesEntries(t *testing.T) {
	net := NewMemNetwork([]int{1, 2}, 1)
	m := core.Message{Type: core.MsgAppendEntries, From: 1, To: 2, MsgID: "1-1", Term: 1, Entries: []core.Entry{{Term: 1, Command: "a"}}}
	net.Send(m)
	m.Entries[0].Command = "changed"
	got, _ := recvMsg(net.Inbox(2), time.Second)
	if got.Entries[0].Command != "a" {
		t.Error("Send did not copy Entries")
	}
}

func TestMemorySinkConcurrent(t *testing.T) {
	s := &MemorySink{}
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Go(func() {
			for i := range 100 {
				s.Write(tracelog.Record{V: tracelog.Version, Node: w + 1, Seq: int64(i + 1), Event: tracelog.EvCrash, Details: map[string]any{}})
			}
		})
	}
	wg.Wait()
	if n := len(s.Records()); n != 400 {
		t.Fatalf("%d records, want 400", n)
	}
	if n := bytes.Count(s.JSONL(), []byte("\n")); n != 400 {
		t.Errorf("%d JSONL lines, want 400", n)
	}
}

func TestFileSink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node1.jsonl")
	fs, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	mem := &MemorySink{}
	st := tracelog.State{Role: "follower", Log: []tracelog.LogPair{}}
	for i := range 3 {
		r := tracelog.Record{V: tracelog.Version, Node: 1, Seq: int64(i + 1), Event: tracelog.EvCrash, Details: map[string]any{}, State: st}
		fs.Write(r)
		mem.Write(r)
	}
	if err := fs.Close(); err != nil {
		t.Fatal(err)
	}
	fs.Write(tracelog.Record{}) // after Close: ignored, no panic
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, mem.JSONL()) {
		t.Errorf("file =\n%s\nwant\n%s", got, mem.JSONL())
	}
	if err := schemacheck.ValidateJSONL(got); err != nil {
		t.Error(err)
	}
	// Reopening appends (a restarted process keeps one log file).
	fs2, err := NewFileSink(path)
	if err != nil {
		t.Fatal(err)
	}
	fs2.Write(tracelog.Record{V: tracelog.Version, Node: 1, Seq: 4, Event: tracelog.EvRestart, Details: map[string]any{}, State: st})
	if err := fs2.Close(); err != nil {
		t.Fatal(err)
	}
	got, _ = os.ReadFile(path)
	if n := strings.Count(string(got), "\n"); n != 4 {
		t.Errorf("%d lines after reopen, want 4", n)
	}
}

func TestElectionTimeoutRange(t *testing.T) {
	cfg := DefaultConfig()
	n := &Node{id: 3, cfg: cfg}
	n.initRand()
	for range 1000 {
		d := n.electionTimeout()
		if d < cfg.ElectionMin || d >= cfg.ElectionMax {
			t.Fatalf("timeout %v outside [%v, %v)", d, cfg.ElectionMin, cfg.ElectionMax)
		}
	}
	// Same seed and id: same sequence.
	a, b := &Node{id: 2, cfg: cfg}, &Node{id: 2, cfg: cfg}
	a.initRand()
	b.initRand()
	for range 10 {
		if a.electionTimeout() != b.electionTimeout() {
			t.Fatal("same seed and id gave different timeouts")
		}
	}
}

// cloneState deep-copies s for the recording persister.
func cloneState(s core.PersistentState) core.PersistentState {
	s.Log = slices.Clone(s.Log)
	return s
}
