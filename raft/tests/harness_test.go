// Package tests holds the MIT-style scenario tests (spec §8.3): whole
// clusters of wrapper nodes (package node) with real goroutines and timers,
// on an in-memory network that can disconnect, drop and delay messages.
//
// The harness is our own; it follows the semantics of the MIT 6.5840 lab
// tester (checkOneLeader, one, nCommitted, ...) but shares no code with it.
// Scenario tests must not call t.Parallel: their timing assertions assume
// they have the machine to themselves.
package tests

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jmanoj0905/raftcheck-agent/raft/internal/schemacheck"
	"github.com/jmanoj0905/raftcheck-agent/raft/node"
)

// electionTimeout is how long the tests allow for one election to finish.
// It is well above node.DefaultConfig's 600 ms upper election timeout, as
// in the MIT tester.
const electionTimeout = time.Second

// oneBudget is how long one() keeps trying to get a command committed.
const oneBudget = 10 * time.Second

// termMoved is what wait returns when some node's term passed startTerm
// before the entry was committed (MIT's wait returns -1 for this). Test
// commands are never this string.
const termMoved = "<term moved on>"

// applied is one committed entry as a node handed it to the application.
type applied struct {
	cmd  string
	term int
}

// cluster is n wrapper nodes, ids 1..n, on one MemNetwork and one
// MemPersister. Each node has its own MemorySink, kept across restarts so a
// node's records form one sequence. Every incarnation of a node gets a fresh
// applyCh and a reader goroutine that checks what it applies.
type cluster struct {
	t     *testing.T
	n     int
	ids   []int
	seed  uint64
	net   *node.MemNetwork
	p     *node.MemPersister
	sinks []*node.MemorySink // index id-1
	begin time.Time
	goros int // goroutines running before the cluster started

	mu        sync.Mutex
	nodes     []*node.Node      // index id-1; nil while crashed
	connected []bool            // index id-1; false while crashed
	starts    []int             // incarnations started, per node; seeds the timers
	readers   []reader          // index id-1; the current incarnation's reader
	logs      []map[int]applied // index id-1; everything the node applied, across incarnations
	agreed    map[int]applied   // index -> first entry any node applied there
	applyErr  error             // first apply-check failure
	nStarts   int               // Start calls that reached a leader, for the log line
}

// reader is the goroutine that drains one incarnation's applyCh.
type reader struct {
	stop chan struct{}
	done chan struct{}
}

// newCluster starts n nodes and registers cleanup. With reliable false the
// network drops 10% of messages and delays each by up to 27 ms (which also
// reorders them).
func newCluster(t *testing.T, n int, reliable bool) *cluster {
	t.Helper()
	ids := make([]int, n)
	for i := range ids {
		ids[i] = i + 1
	}
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	c := &cluster{
		t:         t,
		n:         n,
		ids:       ids,
		seed:      seed,
		net:       node.NewMemNetwork(ids, seed),
		p:         node.NewMemPersister(),
		sinks:     make([]*node.MemorySink, n),
		begin:     time.Now(),
		goros:     runtime.NumGoroutine(),
		nodes:     make([]*node.Node, n),
		connected: make([]bool, n),
		starts:    make([]int, n),
		readers:   make([]reader, n),
		logs:      make([]map[int]applied, n),
		agreed:    map[int]applied{},
	}
	for i := range c.sinks {
		c.sinks[i] = &node.MemorySink{}
		c.logs[i] = map[int]applied{}
	}
	t.Cleanup(c.cleanup)
	c.setUnreliable(!reliable)
	for _, id := range ids {
		c.restart(id)
	}
	return c
}

// ---------------------------------------------------------------------------
// Apply checking

// setErr records err if it is the first apply-check failure.
func (c *cluster) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.applyErr == nil {
		c.applyErr = err
	}
}

// checkApplyErr fails the test if any reader recorded an error.
func (c *cluster) checkApplyErr() {
	c.t.Helper()
	c.mu.Lock()
	err := c.applyErr
	c.mu.Unlock()
	if err != nil {
		c.t.Fatalf("apply check: %v", err)
	}
}

// read drains applyCh for one incarnation of node id until stop is closed.
// Within an incarnation indexes must be 1, 2, 3, ... (a restarted node
// starts again at 1), and every entry must match what any node, this one
// included, applied at that index before.
func (c *cluster) read(id int, applyCh <-chan node.ApplyMsg, r reader) {
	defer close(r.done)
	next := 1
	for {
		var m node.ApplyMsg
		select {
		case m = <-applyCh:
		case <-r.stop:
			return
		}
		if err := c.check(id, m, next); err != nil {
			c.setErr(err)
			// Keep draining so the node is never blocked by the checker.
			continue
		}
		next++
	}
}

func (c *cluster) check(id int, m node.ApplyMsg, next int) error {
	if !m.CommandValid {
		return fmt.Errorf("node %d: ApplyMsg %+v has CommandValid false", id, m)
	}
	if m.CommandIndex != next {
		return fmt.Errorf("node %d: applied index %d, want %d (out of order or a gap)", id, m.CommandIndex, next)
	}
	got := applied{cmd: m.Command, term: m.CommandTerm}
	c.mu.Lock()
	defer c.mu.Unlock()
	if prev, ok := c.agreed[m.CommandIndex]; ok && prev != got {
		return fmt.Errorf("node %d: applied %q (term %d) at index %d, but %q (term %d) was applied there before",
			id, got.cmd, got.term, m.CommandIndex, prev.cmd, prev.term)
	}
	c.agreed[m.CommandIndex] = got
	c.logs[id-1][m.CommandIndex] = got
	return nil
}

// ---------------------------------------------------------------------------
// Cluster control

// crash disconnects node id and kills it, then stops its apply reader. Its
// persisted state stays. Crashing a crashed node does nothing.
func (c *cluster) crash(id int) {
	c.t.Helper()
	c.disconnect(id)
	c.mu.Lock()
	n, r := c.nodes[id-1], c.readers[id-1]
	c.nodes[id-1] = nil
	c.mu.Unlock()
	if n == nil {
		return
	}
	n.Kill()
	// Kill returned, so the node never sends on applyCh again; anything the
	// reader has not taken yet belongs to the dead incarnation.
	close(r.stop)
	<-r.done
}

// restart starts a new incarnation of node id from the shared persister
// (crashing the running one first, if any) and connects it.
func (c *cluster) restart(id int) {
	c.t.Helper()
	c.crash(id)
	c.mu.Lock()
	c.starts[id-1]++
	cfg := node.DefaultConfig()
	// A different seed per incarnation, so a restarted node does not replay
	// its previous life's election timeouts.
	cfg.Seed = c.seed + uint64(c.starts[id-1]-1)
	cfg.Sink = c.sinks[id-1]
	c.mu.Unlock()

	applyCh := make(chan node.ApplyMsg)
	r := reader{stop: make(chan struct{}), done: make(chan struct{})}
	n := node.Make(id, c.ids, c.net, c.p, applyCh, cfg)
	go c.read(id, applyCh, r)

	c.mu.Lock()
	c.nodes[id-1] = n
	c.readers[id-1] = r
	c.mu.Unlock()
	c.connect(id)
}

// disconnect cuts node id off the network.
func (c *cluster) disconnect(id int) {
	c.net.Disconnect(id)
	c.mu.Lock()
	c.connected[id-1] = false
	c.mu.Unlock()
}

// connect puts node id back on the network.
func (c *cluster) connect(id int) {
	c.net.Connect(id)
	c.mu.Lock()
	c.connected[id-1] = true
	c.mu.Unlock()
}

// setUnreliable turns 10% drops and 0-27 ms delays on or off.
func (c *cluster) setUnreliable(unreliable bool) {
	if unreliable {
		c.net.SetDropRate(0.1)
		c.net.SetMaxDelay(27 * time.Millisecond)
	} else {
		c.net.SetDropRate(0)
		c.net.SetMaxDelay(0)
	}
}

// rpcTotal returns how many messages have been sent so far.
func (c *cluster) rpcTotal() int64 { return c.net.MessageCount() }

// live returns the running, connected nodes by id. Crashed nodes are always
// disconnected, and a killed node's GetState keeps its last answer, so only
// these may be asked who leads.
func (c *cluster) live() map[int]*node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[int]*node.Node{}
	for i, n := range c.nodes {
		if n != nil && c.connected[i] {
			out[i+1] = n
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Checks

// checkOneLeader polls up to 10 times, 450-550 ms apart, for a leader among
// the connected nodes. It fails if two nodes lead in one term, or if no
// leader shows up. It returns the leader of the newest term.
func (c *cluster) checkOneLeader() int {
	c.t.Helper()
	for range 10 {
		time.Sleep(450*time.Millisecond + rand.N(100*time.Millisecond))
		c.checkApplyErr()
		leaders := map[int][]int{}
		for id, n := range c.live() {
			if term, ok := n.GetState(); ok {
				leaders[term] = append(leaders[term], id)
			}
		}
		newest := -1
		for term, ids := range leaders {
			if len(ids) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ids), ids)
			}
			newest = max(newest, term)
		}
		if newest >= 0 {
			return leaders[newest][0]
		}
	}
	c.t.Fatalf("expected one leader, got none")
	return 0
}

// checkNoLeader fails if any connected node thinks it leads.
func (c *cluster) checkNoLeader() {
	c.t.Helper()
	c.checkApplyErr()
	for id, n := range c.live() {
		if term, ok := n.GetState(); ok {
			c.t.Fatalf("expected no leader among connected nodes, but node %d leads in term %d", id, term)
		}
	}
}

// checkTerms fails unless every connected node is in the same term, and
// returns that term.
func (c *cluster) checkTerms() int {
	c.t.Helper()
	c.checkApplyErr()
	term := -1
	for id, n := range c.live() {
		t, _ := n.GetState()
		if term == -1 {
			term = t
		} else if t != term {
			c.t.Fatalf("connected nodes disagree on the term: node %d is in %d, another in %d", id, t, term)
		}
	}
	return term
}

// nCommitted returns how many nodes (crashed ones included) have applied an
// entry at index, and its command. It fails if they applied different ones.
func (c *cluster) nCommitted(index int) (count int, cmd string) {
	c.t.Helper()
	c.checkApplyErr()
	var first applied
	var clash error
	c.mu.Lock()
	for i, log := range c.logs {
		e, ok := log[index]
		if !ok {
			continue
		}
		if count > 0 && e != first && clash == nil {
			clash = fmt.Errorf("committed values differ at index %d: node %d has %q (term %d), another has %q (term %d)",
				index, i+1, e.cmd, e.term, first.cmd, first.term)
		}
		first = e
		count++
	}
	c.mu.Unlock()
	if clash != nil {
		c.t.Fatal(clash)
	}
	return count, first.cmd
}

// one submits cmd to whichever connected node is leader, and waits for at
// least expected nodes to apply it. If the entry is not committed in 2 s and
// retry is set, it tries again with another leader; without retry it fails.
// Gives up after 10 s. Returns the index the command was committed at.
func (c *cluster) one(cmd string, expected int, retry bool) int {
	c.t.Helper()
	t0 := time.Now()
	next := 0 // round-robin over nodes, as in the MIT tester
	for time.Since(t0) < oneBudget {
		c.checkApplyErr()
		index := -1
		for range c.n {
			next = next%c.n + 1
			n, ok := c.live()[next]
			if !ok {
				continue
			}
			if i, _, isLeader := n.Start(cmd); isLeader {
				c.mu.Lock()
				c.nStarts++
				c.mu.Unlock()
				index = i
				break
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				if nd, got := c.nCommitted(index); nd > 0 && nd >= expected && got == cmd {
					return index
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%q) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%q) failed to reach agreement in %v", cmd, oneBudget)
	return -1
}

// wait waits for at least n nodes to apply index and returns its command.
// If startTerm >= 0 and some running node's term passes it first, wait
// returns termMoved, since the entry may never commit.
func (c *cluster) wait(index, n, startTerm int) string {
	c.t.Helper()
	to := 10 * time.Millisecond
	for range 30 {
		if nd, _ := c.nCommitted(index); nd >= n {
			break
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
		if startTerm >= 0 {
			for _, nd := range c.running() {
				if t, _ := nd.GetState(); t > startTerm {
					return termMoved
				}
			}
		}
	}
	nd, cmd := c.nCommitted(index)
	if nd < n {
		c.t.Fatalf("only %d nodes committed index %d, want %d", nd, index, n)
	}
	return cmd
}

// running returns every node that is not crashed, connected or not.
func (c *cluster) running() []*node.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*node.Node
	for _, n := range c.nodes {
		if n != nil {
			out = append(out, n)
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Cleanup

// cleanup kills every node and stops every reader, then fails the test on
// any recorded apply error, on an invalid record line or seq gap in any
// node's sink, or on goroutines left running.
func (c *cluster) cleanup() {
	c.t.Helper()
	for _, id := range c.ids {
		c.crash(id)
	}
	c.mu.Lock()
	err := c.applyErr
	c.mu.Unlock()
	if err != nil {
		c.t.Errorf("apply check: %v", err)
	}
	for i, s := range c.sinks {
		if err := checkSink(i+1, s); err != nil {
			c.t.Errorf("node %d sink: %v", i+1, err)
		}
	}
	c.checkGoroutines(2 * time.Second)
	c.mu.Lock()
	starts := c.nStarts
	c.mu.Unlock()
	c.t.Logf("%d nodes, %v, %d messages, %d commands started",
		c.n, time.Since(c.begin).Round(time.Millisecond), c.rpcTotal(), starts)
}

// checkSink validates one node's JSONL against the schema and checks its
// seqs run 1, 2, 3, ... across all of its incarnations.
func checkSink(id int, s *node.MemorySink) error {
	if err := schemacheck.ValidateJSONL(s.JSONL()); err != nil {
		return err
	}
	var errs []error
	want := int64(1)
	for _, r := range s.Records() {
		if r.Node != id {
			errs = append(errs, fmt.Errorf("record from node %d", r.Node))
		}
		if r.Seq != want {
			errs = append(errs, fmt.Errorf("seq %d, want %d", r.Seq, want))
			want = r.Seq
		}
		want++
	}
	return errors.Join(errs...)
}

// checkGoroutines waits for the goroutine count to fall back to what it
// was before the cluster started: no node loop, applier or apply reader
// may outlive cleanup. Delayed-message timers that are still firing are
// short-lived, so a little waiting covers them.
func (c *cluster) checkGoroutines(within time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(within)
	for runtime.NumGoroutine() > c.goros {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<20)
			buf = buf[:runtime.Stack(buf, true)]
			c.t.Errorf("goroutine leak: %d running, %d before the cluster started\n%s",
				runtime.NumGoroutine(), c.goros, buf)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
