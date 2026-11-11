// Package node wraps the deterministic Raft core (package core) with
// goroutines, real timers, a Transport and a Persister, giving the MIT-lab
// style API Make / Start / GetState / Kill (spec §5).
//
// Each Node runs two goroutines:
//   - the loop owns the core. Every event (incoming message, election timer,
//     heartbeat ticker, Start request, Kill) reaches it through a channel,
//     so the core is only ever touched by one goroutine.
//   - the applier moves committed entries from an unbounded queue to
//     applyCh, so a slow or absent reader never stalls the loop.
//
// Kill closes killCh. The loop then writes a crash record, saves the
// counters and exits; the applier exits too; Kill returns once both have.
// No Save, SaveCounters or sink Write happens after Kill returns.
package node

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmanoj0905/raftcheck-agent/raft/core"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// Config holds the wrapper's timing, random seed and record sink.
type Config struct {
	ElectionMin       time.Duration // election timeout is drawn from [ElectionMin, ElectionMax)
	ElectionMax       time.Duration
	HeartbeatInterval time.Duration
	Seed              uint64     // with the node id, seeds the election-timeout draws
	Sink              RecordSink // nil discards records
}

// DefaultConfig returns 300-600 ms election timeouts, a 100 ms heartbeat,
// seed 0 and no sink.
func DefaultConfig() Config {
	return Config{
		ElectionMin:       300 * time.Millisecond,
		ElectionMax:       600 * time.Millisecond,
		HeartbeatInterval: 100 * time.Millisecond,
	}
}

// ApplyMsg is one committed entry handed to the application.
type ApplyMsg struct {
	CommandValid bool
	Command      string
	CommandIndex int
	CommandTerm  int
}

// Node is one running Raft node.
type Node struct {
	id      int
	cfg     Config
	t       Transport
	p       Persister
	sink    RecordSink
	applyCh chan ApplyMsg

	// Owned by the loop goroutine (and by Make before it starts).
	core *core.Node
	rng  *rand.Rand

	state   atomic.Pointer[tracelog.State] // latest core.State(), for GetState
	startCh chan startReq
	killCh  chan struct{}
	kill    sync.Once
	wg      sync.WaitGroup

	// The applier queue. applyWake (buffer 1) says "the queue may be
	// non-empty"; sends to it never block.
	applyMu   sync.Mutex
	applyQ    []ApplyMsg
	applyWake chan struct{}
}

type startReq struct {
	command string
	reply   chan core.ProposeResult // buffer 1: the loop never blocks on it
}

// Make starts node id. peers lists every node id including id. If p holds
// saved state or counters for id, the node is rebuilt with core.Restore
// (writing a restart record); otherwise it starts fresh in term 0.
//
// On a restart Make first drops what the previous life left behind: if t
// is a Restarter (MemNetwork is), t.Restart(id) clears the inbox and every
// message still in flight to or from the old life, so the new life never
// handles one. Other transports only get their inbox drained, so a message
// they deliver late may still arrive.
//
// Any earlier Node with this id must be killed first, since both would
// read the same inbox. Make panics on a bad Config or on peers the core
// rejects.
func Make(id int, peers []int, t Transport, p Persister, applyCh chan ApplyMsg, cfg Config) *Node {
	checkConfig(cfg)
	n := &Node{
		id:        id,
		cfg:       cfg,
		t:         t,
		p:         p,
		sink:      cfg.Sink,
		applyCh:   applyCh,
		startCh:   make(chan startReq),
		killCh:    make(chan struct{}),
		applyWake: make(chan struct{}, 1),
	}
	if n.sink == nil {
		n.sink = discardSink{}
	}
	n.initRand()

	ps, saved := p.Load(id)
	counters := p.LoadCounters(id)
	// A node that was killed before it ever saved state still has counters
	// (its crash record bumped LastSeq), so it is a restart too: starting
	// fresh would reuse seqs.
	if saved || counters != (core.Counters{}) {
		if r, ok := t.(Restarter); ok {
			r.Restart(id)
		} else {
			drainInbox(t.Inbox(id))
		}
		c, out := core.Restore(id, peers, ps, counters)
		n.core = c
		n.process(out)
	} else {
		n.core = core.New(id, peers)
		n.publishState()
	}

	n.wg.Add(2)
	go n.loop()
	go n.applier()
	return n
}

// drainInbox throws away what was waiting in a restarting node's inbox, for
// transports that are not Restarters. Those messages were sent to the
// node's previous life; a real crash loses them with the process. It never
// blocks: it takes at most the messages queued when it starts.
func drainInbox(inbox <-chan core.Message) {
	for range len(inbox) {
		select {
		case <-inbox:
		default:
			return
		}
	}
}

func checkConfig(cfg Config) {
	if cfg.ElectionMin <= 0 || cfg.ElectionMax < cfg.ElectionMin || cfg.HeartbeatInterval <= 0 {
		panic(fmt.Sprintf("node: bad Config timings: election [%v, %v), heartbeat %v",
			cfg.ElectionMin, cfg.ElectionMax, cfg.HeartbeatInterval))
	}
}

// initRand seeds the election-timeout source from (Seed, id).
func (n *Node) initRand() {
	n.rng = rand.New(rand.NewPCG(n.cfg.Seed, uint64(n.id)))
}

// electionTimeout draws a duration uniformly from [ElectionMin, ElectionMax)
// (exactly ElectionMin if the two are equal).
func (n *Node) electionTimeout() time.Duration {
	span := n.cfg.ElectionMax - n.cfg.ElectionMin
	if span <= 0 {
		return n.cfg.ElectionMin
	}
	return n.cfg.ElectionMin + time.Duration(n.rng.Int64N(int64(span)))
}

// Start asks the node to append command. It returns the core's answer:
// (index, term, true) on a leader, (0, term, false) otherwise. On a killed
// node it returns (0, last term, false) without blocking.
func (n *Node) Start(command string) (index, term int, isLeader bool) {
	req := startReq{command: command, reply: make(chan core.ProposeResult, 1)}
	select {
	case n.startCh <- req:
	case <-n.killCh:
		term, _ = n.GetState()
		return 0, term, false
	}
	// The loop took the request, so it always replies (the reply channel is
	// buffered and the loop checks killCh only between events).
	r := <-req.reply
	return r.Index, r.Term, r.IsLeader
}

// GetState returns the node's current term and whether it thinks it is the
// leader. After Kill it keeps returning the last state.
func (n *Node) GetState() (term int, isLeader bool) {
	st := n.state.Load()
	return st.CurrentTerm, st.Role == core.Leader.String()
}

// Kill stops the node: the loop writes a crash record, saves the counters
// and exits, and the applier stops delivering. Kill returns after both
// goroutines have exited. It is idempotent and safe to call concurrently.
func (n *Node) Kill() {
	n.kill.Do(func() { close(n.killCh) })
	n.wg.Wait()
}

// loop is the only goroutine that touches n.core after Make.
func (n *Node) loop() {
	defer n.wg.Done()
	// Since Go 1.23 (go.mod says 1.27) timer channels are unbuffered:
	// Reset and Stop never leave a stale tick behind, so no draining needed.
	election := time.NewTimer(n.electionTimeout())
	defer election.Stop()
	heartbeat := time.NewTicker(n.cfg.HeartbeatInterval)
	defer heartbeat.Stop()
	inbox := n.t.Inbox(n.id)

	for {
		// Kill wins over any ready event, so a busy inbox cannot delay it.
		select {
		case <-n.killCh:
			n.crash()
			return
		default:
		}

		var out core.Output
		var reply chan core.ProposeResult
		fired := false
		select {
		case <-n.killCh:
			n.crash()
			return
		case m := <-inbox:
			out = n.core.Step(core.Receive{Msg: m})
		case <-election.C:
			fired = true
			out = n.core.Step(core.ElectionTimeout{})
		case <-heartbeat.C:
			out = n.core.Step(core.HeartbeatTimeout{})
		case req := <-n.startCh:
			reply = req.reply
			out = n.core.Step(core.Propose{Command: req.command})
		}
		n.process(out)
		if reply != nil {
			reply <- *out.Proposed
		}
		if out.ResetElectionTimer || fired {
			election.Reset(n.electionTimeout())
		}
	}
}

// process carries out one Output in the required order: records to the
// sink, Save (if Persist), SaveCounters, Send each message, queue Apply,
// publish the new state. Save always comes before any Send of the same step.
func (n *Node) process(out core.Output) {
	for _, r := range out.Records {
		n.sink.Write(r)
	}
	if out.Persist != nil {
		n.p.Save(n.id, *out.Persist)
	}
	n.p.SaveCounters(n.id, n.core.Counters())
	for _, m := range out.Messages {
		n.t.Send(m)
	}
	if len(out.Apply) > 0 {
		n.applyMu.Lock()
		for _, a := range out.Apply {
			n.applyQ = append(n.applyQ, ApplyMsg{
				CommandValid: true, Command: a.Command, CommandIndex: a.Index, CommandTerm: a.Term,
			})
		}
		n.applyMu.Unlock()
		select {
		case n.applyWake <- struct{}{}:
		default: // a wake-up is already pending
		}
	}
	n.publishState()
}

func (n *Node) publishState() {
	st := n.core.State()
	n.state.Store(&st)
}

// crash writes the crash record (last state, seq LastSeq+1) and saves the
// bumped counters, so a restart continues the sequence.
func (n *Node) crash() {
	st := n.core.State()
	c := n.core.Counters()
	c.LastSeq++
	n.sink.Write(tracelog.Record{
		V: tracelog.Version, Node: n.id, Seq: c.LastSeq, Event: tracelog.EvCrash,
		Term: st.CurrentTerm, Details: map[string]any{}, State: st,
	})
	n.p.SaveCounters(n.id, c)
}

// applier delivers queued entries to applyCh in order until Kill.
func (n *Node) applier() {
	defer n.wg.Done()
	for {
		n.applyMu.Lock()
		if len(n.applyQ) == 0 {
			n.applyMu.Unlock()
			select {
			case <-n.applyWake:
				continue
			case <-n.killCh:
				return
			}
		}
		msg := n.applyQ[0]
		n.applyQ[0] = ApplyMsg{}
		n.applyQ = n.applyQ[1:]
		n.applyMu.Unlock()

		select {
		case n.applyCh <- msg:
		case <-n.killCh:
			return
		}
	}
}
