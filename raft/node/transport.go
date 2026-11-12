package node

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jmanoj0905/raftcheck-agent/raft/core"
)

// Transport moves messages between nodes. Send is best effort and may drop.
type Transport interface {
	Send(m core.Message)
	Inbox(id int) <-chan core.Message
}

// Restarter is an optional Transport method. Make calls Restart(id) before
// a restarted node starts, so the transport forgets everything addressed
// to or sent by the node's previous life: whatever is in its inbox and
// whatever is still in flight. Transports without it only get their inbox
// drained, so in-flight messages may still reach the new life.
type Restarter interface {
	Restart(id int)
}

// inboxSize is the buffer of each MemNetwork inbox. A message that finds
// the inbox full is dropped.
const inboxSize = 1024

// MemNetwork is an in-memory Transport for tests. It can drop messages at
// random, delay them (which also reorders them), and disconnect nodes. Send
// never blocks. Inboxes are never closed, so a late delivery to a killed
// node is harmless: it lands in the buffer or is dropped. Safe for
// concurrent use.
//
// Each node has an epoch, bumped by Disconnect and by Restart. Send stamps
// a message with the sender's and receiver's epochs, and it is delivered
// only if both are unchanged when its delay ends; the check and the inbox
// push happen together under mu. So a message in flight when either end is
// disconnected or restarted is never delivered, whatever its delay.
type MemNetwork struct {
	inboxes map[int]chan core.Message // fixed at creation; read-only after

	mu        sync.Mutex
	connected map[int]bool
	epoch     map[int]uint64
	dropRate  float64
	maxDelay  time.Duration
	rng       *rand.Rand

	sent atomic.Int64
}

// NewMemNetwork builds a network for ids, all connected, with no drops and
// no delay. seed drives the drop and delay rolls.
func NewMemNetwork(ids []int, seed uint64) *MemNetwork {
	n := &MemNetwork{
		inboxes:   make(map[int]chan core.Message, len(ids)),
		connected: make(map[int]bool, len(ids)),
		epoch:     make(map[int]uint64, len(ids)),
		rng:       rand.New(rand.NewPCG(seed, 0x6e6574)),
	}
	for _, id := range ids {
		n.inboxes[id] = make(chan core.Message, inboxSize)
		n.connected[id] = true
	}
	return n
}

// Inbox returns node id's receive channel. It panics for an unknown id.
func (n *MemNetwork) Inbox(id int) <-chan core.Message {
	ch, ok := n.inboxes[id]
	if !ok {
		panic(fmt.Sprintf("node: MemNetwork has no node %d", id))
	}
	return ch
}

// Send queues m for delivery after a random delay in [0, maxDelay). It is
// dropped if the sender or receiver is disconnected at send time, if either
// was disconnected or restarted before the delay ended, if it loses the
// drop-rate roll, or if the receiver's inbox is full. m.Entries is copied.
func (n *MemNetwork) Send(m core.Message) {
	n.sent.Add(1)
	if _, ok := n.inboxes[m.To]; !ok {
		return
	}
	m.Entries = slices.Clone(m.Entries)

	n.mu.Lock()
	if !n.linkUpLocked(m) || (n.dropRate > 0 && n.rng.Float64() < n.dropRate) {
		n.mu.Unlock()
		return
	}
	var delay time.Duration
	if n.maxDelay > 0 {
		delay = time.Duration(n.rng.Int64N(int64(n.maxDelay)))
	}
	fromEp, toEp := n.epoch[m.From], n.epoch[m.To]
	if delay == 0 {
		n.pushLocked(m)
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()
	time.AfterFunc(delay, func() { n.deliver(m, fromEp, toEp) })
}

// deliver pushes a delayed message if both ends are still connected and in
// the epochs they had when it was sent.
func (n *MemNetwork) deliver(m core.Message, fromEp, toEp uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.linkUpLocked(m) || n.epoch[m.From] != fromEp || n.epoch[m.To] != toEp {
		return
	}
	n.pushLocked(m)
}

// pushLocked does a non-blocking send to the receiver's inbox. Holding mu
// keeps it atomic with the caller's checks and with Restart's drain.
func (n *MemNetwork) pushLocked(m core.Message) {
	select {
	case n.inboxes[m.To] <- m:
	default: // inbox full: dropped
	}
}

func (n *MemNetwork) linkUpLocked(m core.Message) bool {
	return n.connected[m.From] && n.connected[m.To]
}

// Connect puts node id back on the network.
func (n *MemNetwork) Connect(id int) { n.setConnected(id, true) }

// Disconnect cuts node id off: nothing it sends or is sent arrives,
// including messages already in flight, even if it is reconnected before
// their delay ends.
func (n *MemNetwork) Disconnect(id int) { n.setConnected(id, false) }

func (n *MemNetwork) setConnected(id int, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.checkIDLocked(id)
	n.connected[id] = up
	if !up {
		n.epoch[id]++
	}
}

// Restart forgets node id's previous life: it empties id's inbox and bumps
// its epoch, so no message sent to or by the old life is delivered later.
// Both happen under mu, so no delivery can slip in between. Make calls it
// (see Restarter); it does not change whether id is connected.
func (n *MemNetwork) Restart(id int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.checkIDLocked(id)
	n.epoch[id]++
	inbox := n.inboxes[id]
	for range len(inbox) {
		select {
		case <-inbox:
		default:
			return
		}
	}
}

func (n *MemNetwork) checkIDLocked(id int) {
	if _, ok := n.connected[id]; !ok {
		panic(fmt.Sprintf("node: MemNetwork has no node %d", id))
	}
}

// SetDropRate sets the chance in [0, 1] that each message is dropped.
func (n *MemNetwork) SetDropRate(p float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = p
}

// SetMaxDelay sets the upper bound (exclusive) of each message's random
// delivery delay. 0 delivers at once.
func (n *MemNetwork) SetMaxDelay(d time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.maxDelay = d
}

// MessageCount returns the total number of Send calls, dropped or not.
func (n *MemNetwork) MessageCount() int64 { return n.sent.Load() }
