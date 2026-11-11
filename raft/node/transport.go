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

// inboxSize is the buffer of each MemNetwork inbox. A message that finds
// the inbox full is dropped.
const inboxSize = 1024

// MemNetwork is an in-memory Transport for tests. It can drop messages at
// random, delay them (which also reorders them), and disconnect nodes. Send
// never blocks. Inboxes are never closed, so a late delivery to a killed
// node is harmless: it lands in the buffer or is dropped. Safe for
// concurrent use.
type MemNetwork struct {
	inboxes map[int]chan core.Message // fixed at creation; read-only after

	mu        sync.Mutex
	connected map[int]bool
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
// dropped if the sender or receiver is disconnected (at send time or at
// delivery time), if it loses the drop-rate roll, or if the receiver's
// inbox is full. m.Entries is copied.
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
	n.mu.Unlock()

	if delay == 0 {
		n.deliver(m)
		return
	}
	time.AfterFunc(delay, func() { n.deliver(m) })
}

// deliver re-checks the link and does a non-blocking send to the inbox.
func (n *MemNetwork) deliver(m core.Message) {
	n.mu.Lock()
	up := n.linkUpLocked(m)
	n.mu.Unlock()
	if !up {
		return
	}
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

// Disconnect cuts node id off: nothing it sends or is sent arrives, including
// messages already in flight.
func (n *MemNetwork) Disconnect(id int) { n.setConnected(id, false) }

func (n *MemNetwork) setConnected(id int, up bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.connected[id]; !ok {
		panic(fmt.Sprintf("node: MemNetwork has no node %d", id))
	}
	n.connected[id] = up
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
