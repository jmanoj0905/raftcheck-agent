// Package testkit is a tiny deterministic driver for several Raft cores, used
// only by tests (spec §8.2). It is not Spec 2's runner. A Cluster holds the
// cores, the saved state of each node and the messages in flight; every
// action steps at most one core, applies its Output in the order persist ->
// counters -> messages -> apply -> records, and then checks the four safety
// rules and the §8.4 recorded-state rule.
package testkit

import (
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/jmanoj0905/raftcheck-agent/raft/core"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// Cluster is a set of cores with ids 1..n and an explicit network. It is not
// safe for concurrent use.
type Cluster struct {
	ids      []int
	nodes    []*core.Node // index id-1; nil while the node is down
	saved    []core.PersistentState
	counters []core.Counters
	inFlight []core.Message
	records  []tracelog.Record
	actions  []string
	inv      *invariants
}

// NewCluster builds n fresh followers with ids 1..n. It panics if n < 1.
func NewCluster(n int) *Cluster {
	if n < 1 {
		panic(fmt.Sprintf("testkit: cluster of %d nodes", n))
	}
	c := &Cluster{
		nodes:    make([]*core.Node, n),
		saved:    make([]core.PersistentState, n),
		counters: make([]core.Counters, n),
		inv:      newInvariants(),
	}
	for id := 1; id <= n; id++ {
		c.ids = append(c.ids, id)
	}
	for i, id := range c.ids {
		c.nodes[i] = core.New(id, c.ids)
		c.saved[i] = core.PersistentState{Log: []core.Entry{}}
	}
	return c
}

// Timeout fires node id's election timer.
func (c *Cluster) Timeout(id int) error {
	return c.step(id, core.ElectionTimeout{}, "timeout "+strconv.Itoa(id))
}

// Heartbeat fires node id's heartbeat timer (a non-leader ignores it).
func (c *Cluster) Heartbeat(id int) error {
	return c.step(id, core.HeartbeatTimeout{}, "heartbeat "+strconv.Itoa(id))
}

// Propose hands cmd to node id as a client request.
func (c *Cluster) Propose(id int, cmd string) error {
	return c.step(id, core.Propose{Command: cmd}, fmt.Sprintf("propose %d %q", id, cmd))
}

// Deliver removes InFlight()[i] and hands it to its receiver. A message to a
// node that is down is dropped instead.
func (c *Cluster) Deliver(i int) error {
	if err := c.checkIndex(i); err != nil {
		return err
	}
	m := c.inFlight[i]
	if _, err := c.node(m.To); err != nil {
		return fmt.Errorf("message %s: %w", m.MsgID, err)
	}
	c.inFlight = slices.Delete(c.inFlight, i, i+1)
	if c.nodes[m.To-1] == nil {
		return c.done(fmt.Sprintf("deliver %s (node %d down, dropped)", m.MsgID, m.To), nil)
	}
	return c.stepNode(m.To, core.Receive{Msg: m}, "deliver "+m.MsgID)
}

// Drop removes InFlight()[i] without delivering it.
func (c *Cluster) Drop(i int) error {
	if err := c.checkIndex(i); err != nil {
		return err
	}
	m := c.inFlight[i]
	c.inFlight = slices.Delete(c.inFlight, i, i+1)
	return c.done("drop "+m.MsgID, nil)
}

// Duplicate appends an exact copy of InFlight()[i] (same From, To and MsgID)
// to the end of InFlight.
func (c *Cluster) Duplicate(i int) error {
	if err := c.checkIndex(i); err != nil {
		return err
	}
	m := c.inFlight[i]
	m.Entries = slices.Clone(m.Entries)
	c.inFlight = append(c.inFlight, m)
	return c.done("duplicate "+m.MsgID, nil)
}

// Crash throws node id's core away. It writes the crash record itself (the
// node's last state, seq LastSeq+1) and saves the bumped counters, so the
// restart record continues the sequence. Messages already in flight stay.
func (c *Cluster) Crash(id int) error {
	n, err := c.live(id)
	if err != nil {
		return err
	}
	st := n.State()
	cnt := n.Counters()
	cnt.LastSeq++
	r := tracelog.Record{
		V: tracelog.Version, Node: id, Seq: cnt.LastSeq, Event: tracelog.EvCrash,
		Term: st.CurrentTerm, Details: map[string]any{}, State: st,
	}
	c.counters[id-1] = cnt
	c.nodes[id-1] = nil
	c.records = append(c.records, r)
	return c.done("crash "+strconv.Itoa(id), []tracelog.Record{r})
}

// Restart rebuilds a down node with core.Restore from its saved state and
// counters.
func (c *Cluster) Restart(id int) error {
	if _, err := c.node(id); err != nil {
		return err
	}
	if c.nodes[id-1] != nil {
		return fmt.Errorf("node %d is not down", id)
	}
	action := "restart " + strconv.Itoa(id)
	return c.guard(action, func() error {
		n, out := core.Restore(id, c.ids, c.saved[id-1], c.counters[id-1])
		c.nodes[id-1] = n
		return c.applyOutput(id, action, out)
	})
}

// InFlight returns a copy of the messages sent but not yet delivered or
// dropped, oldest first.
func (c *Cluster) InFlight() []core.Message { return slices.Clone(c.inFlight) }

// Records returns every record so far, in step order.
func (c *Cluster) Records() []tracelog.Record { return slices.Clone(c.records) }

// JSONL returns Records() as JSON lines.
func (c *Cluster) JSONL() []byte { return tracelog.AppendJSONL(nil, c.records) }

// Actions returns the actions taken so far, e.g. "deliver 1-3", "crash 2".
func (c *Cluster) Actions() []string { return slices.Clone(c.actions) }

// node checks that id is in 1..n and returns its core (nil if down).
func (c *Cluster) node(id int) (*core.Node, error) {
	if id < 1 || id > len(c.nodes) {
		return nil, fmt.Errorf("no node %d in a %d-node cluster", id, len(c.nodes))
	}
	return c.nodes[id-1], nil
}

// live is node, but a down node is an error.
func (c *Cluster) live(id int) (*core.Node, error) {
	n, err := c.node(id)
	if err == nil && n == nil {
		err = fmt.Errorf("node %d is down", id)
	}
	return n, err
}

func (c *Cluster) checkIndex(i int) error {
	if i < 0 || i >= len(c.inFlight) {
		return fmt.Errorf("no in-flight message %d (%d in flight)", i, len(c.inFlight))
	}
	return nil
}

// step steps live node id with ev.
func (c *Cluster) step(id int, ev core.Event, action string) error {
	if _, err := c.live(id); err != nil {
		return err
	}
	return c.stepNode(id, ev, action)
}

func (c *Cluster) stepNode(id int, ev core.Event, action string) error {
	return c.guard(action, func() error {
		return c.applyOutput(id, action, c.nodes[id-1].Step(ev))
	})
}

// guard turns a core panic during action into an error that carries the
// action list.
func (c *Cluster) guard(action string, f func() error) (err error) {
	defer func() {
		if p := recover(); p != nil {
			c.actions = append(c.actions, action+" (panicked)")
			err = c.fail(fmt.Errorf("panic: %v", p))
		}
	}()
	return f()
}

// applyOutput applies node id's Output in order (persist, counters,
// messages, apply, records), logs the action and runs the checks.
func (c *Cluster) applyOutput(id int, action string, out core.Output) error {
	n := c.nodes[id-1]
	if out.Persist != nil {
		ps := *out.Persist
		ps.Log = slices.Clone(ps.Log)
		if ps.Log == nil {
			ps.Log = []core.Entry{}
		}
		c.saved[id-1] = ps
	}
	c.counters[id-1] = n.Counters()
	c.inFlight = append(c.inFlight, out.Messages...)
	c.records = append(c.records, out.Records...)
	if err := c.done(action, out.Records); err != nil {
		return err
	}
	// Apply must hand out exactly the entries of the step's apply_entry
	// records, in order.
	var fromRecs []core.Applied
	for _, r := range out.Records {
		if r.Event == tracelog.EvApplyEntry {
			idx, _ := intDetail(r, "index")
			term, _ := intDetail(r, "entry_term")
			cmd, _ := r.Details["command"].(string)
			fromRecs = append(fromRecs, core.Applied{Index: idx, Term: term, Command: cmd})
		}
	}
	if !reflect.DeepEqual(out.Apply, fromRecs) {
		return c.fail(fmt.Errorf("node %d: Output.Apply %v does not match apply_entry records %v", id, out.Apply, fromRecs))
	}
	if err := checkReplyLinks(out); err != nil {
		return c.fail(fmt.Errorf("node %d: %w", id, err))
	}
	return nil
}

// checkReplyLinks checks that the step's replies and its recv_request_vote /
// recv_append_entries records pair up one to one: each record's
// reply_msg_id is the MsgID of a reply in Messages of the matching type,
// sent back to the record's "from".
func checkReplyLinks(out core.Output) error {
	replies := map[string]core.Message{}
	for _, m := range out.Messages {
		if m.Type == core.MsgRequestVoteReply || m.Type == core.MsgAppendEntriesReply {
			replies[m.MsgID] = m
		}
	}
	linked := 0
	for _, r := range out.Records {
		var want core.MsgType
		switch r.Event {
		case tracelog.EvRecvRequestVote:
			want = core.MsgRequestVoteReply
		case tracelog.EvRecvAppendEntries:
			want = core.MsgAppendEntriesReply
		default:
			continue
		}
		id, _ := r.Details["reply_msg_id"].(string)
		m, ok := replies[id]
		from, _ := intDetail(r, "from")
		if !ok || m.Type != want || m.To != from {
			return fmt.Errorf("seq %d %s has reply_msg_id %q, which is not a reply of type %d to node %d in this step's messages",
				r.Seq, r.Event, id, want, from)
		}
		linked++
	}
	if linked != len(replies) {
		return fmt.Errorf("%d replies sent but %d recv records link to one", len(replies), linked)
	}
	return nil
}

// done logs action and runs the checks on this step's records.
func (c *Cluster) done(action string, recs []tracelog.Record) error {
	c.actions = append(c.actions, action)
	var live []liveNode
	for i, n := range c.nodes {
		if n == nil {
			continue
		}
		st := n.State()
		if err := checkRecordedState(c.ids[i], st, recs); err != nil {
			return c.fail(err)
		}
		live = append(live, liveNode{ID: c.ids[i], State: st})
	}
	if err := c.inv.checkInvariants(recs, live); err != nil {
		return c.fail(err)
	}
	return nil
}

// fail wraps err with the action count and the last actions taken.
func (c *Cluster) fail(err error) error {
	const tail = 50
	from := max(0, len(c.actions)-tail)
	return fmt.Errorf("after action %d: %w\nactions %d..%d:\n  %s",
		len(c.actions), err, from+1, len(c.actions), strings.Join(c.actions[from:], "\n  "))
}
