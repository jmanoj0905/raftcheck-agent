package core

import (
	"reflect"
	"testing"

	"github.com/jmanoj0905/raftcheck-agent/raft/internal/schemacheck"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

func emptyFollowerState() tracelog.State {
	return tracelog.State{
		Role: "follower", CurrentTerm: 0, VotedFor: 0, Log: []tracelog.LogPair{},
		CommitIndex: 0, LastApplied: 0, LeaderID: 0,
	}
}

func recordEvents(out Output) []string {
	evs := []string{}
	for _, r := range out.Records {
		evs = append(evs, r.Event)
	}
	return evs
}

func mustPanic(t *testing.T, name string, f func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("%s: expected a panic", name)
		}
	}()
	f()
}

func TestNewSortsPeers(t *testing.T) {
	in := []int{3, 1, 2}
	n := New(2, in)
	if n.ID() != 2 {
		t.Errorf("ID() = %d, want 2", n.ID())
	}
	if !reflect.DeepEqual(n.peers, []int{1, 2, 3}) {
		t.Errorf("peers = %v, want [1 2 3]", n.peers)
	}
	if !reflect.DeepEqual(in, []int{3, 1, 2}) {
		t.Errorf("New changed the caller's slice: %v", in)
	}
	st := n.State()
	if !reflect.DeepEqual(st, emptyFollowerState()) {
		t.Errorf("State() = %+v, want %+v", st, emptyFollowerState())
	}
	if st.NextIndex != nil || st.MatchIndex != nil {
		t.Errorf("follower State has maps: %v %v", st.NextIndex, st.MatchIndex)
	}
	if st.Log == nil {
		t.Error("State().Log is nil, want empty non-nil slice")
	}
	if c := n.Counters(); c != (Counters{}) {
		t.Errorf("Counters() = %+v, want zero", c)
	}
}

func TestNewPanics(t *testing.T) {
	mustPanic(t, "id not in peers", func() { New(4, []int{1, 2, 3}) })
	mustPanic(t, "duplicate peer", func() { New(1, []int{1, 1, 2}) })
	mustPanic(t, "peer id 0", func() { New(1, []int{0, 1}) })
}

func TestStateIsCopy(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	n.appendEntry(Entry{Term: 1, Command: "a"})
	st := n.State()
	st.Log[0] = tracelog.LogPair{Term: 9, Command: "changed"}
	if got := n.State().Log; !reflect.DeepEqual(got, []tracelog.LogPair{{Term: 1, Command: "a"}}) {
		t.Errorf("State().Log after mutation = %v", got)
	}

	// Leader maps are copies too, keyed by the other peers' ids.
	n.role = Leader
	n.leaderID = 1
	n.nextIndex = map[int]int{2: 2, 3: 1}
	n.matchIndex = map[int]int{2: 1, 3: 0}
	st = n.State()
	if !reflect.DeepEqual(st.NextIndex, map[string]int{"2": 2, "3": 1}) ||
		!reflect.DeepEqual(st.MatchIndex, map[string]int{"2": 1, "3": 0}) {
		t.Fatalf("leader maps = %v %v", st.NextIndex, st.MatchIndex)
	}
	st.NextIndex["2"] = 99
	st.MatchIndex["3"] = 99
	if n.nextIndex[2] != 2 || n.matchIndex[3] != 0 {
		t.Error("mutating State() maps changed the node")
	}
}

func TestLeaderMapsUseSortedOtherPeers(t *testing.T) {
	n := New(2, []int{3, 1, 2})
	n.role = Leader
	n.nextIndex = map[int]int{1: 1, 3: 1}
	n.matchIndex = map[int]int{1: 0, 3: 0}
	st := n.State()
	if !reflect.DeepEqual(st.NextIndex, map[string]int{"1": 1, "3": 1}) {
		t.Errorf("NextIndex = %v, want keys 1 and 3", st.NextIndex)
	}
	if !reflect.DeepEqual(st.MatchIndex, map[string]int{"1": 0, "3": 0}) {
		t.Errorf("MatchIndex = %v, want keys 1 and 3", st.MatchIndex)
	}
}

func TestRoleString(t *testing.T) {
	for r, want := range map[Role]string{Follower: "follower", Candidate: "candidate", Leader: "leader"} {
		if r.String() != want {
			t.Errorf("%d.String() = %q, want %q", int(r), r.String(), want)
		}
	}
}

func TestStepOnFreshNodeIsEmpty(t *testing.T) {
	// A fresh follower has nothing to do on a heartbeat tick.
	for _, ev := range []Event{HeartbeatTimeout{}} {
		n := New(1, []int{1, 2, 3})
		out := n.Step(ev)
		if out.Persist != nil || len(out.Messages) != 0 || len(out.Apply) != 0 || len(out.Records) != 0 {
			t.Errorf("Step(%T) = %+v, want empty", ev, out)
		}
	}
}

func TestLogHelpers(t *testing.T) {
	n := New(1, []int{1})
	if n.lastIndex() != 0 || n.lastTerm() != 0 || n.termAt(0) != 0 {
		t.Fatalf("empty log: lastIndex %d lastTerm %d termAt(0) %d", n.lastIndex(), n.lastTerm(), n.termAt(0))
	}
	if e := n.entriesFrom(1); e == nil || len(e) != 0 {
		t.Errorf("entriesFrom(1) on empty log = %#v, want empty non-nil", e)
	}
	n.appendEntry(Entry{Term: 1, Command: "a"})
	n.appendEntry(Entry{Term: 1, Command: "b"})
	n.appendEntry(Entry{Term: 3, Command: "c"})
	if n.lastIndex() != 3 || n.lastTerm() != 3 || n.termAt(1) != 1 || n.termAt(3) != 3 {
		t.Errorf("lastIndex %d lastTerm %d termAt(1) %d termAt(3) %d", n.lastIndex(), n.lastTerm(), n.termAt(1), n.termAt(3))
	}
	got := n.entriesFrom(2)
	if !reflect.DeepEqual(got, []Entry{{1, "b"}, {3, "c"}}) {
		t.Errorf("entriesFrom(2) = %v", got)
	}
	got[0].Command = "changed"
	if n.log[1].Command != "b" {
		t.Error("entriesFrom did not return a copy")
	}
	if e := n.entriesFrom(4); len(e) != 0 {
		t.Errorf("entriesFrom(lastIndex+1) = %v, want empty", e)
	}
	mustPanic(t, "termAt past end", func() { n.termAt(4) })
	mustPanic(t, "entriesFrom(0)", func() { n.entriesFrom(0) })
	mustPanic(t, "entriesFrom past end+1", func() { n.entriesFrom(5) })

	n.truncateFrom(2)
	if !reflect.DeepEqual(n.log, []Entry{{1, "a"}}) {
		t.Errorf("after truncateFrom(2) log = %v", n.log)
	}
	mustPanic(t, "truncateFrom(0)", func() { n.truncateFrom(0) })
	mustPanic(t, "truncateFrom past end", func() { n.truncateFrom(2) })
	mustPanic(t, "appendEntry term 0", func() { n.appendEntry(Entry{Term: 0, Command: "z"}) })
}

func TestDirtyOnlyWhenPersistentStateChanges(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	n.setTerm(0)
	n.setVote(0)
	if out := n.finish(); out.Persist != nil || len(out.Records) != 0 {
		t.Errorf("no-op setters produced %+v", out)
	}
	for name, change := range map[string]func(){
		"setTerm":     func() { n.setTerm(n.currentTerm + 1) },
		"setVote":     func() { n.setVote(2) },
		"appendEntry": func() { n.appendEntry(Entry{Term: 1, Command: "x"}) },
		"truncateFrom": func() {
			n.appendEntry(Entry{Term: 1, Command: "y"})
			n.finish()
			n.truncateFrom(1)
		},
	} {
		change()
		if out := n.finish(); out.Persist == nil {
			t.Errorf("%s: Persist is nil, want set", name)
		}
	}
}

// TestFinishOrder checks the persist -> send -> apply order, seq and MsgID
// numbering, the deep-copied Persist, and that the records match the schema.
func TestFinishOrder(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	n.setTerm(2)
	n.setVote(1)
	n.appendEntry(Entry{Term: 2, Command: "set x 1"})
	n.record(tracelog.EvBecomeCandidate, map[string]any{"old_role": "follower"})
	n.send(Message{Type: MsgAppendEntries, To: 3, Term: 2, Entries: n.entriesFrom(1), LeaderCommit: 1})
	n.send(Message{Type: MsgRequestVote, To: 2, Term: 2, LastLogIndex: 1, LastLogTerm: 2})
	n.send(Message{Type: MsgRequestVoteReply, To: 2, Term: 2, VoteGranted: true})
	n.commitIndex = 1
	n.resetTimer = true

	out := n.finish()

	wantEvents := []string{
		tracelog.EvBecomeCandidate, tracelog.EvPersist,
		tracelog.EvSendRequestVote, tracelog.EvSendAppendEntries, tracelog.EvApplyEntry,
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("events = %v, want %v", got, wantEvents)
	}
	for i, r := range out.Records {
		if r.Seq != int64(i+1) || r.Node != 1 || r.V != tracelog.Version || r.Term != 2 {
			t.Errorf("record %d: seq %d node %d v %d term %d", i, r.Seq, r.Node, r.V, r.Term)
		}
	}
	if d := out.Records[1].Details; !reflect.DeepEqual(d, map[string]any{"log_len": 1}) {
		t.Errorf("persist details = %v", d)
	}

	// Stable sort by To: both messages to 2 keep their queue order.
	if len(out.Messages) != 3 {
		t.Fatalf("got %d messages, want 3", len(out.Messages))
	}
	wantMsgs := []struct {
		typ   MsgType
		to    int
		msgID string
	}{{MsgRequestVote, 2, "1-1"}, {MsgRequestVoteReply, 2, "1-2"}, {MsgAppendEntries, 3, "1-3"}}
	for i, w := range wantMsgs {
		m := out.Messages[i]
		if m.Type != w.typ || m.To != w.to || m.MsgID != w.msgID || m.From != 1 {
			t.Errorf("message %d = %+v, want type %d to %d id %s from 1", i, m, w.typ, w.to, w.msgID)
		}
	}
	rv := out.Records[2].Details
	if !reflect.DeepEqual(rv, map[string]any{"to": 2, "msg_id": "1-1", "last_log_index": 1, "last_log_term": 2}) {
		t.Errorf("send_request_vote details = %v", rv)
	}
	ae := out.Records[3].Details
	wantAE := map[string]any{"to": 3, "msg_id": "1-3", "prev_log_index": 0, "prev_log_term": 0, "n_entries": 1, "leader_commit": 1}
	if !reflect.DeepEqual(ae, wantAE) {
		t.Errorf("send_append_entries details = %v", ae)
	}

	if !reflect.DeepEqual(out.Apply, []Applied{{Index: 1, Term: 2, Command: "set x 1"}}) {
		t.Errorf("Apply = %v", out.Apply)
	}
	ap := out.Records[4]
	if !reflect.DeepEqual(ap.Details, map[string]any{"index": 1, "entry_term": 2, "command": "set x 1"}) {
		t.Errorf("apply_entry details = %v", ap.Details)
	}
	if ap.State.LastApplied != 1 || out.Records[3].State.LastApplied != 0 {
		t.Errorf("apply record state LastApplied %d, send record %d", ap.State.LastApplied, out.Records[3].State.LastApplied)
	}
	if !out.ResetElectionTimer {
		t.Error("ResetElectionTimer lost")
	}
	if c := n.Counters(); c != (Counters{LastSeq: 5, LastMsg: 3}) {
		t.Errorf("Counters = %+v, want {5 3}", c)
	}

	want := PersistentState{CurrentTerm: 2, VotedFor: 1, Log: []Entry{{2, "set x 1"}}}
	if out.Persist == nil || !reflect.DeepEqual(*out.Persist, want) {
		t.Fatalf("Persist = %+v, want %+v", out.Persist, want)
	}
	out.Persist.Log[0].Command = "changed"
	if n.log[0].Command != "set x 1" {
		t.Error("Persist.Log aliases the node's log")
	}

	if err := schemacheck.ValidateJSONL(tracelog.AppendJSONL(nil, out.Records)); err != nil {
		t.Errorf("records fail the schema: %v", err)
	}

	// Per-step state is cleared; counters carry on.
	again := n.finish()
	if again.Persist != nil || len(again.Messages) != 0 || len(again.Records) != 0 ||
		len(again.Apply) != 0 || again.ResetElectionTimer || again.Proposed != nil {
		t.Errorf("second finish = %+v, want empty", again)
	}
	n.send(Message{Type: MsgRequestVote, To: 3})
	if out := n.finish(); out.Messages[0].MsgID != "1-4" || out.Records[0].Seq != 6 {
		t.Errorf("next msg %s seq %d, want 1-4 and 6", out.Messages[0].MsgID, out.Records[0].Seq)
	}
}

func TestSameInputsGiveIdenticalJSONL(t *testing.T) {
	run := func() []byte {
		n := New(2, []int{3, 1, 2})
		n.setTerm(1)
		n.setVote(2)
		n.record(tracelog.EvBecomeCandidate, map[string]any{"old_role": "follower"})
		n.send(Message{Type: MsgRequestVote, To: 3, Term: 1})
		n.send(Message{Type: MsgRequestVote, To: 1, Term: 1})
		return tracelog.AppendJSONL(nil, n.finish().Records)
	}
	a, b := run(), run()
	if string(a) != string(b) {
		t.Errorf("JSONL differs:\n%s\n%s", a, b)
	}
}
