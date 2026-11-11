package core

import (
	"reflect"
	"slices"
	"testing"

	"github.com/jmanoj0905/raftcheck-agent/raft/internal/schemacheck"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// Message builders for replication. msgID must look like "<from>-<n>".

func appendEntries(from, to, term, prevIndex, prevTerm int, entries []Entry, leaderCommit int, msgID string) Event {
	return Receive{Msg: Message{
		Type: MsgAppendEntries, From: from, To: to, MsgID: msgID, Term: term,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, Entries: entries, LeaderCommit: leaderCommit,
	}}
}

func appendReply(from, to, term int, success bool, matchIndex, reqPrev int, msgID string) Event {
	return Receive{Msg: Message{
		Type: MsgAppendEntriesReply, From: from, To: to, MsgID: msgID, Term: term,
		Success: success, MatchIndex: matchIndex, ReqPrevLogIndex: reqPrev,
	}}
}

// followerWith builds node id of a 3-node cluster as a follower in term with
// the given log, and clears the setup's scratch state.
func followerWith(id, term int, log ...Entry) *Node {
	n := New(id, []int{1, 2, 3})
	for _, e := range log {
		n.appendEntry(e)
	}
	n.setTerm(term)
	n.finish()
	return n
}

// leaderWith makes node 1 of a 3-node cluster leader in term (> the last log
// term) with the given log, by timing out and getting node 2's vote.
func leaderWith(t *testing.T, term int, log ...Entry) *Node {
	t.Helper()
	n := followerWith(1, term-1, log...)
	step(t, n, ElectionTimeout{})
	step(t, n, voteReply(2, 1, term, true, "2-1"))
	if n.role != Leader || n.currentTerm != term {
		t.Fatalf("setup: role %v term %d, want leader in term %d", n.role, n.currentTerm, term)
	}
	return n
}

// persistentOf returns a deep copy of n's durable state (Log never nil).
func persistentOf(n *Node) PersistentState {
	log := slices.Clone(n.log)
	if log == nil {
		log = []Entry{}
	}
	return PersistentState{CurrentTerm: n.currentTerm, VotedFor: n.votedFor, Log: log}
}

// onlyAppendReply returns the single message in out and checks it is an
// AppendEntriesReply with the given fields.
func onlyAppendReply(t *testing.T, out Output, to, term int, success bool, matchIndex, reqPrev int) {
	t.Helper()
	if len(out.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 reply: %+v", len(out.Messages), out.Messages)
	}
	m := out.Messages[0]
	if m.Type != MsgAppendEntriesReply || m.To != to || m.Term != term || m.Success != success ||
		m.MatchIndex != matchIndex || m.ReqPrevLogIndex != reqPrev {
		t.Fatalf("reply = %+v, want AppendEntriesReply to %d term %d success %v match %d reqPrev %d",
			m, to, term, success, matchIndex, reqPrev)
	}
}

func TestProposeOnLeader(t *testing.T) {
	n := leaderOfTerm(t, 1)
	out := step(t, n, Propose{Command: "set x 1"})
	if out.Proposed == nil || *out.Proposed != (ProposeResult{Index: 1, Term: 1, IsLeader: true}) {
		t.Errorf("Proposed = %+v, want {1 1 true}", out.Proposed)
	}
	wantEvents := []string{tracelog.EvAppendEntry, tracelog.EvPersist, tracelog.EvSendAppendEntries, tracelog.EvSendAppendEntries}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"index": 1, "entry_term": 1, "command": "set x 1"}) {
		t.Errorf("append_entry details = %v", d)
	}
	want := PersistentState{CurrentTerm: 1, VotedFor: 1, Log: []Entry{{1, "set x 1"}}}
	if out.Persist == nil || !reflect.DeepEqual(*out.Persist, want) {
		t.Errorf("Persist = %+v, want %+v", out.Persist, want)
	}
	if len(out.Messages) != 2 {
		t.Fatalf("got %d messages, want 2", len(out.Messages))
	}
	for i, to := range []int{2, 3} {
		m := out.Messages[i]
		if m.Type != MsgAppendEntries || m.To != to || m.Term != 1 || m.PrevLogIndex != 0 || m.PrevLogTerm != 0 ||
			!reflect.DeepEqual(m.Entries, []Entry{{1, "set x 1"}}) || m.LeaderCommit != 0 {
			t.Errorf("message %d = %+v, want AppendEntries to %d prev 0/0 entries [{1 set x 1}]", i, m, to)
		}
	}
	if n.commitIndex != 0 || len(out.Apply) != 0 {
		t.Errorf("commitIndex %d Apply %v before any ack, want 0 and none", n.commitIndex, out.Apply)
	}
}

func TestProposeOnSingleNodeCommitsAtOnce(t *testing.T) {
	n := New(1, []int{1})
	step(t, n, ElectionTimeout{})
	out := step(t, n, Propose{Command: "a"})
	wantEvents := []string{tracelog.EvAppendEntry, tracelog.EvAdvanceCommit, tracelog.EvPersist, tracelog.EvApplyEntry}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := detailsOf(t, out, tracelog.EvAdvanceCommit); !reflect.DeepEqual(d, map[string]any{"old": 0, "new": 1}) {
		t.Errorf("advance_commit details = %v", d)
	}
	if !reflect.DeepEqual(out.Apply, []Applied{{Index: 1, Term: 1, Command: "a"}}) {
		t.Errorf("Apply = %+v", out.Apply)
	}
	if *out.Proposed != (ProposeResult{Index: 1, Term: 1, IsLeader: true}) {
		t.Errorf("Proposed = %+v", out.Proposed)
	}
}

func TestC7ProposeOnFollower(t *testing.T) {
	n := followerWith(1, 3, Entry{1, "a"})
	n.leaderID = 2
	before := n.State()
	out := step(t, n, Propose{Command: "set x 1"})
	if !reflect.DeepEqual(n.State(), before) {
		t.Errorf("state changed: %+v -> %+v", before, n.State())
	}
	if out.Persist != nil || len(out.Messages) != 0 || len(out.Apply) != 0 || out.ResetElectionTimer {
		t.Errorf("output = %+v, want only a record and Proposed", out)
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvProposeRejected}) {
		t.Errorf("events = %v, want [propose_rejected]", got)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"command": "set x 1"}) {
		t.Errorf("propose_rejected details = %v", d)
	}
	if out.Proposed == nil || *out.Proposed != (ProposeResult{Index: 0, Term: 3, IsLeader: false}) {
		t.Errorf("Proposed = %+v, want {0 3 false}", out.Proposed)
	}
}

func TestProposeOnCandidateRejected(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	step(t, n, ElectionTimeout{})
	out := step(t, n, Propose{Command: "x"})
	if out.Proposed == nil || out.Proposed.IsLeader || out.Proposed.Term != 1 || len(n.log) != 0 {
		t.Errorf("Proposed = %+v log %v, want rejected in term 1", out.Proposed, n.log)
	}
}

func TestHeartbeatOnNonLeaderIsEmpty(t *testing.T) {
	for _, n := range []*Node{followerWith(1, 2, Entry{1, "a"}), func() *Node {
		c := New(1, []int{1, 2, 3})
		c.Step(ElectionTimeout{})
		return c
	}()} {
		before := n.State()
		out := step(t, n, HeartbeatTimeout{})
		if !reflect.DeepEqual(out, Output{}) {
			t.Errorf("%v heartbeat output = %+v, want empty", n.role, out)
		}
		if !reflect.DeepEqual(n.State(), before) {
			t.Errorf("%v state changed", n.role)
		}
	}
}

func TestHeartbeatCarriesMissingEntries(t *testing.T) {
	n := leaderOfTerm(t, 1)
	step(t, n, Propose{Command: "a"}) // both AppendEntries are dropped
	out := step(t, n, HeartbeatTimeout{})
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvSendAppendEntries, tracelog.EvSendAppendEntries}) {
		t.Errorf("events = %v, want two send_append_entries", got)
	}
	if out.Persist != nil || out.ResetElectionTimer {
		t.Errorf("heartbeat Persist %v reset %v", out.Persist, out.ResetElectionTimer)
	}
	for i, to := range []int{2, 3} {
		m := out.Messages[i]
		if m.To != to || m.PrevLogIndex != 0 || !reflect.DeepEqual(m.Entries, []Entry{{1, "a"}}) {
			t.Errorf("message %d = %+v, want to %d prev 0 entries [{1 a}]", i, m, to)
		}
	}

	// Node 2 acks "a"; then "b" is proposed and dropped. The next heartbeat
	// sends node 2 only "b" but node 3 both entries.
	step(t, n, appendReply(2, 1, 1, true, 1, 0, "2-2"))
	step(t, n, Propose{Command: "b"})
	out = step(t, n, HeartbeatTimeout{})
	m2, m3 := out.Messages[0], out.Messages[1]
	if m2.PrevLogIndex != 1 || m2.PrevLogTerm != 1 || !reflect.DeepEqual(m2.Entries, []Entry{{1, "b"}}) || m2.LeaderCommit != 1 {
		t.Errorf("to 2 = %+v, want prev 1/1 entries [{1 b}] commit 1", m2)
	}
	if m3.PrevLogIndex != 0 || !reflect.DeepEqual(m3.Entries, []Entry{{1, "a"}, {1, "b"}}) {
		t.Errorf("to 3 = %+v, want prev 0 entries [a b]", m3)
	}
	// The message holds a copy: changing it does not touch the leader's log.
	m3.Entries[0].Command = "changed"
	if n.log[0].Command != "a" {
		t.Error("AppendEntries shares memory with the leader's log")
	}
}

func TestAppendEntriesOnEmptyFollower(t *testing.T) {
	n := New(2, []int{1, 2, 3})
	out := step(t, n, appendEntries(1, 2, 1, 0, 0, []Entry{{1, "a"}, {1, "b"}}, 0, "1-3"))
	wantEvents := []string{
		tracelog.EvUpdateTerm, tracelog.EvAppendEntry, tracelog.EvAppendEntry,
		tracelog.EvRecvAppendEntries, tracelog.EvPersist,
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[2].Details; !reflect.DeepEqual(d, map[string]any{"index": 2, "entry_term": 1, "command": "b"}) {
		t.Errorf("append_entry details = %v", d)
	}
	want := map[string]any{"from": 1, "msg_id": "1-3", "success": true, "reason": "ok"}
	if d := out.Records[3].Details; !reflect.DeepEqual(d, want) {
		t.Errorf("recv_append_entries details = %v, want %v", d, want)
	}
	onlyAppendReply(t, out, 1, 1, true, 2, 0)
	if !out.ResetElectionTimer || n.leaderID != 1 {
		t.Errorf("reset %v leaderID %d, want true and 1", out.ResetElectionTimer, n.leaderID)
	}
	if !reflect.DeepEqual(n.log, []Entry{{1, "a"}, {1, "b"}}) {
		t.Errorf("log = %v", n.log)
	}
}

func TestC5MatchingPrefixNeverTruncates(t *testing.T) {
	n := followerWith(2, 1, Entry{1, "a"}, Entry{1, "b"}, Entry{1, "c"})
	out := step(t, n, appendEntries(1, 2, 1, 1, 1, []Entry{{1, "b"}}, 0, "1-5"))
	if !reflect.DeepEqual(n.log, []Entry{{1, "a"}, {1, "b"}, {1, "c"}}) {
		t.Errorf("log = %v, want unchanged", n.log)
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvAppendEntries}) {
		t.Errorf("events = %v, want [recv_append_entries]", got)
	}
	if out.Persist != nil {
		t.Errorf("Persist = %+v, want nil", out.Persist)
	}
	onlyAppendReply(t, out, 1, 1, true, 2, 1)
}

func TestC5ConflictTruncates(t *testing.T) {
	n := followerWith(2, 1, Entry{1, "a"}, Entry{1, "b"}, Entry{1, "c"})
	out := step(t, n, appendEntries(1, 2, 2, 1, 1, []Entry{{2, "x"}}, 0, "1-5"))
	if !reflect.DeepEqual(n.log, []Entry{{1, "a"}, {2, "x"}}) {
		t.Errorf("log = %v, want [{1 a} {2 x}]", n.log)
	}
	wantEvents := []string{
		tracelog.EvUpdateTerm, tracelog.EvTruncateLog, tracelog.EvAppendEntry,
		tracelog.EvRecvAppendEntries, tracelog.EvPersist,
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[1].Details; !reflect.DeepEqual(d, map[string]any{"from_index": 2, "removed": 2}) {
		t.Errorf("truncate_log details = %v", d)
	}
	if st := out.Records[1].State; len(st.Log) != 1 {
		t.Errorf("truncate_log snapshot log = %v, want 1 entry", st.Log)
	}
	if d := out.Records[2].Details; !reflect.DeepEqual(d, map[string]any{"index": 2, "entry_term": 2, "command": "x"}) {
		t.Errorf("append_entry details = %v", d)
	}
	want := PersistentState{CurrentTerm: 2, VotedFor: 0, Log: []Entry{{1, "a"}, {2, "x"}}}
	if out.Persist == nil || !reflect.DeepEqual(*out.Persist, want) {
		t.Errorf("Persist = %+v, want %+v", out.Persist, want)
	}
	onlyAppendReply(t, out, 1, 2, true, 2, 1)
}

// A conflict partway through the entries keeps the matching ones before it.
func TestConflictAfterMatchingEntries(t *testing.T) {
	n := followerWith(2, 2, Entry{1, "a"}, Entry{1, "b"}, Entry{1, "c"})
	out := step(t, n, appendEntries(1, 2, 2, 0, 0, []Entry{{1, "a"}, {1, "b"}, {2, "y"}, {2, "z"}}, 0, "1-5"))
	if !reflect.DeepEqual(n.log, []Entry{{1, "a"}, {1, "b"}, {2, "y"}, {2, "z"}}) {
		t.Errorf("log = %v", n.log)
	}
	if d := detailsOf(t, out, tracelog.EvTruncateLog); !reflect.DeepEqual(d, map[string]any{"from_index": 3, "removed": 1}) {
		t.Errorf("truncate_log details = %v", d)
	}
	onlyAppendReply(t, out, 1, 2, true, 4, 0)
}

// An old, shorter AppendEntries arriving late must not cut off entries that
// a newer one already added.
func TestReorderedShortAppendKeepsLaterEntries(t *testing.T) {
	n := New(2, []int{1, 2, 3})
	step(t, n, appendEntries(1, 2, 1, 0, 0, []Entry{{1, "a"}, {1, "b"}}, 0, "1-4"))
	out := step(t, n, appendEntries(1, 2, 1, 0, 0, []Entry{{1, "a"}}, 0, "1-3"))
	if !reflect.DeepEqual(n.log, []Entry{{1, "a"}, {1, "b"}}) {
		t.Errorf("log = %v, want both entries kept", n.log)
	}
	if out.Persist != nil {
		t.Errorf("Persist = %+v, want nil", out.Persist)
	}
	onlyAppendReply(t, out, 1, 1, true, 1, 0)
}

func TestLogMismatchRejected(t *testing.T) {
	cases := []struct {
		name               string
		prevIndex, prevTer int
	}{
		{"prev beyond end", 5, 1},
		{"prev term differs", 2, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := followerWith(2, 2, Entry{1, "a"}, Entry{1, "b"})
			out := step(t, n, appendEntries(1, 2, 2, c.prevIndex, c.prevTer, []Entry{{2, "x"}}, 2, "1-7"))
			want := map[string]any{"from": 1, "msg_id": "1-7", "success": false, "reason": "log_mismatch"}
			if d := detailsOf(t, out, tracelog.EvRecvAppendEntries); !reflect.DeepEqual(d, want) {
				t.Errorf("details = %v, want %v", d, want)
			}
			if got := recordEvents(out); len(got) != 1 {
				t.Errorf("events = %v, want only recv_append_entries", got)
			}
			onlyAppendReply(t, out, 1, 2, false, 0, c.prevIndex)
			if !out.ResetElectionTimer || n.leaderID != 1 {
				t.Errorf("reset %v leaderID %d, want true and 1", out.ResetElectionTimer, n.leaderID)
			}
			if out.Persist != nil || n.commitIndex != 0 || !reflect.DeepEqual(n.log, []Entry{{1, "a"}, {1, "b"}}) {
				t.Errorf("Persist %v commit %d log %v, want nothing changed", out.Persist, n.commitIndex, n.log)
			}
		})
	}
}

func TestStaleAppendEntriesRejected(t *testing.T) {
	n := followerWith(2, 3, Entry{1, "a"})
	n.leaderID = 3
	out := step(t, n, appendEntries(1, 2, 2, 1, 1, []Entry{{2, "x"}}, 1, "1-9"))
	want := map[string]any{"from": 1, "msg_id": "1-9", "success": false, "reason": "stale_term"}
	if d := detailsOf(t, out, tracelog.EvRecvAppendEntries); !reflect.DeepEqual(d, want) {
		t.Errorf("details = %v, want %v", d, want)
	}
	onlyAppendReply(t, out, 1, 3, false, 0, 1)
	if out.ResetElectionTimer || out.Persist != nil || n.leaderID != 3 || n.commitIndex != 0 || len(n.log) != 1 {
		t.Errorf("reset %v Persist %v leaderID %d commit %d log %v, want nothing changed",
			out.ResetElectionTimer, out.Persist, n.leaderID, n.commitIndex, n.log)
	}
}

func TestCommitCappedAtLastNewEntry(t *testing.T) {
	n := followerWith(2, 1, Entry{1, "a"}, Entry{1, "b"}, Entry{1, "c"})
	out := step(t, n, appendEntries(1, 2, 1, 1, 1, []Entry{}, 3, "1-6"))
	if n.commitIndex != 1 {
		t.Errorf("commitIndex = %d, want 1 (not 3)", n.commitIndex)
	}
	wantEvents := []string{tracelog.EvAdvanceCommit, tracelog.EvRecvAppendEntries, tracelog.EvApplyEntry}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"old": 0, "new": 1}) {
		t.Errorf("advance_commit details = %v", d)
	}
	onlyAppendReply(t, out, 1, 1, true, 1, 1)
}

// A smaller leaderCommit (an old message) never moves commitIndex back.
func TestCommitNeverGoesBack(t *testing.T) {
	n := followerWith(2, 1, Entry{1, "a"}, Entry{1, "b"})
	step(t, n, appendEntries(1, 2, 1, 2, 1, nil, 2, "1-6"))
	out := step(t, n, appendEntries(1, 2, 1, 1, 1, nil, 2, "1-5"))
	if n.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2", n.commitIndex)
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvAppendEntries}) {
		t.Errorf("events = %v", got)
	}
}

func TestFollowerApplies(t *testing.T) {
	n := New(2, []int{1, 2, 3})
	out := step(t, n, appendEntries(1, 2, 1, 0, 0, []Entry{{1, "a"}, {1, "b"}}, 0, "1-3"))
	if len(out.Apply) != 0 || n.commitIndex != 0 {
		t.Fatalf("applied %v before commit", out.Apply)
	}
	out = step(t, n, appendEntries(1, 2, 1, 2, 1, []Entry{}, 2, "1-5"))
	want := []Applied{{Index: 1, Term: 1, Command: "a"}, {Index: 2, Term: 1, Command: "b"}}
	if !reflect.DeepEqual(out.Apply, want) {
		t.Errorf("Apply = %+v, want %+v", out.Apply, want)
	}
	got := recordEvents(out)
	if len(got) < 2 || got[len(got)-1] != tracelog.EvApplyEntry || got[len(got)-2] != tracelog.EvApplyEntry {
		t.Errorf("events = %v, want two apply_entry at the end", got)
	}
	last := out.Records[len(out.Records)-1]
	if !reflect.DeepEqual(last.Details, map[string]any{"index": 2, "entry_term": 1, "command": "b"}) {
		t.Errorf("last apply_entry details = %v", last.Details)
	}
	if n.State().LastApplied != 2 || n.State().CommitIndex != 2 {
		t.Errorf("state = %+v, want commit 2 applied 2", n.State())
	}
}

func TestC6Figure8(t *testing.T) {
	n := leaderWith(t, 3, Entry{2, "old"})
	step(t, n, appendReply(2, 1, 3, true, 1, 1, "2-2"))
	out := step(t, n, appendReply(3, 1, 3, true, 1, 1, "3-2"))
	if n.commitIndex != 0 || len(out.Apply) != 0 {
		t.Fatalf("commitIndex = %d after all peers match an old-term entry, want 0", n.commitIndex)
	}
	if !reflect.DeepEqual(n.State().MatchIndex, map[string]int{"2": 1, "3": 1}) {
		t.Errorf("matchIndex = %v", n.State().MatchIndex)
	}
	step(t, n, Propose{Command: "new"})
	out = step(t, n, appendReply(2, 1, 3, true, 2, 1, "2-3"))
	if n.commitIndex != 2 {
		t.Errorf("commitIndex = %d, want 2", n.commitIndex)
	}
	wantEvents := []string{tracelog.EvRecvAppendReply, tracelog.EvAdvanceCommit, tracelog.EvApplyEntry, tracelog.EvApplyEntry}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := detailsOf(t, out, tracelog.EvAdvanceCommit); !reflect.DeepEqual(d, map[string]any{"old": 0, "new": 2}) {
		t.Errorf("advance_commit details = %v", d)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"from": 2, "msg_id": "2-3", "success": true, "match_index": 2}) {
		t.Errorf("recv_append_reply details = %v", d)
	}
	want := []Applied{{Index: 1, Term: 2, Command: "old"}, {Index: 2, Term: 3, Command: "new"}}
	if !reflect.DeepEqual(out.Apply, want) {
		t.Errorf("Apply = %+v, want %+v", out.Apply, want)
	}
}

// In a 5-node cluster the leader needs two followers, plus itself.
func TestCommitNeedsMajorityOfFive(t *testing.T) {
	n := New(1, []int{1, 2, 3, 4, 5})
	stepAll(t, n, ElectionTimeout{}, voteReply(2, 1, 1, true, "2-1"), voteReply(3, 1, 1, true, "3-1"))
	step(t, n, Propose{Command: "a"})
	step(t, n, appendReply(4, 1, 1, true, 1, 0, "4-2"))
	if n.commitIndex != 0 {
		t.Fatalf("committed with 2 of 5")
	}
	step(t, n, appendReply(4, 1, 1, true, 1, 0, "4-3")) // a duplicate counts once
	if n.commitIndex != 0 {
		t.Fatalf("committed on a duplicate ack")
	}
	step(t, n, appendReply(5, 1, 1, true, 1, 0, "5-2"))
	if n.commitIndex != 1 {
		t.Errorf("commitIndex = %d with 3 of 5, want 1", n.commitIndex)
	}
}

func TestSuccessReplyUpdatesIndexes(t *testing.T) {
	n := leaderWith(t, 2, Entry{1, "a"}, Entry{1, "b"})
	step(t, n, Propose{Command: "c"})
	out := step(t, n, appendReply(3, 1, 2, true, 3, 0, "3-2"))
	if !reflect.DeepEqual(n.State().MatchIndex, map[string]int{"2": 0, "3": 3}) ||
		!reflect.DeepEqual(n.State().NextIndex, map[string]int{"2": 3, "3": 4}) {
		t.Errorf("match %v next %v", n.State().MatchIndex, n.State().NextIndex)
	}
	if n.commitIndex != 3 {
		t.Errorf("commitIndex = %d, want 3", n.commitIndex)
	}
	if len(out.Messages) != 0 {
		t.Errorf("success reply sent %v", out.Messages)
	}
}

func TestDuplicateFailureReplyDecrementsOnce(t *testing.T) {
	n := leaderWith(t, 2, Entry{1, "a"}, Entry{1, "b"})
	if n.nextIndex[2] != 3 {
		t.Fatalf("setup: nextIndex[2] = %d, want 3", n.nextIndex[2])
	}
	out := step(t, n, appendReply(2, 1, 2, false, 0, 2, "2-2"))
	if n.nextIndex[2] != 2 {
		t.Errorf("nextIndex[2] = %d after one failure, want 2", n.nextIndex[2])
	}
	wantEvents := []string{tracelog.EvRecvAppendReply, tracelog.EvSendAppendEntries}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"from": 2, "msg_id": "2-2", "success": false, "match_index": 0}) {
		t.Errorf("recv_append_reply details = %v", d)
	}
	if len(out.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 resend", len(out.Messages))
	}
	if m := out.Messages[0]; m.To != 2 || m.PrevLogIndex != 1 || m.PrevLogTerm != 1 || !reflect.DeepEqual(m.Entries, []Entry{{1, "b"}}) {
		t.Errorf("resend = %+v, want to 2 prev 1/1 entries [{1 b}]", m)
	}

	out = step(t, n, appendReply(2, 1, 2, false, 0, 2, "2-3"))
	if n.nextIndex[2] != 2 {
		t.Errorf("nextIndex[2] = %d after a duplicate failure, want still 2", n.nextIndex[2])
	}
	if len(out.Messages) != 0 {
		t.Errorf("duplicate failure sent %v", out.Messages)
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvAppendReply}) {
		t.Errorf("events = %v, want [recv_append_reply]", got)
	}
}

func TestNextIndexNeverBelowOne(t *testing.T) {
	n := leaderOfTerm(t, 1)
	out := step(t, n, appendReply(2, 1, 1, false, 0, 0, "2-2"))
	if n.nextIndex[2] != 1 {
		t.Errorf("nextIndex[2] = %d, want 1", n.nextIndex[2])
	}
	if len(out.Messages) != 0 {
		t.Errorf("sent %v", out.Messages)
	}
}

func TestStaleSuccessReplyNeverLowersMatch(t *testing.T) {
	n := leaderWith(t, 2, Entry{1, "a"}, Entry{1, "b"}, Entry{1, "c"})
	step(t, n, appendReply(2, 1, 2, true, 3, 3, "2-2"))
	if n.matchIndex[2] != 3 {
		t.Fatalf("setup: matchIndex[2] = %d, want 3", n.matchIndex[2])
	}
	out := step(t, n, appendReply(2, 1, 2, true, 1, 0, "2-1"))
	if n.matchIndex[2] != 3 || n.nextIndex[2] != 4 {
		t.Errorf("matchIndex[2] %d nextIndex[2] %d, want 3 and 4", n.matchIndex[2], n.nextIndex[2])
	}
	if d := detailsOf(t, out, tracelog.EvRecvAppendReply); d["match_index"] != 1 {
		t.Errorf("recv_append_reply details = %v, want the reply's match_index 1", d)
	}
}

// A reply from an older term, or to a node that is no longer leader, is
// recorded but changes nothing.
func TestOldTermAppendReplyIgnored(t *testing.T) {
	n := leaderWith(t, 3, Entry{1, "a"})
	before := n.State()
	out := step(t, n, appendReply(2, 1, 2, true, 1, 0, "2-2"))
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvAppendReply}) {
		t.Errorf("events = %v", got)
	}
	out = step(t, n, appendReply(2, 1, 2, false, 0, 1, "2-3"))
	if len(out.Messages) != 0 || !reflect.DeepEqual(n.State(), before) {
		t.Errorf("old-term reply changed state %+v or sent %v", n.State(), out.Messages)
	}

	f := followerWith(1, 2, Entry{1, "a"})
	before = f.State()
	out = step(t, f, appendReply(2, 1, 2, false, 0, 1, "2-1"))
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvAppendReply}) || len(out.Messages) != 0 {
		t.Errorf("follower events %v messages %v", got, out.Messages)
	}
	if !reflect.DeepEqual(f.State(), before) {
		t.Errorf("follower state changed")
	}
}

func TestCandidateStepsDownOnSameTermAppend(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	stepAll(t, n, ElectionTimeout{}, ElectionTimeout{})
	out := step(t, n, appendEntries(2, 1, 2, 0, 0, []Entry{}, 0, "2-5"))
	wantEvents := []string{tracelog.EvBecomeFollower, tracelog.EvRecvAppendEntries}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"old_role": "candidate"}) {
		t.Errorf("become_follower details = %v", d)
	}
	st := n.State()
	if st.Role != "follower" || st.LeaderID != 2 || st.CurrentTerm != 2 || st.VotedFor != 1 {
		t.Errorf("state = %+v, want follower term 2 leader 2 still voted 1", st)
	}
	if n.votesGranted != nil {
		t.Error("candidate votes survived stepping down")
	}
	if !out.ResetElectionTimer || out.Persist != nil {
		t.Errorf("reset %v Persist %v, want true and nil", out.ResetElectionTimer, out.Persist)
	}
	onlyAppendReply(t, out, 2, 2, true, 0, 0)
}

func TestHigherTermAppendOnLeader(t *testing.T) {
	n := leaderOfTerm(t, 1)
	out := step(t, n, appendEntries(2, 1, 4, 0, 0, []Entry{{4, "z"}}, 0, "2-9"))
	wantEvents := []string{
		tracelog.EvUpdateTerm, tracelog.EvBecomeFollower, tracelog.EvAppendEntry,
		tracelog.EvRecvAppendEntries, tracelog.EvPersist,
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if n.role != Follower || n.leaderID != 2 || n.currentTerm != 4 || n.votedFor != 0 {
		t.Errorf("role %v leader %d term %d vote %d", n.role, n.leaderID, n.currentTerm, n.votedFor)
	}
	onlyAppendReply(t, out, 2, 4, true, 1, 0)
}

func TestC4HigherTermStepsDown_Append(t *testing.T) {
	n := leaderOfTerm(t, 2)
	out := step(t, n, appendReply(3, 1, 5, false, 0, 0, "3-4"))
	wantEvents := []string{tracelog.EvUpdateTerm, tracelog.EvBecomeFollower, tracelog.EvRecvAppendReply, tracelog.EvPersist}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	st := n.State()
	if st.Role != "follower" || st.CurrentTerm != 5 || st.VotedFor != 0 || st.LeaderID != 0 {
		t.Errorf("state = %+v, want follower term 5 voted 0 leader 0", st)
	}
	if n.nextIndex != nil || n.matchIndex != nil || len(out.Messages) != 0 {
		t.Errorf("leader maps %v %v messages %v", n.nextIndex, n.matchIndex, out.Messages)
	}
	if out.Persist == nil || out.Persist.CurrentTerm != 5 || out.Persist.VotedFor != 0 {
		t.Errorf("Persist = %+v", out.Persist)
	}
}

func TestC8PersistWheneverDurableChanged(t *testing.T) {
	type tc struct {
		name  string
		setup func(t *testing.T) *Node
		ev    Event
		// changed says whether this case really changes durable state, so
		// that each row tests what its name says.
		changed bool
	}
	fresh := func(t *testing.T) *Node { return New(1, []int{1, 2, 3}) }
	candidate := func(t *testing.T) *Node {
		n := New(1, []int{1, 2, 3})
		step(t, n, ElectionTimeout{})
		return n
	}
	leader1 := func(t *testing.T) *Node { return leaderOfTerm(t, 1) }
	leaderLog := func(t *testing.T) *Node { return leaderWith(t, 2, Entry{1, "a"}, Entry{1, "b"}) }
	follower3 := func(t *testing.T) *Node { return followerWith(1, 1, Entry{1, "a"}, Entry{1, "b"}, Entry{1, "c"}) }
	voted := func(t *testing.T) *Node {
		n := New(1, []int{1, 2, 3})
		step(t, n, requestVote(2, 1, 1, 0, 0, "2-1"))
		return n
	}
	cases := []tc{
		// Election handlers.
		{"election timeout on follower", fresh, ElectionTimeout{}, true},
		{"election timeout on candidate", candidate, ElectionTimeout{}, true},
		{"election timeout on leader", leader1, ElectionTimeout{}, false},
		{"request vote granted", fresh, requestVote(2, 1, 1, 0, 0, "2-1"), true},
		{"request vote granted again", voted, requestVote(2, 1, 1, 0, 0, "2-2"), false},
		{"request vote already voted", voted, requestVote(3, 1, 1, 0, 0, "3-1"), false},
		{"request vote stale", voted, requestVote(3, 1, 0, 0, 0, "3-1"), false},
		{"request vote higher term, log behind", follower3, requestVote(2, 1, 5, 0, 0, "2-1"), true},
		{"vote reply wins", candidate, voteReply(2, 1, 1, true, "2-1"), false},
		{"vote reply refused", candidate, voteReply(2, 1, 1, false, "2-1"), false},
		{"vote reply higher term", leader1, voteReply(3, 1, 4, false, "3-1"), true},
		// Replication handlers.
		{"heartbeat on leader", leaderLog, HeartbeatTimeout{}, false},
		{"heartbeat on follower", follower3, HeartbeatTimeout{}, false},
		{"propose on leader", leader1, Propose{Command: "x"}, true},
		{"propose on follower", follower3, Propose{Command: "x"}, false},
		{"append entries new", fresh, appendEntries(2, 1, 1, 0, 0, []Entry{{1, "a"}}, 0, "2-1"), true},
		{"append entries matching prefix", follower3, appendEntries(2, 1, 1, 1, 1, []Entry{{1, "b"}}, 0, "2-1"), false},
		{"append entries conflict", follower3, appendEntries(2, 1, 2, 1, 1, []Entry{{2, "x"}}, 0, "2-1"), true},
		{"append entries commit only", follower3, appendEntries(2, 1, 1, 3, 1, nil, 3, "2-1"), false},
		{"append entries mismatch", follower3, appendEntries(2, 1, 1, 9, 1, nil, 0, "2-1"), false},
		{"append entries stale", follower3, appendEntries(2, 1, 0, 0, 0, nil, 0, "2-1"), false},
		{"append entries candidate same term", candidate, appendEntries(2, 1, 1, 0, 0, nil, 0, "2-1"), false},
		{"append entries higher term on leader", leader1, appendEntries(2, 1, 3, 0, 0, []Entry{{3, "z"}}, 0, "2-1"), true},
		{"append reply success", leaderLog, appendReply(2, 1, 2, true, 2, 0, "2-2"), false},
		{"append reply failure", leaderLog, appendReply(2, 1, 2, false, 0, 2, "2-2"), false},
		{"append reply stale", leaderLog, appendReply(2, 1, 1, true, 2, 0, "2-2"), false},
		{"append reply higher term", leaderLog, appendReply(2, 1, 7, false, 0, 2, "2-2"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := c.setup(t)
			before := persistentOf(n)
			out := step(t, n, c.ev)
			after := persistentOf(n)
			changed := !reflect.DeepEqual(before, after)
			if changed != c.changed {
				t.Fatalf("durable state changed = %v, want %v (%+v -> %+v)", changed, c.changed, before, after)
			}
			if (out.Persist != nil) != changed {
				t.Fatalf("Persist = %+v but durable state changed = %v (%+v -> %+v)", out.Persist, changed, before, after)
			}
			persistAt := -1
			for i, r := range out.Records {
				if r.Event == tracelog.EvPersist {
					if persistAt >= 0 {
						t.Fatalf("two persist records")
					}
					persistAt = i
				}
			}
			if (persistAt >= 0) != changed {
				t.Fatalf("persist record at %d, changed = %v", persistAt, changed)
			}
			if !changed {
				return
			}
			if !reflect.DeepEqual(*out.Persist, after) {
				t.Errorf("Persist = %+v, want %+v", *out.Persist, after)
			}
			for i, r := range out.Records {
				if (r.Event == tracelog.EvSendAppendEntries || r.Event == tracelog.EvSendRequestVote) && i < persistAt {
					t.Errorf("%s at %d comes before persist at %d", r.Event, i, persistAt)
				}
			}
		})
	}
}

// A 3-node cluster with every message delivered in FIFO order ends with every log the same and everything applied.
func TestClusterReplicatesAndAppliesInOrder(t *testing.T) {
	run := func() ([]byte, map[int][]Applied, []*Node) {
		nodes := []*Node{New(1, []int{1, 2, 3}), New(2, []int{1, 2, 3}), New(3, []int{1, 2, 3})}
		applied := map[int][]Applied{}
		var all []tracelog.Record
		var queue []Message
		do := func(i int, ev Event) {
			out := step(t, nodes[i], ev)
			all = append(all, out.Records...)
			queue = append(queue, out.Messages...)
			applied[i+1] = append(applied[i+1], out.Apply...)
		}
		drain := func() {
			for len(queue) > 0 {
				m := queue[0]
				queue = queue[1:]
				do(m.To-1, Receive{Msg: m})
			}
		}
		do(0, ElectionTimeout{})
		drain()
		for _, c := range []string{"set x 1", "set y 2", "del x"} {
			do(0, Propose{Command: c})
		}
		drain()
		do(0, HeartbeatTimeout{}) // tells followers the final commitIndex
		drain()
		return tracelog.AppendJSONL(nil, all), applied, nodes
	}
	x, applied, nodes := run()
	want := []Applied{{1, 1, "set x 1"}, {2, 1, "set y 2"}, {3, 1, "del x"}}
	for id := 1; id <= 3; id++ {
		if !reflect.DeepEqual(applied[id], want) {
			t.Errorf("node %d applied %+v, want %+v", id, applied[id], want)
		}
		if st := nodes[id-1].State(); st.CommitIndex != 3 || st.LastApplied != 3 || len(st.Log) != 3 {
			t.Errorf("node %d state %+v", id, st)
		}
	}
	y, _, _ := run()
	if string(x) != string(y) {
		t.Error("two identical runs gave different JSONL")
	}
	if err := schemacheck.ValidateJSONL(x); err != nil {
		t.Errorf("schema: %v", err)
	}
}
