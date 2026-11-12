package core

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/jmanoj0905/raftcheck-agent/raft/internal/schemacheck"
	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// step runs one event and checks that every record it produced matches the
// JSON Schema, so every election test also proves its records are valid.
func step(t *testing.T, n *Node, ev Event) Output {
	t.Helper()
	out := n.Step(ev)
	if err := schemacheck.ValidateJSONL(tracelog.AppendJSONL(nil, out.Records)); err != nil {
		t.Fatalf("Step(%T) records fail the schema: %v", ev, err)
	}
	return out
}

// stepAll runs the events in order and returns the last Output.
func stepAll(t *testing.T, n *Node, evs ...Event) Output {
	t.Helper()
	var out Output
	for _, ev := range evs {
		out = step(t, n, ev)
	}
	return out
}

// Message builders. msgID must look like "<from>-<n>" for the schema.

func requestVote(from, to, term, lastLogIndex, lastLogTerm int, msgID string) Event {
	return Receive{Msg: Message{
		Type: MsgRequestVote, From: from, To: to, MsgID: msgID, Term: term,
		LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm,
	}}
}

func voteReply(from, to, term int, granted bool, msgID string) Event {
	return Receive{Msg: Message{
		Type: MsgRequestVoteReply, From: from, To: to, MsgID: msgID, Term: term, VoteGranted: granted,
	}}
}

// detailsOf returns the details of the only record with the given event.
func detailsOf(t *testing.T, out Output, event string) map[string]any {
	t.Helper()
	var found map[string]any
	count := 0
	for _, r := range out.Records {
		if r.Event == event {
			found = r.Details
			count++
		}
	}
	if count != 1 {
		t.Fatalf("found %d %s records, want 1 (events %v)", count, event, recordEvents(out))
	}
	return found
}

// onlyReply returns the single message in out and checks it is a vote reply.
func onlyReply(t *testing.T, out Output, to, term int, granted bool) Message {
	t.Helper()
	if len(out.Messages) != 1 {
		t.Fatalf("got %d messages, want 1 reply", len(out.Messages))
	}
	m := out.Messages[0]
	if m.Type != MsgRequestVoteReply || m.To != to || m.Term != term || m.VoteGranted != granted {
		t.Fatalf("reply = %+v, want RequestVoteReply to %d term %d granted %v", m, to, term, granted)
	}
	return m
}

// leaderOfTerm makes node 1 of a 3-node cluster leader in the given term
// (term >= 1) by timing out term times and getting a vote from node 2.
func leaderOfTerm(t *testing.T, term int) *Node {
	t.Helper()
	n := New(1, []int{1, 2, 3})
	for i := 0; i < term; i++ {
		step(t, n, ElectionTimeout{})
	}
	step(t, n, voteReply(2, 1, term, true, "2-1"))
	if n.role != Leader || n.currentTerm != term {
		t.Fatalf("setup: role %v term %d, want leader in term %d", n.role, n.currentTerm, term)
	}
	return n
}

func TestC1ElectionTimeout(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	out := step(t, n, ElectionTimeout{})

	st := n.State()
	if st.Role != "candidate" || st.CurrentTerm != 1 || st.VotedFor != 1 || st.LeaderID != 0 {
		t.Errorf("state = %+v, want candidate term 1 voted 1 leader 0", st)
	}
	want := PersistentState{CurrentTerm: 1, VotedFor: 1, Log: []Entry{}}
	if out.Persist == nil || !reflect.DeepEqual(*out.Persist, want) {
		t.Errorf("Persist = %+v, want %+v", out.Persist, want)
	}
	if len(out.Messages) != 2 {
		t.Fatalf("got %d messages, want 2", len(out.Messages))
	}
	for i, to := range []int{2, 3} {
		m := out.Messages[i]
		wantMsg := Message{Type: MsgRequestVote, From: 1, To: to, MsgID: fmt.Sprintf("1-%d", i+1), Term: 1}
		if !reflect.DeepEqual(m, wantMsg) {
			t.Errorf("message %d = %+v, want %+v", i, m, wantMsg)
		}
	}
	wantEvents := []string{tracelog.EvBecomeCandidate, tracelog.EvPersist, tracelog.EvSendRequestVote, tracelog.EvSendRequestVote}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"old_role": "follower"}) {
		t.Errorf("become_candidate details = %v", d)
	}
	if out.ResetElectionTimer {
		t.Error("ElectionTimeout set ResetElectionTimer; the runner restarts the timer itself")
	}
}

func TestC1RequestVoteCarriesLastLog(t *testing.T) {
	n := New(2, []int{1, 2, 3})
	n.appendEntry(Entry{Term: 1, Command: "a"})
	n.appendEntry(Entry{Term: 3, Command: "b"})
	n.setTerm(3)
	n.finish()
	out := step(t, n, ElectionTimeout{})
	for i, to := range []int{1, 3} {
		m := out.Messages[i]
		if m.To != to || m.Term != 4 || m.LastLogIndex != 2 || m.LastLogTerm != 3 {
			t.Errorf("message %d = %+v, want to %d term 4 last 2/3", i, m, to)
		}
	}
}

func TestCandidateTimesOutAgain(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	step(t, n, ElectionTimeout{})
	out := step(t, n, ElectionTimeout{})
	wantEvents := []string{tracelog.EvBecomeCandidate, tracelog.EvPersist, tracelog.EvSendRequestVote, tracelog.EvSendRequestVote}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"old_role": "candidate"}) {
		t.Errorf("become_candidate details = %v", d)
	}
	if n.currentTerm != 2 || n.votedFor != 1 || n.role != Candidate {
		t.Errorf("term %d votedFor %d role %v, want 2/1/candidate", n.currentTerm, n.votedFor, n.role)
	}
	// Votes from the old term were cleared: a term-1 grant does not count.
	step(t, n, voteReply(2, 1, 1, true, "2-1"))
	if n.role != Candidate {
		t.Errorf("old-term vote made the node %v", n.role)
	}
	// A vote in the new term does.
	out = step(t, n, voteReply(3, 1, 2, true, "3-1"))
	if n.role != Leader {
		t.Fatalf("role = %v, want leader", n.role)
	}
	if d := detailsOf(t, out, tracelog.EvBecomeLeader); !reflect.DeepEqual(d, map[string]any{"votes": []int{1, 3}}) {
		t.Errorf("become_leader details = %v", d)
	}
}

func TestSingleNodeBecomesLeaderImmediately(t *testing.T) {
	n := New(1, []int{1})
	out := step(t, n, ElectionTimeout{})
	wantEvents := []string{tracelog.EvBecomeCandidate, tracelog.EvBecomeLeader, tracelog.EvPersist}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if st := n.State(); st.Role != "leader" || st.LeaderID != 1 || st.CurrentTerm != 1 {
		t.Errorf("state = %+v, want leader 1 in term 1", st)
	}
	if d := detailsOf(t, out, tracelog.EvBecomeLeader); !reflect.DeepEqual(d, map[string]any{"votes": []int{1}}) {
		t.Errorf("become_leader details = %v", d)
	}
	if len(out.Messages) != 0 {
		t.Errorf("single node sent %v", out.Messages)
	}
}

func TestC2VoteOncePerTerm(t *testing.T) {
	n := New(1, []int{1, 2, 3})

	out := step(t, n, requestVote(2, 1, 1, 0, 0, "2-1"))
	onlyReply(t, out, 2, 1, true)
	if !out.ResetElectionTimer {
		t.Error("granted vote did not set ResetElectionTimer")
	}
	if n.votedFor != 2 || out.Persist == nil {
		t.Errorf("votedFor %d Persist %v, want 2 and set", n.votedFor, out.Persist)
	}
	wantEvents := []string{tracelog.EvUpdateTerm, tracelog.EvRecvRequestVote, tracelog.EvPersist}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := detailsOf(t, out, tracelog.EvUpdateTerm); !reflect.DeepEqual(d, map[string]any{"old_term": 0, "from": 2}) {
		t.Errorf("update_term details = %v", d)
	}
	wantRV := map[string]any{"from": 2, "msg_id": "2-1", "granted": true, "reason": "ok"}
	if d := detailsOf(t, out, tracelog.EvRecvRequestVote); !reflect.DeepEqual(d, wantRV) {
		t.Errorf("recv_request_vote details = %v, want %v", d, wantRV)
	}

	out = step(t, n, requestVote(3, 1, 1, 0, 0, "3-1"))
	onlyReply(t, out, 3, 1, false)
	if out.ResetElectionTimer || out.Persist != nil {
		t.Errorf("refused vote: reset %v Persist %v, want false and nil", out.ResetElectionTimer, out.Persist)
	}
	wantRV = map[string]any{"from": 3, "msg_id": "3-1", "granted": false, "reason": "already_voted"}
	if d := detailsOf(t, out, tracelog.EvRecvRequestVote); !reflect.DeepEqual(d, wantRV) {
		t.Errorf("recv_request_vote details = %v, want %v", d, wantRV)
	}
	if n.votedFor != 2 {
		t.Errorf("votedFor = %d, want 2", n.votedFor)
	}

	// A repeated request from the same candidate is granted again, and since
	// nothing durable changed there is nothing to persist.
	out = step(t, n, requestVote(2, 1, 1, 0, 0, "2-2"))
	onlyReply(t, out, 2, 1, true)
	if !out.ResetElectionTimer || out.Persist != nil {
		t.Errorf("repeat grant: reset %v Persist %v, want true and nil", out.ResetElectionTimer, out.Persist)
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvRequestVote}) {
		t.Errorf("events = %v, want [recv_request_vote]", got)
	}
}

func TestC3LogUpToDate(t *testing.T) {
	newVoter := func() *Node {
		n := New(1, []int{1, 2, 3})
		n.appendEntry(Entry{Term: 1, Command: "a"})
		n.appendEntry(Entry{Term: 2, Command: "b"})
		n.setTerm(2)
		n.finish()
		return n
	}
	cases := []struct {
		name                string
		lastIndex, lastTerm int
		granted             bool
		reason              string
	}{
		{"last term lower", 5, 1, false, "log_not_up_to_date"},
		{"same term, shorter", 1, 2, false, "log_not_up_to_date"},
		{"same term, same length", 2, 2, true, "ok"},
		{"same term, longer", 3, 2, true, "ok"},
		{"last term higher, shorter", 1, 3, true, "ok"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			n := newVoter()
			out := step(t, n, requestVote(2, 1, 3, c.lastIndex, c.lastTerm, "2-1"))
			onlyReply(t, out, 2, 3, c.granted)
			d := detailsOf(t, out, tracelog.EvRecvRequestVote)
			if d["reason"] != c.reason || d["granted"] != c.granted {
				t.Errorf("details = %v, want granted %v reason %s", d, c.granted, c.reason)
			}
			if out.ResetElectionTimer != c.granted {
				t.Errorf("ResetElectionTimer = %v, want %v", out.ResetElectionTimer, c.granted)
			}
			wantVote := 0
			if c.granted {
				wantVote = 2
			}
			if n.votedFor != wantVote {
				t.Errorf("votedFor = %d, want %d", n.votedFor, wantVote)
			}
		})
	}
}

func TestStaleRequestVoteRejected(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	n.setTerm(3)
	n.finish()
	out := step(t, n, requestVote(2, 1, 2, 9, 9, "2-1"))
	onlyReply(t, out, 2, 3, false)
	if out.Persist != nil || out.ResetElectionTimer {
		t.Errorf("Persist %v reset %v, want nil and false", out.Persist, out.ResetElectionTimer)
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvRequestVote}) {
		t.Errorf("events = %v, want [recv_request_vote]", got)
	}
	want := map[string]any{"from": 2, "msg_id": "2-1", "granted": false, "reason": "stale_term"}
	if d := detailsOf(t, out, tracelog.EvRecvRequestVote); !reflect.DeepEqual(d, want) {
		t.Errorf("details = %v, want %v", d, want)
	}
}

// The stale-term reason wins over already_voted: it is checked first.
func TestStaleTermCheckedBeforeVote(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	n.setTerm(3)
	n.setVote(3)
	n.finish()
	out := step(t, n, requestVote(2, 1, 2, 0, 0, "2-1"))
	if d := detailsOf(t, out, tracelog.EvRecvRequestVote); d["reason"] != "stale_term" {
		t.Errorf("reason = %v, want stale_term", d["reason"])
	}
}

func TestC4HigherTermStepsDown_Vote(t *testing.T) {
	n := leaderOfTerm(t, 2)
	out := step(t, n, voteReply(3, 1, 5, false, "3-4"))

	got := recordEvents(out)
	wantPrefix := []string{tracelog.EvUpdateTerm, tracelog.EvBecomeFollower, tracelog.EvRecvVoteReply}
	if len(got) < 3 || !reflect.DeepEqual(got[:3], wantPrefix) {
		t.Fatalf("events = %v, want prefix %v", got, wantPrefix)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"old_term": 2, "from": 3}) {
		t.Errorf("update_term details = %v", d)
	}
	if d := out.Records[1].Details; !reflect.DeepEqual(d, map[string]any{"old_role": "leader"}) {
		t.Errorf("become_follower details = %v", d)
	}
	if d := out.Records[2].Details; !reflect.DeepEqual(d, map[string]any{"from": 3, "msg_id": "3-4", "granted": false}) {
		t.Errorf("recv_vote_reply details = %v", d)
	}
	st := n.State()
	if st.CurrentTerm != 5 || st.VotedFor != 0 || st.Role != "follower" || st.LeaderID != 0 {
		t.Errorf("state = %+v, want follower term 5 voted 0 leader 0", st)
	}
	if st.NextIndex != nil || st.MatchIndex != nil || n.nextIndex != nil || n.matchIndex != nil {
		t.Error("leader maps survived stepping down")
	}
	if out.Persist == nil || out.Persist.CurrentTerm != 5 || out.Persist.VotedFor != 0 {
		t.Errorf("Persist = %+v, want term 5 voted 0", out.Persist)
	}
	if len(out.Messages) != 0 {
		t.Errorf("reply handling sent %v", out.Messages)
	}
}

func TestC4HigherTermStepsDown_RequestVote(t *testing.T) {
	// A candidate in term 1 (voted for itself) gets a term-3 RequestVote:
	// it steps down, clears its vote and can then vote for the sender.
	n := New(1, []int{1, 2, 3})
	step(t, n, ElectionTimeout{})
	out := step(t, n, requestVote(2, 1, 3, 0, 0, "2-1"))
	wantEvents := []string{tracelog.EvUpdateTerm, tracelog.EvBecomeFollower, tracelog.EvRecvRequestVote, tracelog.EvPersist}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[1].Details; !reflect.DeepEqual(d, map[string]any{"old_role": "candidate"}) {
		t.Errorf("become_follower details = %v", d)
	}
	// The update_term snapshot already shows the cleared vote.
	if st := out.Records[0].State; st.CurrentTerm != 3 || st.VotedFor != 0 {
		t.Errorf("update_term state term %d votedFor %d, want 3 and 0", st.CurrentTerm, st.VotedFor)
	}
	onlyReply(t, out, 2, 3, true)
	if n.role != Follower || n.currentTerm != 3 || n.votedFor != 2 {
		t.Errorf("role %v term %d votedFor %d, want follower/3/2", n.role, n.currentTerm, n.votedFor)
	}
	// A late vote for the old election does not count any more.
	step(t, n, voteReply(3, 1, 1, true, "3-1"))
	if n.role != Follower {
		t.Errorf("old vote made the node %v", n.role)
	}
}

func TestHigherTermOnFollowerHasNoBecomeFollower(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	n.leaderID = 2
	out := step(t, n, voteReply(3, 1, 4, false, "3-1"))
	wantEvents := []string{tracelog.EvUpdateTerm, tracelog.EvRecvVoteReply, tracelog.EvPersist}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Errorf("events = %v, want %v", got, wantEvents)
	}
	if n.leaderID != 0 || n.currentTerm != 4 {
		t.Errorf("leaderID %d term %d, want 0 and 4", n.leaderID, n.currentTerm)
	}
}

func TestWinsElectionWithMajority(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	n.appendEntry(Entry{Term: 1, Command: "a"})
	n.finish()
	step(t, n, ElectionTimeout{}) // term 1, msgs 1-1 and 1-2
	out := step(t, n, voteReply(2, 1, 1, true, "2-1"))

	wantEvents := []string{
		tracelog.EvRecvVoteReply, tracelog.EvBecomeLeader,
		tracelog.EvSendAppendEntries, tracelog.EvSendAppendEntries,
	}
	if got := recordEvents(out); !reflect.DeepEqual(got, wantEvents) {
		t.Fatalf("events = %v, want %v", got, wantEvents)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"from": 2, "msg_id": "2-1", "granted": true}) {
		t.Errorf("recv_vote_reply details = %v", d)
	}
	bl := out.Records[1]
	if !reflect.DeepEqual(bl.Details, map[string]any{"votes": []int{1, 2}}) {
		t.Errorf("become_leader details = %v", bl.Details)
	}
	if bl.State.Role != "leader" || bl.State.LeaderID != 1 {
		t.Errorf("become_leader state = %+v", bl.State)
	}
	if !reflect.DeepEqual(bl.State.NextIndex, map[string]int{"2": 2, "3": 2}) ||
		!reflect.DeepEqual(bl.State.MatchIndex, map[string]int{"2": 0, "3": 0}) {
		t.Errorf("leader maps = %v %v", bl.State.NextIndex, bl.State.MatchIndex)
	}
	if out.Persist != nil {
		t.Errorf("becoming leader persisted %+v", out.Persist)
	}
	if len(out.Messages) != 2 {
		t.Fatalf("got %d messages, want 2", len(out.Messages))
	}
	for i, to := range []int{2, 3} {
		m := out.Messages[i]
		want := Message{
			Type: MsgAppendEntries, From: 1, To: to, MsgID: fmt.Sprintf("1-%d", i+3), Term: 1,
			PrevLogIndex: 1, PrevLogTerm: 1, Entries: []Entry{}, LeaderCommit: 0,
		}
		if !reflect.DeepEqual(m, want) {
			t.Errorf("message %d = %+v, want %+v", i, m, want)
		}
	}

	// More votes after winning are recorded but change nothing.
	out = step(t, n, voteReply(3, 1, 1, true, "3-1"))
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvVoteReply}) {
		t.Errorf("late vote events = %v", got)
	}
}

func TestWinsElectionEmptyLog(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	step(t, n, ElectionTimeout{})
	out := step(t, n, voteReply(2, 1, 1, true, "2-1"))
	st := n.State()
	if !reflect.DeepEqual(st.NextIndex, map[string]int{"2": 1, "3": 1}) ||
		!reflect.DeepEqual(st.MatchIndex, map[string]int{"2": 0, "3": 0}) {
		t.Errorf("leader maps = %v %v", st.NextIndex, st.MatchIndex)
	}
	if d := detailsOf(t, out, tracelog.EvBecomeLeader); !reflect.DeepEqual(d, map[string]any{"votes": []int{1, 2}}) {
		t.Errorf("become_leader details = %v", d)
	}
	if len(out.Messages) != 2 || out.Messages[0].To != 2 || out.Messages[1].To != 3 ||
		out.Messages[0].Type != MsgAppendEntries || out.Messages[0].PrevLogIndex != 0 {
		t.Errorf("messages = %+v, want heartbeats to 2 and 3", out.Messages)
	}
}

func TestFiveNodesNeedThreeVotes(t *testing.T) {
	n := New(1, []int{1, 2, 3, 4, 5})
	stepAll(t, n,
		ElectionTimeout{},
		voteReply(4, 1, 1, true, "4-1"),
		voteReply(4, 1, 1, true, "4-2"), // a duplicate counts once
		voteReply(2, 1, 1, false, "2-1"),
	)
	if n.role != Candidate {
		t.Fatalf("role = %v after 2 votes of 5, want candidate", n.role)
	}
	out := step(t, n, voteReply(5, 1, 1, true, "5-1"))
	if d := detailsOf(t, out, tracelog.EvBecomeLeader); !reflect.DeepEqual(d, map[string]any{"votes": []int{1, 4, 5}}) {
		t.Errorf("become_leader details = %v", d)
	}
}

func TestOldTermVoteReplyNotCounted(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	out := stepAll(t, n, ElectionTimeout{}, ElectionTimeout{}, voteReply(2, 1, 1, true, "2-1"))
	if got := recordEvents(out); !reflect.DeepEqual(got, []string{tracelog.EvRecvVoteReply}) {
		t.Errorf("events = %v, want [recv_vote_reply]", got)
	}
	if d := out.Records[0].Details; !reflect.DeepEqual(d, map[string]any{"from": 2, "msg_id": "2-1", "granted": true}) {
		t.Errorf("recv_vote_reply details = %v", d)
	}
	if n.role != Candidate || n.currentTerm != 2 {
		t.Errorf("role %v term %d, want candidate in term 2", n.role, n.currentTerm)
	}
}

func TestVoteReplyIgnoredWhenNotCandidate(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	n.setTerm(1)
	n.finish()
	step(t, n, voteReply(2, 1, 1, true, "2-1"))
	step(t, n, voteReply(3, 1, 1, true, "3-1"))
	if n.role != Follower {
		t.Errorf("follower became %v from unsolicited votes", n.role)
	}
}

func TestMisaddressedMessagePanics(t *testing.T) {
	n := New(1, []int{1, 2, 3})
	expectPanic := func(name string, ev Event, want string) {
		t.Helper()
		defer func() {
			r := recover()
			if r == nil {
				t.Errorf("%s: expected a panic", name)
				return
			}
			if r != want {
				t.Errorf("%s: panic %q, want %q", name, r, want)
			}
		}()
		n.Step(ev)
	}
	expectPanic("wrong To", requestVote(2, 2, 1, 0, 0, "2-1"), "core: node 1 got message 2-1 for node 2 from 2")
	expectPanic("unknown From", requestVote(9, 1, 1, 0, 0, "9-1"), "core: node 1 got message 9-1 for node 1 from 9")
	expectPanic("message to itself", appendEntries(1, 1, 1, 0, 0, nil, 0, "1-1"), "core: node 1 got message 1-1 for node 1 from 1")
	expectPanic("unknown type", Receive{Msg: Message{Type: 99, From: 2, To: 1, MsgID: "2-1"}},
		"core: node 1 got message 2-1 with unknown type 99")
}

func TestLeaderIgnoresElectionTimeout(t *testing.T) {
	n := leaderOfTerm(t, 1)
	before := n.State()
	out := step(t, n, ElectionTimeout{})
	if len(out.Records) != 0 || out.Persist != nil || len(out.Messages) != 0 || out.ResetElectionTimer {
		t.Errorf("leader timeout output = %+v, want empty", out)
	}
	if !reflect.DeepEqual(n.State(), before) {
		t.Errorf("leader state changed: %+v -> %+v", before, n.State())
	}
}

// A whole 3-node election, run twice, gives byte-identical JSONL.
func TestElectionJSONLIsDeterministic(t *testing.T) {
	run := func() []byte {
		a, b := New(1, []int{1, 2, 3}), New(2, []int{1, 2, 3})
		var all []tracelog.Record
		out := step(t, a, ElectionTimeout{})
		all = append(all, out.Records...)
		rv := out.Messages[0]
		out = step(t, b, Receive{Msg: rv})
		all = append(all, out.Records...)
		out = step(t, a, Receive{Msg: out.Messages[0]})
		all = append(all, out.Records...)
		return tracelog.AppendJSONL(nil, all)
	}
	x, y := run(), run()
	if string(x) != string(y) {
		t.Errorf("JSONL differs:\n%s\n%s", x, y)
	}
	if err := schemacheck.ValidateJSONL(x); err != nil {
		t.Errorf("schema: %v", err)
	}
}

// TestStepDownSnapshotsAreFollower pins the order inside the higher-term
// rule: the whole step-down happens before update_term is recorded, so the
// update_term and become_follower snapshots both show a follower in the new
// term. No record of the step may show a leader or candidate in a term the
// node never won or started.
func TestStepDownSnapshotsAreFollower(t *testing.T) {
	msgs := map[string]func(term int) Event{
		"RequestVote":        func(term int) Event { return requestVote(2, 1, term, 9, 9, "2-7") },
		"RequestVoteReply":   func(term int) Event { return voteReply(2, 1, term, false, "2-7") },
		"AppendEntries":      func(term int) Event { return appendEntries(2, 1, term, 0, 0, nil, 0, "2-7") },
		"AppendEntriesReply": func(term int) Event { return appendReply(2, 1, term, false, 0, 0, "2-7") },
	}
	setups := map[string]func(t *testing.T) *Node{
		"leader": func(t *testing.T) *Node { return leaderOfTerm(t, 1) },
		"candidate": func(t *testing.T) *Node {
			n := New(1, []int{1, 2, 3})
			step(t, n, ElectionTimeout{})
			return n
		},
	}
	for _, role := range []string{"leader", "candidate"} {
		for _, kind := range []string{"RequestVote", "RequestVoteReply", "AppendEntries", "AppendEntriesReply"} {
			t.Run(role+"/"+kind, func(t *testing.T) {
				n := setups[role](t)
				wonTerm := n.currentTerm // the term it became leader or candidate in
				out := step(t, n, msgs[kind](2))
				if got := recordEvents(out)[:2]; !reflect.DeepEqual(got, []string{tracelog.EvUpdateTerm, tracelog.EvBecomeFollower}) {
					t.Fatalf("first events = %v, want update_term, become_follower", got)
				}
				if d := out.Records[1].Details; !reflect.DeepEqual(d, map[string]any{"old_role": role}) {
					t.Errorf("become_follower details = %v", d)
				}
				for _, r := range out.Records[:2] {
					st := r.State
					if st.Role != "follower" || st.CurrentTerm != 2 || r.Term != 2 || st.VotedFor != 0 ||
						st.LeaderID != 0 || st.NextIndex != nil || st.MatchIndex != nil {
						t.Errorf("%s snapshot = %+v (term %d), want a follower in term 2 with no vote, leader or maps",
							r.Event, st, r.Term)
					}
				}
				for _, r := range out.Records {
					if r.State.Role != "follower" && r.State.CurrentTerm > wonTerm {
						t.Errorf("seq %d %s shows a %s in term %d, but the node only reached term %d as %s",
							r.Seq, r.Event, r.State.Role, r.State.CurrentTerm, wonTerm, role)
					}
				}
			})
		}
	}
}
