package core

import (
	"fmt"
	"slices"

	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// Leader election: spec §4.5 rules 1-5.

// receive checks that m is addressed to this node from a known peer, applies
// the higher-term rule, and then hands m to its handler.
func (n *Node) receive(m Message) {
	if _, known := slices.BinarySearch(n.peers, m.From); m.To != n.id || !known {
		panic(fmt.Sprintf("core: node %d got message %s for node %d from %d", n.id, m.MsgID, m.To, m.From))
	}
	n.observeTerm(m.Term, m.From)
	switch m.Type {
	case MsgRequestVote:
		n.handleRequestVote(m)
	case MsgRequestVoteReply:
		n.handleVoteReply(m)
	case MsgAppendEntries, MsgAppendEntriesReply:
		// Handled in Task 5.
	default:
		panic(fmt.Sprintf("core: node %d got message %s with unknown type %d", n.id, m.MsgID, int(m.Type)))
	}
}

// observeTerm is the higher-term rule (rule 1). If term > currentTerm it sets
// currentTerm = term, votedFor = 0 and leaderId = 0, and records update_term
// {old_term, from}; if the node was a candidate or leader it then becomes a
// follower and records become_follower {old_role}. It returns true if the
// term changed. A term <= currentTerm changes nothing.
func (n *Node) observeTerm(term, from int) bool {
	if term <= n.currentTerm {
		return false
	}
	old := n.currentTerm
	n.setTerm(term)
	n.setVote(0)
	n.leaderID = 0
	n.record(tracelog.EvUpdateTerm, map[string]any{"old_term": old, "from": from})
	if n.role != Follower {
		oldRole := n.role
		n.becomeFollower()
		n.record(tracelog.EvBecomeFollower, map[string]any{"old_role": oldRole.String()})
	}
	return true
}

// becomeFollower sets the role to follower and drops candidate and leader
// state. It writes no record and does not touch term, vote or leaderId.
func (n *Node) becomeFollower() {
	n.role = Follower
	n.votesGranted = nil
	n.nextIndex = nil
	n.matchIndex = nil
}

// electionTimeout starts a new election (rule 3). A leader ignores it.
func (n *Node) electionTimeout() {
	if n.role == Leader {
		return
	}
	oldRole := n.role
	n.setTerm(n.currentTerm + 1)
	n.setVote(n.id)
	n.role = Candidate
	n.leaderID = 0
	n.votesGranted = map[int]bool{n.id: true}
	n.record(tracelog.EvBecomeCandidate, map[string]any{"old_role": oldRole.String()})
	for _, p := range n.peers {
		if p == n.id {
			continue
		}
		n.send(Message{
			Type: MsgRequestVote, To: p, Term: n.currentTerm,
			LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(),
		})
	}
	n.maybeWin()
}

// handleRequestVote decides a vote (rules 2 and 4) after the higher-term
// rule. It always records recv_request_vote and always replies.
func (n *Node) handleRequestVote(m Message) {
	var reason string
	switch {
	case m.Term < n.currentTerm:
		reason = "stale_term"
	case n.votedFor != 0 && n.votedFor != m.From:
		reason = "already_voted"
	case !n.candidateLogUpToDate(m.LastLogTerm, m.LastLogIndex):
		reason = "log_not_up_to_date"
	default:
		reason = "ok"
	}
	granted := reason == "ok"
	if granted {
		n.setVote(m.From)
		n.resetTimer = true
	}
	n.record(tracelog.EvRecvRequestVote, map[string]any{
		"from": m.From, "msg_id": m.MsgID, "granted": granted, "reason": reason,
	})
	n.send(Message{Type: MsgRequestVoteReply, To: m.From, Term: n.currentTerm, VoteGranted: granted})
}

// candidateLogUpToDate reports whether a log ending at (lastTerm, lastIndex)
// is at least as up-to-date as ours: compare last terms, then last indexes.
func (n *Node) candidateLogUpToDate(lastTerm, lastIndex int) bool {
	if lastTerm != n.lastTerm() {
		return lastTerm > n.lastTerm()
	}
	return lastIndex >= n.lastIndex()
}

// handleVoteReply records every reply (after the higher-term rule) but
// counts it only for a candidate, in the current term, when granted.
func (n *Node) handleVoteReply(m Message) {
	n.record(tracelog.EvRecvVoteReply, map[string]any{
		"from": m.From, "msg_id": m.MsgID, "granted": m.VoteGranted,
	})
	if n.role != Candidate || m.Term != n.currentTerm || !m.VoteGranted {
		return
	}
	n.votesGranted[m.From] = true
	n.maybeWin()
}

// maybeWin makes a candidate leader once it holds a majority of votes.
func (n *Node) maybeWin() {
	if n.role == Candidate && len(n.votesGranted) > len(n.peers)/2 {
		n.becomeLeader()
	}
}

// becomeLeader (rule 5) sets role = leader, leaderId = id, and for every
// other peer nextIndex = lastIndex+1 and matchIndex = 0; records
// become_leader {votes: sorted ids}; then queues AppendEntries to each other
// peer in sorted order, built by appendEntriesFor.
func (n *Node) becomeLeader() {
	votes := []int{}
	for _, p := range n.peers {
		if n.votesGranted[p] {
			votes = append(votes, p)
		}
	}
	n.role = Leader
	n.leaderID = n.id
	n.votesGranted = nil
	n.nextIndex = map[int]int{}
	n.matchIndex = map[int]int{}
	for _, p := range n.peers {
		if p != n.id {
			n.nextIndex[p] = n.lastIndex() + 1
			n.matchIndex[p] = 0
		}
	}
	n.record(tracelog.EvBecomeLeader, map[string]any{"votes": votes})
	for _, p := range n.peers {
		if p != n.id {
			n.send(n.appendEntriesFor(p))
		}
	}
}

// appendEntriesFor builds (but does not queue) the AppendEntries for peer p
// from nextIndex[p]: prevLogIndex = nextIndex[p]-1, its term, the entries
// from nextIndex[p] on (a copy), and leaderCommit = commitIndex. The caller
// passes it to send. It panics if nextIndex[p] is outside 1..lastIndex+1.
func (n *Node) appendEntriesFor(p int) Message {
	next := n.nextIndex[p]
	return Message{
		Type: MsgAppendEntries, To: p, Term: n.currentTerm,
		PrevLogIndex: next - 1, PrevLogTerm: n.termAt(next - 1),
		Entries: n.entriesFrom(next), LeaderCommit: n.commitIndex,
	}
}
