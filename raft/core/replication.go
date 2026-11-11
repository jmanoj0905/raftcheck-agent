package core

import "github.com/jmanoj0905/raftcheck-agent/raft/tracelog"

// Log replication, commit and apply: spec §4.5 rules 6-11. Applying itself
// (rule 11) happens in finish, which hands out every entry up to commitIndex.

// heartbeatTimeout sends AppendEntries to every other peer from its
// nextIndex, so a heartbeat also repairs dropped entries. A non-leader does
// nothing.
func (n *Node) heartbeatTimeout() {
	if n.role != Leader {
		return
	}
	n.broadcastAppendEntries()
}

// broadcastAppendEntries queues appendEntriesFor(p) to every other peer in
// sorted peer order.
func (n *Node) broadcastAppendEntries() {
	for _, p := range n.peers {
		if p != n.id {
			n.send(n.appendEntriesFor(p))
		}
	}
}

// propose (rule 10) appends {currentTerm, command} on a leader, records
// append_entry, tries to commit (a 1-node cluster commits at once) and sends
// AppendEntries to every other peer. A non-leader records propose_rejected
// and changes nothing else. Either way it sets the step's ProposeResult.
func (n *Node) propose(command string) {
	if n.role != Leader {
		n.record(tracelog.EvProposeRejected, map[string]any{"command": command})
		n.proposed = &ProposeResult{Index: 0, Term: n.currentTerm, IsLeader: false}
		return
	}
	n.appendEntry(Entry{Term: n.currentTerm, Command: command})
	n.record(tracelog.EvAppendEntry, map[string]any{
		"index": n.lastIndex(), "entry_term": n.currentTerm, "command": command,
	})
	n.advanceCommit()
	n.broadcastAppendEntries()
	n.proposed = &ProposeResult{Index: n.lastIndex(), Term: n.currentTerm, IsLeader: true}
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

// handleAppendEntries is rule 7, run after the higher-term rule. It always
// records recv_append_entries (after any log and commit changes) and always
// replies with ReqPrevLogIndex = m.PrevLogIndex.
func (n *Node) handleAppendEntries(m Message) {
	if m.Term < n.currentTerm {
		n.replyAppend(m, "stale_term", 0)
		return
	}

	// m comes from the leader of our current term. A candidate (or, only if
	// election safety is already broken, a leader) steps down to follower.
	if n.role != Follower {
		oldRole := n.role
		n.becomeFollower()
		n.record(tracelog.EvBecomeFollower, map[string]any{"old_role": oldRole.String()})
	}
	n.leaderID = m.From
	n.resetTimer = true

	if m.PrevLogIndex > n.lastIndex() || n.termAt(m.PrevLogIndex) != m.PrevLogTerm {
		n.replyAppend(m, "log_mismatch", 0)
		return
	}

	// Skip entries we already have; at the first conflict, cut the log
	// there and append the rest. Matching entries are never removed, so a
	// late, shorter AppendEntries cannot undo a newer one.
	for i, e := range m.Entries {
		idx := m.PrevLogIndex + 1 + i
		if idx <= n.lastIndex() {
			if n.termAt(idx) == e.Term {
				continue
			}
			removed := n.lastIndex() - idx + 1
			n.truncateFrom(idx)
			n.record(tracelog.EvTruncateLog, map[string]any{"from_index": idx, "removed": removed})
		}
		n.appendEntry(e)
		n.record(tracelog.EvAppendEntry, map[string]any{
			"index": idx, "entry_term": e.Term, "command": e.Command,
		})
	}

	// Only the prefix this message proved matches the leader's log may be
	// committed, never our own entries past it.
	matched := m.PrevLogIndex + len(m.Entries)
	if m.LeaderCommit > n.commitIndex {
		if c := min(m.LeaderCommit, matched); c > n.commitIndex {
			n.setCommit(c)
		}
	}
	n.replyAppend(m, "ok", matched)
}

// replyAppend records recv_append_entries with the given reason and queues
// the reply. success is reason == "ok"; matchIndex is sent only on success.
func (n *Node) replyAppend(m Message, reason string, matchIndex int) {
	success := reason == "ok"
	n.record(tracelog.EvRecvAppendEntries, map[string]any{
		"from": m.From, "msg_id": m.MsgID, "success": success, "reason": reason,
	})
	n.send(Message{
		Type: MsgAppendEntriesReply, To: m.From, Term: n.currentTerm,
		Success: success, MatchIndex: matchIndex, ReqPrevLogIndex: m.PrevLogIndex,
	})
}

// handleAppendReply is rule 8, run after the higher-term rule. It always
// records recv_append_reply, but acts only on a leader and only for a reply
// from the current term. Replies can be duplicated or reordered, so:
//   - on success matchIndex and nextIndex only ever grow;
//   - on failure nextIndex steps back by one only if the reply answers the
//     AppendEntries that nextIndex[p] would send now, and never below 1.
func (n *Node) handleAppendReply(m Message) {
	act := n.role == Leader && m.Term == n.currentTerm
	resend := false
	if act {
		p := m.From
		if m.Success {
			n.matchIndex[p] = max(n.matchIndex[p], m.MatchIndex)
			n.nextIndex[p] = max(n.nextIndex[p], n.matchIndex[p]+1)
		} else if n.nextIndex[p] == m.ReqPrevLogIndex+1 && n.nextIndex[p] > 1 {
			n.nextIndex[p]--
			resend = true
		}
	}
	n.record(tracelog.EvRecvAppendReply, map[string]any{
		"from": m.From, "msg_id": m.MsgID, "success": m.Success, "match_index": m.MatchIndex,
	})
	if act && m.Success {
		n.advanceCommit()
	}
	if resend {
		n.send(n.appendEntriesFor(m.From))
	}
}

// advanceCommit is the leader's commit rule (rule 9). It looks for the
// highest N > commitIndex with log[N].term == currentTerm (the Figure 8 rule)
// that a majority, counting the leader itself, has matched, and commits up
// to it. Only a leader calls it (propose and handleAppendReply check first).
func (n *Node) advanceCommit() {
	for idx := n.lastIndex(); idx > n.commitIndex; idx-- {
		if n.termAt(idx) != n.currentTerm {
			continue
		}
		count := 0
		for _, p := range n.peers {
			if p == n.id || n.matchIndex[p] >= idx {
				count++
			}
		}
		if count > len(n.peers)/2 {
			n.setCommit(idx)
			return
		}
	}
}

// setCommit raises commitIndex to c and records advance_commit {old, new}.
// finish then applies the newly committed entries.
func (n *Node) setCommit(c int) {
	old := n.commitIndex
	n.commitIndex = c
	n.record(tracelog.EvAdvanceCommit, map[string]any{"old": old, "new": c})
}
