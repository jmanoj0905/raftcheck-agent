// Package core is the deterministic Raft state machine (spec §4). It has no
// goroutines, clocks, randomness or I/O: Step(event) returns everything the
// caller must do, in the order persist -> send messages -> apply.
package core

import (
	"fmt"

	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// Role is a node's Raft role.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// String returns the role name used in log records ("follower", ...).
func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("Role(%d)", int(r))
}

// Entry is one log entry. Log indexes start at 1.
type Entry struct {
	Term    int
	Command string
}

// MsgType says which of the four Raft messages a Message is.
type MsgType int

const (
	MsgRequestVote MsgType = iota + 1
	MsgRequestVoteReply
	MsgAppendEntries
	MsgAppendEntriesReply
)

// Message is one Raft message. From is the candidate or leader id where the
// paper has candidateId / leaderId. MsgID is "<from>-<n>" (spec §4.3).
type Message struct {
	Type  MsgType
	From  int
	To    int
	MsgID string
	Term  int

	// RequestVote
	LastLogIndex int
	LastLogTerm  int

	// RequestVoteReply
	VoteGranted bool

	// AppendEntries
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int

	// AppendEntriesReply. MatchIndex is used only when Success.
	// ReqPrevLogIndex echoes the request's PrevLogIndex.
	Success         bool
	MatchIndex      int
	ReqPrevLogIndex int
}

// Event is an input to Step. Only the types in this package implement it.
type Event interface{ isEvent() }

// ElectionTimeout means this node's election timer fired.
type ElectionTimeout struct{}

// HeartbeatTimeout means a leader should send AppendEntries to everyone.
type HeartbeatTimeout struct{}

// Receive delivers a message to this node.
type Receive struct{ Msg Message }

// Propose asks this node to add a command to the log.
type Propose struct{ Command string }

func (ElectionTimeout) isEvent()  {}
func (HeartbeatTimeout) isEvent() {}
func (Receive) isEvent()          {}
func (Propose) isEvent()          {}

// PersistentState is what must survive a crash (D6: counters are separate).
type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []Entry
}

// Counters are the per-node record and message counters (D6). Both start at
// 0; the first record has seq 1 and the first message id is "<id>-1".
type Counters struct {
	LastSeq int64
	LastMsg int64
}

// Applied is a committed entry handed to the application.
type Applied struct {
	Index   int
	Term    int
	Command string
}

// ProposeResult is Start()'s (index, term, isLeader).
type ProposeResult struct {
	Index    int
	Term     int
	IsLeader bool
}

// Output is everything one Step asks the caller to do. The caller must save
// Persist (if non-nil) before sending Messages, and send before applying.
type Output struct {
	Persist            *PersistentState
	Messages           []Message
	Apply              []Applied
	Records            []tracelog.Record
	ResetElectionTimer bool
	Proposed           *ProposeResult // non-nil only for Propose
}
