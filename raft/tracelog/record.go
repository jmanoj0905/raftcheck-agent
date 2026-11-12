// Package tracelog defines the JSONL record format shared by the Go Raft
// and the Python tooling. schema.json is the single source of truth.
package tracelog

import (
	"encoding/json"
	"fmt"
)

// Version is the schema version written in every record's "v" field.
const Version = 1

// Event names, as in spec section 6.3, plus EvCrash.
const (
	EvUpdateTerm        = "update_term"
	EvBecomeFollower    = "become_follower"
	EvBecomeCandidate   = "become_candidate"
	EvBecomeLeader      = "become_leader"
	EvSendRequestVote   = "send_request_vote"
	EvRecvRequestVote   = "recv_request_vote"
	EvRecvVoteReply     = "recv_vote_reply"
	EvSendAppendEntries = "send_append_entries"
	EvRecvAppendEntries = "recv_append_entries"
	EvRecvAppendReply   = "recv_append_reply"
	EvAppendEntry       = "append_entry"
	EvTruncateLog       = "truncate_log"
	EvAdvanceCommit     = "advance_commit"
	EvApplyEntry        = "apply_entry"
	EvPersist           = "persist"
	EvRestart           = "restart"
	EvProposeRejected   = "propose_rejected"
	EvCrash             = "crash"
)

// AllEvents lists the 18 event names in spec order.
var AllEvents = []string{
	EvUpdateTerm, EvBecomeFollower, EvBecomeCandidate, EvBecomeLeader,
	EvSendRequestVote, EvRecvRequestVote, EvRecvVoteReply,
	EvSendAppendEntries, EvRecvAppendEntries, EvRecvAppendReply,
	EvAppendEntry, EvTruncateLog, EvAdvanceCommit, EvApplyEntry,
	EvPersist, EvRestart, EvProposeRejected, EvCrash,
}

// LogPair is one log entry; it marshals as [term, "command"].
type LogPair struct {
	Term    int
	Command string
}

// MarshalJSON writes the pair as [term,"command"].
func (p LogPair) MarshalJSON() ([]byte, error) {
	return json.Marshal([]any{p.Term, p.Command})
}

// UnmarshalJSON reads [term,"command"].
func (p *LogPair) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if len(raw) != 2 {
		return fmt.Errorf("log pair must have 2 elements, got %d", len(raw))
	}
	if err := json.Unmarshal(raw[0], &p.Term); err != nil {
		return err
	}
	return json.Unmarshal(raw[1], &p.Command)
}

// State is the snapshot taken after each change (spec 6.4).
type State struct {
	Role        string         `json:"role"`
	CurrentTerm int            `json:"current_term"`
	VotedFor    int            `json:"voted_for"`
	Log         []LogPair      `json:"log"`
	CommitIndex int            `json:"commit_index"`
	LastApplied int            `json:"last_applied"`
	LeaderID    int            `json:"leader_id"`
	NextIndex   map[string]int `json:"next_index,omitempty"`
	MatchIndex  map[string]int `json:"match_index,omitempty"`
}

// Record is one JSONL line.
type Record struct {
	V       int            `json:"v"`
	Node    int            `json:"node"`
	Seq     int64          `json:"seq"`
	Event   string         `json:"event"`
	Term    int            `json:"term"`
	Details map[string]any `json:"details"`
	State   State          `json:"state"`
	Step    *int64         `json:"step,omitempty"`
	Cause   string         `json:"cause,omitempty"`
}

// Marshal encodes r on one line with no trailing newline. Field order is the
// struct order; map keys are sorted by encoding/json.
func Marshal(r Record) ([]byte, error) {
	if r.Details == nil {
		r.Details = map[string]any{}
	}
	if r.State.Log == nil {
		r.State.Log = []LogPair{}
	}
	return json.Marshal(r)
}

// AppendJSONL appends each record plus "\n" to dst. It panics on a marshal
// error, which cannot happen for the closed set of types used.
func AppendJSONL(dst []byte, rs []Record) []byte {
	for _, r := range rs {
		b, err := Marshal(r)
		if err != nil {
			panic(err)
		}
		dst = append(dst, b...)
		dst = append(dst, '\n')
	}
	return dst
}
