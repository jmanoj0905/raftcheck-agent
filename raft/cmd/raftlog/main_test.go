package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

func TestFormatLine(t *testing.T) {
	r := tracelog.Record{
		V: 1, Node: 2, Seq: 7, Event: tracelog.EvBecomeLeader, Term: 5,
		Details: map[string]any{"votes": []int{1, 2}},
		State:   tracelog.State{Role: "leader", CurrentTerm: 5},
	}
	want := "[n2 t5 LEADER] become_leader votes=[1 2]"
	if got := FormatLine(r); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestFormatLineSortedKeysAndJSONNumbers(t *testing.T) {
	r := tracelog.Record{
		Node: 1, Event: tracelog.EvSendAppendEntries, Term: 3,
		Details: map[string]any{"to": float64(2), "msg_id": "1-4", "votes": []any{float64(1), float64(2)}, "granted": true},
		State:   tracelog.State{Role: "follower"},
	}
	want := "[n1 t3 FOLLOWER] send_append_entries granted=true msg_id=1-4 to=2 votes=[1 2]"
	if got := FormatLine(r); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	r.Details = nil
	if got := FormatLine(r); got != "[n1 t3 FOLLOWER] send_append_entries" {
		t.Errorf("no details: %q", got)
	}
}

func TestRun(t *testing.T) {
	good := `{"v":1,"node":2,"seq":1,"event":"become_leader","term":5,"details":{"votes":[1,2]},"state":{"role":"leader","current_term":5,"voted_for":2,"log":[],"commit_index":0,"last_applied":0,"leader_id":2}}`
	var out bytes.Buffer
	if err := run(strings.NewReader(good+"\n\n"), &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != "[n2 t5 LEADER] become_leader votes=[1 2]\n" {
		t.Errorf("got %q", got)
	}
	err := run(strings.NewReader(good+"\nnot json\n"), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "line 2") {
		t.Errorf("want error naming line 2, got %v", err)
	}
}
