package tracelog

import (
	"encoding/json"
	"strings"
	"testing"
)

func goldenRecord() Record {
	return Record{
		V: 1, Node: 2, Seq: 45, Event: EvBecomeLeader, Term: 5,
		Details: map[string]any{"votes": []int{1, 2}},
		State: State{
			Role: "leader", CurrentTerm: 5, VotedFor: 2,
			Log:         []LogPair{{1, "set x 1"}, {3, "set y 2"}},
			CommitIndex: 1, LastApplied: 1, LeaderID: 2,
			NextIndex:  map[string]int{"1": 3, "3": 2},
			MatchIndex: map[string]int{"1": 2, "3": 0},
		},
	}
}

const goldenLine = `{"v":1,"node":2,"seq":45,"event":"become_leader","term":5,"details":{"votes":[1,2]},"state":{"role":"leader","current_term":5,"voted_for":2,"log":[[1,"set x 1"],[3,"set y 2"]],"commit_index":1,"last_applied":1,"leader_id":2,"next_index":{"1":3,"3":2},"match_index":{"1":2,"3":0}}}`

func TestMarshalGolden(t *testing.T) {
	b, err := Marshal(goldenRecord())
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != goldenLine {
		t.Fatalf("got  %s\nwant %s", b, goldenLine)
	}
}

func TestEmptyLogAndDetails(t *testing.T) {
	b, err := Marshal(Record{V: 1, Node: 1, Seq: 1, Event: EvCrash, State: State{Role: "follower"}})
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, `"log":[]`) || !strings.Contains(s, `"details":{}`) || strings.Contains(s, "null") {
		t.Fatalf("bad empty handling: %s", s)
	}
}

func TestFollowerOmitsIndexMaps(t *testing.T) {
	b, _ := Marshal(Record{V: 1, Node: 1, Seq: 1, Event: EvBecomeFollower, State: State{Role: "follower"}})
	s := string(b)
	if strings.Contains(s, "next_index") || strings.Contains(s, "match_index") {
		t.Fatalf("follower has index maps: %s", s)
	}
}

func TestLogPairRoundTrip(t *testing.T) {
	var lp []LogPair
	if err := json.Unmarshal([]byte(`[[2,"a"]]`), &lp); err != nil {
		t.Fatal(err)
	}
	if len(lp) != 1 || lp[0] != (LogPair{2, "a"}) {
		t.Fatalf("got %+v", lp)
	}
}

func TestAllEventsCount(t *testing.T) {
	if len(AllEvents) != 18 {
		t.Fatalf("len = %d", len(AllEvents))
	}
	seen := map[string]bool{}
	for _, e := range AllEvents {
		if seen[e] {
			t.Fatalf("duplicate %s", e)
		}
		seen[e] = true
	}
}

func TestAppendJSONL(t *testing.T) {
	out := AppendJSONL(nil, []Record{goldenRecord(), goldenRecord()})
	if string(out) != goldenLine+"\n"+goldenLine+"\n" {
		t.Fatalf("got %s", out)
	}
}
