package schemacheck

import (
	"strings"
	"testing"
)

const golden = `{"v":1,"node":2,"seq":45,"event":"become_leader","term":5,"details":{"votes":[1,2]},"state":{"role":"leader","current_term":5,"voted_for":2,"log":[[1,"set x 1"],[3,"set y 2"]],"commit_index":1,"last_applied":1,"leader_id":2,"next_index":{"1":3,"3":2},"match_index":{"1":2,"3":0}}}`

func TestGoldenRecordValid(t *testing.T) {
	if err := ValidateLine([]byte(golden)); err != nil {
		t.Fatal(err)
	}
}

const st = `"state":{"role":"follower","current_term":1,"voted_for":0,"log":[],"commit_index":0,"last_applied":0,"leader_id":0}`

func TestRejects(t *testing.T) {
	cases := map[string]string{
		"missing seq":    `{"v":1,"node":1,"event":"persist","term":1,"details":{"log_len":0},` + st + `}`,
		"bad event":      `{"v":1,"node":1,"seq":1,"event":"vote","term":1,"details":{},` + st + `}`,
		"bad reason":     `{"v":1,"node":1,"seq":1,"event":"recv_request_vote","term":1,"details":{"from":2,"msg_id":"2-1","granted":false,"reason":"nope","reply_msg_id":"1-1"},` + st + `}`,
		"no reply id rv": `{"v":1,"node":1,"seq":1,"event":"recv_request_vote","term":1,"details":{"from":2,"msg_id":"2-1","granted":false,"reason":"stale_term"},` + st + `}`,
		"no reply id ae": `{"v":1,"node":1,"seq":1,"event":"recv_append_entries","term":1,"details":{"from":2,"msg_id":"2-1","success":false,"reason":"stale_term"},` + st + `}`,
		"bad reply id":   `{"v":1,"node":1,"seq":1,"event":"recv_append_entries","term":1,"details":{"from":2,"msg_id":"2-1","success":false,"reason":"stale_term","reply_msg_id":"1-0"},` + st + `}`,
		"short log item": `{"v":1,"node":1,"seq":1,"event":"persist","term":1,"details":{"log_len":1},"state":{"role":"follower","current_term":1,"voted_for":0,"log":[[1]],"commit_index":0,"last_applied":0,"leader_id":0}}`,
		"extra key":      `{"v":1,"node":1,"seq":1,"event":"persist","term":1,"details":{"log_len":0},` + st + `,"x":1}`,
		"crash details":  `{"v":1,"node":1,"seq":1,"event":"crash","term":1,"details":{"a":1},` + st + `}`,
		"bad msg_id":     `{"v":1,"node":1,"seq":1,"event":"recv_vote_reply","term":1,"details":{"from":2,"msg_id":"17","granted":true},` + st + `}`,
		"missing detail": `{"v":1,"node":1,"seq":1,"event":"persist","term":1,"details":{},` + st + `}`,
	}
	for name, line := range cases {
		if err := ValidateLine([]byte(line)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestValidRecvWithReplyID(t *testing.T) {
	for _, line := range []string{
		`{"v":1,"node":1,"seq":1,"event":"recv_request_vote","term":1,"details":{"from":2,"msg_id":"2-1","granted":true,"reason":"ok","reply_msg_id":"1-12"},` + st + `}`,
		`{"v":1,"node":1,"seq":1,"event":"recv_append_entries","term":1,"details":{"from":2,"msg_id":"2-1","success":false,"reason":"log_mismatch","reply_msg_id":"1-3"},` + st + `}`,
	} {
		if err := ValidateLine([]byte(line)); err != nil {
			t.Errorf("%s: %v", line, err)
		}
	}
}

func TestValidCrashAndStepCause(t *testing.T) {
	line := `{"v":1,"node":1,"seq":1,"event":"crash","term":1,"details":{},` + st + `,"step":3,"cause":"msg 1-2"}`
	if err := ValidateLine([]byte(line)); err != nil {
		t.Fatal(err)
	}
}

func TestValidateJSONLLineNumber(t *testing.T) {
	data := golden + "\n" + golden + "\n" + `{"bad":1}` + "\n"
	err := ValidateJSONL([]byte(data))
	if err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Fatalf("got %v", err)
	}
}
