package tests

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"
)

// Replication scenarios, our own versions of MIT Lab 3B. Ids run 1..n, so
// "k places after node id" is (id-1+k)%n + 1 (see after).

// after returns the id k places after id in the ring 1..n.
func after(id, k, n int) int { return (id-1+k)%n + 1 }

func TestBasicAgree3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)

	for index := 1; index <= 3; index++ {
		if nd, _ := c.nCommitted(index); nd > 0 {
			t.Fatalf("%d nodes committed index %d before Start", nd, index)
		}
		if got := c.one(fmt.Sprintf("basic-%d", index), n, false); got != index {
			t.Fatalf("committed at index %d, want %d", got, index)
		}
	}
}

func TestFollowerFailure3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)

	c.one("ff-101", n, false)

	// One follower gone: the other two still agree.
	leader1 := c.checkOneLeader()
	c.disconnect(after(leader1, 1, n))
	c.one("ff-102", n-1, false)
	time.Sleep(electionTimeout)
	c.one("ff-103", n-1, false)

	// Both followers gone: nothing can commit.
	leader2 := c.checkOneLeader()
	c.disconnect(after(leader2, 1, n))
	c.disconnect(after(leader2, 2, n))
	index, _, ok := c.get(leader2).Start("ff-104")
	if !ok {
		t.Fatalf("leader %d rejected Start", leader2)
	}
	if index != 4 {
		t.Fatalf("Start returned index %d, want 4", index)
	}
	time.Sleep(2 * electionTimeout)
	if nd, _ := c.nCommitted(index); nd > 0 {
		t.Fatalf("%d nodes committed index %d without a majority", nd, index)
	}
}

func TestLeaderFailure3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)

	c.one("lf-101", n, false)

	// The leader gone: the other two elect a new one and agree.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	c.one("lf-102", n-1, false)
	time.Sleep(electionTimeout)
	c.one("lf-103", n-1, false)

	// The new leader gone too: one node left, nothing can commit.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	for _, id := range c.ids {
		c.get(id).Start("lf-104")
	}
	time.Sleep(2 * electionTimeout)
	if nd, _ := c.nCommitted(4); nd > 0 {
		t.Fatalf("%d nodes committed index 4 without a majority", nd)
	}
}

func TestFailNoAgree3B(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true)

	c.one("fna-10", n, false)

	// Three of five followers gone: no majority.
	leader := c.checkOneLeader()
	for k := 1; k <= 3; k++ {
		c.disconnect(after(leader, k, n))
	}
	index, _, ok := c.get(leader).Start("fna-20")
	if !ok {
		t.Fatalf("leader %d rejected Start", leader)
	}
	if index != 2 {
		t.Fatalf("Start returned index %d, want 2", index)
	}
	time.Sleep(2 * electionTimeout)
	if nd, _ := c.nCommitted(index); nd > 0 {
		t.Fatalf("%d nodes committed index %d without a majority", nd, index)
	}

	// Heal. The three come back with higher terms, so the leader may
	// change more than once before things settle; take the first leader
	// that accepts a Start. Index 2 was never committed, so the new entry
	// lands at 2 (it was overwritten) or 3 (it survived).
	for k := 1; k <= 3; k++ {
		c.connect(after(leader, k, n))
	}
	index2, ok2 := -1, false
	for range 5 {
		leader2 := c.checkOneLeader()
		if index2, _, ok2 = c.get(leader2).Start("fna-30"); ok2 {
			break
		}
	}
	if !ok2 {
		t.Fatalf("no leader accepted Start after healing")
	}
	if index2 < 2 || index2 > 3 {
		t.Fatalf("Start returned index %d, want 2 or 3", index2)
	}

	c.one("fna-1000", n, true)
}

func TestConcurrentStarts3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)

	success := false
loop:
	for try := range 5 {
		if try > 0 {
			// Give the cluster time to settle after a term change.
			time.Sleep(3 * time.Second)
		}
		leader := c.checkOneLeader()
		_, term, ok := c.get(leader).Start(fmt.Sprintf("cs-%d-first", try))
		if !ok {
			continue // lost leadership before we could use it
		}

		const iters = 5
		cmds := make([]string, iters)
		indexes := make(chan int, iters)
		var wg sync.WaitGroup
		for i := range iters {
			cmds[i] = fmt.Sprintf("cs-%d-%d", try, i)
			wg.Go(func() {
				index, term1, ok := c.get(leader).Start(cmds[i])
				if term1 != term || !ok {
					return
				}
				indexes <- index
			})
		}
		wg.Wait()
		close(indexes)

		for _, nd := range c.running() {
			if t1, _ := nd.GetState(); t1 != term {
				continue loop // the term moved on: try again
			}
		}

		got := map[string]int{}
		for index := range indexes {
			cmd := c.wait(index, n, term)
			if cmd == termMoved {
				continue loop
			}
			got[cmd]++
		}
		for _, cmd := range cmds {
			if got[cmd] != 1 {
				t.Fatalf("command %q committed %d times, want exactly once (committed: %v)", cmd, got[cmd], got)
			}
		}
		success = true
		break
	}
	if !success {
		t.Fatalf("the term changed too often")
	}
}

func TestRejoin3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)

	c.one("rj-101", n, true)

	// The leader is cut off and takes entries that can never commit.
	leader1 := c.checkOneLeader()
	c.disconnect(leader1)
	for _, cmd := range []string{"rj-lost-102", "rj-lost-103", "rj-lost-104"} {
		c.get(leader1).Start(cmd)
	}

	// The other two elect a new leader and commit index 2.
	c.one("rj-103", 2, true)

	// The new leader is cut off and the old one comes back.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	c.connect(leader1)
	c.one("rj-104", 2, true)

	// Everyone back: the old leader's extra entries are overwritten.
	c.connect(leader2)
	last := c.one("rj-105", n, true)

	for index := 1; index <= last; index++ {
		cmd := c.wait(index, n, -1)
		if strings.HasPrefix(cmd, "rj-lost") {
			t.Fatalf("index %d committed %q, which only the cut-off leader had", index, cmd)
		}
	}
}

func TestBackup3B(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true)
	rng := rand.New(rand.NewPCG(c.seed, 4))
	cmd := func(tag string, i int) string { return fmt.Sprintf("bk-%s-%d-%d", tag, i, rng.IntN(1_000_000)) }

	c.one(cmd("first", 0), n, true)

	// Leader and one follower on their own; they take 50 entries that
	// cannot commit.
	leader1 := c.checkOneLeader()
	c.disconnect(after(leader1, 2, n))
	c.disconnect(after(leader1, 3, n))
	c.disconnect(after(leader1, 4, n))
	for i := range 50 {
		c.get(leader1).Start(cmd("minority1", i))
	}
	time.Sleep(electionTimeout / 2)
	c.disconnect(after(leader1, 0, n))
	c.disconnect(after(leader1, 1, n))

	// The other three elect a leader and commit 50 entries.
	c.connect(after(leader1, 2, n))
	c.connect(after(leader1, 3, n))
	c.connect(after(leader1, 4, n))
	for i := range 50 {
		c.one(cmd("majority1", i), 3, true)
	}

	// Their leader and one follower are cut off from the third; the leader
	// takes 50 more entries that cannot commit.
	leader2 := c.checkOneLeader()
	other := after(leader1, 2, n)
	if leader2 == other {
		other = after(leader2, 1, n)
	}
	c.disconnect(other)
	for i := range 50 {
		c.get(leader2).Start(cmd("minority2", i))
	}
	time.Sleep(electionTimeout / 2)

	// The first leader, its follower and that third node: a majority whose
	// logs disagree. They must back up and agree on 50 new entries.
	for _, id := range c.ids {
		c.disconnect(id)
	}
	c.connect(after(leader1, 0, n))
	c.connect(after(leader1, 1, n))
	c.connect(other)
	for i := range 50 {
		c.one(cmd("majority2", i), 3, true)
	}

	// Everyone back.
	for _, id := range c.ids {
		c.connect(id)
	}
	c.one(cmd("last", 0), n, true)
}

// TestCount3B checks the cluster sends no more RPCs than it needs. The
// budgets are MIT's, and MIT counts RPCs: a request and its reply are one.
// So they are checked against rpcCount (requests only), which equals MIT's
// RPC count; the counts with replies are logged too. Each Start sends an
// AppendEntries to each follower at once, so the 12 Starts below alone cost
// 24 requests, or 48 messages with the replies, which is over the 42 budget.
func TestCount3B(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)

	c.checkOneLeader()
	if total := c.rpcCount(); total > 30 || total < 1 {
		t.Fatalf("%d RPCs for the initial election, want 1..30", total)
	}
	t.Logf("initial election: %d RPCs, %d messages", c.rpcCount(), c.rpcTotal())

	success := false
	var total2 int64
loop:
	for try := range 5 {
		if try > 0 {
			// Give the cluster time to settle after a term change.
			time.Sleep(3 * time.Second)
		}
		leader := c.checkOneLeader()
		total1, msgs1 := c.rpcCount(), c.rpcTotal()

		const iters = 10
		starti, term, ok := c.get(leader).Start(fmt.Sprintf("count-%d-first", try))
		if !ok {
			continue // lost leadership before we could use it
		}
		cmds := make([]string, 0, iters+1)
		for i := 1; i < iters+2; i++ {
			cmd := fmt.Sprintf("count-%d-%d", try, i)
			cmds = append(cmds, cmd)
			index, term1, ok := c.get(leader).Start(cmd)
			if term1 != term || !ok {
				continue loop
			}
			if index != starti+i {
				t.Fatalf("Start returned index %d, want %d", index, starti+i)
			}
		}
		for i := 1; i <= iters; i++ {
			if got := c.wait(starti+i, n, term); got != cmds[i-1] {
				if got == termMoved {
					continue loop
				}
				t.Fatalf("index %d committed %q, want %q", starti+i, got, cmds[i-1])
			}
		}
		for _, nd := range c.running() {
			if t1, _ := nd.GetState(); t1 != term {
				continue loop
			}
		}
		total2 = c.rpcCount()
		used := total2 - total1
		if limit := int64((iters + 1 + 3) * n); used > limit {
			t.Fatalf("%d RPCs for %d entries, want at most %d", used, iters, limit)
		}
		t.Logf("%d entries: %d RPCs, %d messages", iters, used, c.rpcTotal()-msgs1)
		success = true
		break
	}
	if !success {
		t.Fatalf("the term changed too often")
	}

	msgs2 := c.rpcTotal()
	time.Sleep(electionTimeout)
	idle := c.rpcCount() - total2
	if idle > 3*20 {
		t.Fatalf("%d RPCs in %v of idleness, want at most %d", idle, electionTimeout, 3*20)
	}
	t.Logf("%v idle: %d RPCs, %d messages", electionTimeout, idle, c.rpcTotal()-msgs2)
}
