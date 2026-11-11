package tests

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Persistence scenarios, our own versions of MIT Lab 3C. restart(id) is a
// crash (disconnect + Kill) followed by Make from the same persister and a
// connect, so a restarted node has only what it persisted.

// checkAll fails unless indexes 1..last are applied by all n nodes, each
// with the command in want (when want has that index).
func checkAll(c *cluster, last, n int, want map[int]string) {
	c.t.Helper()
	for index := 1; index <= last; index++ {
		got := c.wait(index, n, -1)
		if w, ok := want[index]; ok && got != w {
			c.t.Fatalf("index %d holds %q, want %q", index, got, w)
		}
	}
}

func TestPersist13C(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)
	want := map[int]string{}
	one := func(cmd string, expected int) {
		t.Helper()
		want[c.one(cmd, expected, true)] = cmd
	}

	one("p1-11", n)

	// Every node crashes and restarts.
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.restart(id)
	}
	one("p1-12", n)
	checkAll(c, 2, n, want)

	// The leader crashes and restarts.
	leader1 := c.checkOneLeader()
	c.restart(leader1)
	one("p1-13", n)

	// The leader is cut off, misses an entry, then crashes and restarts.
	leader2 := c.checkOneLeader()
	c.disconnect(leader2)
	one("p1-14", n-1)
	c.restart(leader2)
	c.wait(4, n, -1) // the restarted node catches up

	// A follower is cut off, misses an entry, then crashes and restarts.
	i3 := after(c.checkOneLeader(), 1, n)
	c.disconnect(i3)
	one("p1-15", n-1)
	c.restart(i3)
	one("p1-16", n)

	checkAll(c, 6, n, want)
}

func TestPersist23C(t *testing.T) {
	const n = 5
	c := newCluster(t, n, true)

	index := 1
	for range 5 {
		c.one(fmt.Sprintf("p2-%d", index), n, true)
		index++

		leader1 := c.checkOneLeader()
		at := func(k int) int { return after(leader1, k, n) }

		// Two followers cut off; the other three commit without them.
		c.disconnect(at(1))
		c.disconnect(at(2))
		c.one(fmt.Sprintf("p2-%d", index), n-2, true)
		index++

		// The three that have that entry are cut off, and the two that
		// missed it restart. Then one that has it restarts too: it must
		// win the election, since only it has the newest entry.
		c.disconnect(at(0))
		c.disconnect(at(3))
		c.disconnect(at(4))
		c.restart(at(1))
		c.restart(at(2))
		time.Sleep(electionTimeout)
		c.restart(at(3))
		c.one(fmt.Sprintf("p2-%d", index), n-2, true)
		index++

		c.connect(at(4))
		c.connect(at(0))
	}

	last := c.one("p2-1000", n, true)
	checkAll(c, last, n, nil)
}

// TestPersist33C: a leader takes an entry and crashes before it is
// committed, then restarts. The entry must end up committed on every node
// at the index the leader gave it, or nowhere.
func TestPersist33C(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)

	c.one("p3-1", n, true)

	// a. The entry may reach the followers before the leader crashes: then
	// a later leader may commit it along with its own entries.
	for round := range 3 {
		leader := c.checkOneLeader()
		cmd := fmt.Sprintf("p3-a-%d", round)
		index, _, ok := c.get(leader).Start(cmd)
		c.crash(leader)
		c.one(fmt.Sprintf("p3-a-%d-next", round), n-1, true)
		c.restart(leader)
		last := c.one(fmt.Sprintf("p3-a-%d-after", round), n, true)
		if ok {
			checkCommittedAtOrNowhere(c, cmd, index, last, n)
		}
	}

	// b. The leader is cut off first, so the entry exists only in its log:
	// the others move on without it, and it must be overwritten.
	leader := c.checkOneLeader()
	c.disconnect(leader)
	index, _, ok := c.get(leader).Start("p3-b-lost")
	if !ok {
		t.Fatalf("leader %d rejected Start", leader)
	}
	c.crash(leader)
	c.one("p3-b-next", n-1, true)
	c.restart(leader)
	last := c.one("p3-b-after", n, true)
	checkCommittedAtOrNowhere(c, "p3-b-lost", index, last, n)
	if got := c.wait(index, n, -1); got == "p3-b-lost" {
		t.Fatalf("index %d committed the entry only the cut-off leader had", index)
	}
}

// checkCommittedAtOrNowhere fails unless cmd is, among indexes 1..last
// (all applied by n nodes), either at index only or nowhere.
func checkCommittedAtOrNowhere(c *cluster, cmd string, index, last, n int) {
	c.t.Helper()
	for i := 1; i <= last; i++ {
		if got := c.wait(i, n, -1); got == cmd && i != index {
			c.t.Fatalf("%q committed at index %d, but the leader put it at %d", cmd, i, index)
		}
	}
}

// figure8 is TestFigure83C and TestFigure8Unreliable3C. Each round every
// node is offered a command; then, after a short (usually) or long pause,
// the leader is crashed (reliable) or cut off half the time (unreliable),
// and a lost node comes back if fewer than three are up. This builds the
// Figure 8 situations: entries from old terms on some nodes, which a new
// leader must not commit by counting replicas.
func figure8(t *testing.T, reliable bool, iters int) {
	const n = 5
	c := newCluster(t, n, reliable)
	rng := rand.New(rand.NewPCG(c.seed, 8))
	seq := 0
	cmd := func() string { seq++; return fmt.Sprintf("f8-%d", seq) }

	c.one(cmd(), 1, true)

	up := n
	for range iters {
		leader := -1
		for _, id := range c.ids {
			if nd := c.get(id); nd != nil {
				if _, _, ok := nd.Start(cmd()); ok && c.isConnected(id) {
					leader = id
				}
			}
		}

		if rng.IntN(1000) < 100 {
			time.Sleep(rng.N(electionTimeout / 2))
		} else {
			time.Sleep(rng.N(13 * time.Millisecond))
		}

		if leader != -1 {
			if reliable {
				c.crash(leader)
				up--
			} else if rng.IntN(1000) < int(electionTimeout/time.Millisecond)/2 {
				c.disconnect(leader)
				up--
			}
		}

		if up < 3 {
			if s := rng.IntN(n) + 1; !c.isConnected(s) {
				if c.get(s) == nil {
					c.restart(s)
				} else {
					c.connect(s)
				}
				up++
			}
		}
	}

	for _, id := range c.ids {
		if c.get(id) == nil {
			c.restart(id)
		} else if !c.isConnected(id) {
			c.connect(id)
		}
	}
	c.one(cmd(), n, true)
}

func TestFigure83C(t *testing.T) {
	figure8(t, true, 1000)
}

// TestFigure8Unreliable3C runs fewer rounds than TestFigure83C. The plan
// and MIT use 1000; D5 (step-back-by-one) makes catch-up linear in the lag;
// reduced by user decision 2026-11-11. Each round adds entries that the
// final one() must back the followers over, one rejected AppendEntries at a
// time, on a network that drops and delays. Measured under -race on an M1
// (final one, 10 s budget): 1000 rounds took 25-27 s, 200 up to 9.7 s, 100
// up to 4.6 s, 75 up to 3.6 s; 50 at most 2.6 s over 24 runs and 30 at most
// 2.2 s over 8, both at least 3x inside the budget.
func TestFigure8Unreliable3C(t *testing.T) {
	iters := 50
	if testing.Short() {
		iters = 30
	}
	figure8(t, false, iters)
}

func TestUnreliableAgree3C(t *testing.T) {
	const n = 5
	c := newCluster(t, n, false)

	var wg sync.WaitGroup
	t.Cleanup(wg.Wait) // runs before the cluster's cleanup (LIFO)
	errs := make(chan error, 49*4)
	for iter := 1; iter < 50; iter++ {
		for j := range 4 {
			wg.Go(func() {
				if _, err := c.tryOne(fmt.Sprintf("ua-%d-%d", iter, j), 1, true); err != nil {
					errs <- err
				}
			})
		}
		c.one(fmt.Sprintf("ua-%d", iter), 1, true)
	}

	c.setUnreliable(false)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	last := c.one("ua-last", n, true)
	checkAll(c, last, n, nil)
}

// churn is TestReliableChurn3C and TestUnreliableChurn3C. Three clients
// keep offering commands to every node while, for churnFor, nodes are
// disconnected, restarted and crashed at random. Then everything heals,
// and every command a client saw committed must be in the final log,
// which every node must have applied identically.
func churn(t *testing.T, reliable bool) {
	const (
		n        = 5
		clients  = 3
		churnFor = 5 * time.Second
	)
	c := newCluster(t, n, reliable)
	rng := rand.New(rand.NewPCG(c.seed, 9))

	var stop atomic.Bool
	defer stop.Store(true)
	type result struct {
		seen []string // commands this client saw committed
		err  error
	}
	results := make(chan result, clients)
	for me := range clients {
		go func() {
			var r result
			defer func() { results <- r }()
			for k := 0; !stop.Load(); k++ {
				cmd := fmt.Sprintf("churn-%d-%d", me, k)
				// Try every node: one of them may be the leader.
				index, ok := -1, false
				for _, id := range c.ids {
					if nd := c.get(id); nd != nil {
						if i, _, isLeader := nd.Start(cmd); isLeader {
							index, ok = i, true
						}
					}
				}
				if !ok {
					time.Sleep(time.Duration(79+me*17) * time.Millisecond)
					continue
				}
				// The leader may or may not commit it; don't wait forever.
				for _, to := range []time.Duration{10, 20, 50, 100, 200} {
					nd, got, err := c.nCommittedErr(index)
					if err != nil {
						r.err = err
						return
					}
					if nd > 0 {
						if got == cmd {
							r.seen = append(r.seen, cmd)
						}
						break
					}
					time.Sleep(to * time.Millisecond)
				}
			}
		}()
	}

	for start := time.Now(); time.Since(start) < churnFor; {
		if rng.IntN(1000) < 200 {
			c.disconnect(rng.IntN(n) + 1)
		}
		if rng.IntN(1000) < 500 {
			id := rng.IntN(n) + 1
			if c.get(id) == nil {
				c.restart(id)
			} else {
				c.connect(id)
			}
		}
		if rng.IntN(1000) < 200 {
			c.crash(rng.IntN(n) + 1)
		}
		// Often enough that the nodes rarely settle, rarely enough that
		// they can sometimes catch up: a bit under one election timeout.
		time.Sleep(electionTimeout * 7 / 10)
	}

	time.Sleep(electionTimeout)
	c.setUnreliable(false)
	for _, id := range c.ids {
		if c.get(id) == nil {
			c.restart(id)
		} else {
			c.connect(id)
		}
	}
	stop.Store(true)

	var seen []string
	var errs []error
	for range clients {
		r := <-results
		seen = append(seen, r.seen...)
		errs = append(errs, r.err)
	}
	if err := errors.Join(errs...); err != nil {
		t.Fatalf("client: %v", err)
	}

	time.Sleep(electionTimeout)
	last := c.one("churn-last", n, true)
	final := map[string]bool{}
	for index := 1; index <= last; index++ {
		final[c.wait(index, n, -1)] = true
	}
	var missing []string
	for _, cmd := range seen {
		if !final[cmd] {
			missing = append(missing, cmd)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("committed commands missing from the final log: %s", strings.Join(missing, ", "))
	}
	t.Logf("%d commands seen committed, final log has %d entries", len(seen), last)
}

func TestReliableChurn3C(t *testing.T) {
	churn(t, true)
}

func TestUnreliableChurn3C(t *testing.T) {
	churn(t, false)
}
