package tests

import (
	"math/rand/v2"
	"testing"
	"time"
)

// Election scenarios, our own versions of MIT Lab 3A.

func TestInitialElection3A(t *testing.T) {
	c := newCluster(t, 3, true)

	c.checkOneLeader()

	// Give the followers time to learn the leader's term.
	time.Sleep(50 * time.Millisecond)
	term1 := c.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %d, but it should be at least 1", term1)
	}

	// With no failures, the leader and the term must stay put.
	time.Sleep(2 * electionTimeout)
	term2 := c.checkTerms()
	if term1 != term2 {
		t.Fatalf("term changed from %d to %d even though there were no failures", term1, term2)
	}

	c.checkOneLeader()
}

func TestReElection3A(t *testing.T) {
	const n = 3
	c := newCluster(t, n, true)

	leader1 := c.checkOneLeader()

	// If the leader disconnects, a new one should be elected.
	c.disconnect(leader1)
	c.checkOneLeader()

	// The old leader rejoining must not disturb the new leader.
	c.connect(leader1)
	leader2 := c.checkOneLeader()

	// Without a quorum no leader may be elected.
	other := leader2%n + 1
	c.disconnect(leader2)
	c.disconnect(other)
	time.Sleep(2 * electionTimeout)
	c.checkNoLeader()

	// A quorum is back: a leader again.
	c.connect(other)
	c.checkOneLeader()

	// Rejoining the last node must not stop there being a leader.
	c.connect(leader2)
	c.checkOneLeader()
}

func TestManyElections3A(t *testing.T) {
	const n = 7
	c := newCluster(t, n, true)
	rng := rand.New(rand.NewPCG(c.seed, 3))

	c.checkOneLeader()

	for range 10 {
		// Disconnect three nodes (maybe fewer, if the picks repeat), so at
		// least four of seven stay connected: still a quorum.
		i1, i2, i3 := rng.IntN(n)+1, rng.IntN(n)+1, rng.IntN(n)+1
		c.disconnect(i1)
		c.disconnect(i2)
		c.disconnect(i3)

		// Either the old leader still leads or a new one was elected.
		c.checkOneLeader()

		c.connect(i1)
		c.connect(i2)
		c.connect(i3)
	}

	c.checkOneLeader()
}
