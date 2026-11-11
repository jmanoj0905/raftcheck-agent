package testkit

import (
	"math/rand/v2"
	"strconv"
)

// Random runs (spec §8.2). Every choice comes from one PCG stream seeded
// with the run's seed and is made over sorted slices, never over a map, so
// the same seed gives byte-identical records (C10).

type actionKind int

const (
	actDeliver actionKind = iota
	actHeartbeat
	actTimeout
	actPropose
	actDrop
	actDuplicate
	actCrash
	actRestart
)

// actionWeights are the action weights doubled, so crash and restart (2.5)
// are whole numbers. Weights (out of 100): deliver 58, heartbeat 15,
// timeout 2, propose 10, drop 5, duplicate 5, crash 2.5, restart 2.5.
//
// The plan had timeout 10 and deliver 50. At that rate a timeout knocked
// the leader out before it could replicate, and 92 of 100 five-node seeds
// never committed anything. The user decided on 2026-11-11 to lower timeout
// to 2 and give the spare 8 to deliver. TestWalkCoverage guards this.
var actionWeights = []struct {
	kind   actionKind
	weight int
}{
	{actDeliver, 116},
	{actHeartbeat, 30},
	{actTimeout, 4},
	{actPropose, 20},
	{actDrop, 10},
	{actDuplicate, 10},
	{actCrash, 5},
	{actRestart, 5},
}

// busyInFlight: with more messages than this in flight, only deliver and
// drop are drawn, so the network drains.
const busyInFlight = 200

// RandomRun builds a cluster of nodes nodes and takes steps random actions
// drawn from math/rand/v2 PCG(seed, 0). It stops at the first violation and
// returns the cluster either way, so Actions() can be printed.
func RandomRun(nodes int, seed int64, steps int) (*Cluster, error) {
	r := &randomRunner{
		c:   NewCluster(nodes),
		rng: rand.New(rand.NewPCG(uint64(seed), 0)),
	}
	for range steps {
		if err := r.act(); err != nil {
			return r.c, err
		}
	}
	return r.c, nil
}

type randomRunner struct {
	c       *Cluster
	rng     *rand.Rand
	lastCmd int // proposals are "c1", "c2", ...
}

// act draws actions until one is possible in the current state and takes
// it. Something is always possible: a timeout if any node is up, a restart
// otherwise.
func (r *randomRunner) act() error {
	c := r.c
	for {
		inFlight := len(c.inFlight)
		up, down := r.upAndDown()
		switch r.draw(inFlight > busyInFlight) {
		case actDeliver:
			if inFlight > 0 {
				return c.Deliver(r.rng.IntN(inFlight))
			}
		case actDrop:
			if inFlight > 0 {
				return c.Drop(r.rng.IntN(inFlight))
			}
		case actDuplicate:
			if inFlight > 0 {
				return c.Duplicate(r.rng.IntN(inFlight))
			}
		case actHeartbeat:
			if leaders := r.leaders(up); len(leaders) > 0 {
				return c.Heartbeat(r.pick(leaders))
			}
		case actTimeout:
			if len(up) > 0 {
				return c.Timeout(r.pick(up))
			}
		case actPropose:
			if len(up) > 0 {
				id := r.pick(up)
				r.lastCmd++
				return c.Propose(id, "c"+strconv.Itoa(r.lastCmd))
			}
		case actCrash:
			// "Fewer than n nodes down" is the same as "some node is up".
			if len(up) > 0 {
				return c.Crash(r.pick(up))
			}
		case actRestart:
			if len(down) > 0 {
				return c.Restart(r.pick(down))
			}
		}
	}
}

// draw picks an action kind by weight; when busy, only deliver and drop.
func (r *randomRunner) draw(busy bool) actionKind {
	allowed := func(k actionKind) bool { return !busy || k == actDeliver || k == actDrop }
	total := 0
	for _, a := range actionWeights {
		if allowed(a.kind) {
			total += a.weight
		}
	}
	x := r.rng.IntN(total)
	for _, a := range actionWeights {
		if !allowed(a.kind) {
			continue
		}
		if x < a.weight {
			return a.kind
		}
		x -= a.weight
	}
	panic("testkit: weights do not add up")
}

// upAndDown returns the ids of the running and the crashed nodes, sorted.
func (r *randomRunner) upAndDown() (up, down []int) {
	for i, n := range r.c.nodes {
		if n == nil {
			down = append(down, r.c.ids[i])
		} else {
			up = append(up, r.c.ids[i])
		}
	}
	return up, down
}

// leaders returns the running nodes that think they are leader, in the
// order of up.
func (r *randomRunner) leaders(up []int) []int {
	var ls []int
	for _, id := range up {
		if r.c.nodes[id-1].State().Role == "leader" {
			ls = append(ls, id)
		}
	}
	return ls
}

func (r *randomRunner) pick(ids []int) int { return ids[r.rng.IntN(len(ids))] }
