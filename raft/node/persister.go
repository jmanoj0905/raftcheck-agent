package node

import (
	"slices"
	"sync"

	"github.com/jmanoj0905/raftcheck-agent/raft/core"
)

// Persister stores what a node needs to come back after a crash: Raft's
// persistent state, and (separately, D6) the record and message counters.
type Persister interface {
	Save(id int, s core.PersistentState)
	Load(id int) (core.PersistentState, bool)
	SaveCounters(id int, c core.Counters) // after every step (D6)
	LoadCounters(id int) core.Counters    // zero Counters if never saved
}

// MemPersister is an in-memory Persister for tests. It deep-copies the log
// on Save and on Load, so neither side can change the other's copy. It is
// safe for concurrent use.
type MemPersister struct {
	mu       sync.Mutex
	states   map[int]core.PersistentState
	counters map[int]core.Counters
	saves    map[int]int
}

// NewMemPersister returns an empty MemPersister.
func NewMemPersister() *MemPersister {
	return &MemPersister{
		states:   map[int]core.PersistentState{},
		counters: map[int]core.Counters{},
		saves:    map[int]int{},
	}
}

// Save stores a deep copy of s for node id.
func (p *MemPersister) Save(id int, s core.PersistentState) {
	s.Log = cloneLog(s.Log)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.states[id] = s
	p.saves[id]++
}

// Load returns a deep copy of node id's saved state, and false if it never
// saved any.
func (p *MemPersister) Load(id int) (core.PersistentState, bool) {
	p.mu.Lock()
	s, ok := p.states[id]
	p.mu.Unlock()
	s.Log = cloneLog(s.Log)
	return s, ok
}

// SaveCounters stores node id's counters.
func (p *MemPersister) SaveCounters(id int, c core.Counters) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.counters[id] = c
}

// LoadCounters returns node id's counters (zero if never saved).
func (p *MemPersister) LoadCounters(id int) core.Counters {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.counters[id]
}

// SaveCount returns how many times Save was called for node id. Tests use
// it to check that nothing is saved after Kill.
func (p *MemPersister) SaveCount(id int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.saves[id]
}

// cloneLog copies l; the copy is never nil.
func cloneLog(l []core.Entry) []core.Entry {
	if l == nil {
		return []core.Entry{}
	}
	return slices.Clone(l)
}
