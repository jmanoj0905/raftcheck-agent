package core

import "fmt"

// Log helpers. Indexes are 1-based: index i is n.log[i-1], and index 0 is the
// empty prefix before the first entry (term 0).

// lastIndex returns the index of the last entry, or 0 if the log is empty.
func (n *Node) lastIndex() int { return len(n.log) }

// lastTerm returns the term of the last entry, or 0 if the log is empty.
func (n *Node) lastTerm() int { return n.termAt(n.lastIndex()) }

// termAt returns the term of entry i, or 0 for i == 0. It panics unless
// 0 <= i <= lastIndex(); callers check that prevLogIndex exists first.
func (n *Node) termAt(i int) int {
	if i < 0 || i > n.lastIndex() {
		panic(fmt.Sprintf("core: node %d termAt(%d) with lastIndex %d", n.id, i, n.lastIndex()))
	}
	if i == 0 {
		return 0
	}
	return n.log[i-1].Term
}

// entriesFrom returns a copy of entries i..lastIndex(). It is empty (not nil)
// when i == lastIndex()+1, and panics unless 1 <= i <= lastIndex()+1.
func (n *Node) entriesFrom(i int) []Entry {
	if i < 1 || i > n.lastIndex()+1 {
		panic(fmt.Sprintf("core: node %d entriesFrom(%d) with lastIndex %d", n.id, i, n.lastIndex()))
	}
	out := make([]Entry, n.lastIndex()-i+1)
	copy(out, n.log[i-1:])
	return out
}
