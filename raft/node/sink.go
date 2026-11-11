package node

import (
	"os"
	"slices"
	"sync"

	"github.com/jmanoj0905/raftcheck-agent/raft/tracelog"
)

// RecordSink receives every log record a node writes. Implementations must
// be safe for concurrent use: several nodes may share one sink.
type RecordSink interface {
	Write(r tracelog.Record)
}

// discardSink drops every record; it is used when Config.Sink is nil.
type discardSink struct{}

func (discardSink) Write(tracelog.Record) {}

// MemorySink keeps records in memory, in write order. The zero value is
// ready to use and safe for concurrent Write.
type MemorySink struct {
	mu   sync.Mutex
	recs []tracelog.Record
}

// Write appends r.
func (s *MemorySink) Write(r tracelog.Record) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs = append(s.recs, r)
}

// Records returns a copy of the records written so far. The records
// themselves are shared; callers must not change them.
func (s *MemorySink) Records() []tracelog.Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.recs)
}

// JSONL returns the records as JSON lines.
func (s *MemorySink) JSONL() []byte {
	return tracelog.AppendJSONL(nil, s.Records())
}

// FileSink writes each record as one JSON line to a file, with one write
// call per record so a crash loses at most a partial last line. It appends
// to an existing file, so a restarted process keeps one log. Safe for
// concurrent Write.
type FileSink struct {
	mu     sync.Mutex
	f      *os.File
	err    error // first write error, returned by Close
	closed bool
}

// NewFileSink opens (creating if needed) path for appending.
func NewFileSink(path string) (*FileSink, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &FileSink{f: f}, nil
}

// Write appends r as one line. After an error or Close it does nothing;
// the first error is reported by Close.
func (s *FileSink) Write(r tracelog.Record) {
	line := tracelog.AppendJSONL(nil, []tracelog.Record{r})
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.err != nil {
		return
	}
	if _, err := s.f.Write(line); err != nil {
		s.err = err
	}
}

// Close closes the file and returns the first write or close error.
// Calling it again returns nil.
func (s *FileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if err := s.f.Close(); err != nil && s.err == nil {
		s.err = err
	}
	return s.err
}
