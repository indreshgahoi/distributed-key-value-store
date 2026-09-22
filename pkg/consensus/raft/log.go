package raft

import "sync"

// RaftLog provides thread-safe access to replicated log entries.
// Index 0 is reserved as a dummy sentinel entry
type RaftLog struct {
	mu      sync.RWMutex
	entries []LogEntry // entries[0] is a dummy sentinel entry at index 0
}

// NewRaftLog initializes a log with a dummy entry at index 0.
func NewRaftLog() *RaftLog {
	return &RaftLog{
		entries: []LogEntry{{Index: 0, Term: 0}}, // Entinel Entry
	}
}

func (l *RaftLog) FirstIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[0].Index + 1
}

func (l *RaftLog) toSliceIndex(raftIndex uint64) (int, bool) {
	firstIndex := l.entries[0].Index
	if raftIndex < firstIndex {
		return 0, false // Compacted entry
	}
	idx := int(raftIndex - firstIndex)
	if idx >= len(l.entries) {
		return 0, false // Out of bounds
	}
	return idx, true
}

// LastIndex returns the 1-based index of the most recent entry in the log.
func (l *RaftLog) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the most recent entry in the log.
func (l *RaftLog) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

// TermAt returns the term of the entry at the specified 1-based index.
func (l *RaftLog) TermAt(index uint64) (uint64, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	idx, ok := l.toSliceIndex(index)
	if !ok {
		return 0, false
	}
	return l.entries[idx].Term, true
}

// Append creates and stores a new log entry at index len(entries).
func (l *RaftLog) Append(term uint64, data []byte) LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	entry := LogEntry{
		Index: uint64(len(l.entries)),
		Term:  term,
		Data:  data,
	}
	l.entries = append(l.entries, entry)
	return entry
}

// TruncateAndAppend enforces the Raft Log Matching Invariant:
// If an existing entry conflicts with a new one (same index, different terms),
// it deletes the existing entry and all that follow it, then appends the new entries.
func (l *RaftLog) TruncateAndAppend(prevIndex uint64, newEntries []LogEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()

	for i, entry := range newEntries {
		idx := prevIndex + 1 + uint64(i)
		if idx < uint64(len(l.entries)) {
			if l.entries[idx].Term != entry.Term {
				l.entries = l.entries[:idx]
				l.entries = append(l.entries, entry)
			}
		} else {
			l.entries = append(l.entries, entry)
		}
	}
}

// Slice returns a copy of log entries starting from fromIndex to the end.
func (l *RaftLog) Slice(fromIndex uint64) []LogEntry {
	l.mu.RLock()
	defer l.mu.RUnlock()

	sliceIdx, ok := l.toSliceIndex(fromIndex)
	if !ok {
		return nil
	}
	res := make([]LogEntry, len(l.entries)-sliceIdx)
	copy(res, l.entries[sliceIdx:])
	return res
}

func (l *RaftLog) CompactLog(lastIncludedIndex uint64, lastIncludedTerm uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lastIncludedIndex <= l.entries[0].Index {
		return
	}

	offset := lastIncludedIndex - l.entries[0].Index
	var remaining []LogEntry

	if offset < uint64(len(l.entries)) {
		remaining = make([]LogEntry, len(l.entries)-int(offset))
		copy(remaining, l.entries[offset:])
		remaining[0] = LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm}
	} else {
		remaining = []LogEntry{{Index: lastIncludedIndex, Term: lastIncludedTerm}}
	}
	l.entries = remaining
}
