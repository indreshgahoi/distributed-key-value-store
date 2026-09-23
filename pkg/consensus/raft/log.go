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
	// Bounds-check in uint64 space before converting to int: if raftIndex is
	// ever a huge underflowed value (e.g. a caller computed nextIndex-1 with
	// nextIndex==0), raftIndex-firstIndex is a huge uint64 whose int(...)
	// conversion wraps around to a negative number. Checking `offset >=
	// len(l.entries)` first guarantees offset is small enough to convert
	// safely - a negative offset can never slip through as "in bounds" once
	// this comparison happens in unsigned space.
	offset := raftIndex - firstIndex
	if offset >= uint64(len(l.entries)) {
		return 0, false // Out of bounds
	}
	return int(offset), true
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

// Append creates and stores a new log entry at the next index.
func (l *RaftLog) Append(term uint64, data []byte) LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	// len(l.entries) alone is only the correct "next index" while
	// entries[0].Index == 0 (nothing ever compacted). Once CompactLog has
	// repositioned entries[0] to a snapshot boundary, the real next index is
	// entries[0].Index + len(entries) - omitting the offset silently
	// computes an index that's too low by entries[0].Index, e.g. Propose()
	// returning the same index twice in a row right after the first
	// snapshot.
	entry := LogEntry{
		Index: l.entries[0].Index + uint64(len(l.entries)),
		Term:  term,
		Data:  data,
	}
	l.entries = append(l.entries, entry)
	return entry
}

// TruncateAndAppend enforces the Raft Log Matching Invariant:
// If an existing entry conflicts with a new one (same index, different terms),
// it deletes the existing entry and all that follow it, then appends the new entries.
//
// It returns the suffix of newEntries that was actually written - everything
// from the first conflicting or previously-absent index onward - or nil if
// every entry was already present. Callers must persist exactly that suffix:
// Storage.Save replaces the durable log from entries[0].Index onward, so
// handing it the raw newEntries of a stale, reordered AppendEntries (an
// already-matching prefix) would truncate acknowledged entries on disk that
// this function correctly kept in memory.
func (l *RaftLog) TruncateAndAppend(prevIndex uint64, newEntries []LogEntry) []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	// idx is a real Raft log index; l.entries is a slice that may start at a
	// nonzero index once compacted. Comparing/indexing idx directly against
	// len(l.entries) (as this used to) is only valid pre-compaction - same
	// class of bug as Append, fixed the same way: subtract entries[0].Index
	// to convert to a slice position before touching l.entries.
	firstIndex := l.entries[0].Index
	for i, entry := range newEntries {
		idx := prevIndex + 1 + uint64(i)
		if idx < firstIndex {
			continue // already compacted away; nothing to conflict-check
		}
		sliceIdx := idx - firstIndex
		if sliceIdx < uint64(len(l.entries)) && l.entries[sliceIdx].Term == entry.Term {
			continue // already present; a duplicate or reordered RPC must not truncate
		}
		// First conflict (or first index past our tail): everything from
		// here on is replaced by the rest of newEntries.
		l.entries = append(l.entries[:sliceIdx], newEntries[i:]...)
		return newEntries[i:]
	}
	return nil
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
