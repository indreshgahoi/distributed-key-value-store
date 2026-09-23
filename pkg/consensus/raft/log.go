package raft

// RaftLog is the in-memory copy of the replicated log.
//
// entries[0] is a sentinel holding the index and term of the last entry
// covered by the most recent snapshot (0/0 before any compaction), so the
// term of the entry just before the first real entry is always known - the
// AppendEntries consistency check needs it.
//
// RaftLog is not safe for concurrent use; RaftNode guards it with rn.mu.
type RaftLog struct {
	entries []LogEntry
}

// NewRaftLog initializes an empty log (sentinel at index 0, term 0).
func NewRaftLog() *RaftLog {
	return &RaftLog{entries: []LogEntry{{Index: 0, Term: 0}}}
}

// SnapshotIndex is the last index covered by the snapshot (the sentinel).
func (l *RaftLog) SnapshotIndex() uint64 { return l.entries[0].Index }

// SnapshotTerm is the term of the last entry covered by the snapshot.
func (l *RaftLog) SnapshotTerm() uint64 { return l.entries[0].Term }

// FirstIndex is the index of the first entry still held in memory.
func (l *RaftLog) FirstIndex() uint64 { return l.entries[0].Index + 1 }

// LastIndex returns the 1-based index of the most recent entry in the log.
func (l *RaftLog) LastIndex() uint64 { return l.entries[len(l.entries)-1].Index }

// LastTerm returns the term of the most recent entry in the log.
func (l *RaftLog) LastTerm() uint64 { return l.entries[len(l.entries)-1].Term }

// toSliceIndex converts a Raft index into a position in l.entries.
func (l *RaftLog) toSliceIndex(raftIndex uint64) (int, bool) {
	firstIndex := l.entries[0].Index
	if raftIndex < firstIndex {
		return 0, false // compacted
	}
	// Compare in uint64 space before converting: a huge (e.g. underflowed)
	// raftIndex would otherwise wrap to a negative int and pass the check.
	offset := raftIndex - firstIndex
	if offset >= uint64(len(l.entries)) {
		return 0, false // not yet written
	}
	return int(offset), true
}

// TermAt returns the term of the entry at index; the snapshot boundary
// itself is answerable from the sentinel.
func (l *RaftLog) TermAt(index uint64) (uint64, bool) {
	idx, ok := l.toSliceIndex(index)
	if !ok {
		return 0, false
	}
	return l.entries[idx].Term, true
}

// Append adds a client command at the next index.
func (l *RaftLog) Append(term uint64, data []byte) LogEntry {
	return l.appendEntry(LogEntry{Term: term, Type: EntryNormal, Data: data})
}

// AppendNoOp adds a leader's no-op entry at the next index.
func (l *RaftLog) AppendNoOp(term uint64) LogEntry {
	return l.appendEntry(LogEntry{Term: term, Type: EntryNoOp})
}

func (l *RaftLog) appendEntry(e LogEntry) LogEntry {
	e.Index = l.LastIndex() + 1
	l.entries = append(l.entries, e)
	return e
}

// TruncateAndAppend enforces the Raft Log Matching Invariant: an existing
// entry that conflicts with a new one (same index, different term) is
// deleted along with everything after it, then the new entries are appended.
//
// It returns the suffix of newEntries that was actually written - everything
// from the first conflicting or previously-absent index onward - or nil if
// every entry was already present. Callers must persist exactly that suffix:
// Storage.Save replaces the durable log from entries[0].Index onward, so
// handing it the raw newEntries of a stale, reordered AppendEntries (an
// already-matching prefix) would truncate acknowledged entries on disk that
// this function correctly kept in memory.
func (l *RaftLog) TruncateAndAppend(prevIndex uint64, newEntries []LogEntry) []LogEntry {
	firstIndex := l.entries[0].Index
	for i, entry := range newEntries {
		idx := prevIndex + 1 + uint64(i)
		if idx <= firstIndex {
			// Covered by our snapshot: committed, so it cannot conflict.
			continue
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

// Slice returns a copy of all entries from fromIndex to the end.
func (l *RaftLog) Slice(fromIndex uint64) []LogEntry {
	return l.SliceN(fromIndex, 0)
}

// SliceN returns a copy of at most max entries (0 = unlimited) starting at
// fromIndex, or nil if fromIndex is compacted (the sentinel included - it is
// bookkeeping, not a shippable entry) or past the end.
func (l *RaftLog) SliceN(fromIndex uint64, max int) []LogEntry {
	sliceIdx, ok := l.toSliceIndex(fromIndex)
	if !ok || sliceIdx == 0 {
		return nil
	}
	tail := l.entries[sliceIdx:]
	if max > 0 && len(tail) > max {
		tail = tail[:max]
	}
	res := make([]LogEntry, len(tail))
	copy(res, tail)
	return res
}

// FirstIndexOfTerm walks back from index to the first entry that shares its
// term - the ConflictIndex hint a follower sends on a term mismatch.
func (l *RaftLog) FirstIndexOfTerm(index uint64) uint64 {
	idx, ok := l.toSliceIndex(index)
	if !ok {
		return index
	}
	term := l.entries[idx].Term
	for idx > 1 && l.entries[idx-1].Term == term {
		idx--
	}
	return l.entries[idx].Index
}

// LastIndexOfTerm returns the last index holding an entry of the given term,
// or 0 if the log (as held in memory) has none.
func (l *RaftLog) LastIndexOfTerm(term uint64) uint64 {
	for i := len(l.entries) - 1; i >= 1; i-- {
		switch t := l.entries[i].Term; {
		case t == term:
			return l.entries[i].Index
		case t < term:
			return 0 // terms only decrease going back
		}
	}
	return 0
}

// CompactLog discards entries up to and including lastIncludedIndex,
// keeping any that follow. Used when those entries are known to match the
// snapshot's history.
func (l *RaftLog) CompactLog(lastIncludedIndex uint64, lastIncludedTerm uint64) {
	if lastIncludedIndex <= l.entries[0].Index {
		return
	}
	offset := lastIncludedIndex - l.entries[0].Index
	var remaining []LogEntry
	if offset < uint64(len(l.entries)) {
		remaining = make([]LogEntry, len(l.entries)-int(offset))
		copy(remaining, l.entries[offset:])
	} else {
		remaining = make([]LogEntry, 1)
	}
	remaining[0] = LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm}
	l.entries = remaining
}

// ResetToSnapshot discards the whole log: used when an installed snapshot
// doesn't match our history at its boundary (Raft §7).
func (l *RaftLog) ResetToSnapshot(lastIncludedIndex, lastIncludedTerm uint64) {
	l.entries = []LogEntry{{Index: lastIncludedIndex, Term: lastIncludedTerm}}
}
