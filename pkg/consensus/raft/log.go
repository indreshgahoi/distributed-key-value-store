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
	if index >= uint64(len(l.entries)) {
		return 0, false
	}
	return l.entries[index].Term, true
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
	if fromIndex >= uint64(len(l.entries)) {
		return nil
	}
	res := make([]LogEntry, len(l.entries)-int(fromIndex))
	copy(res, l.entries[fromIndex:])
	return res
}

// Propose submits a client mutation command to the leader's replicated log.
// Returns (index, term, isLeader).
func (rn *RaftNode) Propose(command []byte) (uint64, uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != RoleLeader {
		return 0, 0, false
	}

	entry := rn.log.Append(rn.currentTerm, command)
	rn.matchIndex[rn.cfg.NodeID] = entry.Index
	rn.nextIndex[rn.cfg.NodeID] = entry.Index + 1

	rn.broadcastAppendEntriesLocked()
	return entry.Index, entry.Term, true
}

// GetState returns current term and leadership boolean.
func (rn *RaftNode) GetState() (uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm, rn.role == RoleLeader
}

// Stop cleanly terminates timers and background routines.
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
}
