package raft

// HardState is the consensus state that must be durable before a node acts
// on it (Raft §5.2). Term and Vote are mandatory for safety; Commit is a
// lower-bound hint that only speeds up recovery.
type HardState struct {
	Term   uint64 `json:"term"`
	Vote   uint64 `json:"vote"`
	Commit uint64 `json:"commit"`
}

// SnapshotMeta identifies the last log entry a snapshot covers.
type SnapshotMeta struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

// Storage is the durable backing store for one Raft node: its HardState, its
// log after the latest snapshot, and that snapshot.
//
// Every method that mutates state must be durable when it returns - Raft
// replies to peers immediately afterwards, and a reply is a promise.
type Storage interface {
	// InitialState returns the persisted HardState and snapshot bounds on boot.
	InitialState() (HardState, SnapshotMeta, error)

	// Save durably persists hs and entries. A zero HardState means "unchanged".
	// entries replace the durable log from entries[0].Index onward: any
	// existing entry at or after that index is discarded, regardless of its
	// term. Conflict detection is the caller's job (RaftLog.TruncateAndAppend
	// returns exactly the suffix to pass here) - passing an already-matching
	// prefix would truncate acknowledged entries.
	Save(hs HardState, entries []LogEntry) error

	// Entries returns the log entries in [low, high), stopping early once
	// maxBytes of payload is exceeded (at least one entry is returned).
	Entries(low, high uint64, maxBytes uint64) ([]LogEntry, error)

	// Term returns the term of the entry at index; the snapshot's last
	// included index is answerable from the snapshot metadata.
	Term(index uint64) (uint64, error)

	// FirstIndex returns the first index still in the log (snapshot index + 1).
	FirstIndex() (uint64, error)

	// LastIndex returns the highest index in the log, or the snapshot index
	// if the log is empty.
	LastIndex() (uint64, error)

	// CreateSnapshot records a snapshot the local state machine took at
	// meta.LastIncludedIndex, which must be in the log, and discards the
	// log up to and including it.
	CreateSnapshot(meta SnapshotMeta, data []byte) error

	// ApplySnapshot installs a snapshot received from the leader. If the log
	// holds an entry at meta.LastIncludedIndex with meta.LastIncludedTerm,
	// entries after it are kept (they are consistent with the snapshot);
	// otherwise the whole log is discarded (Raft §7).
	ApplySnapshot(meta SnapshotMeta, data []byte) error

	// SnapshotData returns the latest snapshot's payload (nil if none).
	SnapshotData() ([]byte, error)

	// Close flushes and releases underlying resources.
	Close() error
}
