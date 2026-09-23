package raft

import "errors"

// Storage errors.
var (
	// ErrCompacted is returned when requesting log entries that were pruned by a snapshot.
	ErrCompacted = errors.New("raft/storage: requested entry has been compacted by snapshot")

	// ErrUnavailable is returned when requesting log entries past the latest known index.
	ErrUnavailable = errors.New("raft/storage: requested entry is not yet available in log")

	// ErrSnapshotOutOfDate is returned when attempting to apply a snapshot older than current state.
	ErrSnapshotOutOfDate = errors.New("raft/storage: snapshot is older than current storage watermark")

	// ErrIncompatibleFormat is returned when a data directory was written by an
	// incompatible version of the on-disk format.
	ErrIncompatibleFormat = errors.New("raft/storage: data directory uses an incompatible on-disk format")
)

// Node errors.
var (
	// ErrNotLeader is returned by leader-only operations (e.g. ReadIndex) when
	// called against a node that is not currently the Leader.
	ErrNotLeader = errors.New("raft: node is not the leader")

	// ErrQuorumUnreachable is returned by ReadIndex when a quorum of peers
	// could not be confirmed to still recognize this node as leader before
	// the caller's context expired - e.g. this node is the Leader of a
	// minority partition and doesn't know it yet.
	ErrQuorumUnreachable = errors.New("raft: could not confirm leadership with a quorum before deadline")

	// ErrSnapshotAheadOfApplied is returned by Snapshot when asked to compact
	// past the index the state machine has confirmed via ReportApplied.
	ErrSnapshotAheadOfApplied = errors.New("raft: snapshot index is beyond the state machine's reported applied index")

	// ErrStopped is returned by operations on a node that has been stopped,
	// either by Stop or because it halted on a storage failure (see Err).
	ErrStopped = errors.New("raft: node is stopped")
)
