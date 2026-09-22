package raft

// InstallSnapshotArgs carries snapshot chunks or bounds from the Leader.
type InstallSnapshotArgs struct {
	Term              uint64
	LeaderID          uint64
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Data              []byte
}

// InstallSnapshotReply response to InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term uint64
}
