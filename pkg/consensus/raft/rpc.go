package raft

// RequestVoteArgs contains the payload sent by candidates to request votes.
type RequestVoteArgs struct {
	// Term is the candidate's term - for a Pre-Vote, the term it *would* use.
	Term uint64

	// CandidateID is the unique identifier of the candidate requesting the vote.
	CandidateID uint64

	// LastLogIndex is the index of the candidate's last log entry.
	LastLogIndex uint64

	// LastLogTerm is the term of the candidate's last log entry.
	LastLogTerm uint64

	// IsPreVote marks a non-binding trial vote (Raft §9.6).
	IsPreVote bool
}

// RequestVoteReply contains the voter's decision.
type RequestVoteReply struct {
	// Term is the voter's current term (allows candidate to step down if out-of-date).
	Term uint64

	// VoteGranted is true if the candidate received the vote.
	VoteGranted bool
}

// AppendEntriesArgs contains log entries for replication and empty heartbeats.
type AppendEntriesArgs struct {
	// Term is the leader's current term.
	Term uint64

	// LeaderID allows followers to redirect clients to the current leader.
	LeaderID uint64

	// PrevLogIndex is the index of the log entry immediately preceding the new entries.
	PrevLogIndex uint64

	// PrevLogTerm is the term of the PrevLogIndex entry.
	PrevLogTerm uint64

	// Entries contains log entries to replicate (empty for periodic heartbeats).
	Entries []LogEntry

	// LeaderCommit is the leader's commitIndex.
	LeaderCommit uint64
}

// AppendEntriesReply contains the follower's replication status.
type AppendEntriesReply struct {
	// Term is the follower's current term.
	Term uint64

	// Success is true if follower contained an entry matching PrevLogIndex and PrevLogTerm.
	Success bool

	// ConflictTerm and ConflictIndex are the follower's hint on rejection, so
	// the leader can skip back a whole term per round trip instead of one
	// entry (Raft §5.3). ConflictTerm is the term of the follower's entry at
	// PrevLogIndex (0 if it has no entry there); ConflictIndex is the first
	// index the leader should consider resending from.
	ConflictTerm  uint64
	ConflictIndex uint64
}

// InstallSnapshotArgs carries a full state machine snapshot from the Leader
// to a follower whose required log entries were already compacted away.
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

	// Success is false if the follower could not durably install the
	// snapshot, so the leader must not count it as caught up.
	Success bool
}
