// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout).
// It manages a replicated log and coordinates state machine mutations across nodes.
package raft

// NodeRole defines the consensus lifecycle state of a replica.
type NodeRole int

const (
	// RoleFollower is the passive state. Replicas accept AppendEntries from leaders
	// and RequestVote from candidates. If no heartbeat is received before the
	// election timeout elapses, the follower transitions to RoleCandidate.
	RoleFollower NodeRole = iota

	// RoleCandidate is the election state. The node increments its term, votes for
	// itself, and broadcasts RequestVote RPCs to gather a majority quorum.
	RoleCandidate

	// RoleLeader manages all client writes, appends entries to its local log,
	// and drives log replication via AppendEntries RPCs.
	RoleLeader
)

// String implements the fmt.Stringer interface.
func (r NodeRole) String() string {
	switch r {
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// LogEntry represents an atomic mutation instruction in the replicated log.
type LogEntry struct {
	// Index is the 1-based sequential position of this entry in the log.
	Index uint64

	// Term is the consensus epoch in which this entry was proposed by the leader.
	Term uint64

	// Data contains the serialized state machine command (e.g. Put/Delete for MVCC).
	Data []byte
}

// RequestVoteArgs contains the payload sent by candidates to request votes.
type RequestVoteArgs struct {
	// Term is the candidate's current term.
	Term uint64

	// CandidateID is the unique identifier of the candidate requesting the vote.
	CandidateID uint64

	// LastLogIndex is the index of the candidate's last log entry.
	LastLogIndex uint64

	// LastLogTerm is the term of the candidate's last log entry.
	LastLogTerm uint64
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

	// MatchIndex is the highest log index successfully synchronized on this follower.
	MatchIndex uint64
}

// ApplyMsg communicates committed, linearizable commands to the state machine (Milestone 1 MVCC).
type ApplyMsg struct {
	// CommandValid indicates whether Command contains a valid state machine payload.
	CommandValid bool

	// CommandIndex is the 1-based log index of the committed entry.
	CommandIndex uint64

	// CommandTerm is the term during which the entry was proposed.
	CommandTerm uint64

	// Command is the raw byte payload applied to the storage engine.
	Command []byte
}
