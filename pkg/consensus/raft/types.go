// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout).
// It manages a replicated log and coordinates state machine mutations across nodes.
//
// Beyond the core paper it implements Pre-Vote (§9.6), log compaction and
// InstallSnapshot (§7), Read Index linearizable reads (§6.4), a no-op entry
// on election (§8), and fast log backtracking via conflict hints (§5.3).
// See docs/architecture.md for how the pieces fit together.
package raft

// NodeRole defines the consensus lifecycle state of a replica.
type NodeRole int

const (
	// RoleFollower is the passive state. Replicas accept AppendEntries from leaders
	// and RequestVote from candidates. If no heartbeat is received before the
	// election timeout elapses, the follower starts a Pre-Vote.
	RoleFollower NodeRole = iota

	// RolePreCandidate runs a trial election that does not increment
	// currentTerm, so a partitioned node can't disrupt a healthy leader.
	RolePreCandidate

	// RoleCandidate is the real election state: the node increments its term,
	// votes for itself, and asks peers for binding votes.
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
	case RolePreCandidate:
		return "PreCandidate"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	default:
		return "Unknown"
	}
}

// EntryType distinguishes client commands from entries Raft itself writes.
type EntryType uint8

const (
	// EntryNormal carries a client command, delivered to the state machine.
	EntryNormal EntryType = iota

	// EntryNoOp is appended by every new leader (Raft §8). Committing an
	// entry from its own term is what lets a leader learn which earlier
	// entries are committed - required before it can serve ReadIndex reads.
	// No-ops are never delivered on applyCh.
	EntryNoOp
)

// LogEntry represents an atomic mutation instruction in the replicated log.
type LogEntry struct {
	// Index is the 1-based sequential position of this entry in the log.
	Index uint64

	// Term is the consensus epoch in which this entry was proposed by the leader.
	Term uint64

	// Type says whether Data is a client command or an internal entry.
	Type EntryType

	// Data contains the serialized state machine command (e.g. Put/Delete for MVCC).
	Data []byte
}

// ApplyMsg communicates committed commands (or a snapshot) to the state machine.
//
// Messages arrive in log order. After processing each one - successfully or
// not - the state machine must call RaftNode.ReportApplied(CommandIndex).
type ApplyMsg struct {
	// CommandValid is true for a client command; false means Command holds a
	// snapshot that must replace the state machine's entire state.
	CommandValid bool

	// CommandIndex is the 1-based log index of the committed entry (or the
	// last index a snapshot covers).
	CommandIndex uint64

	// CommandTerm is the term during which the entry was proposed.
	CommandTerm uint64

	// Command is the raw command (or snapshot) payload.
	Command []byte
}
