package raft

import (
	"context"
	"time"
)

// maxEntriesPerAppend bounds a single AppendEntries message, so a follower
// that is far behind is caught up in bounded-size steps.
const maxEntriesPerAppend = 256

// Propose submits a client command to the leader's replicated log.
// It returns once the entry is durable locally - not when it commits.
// Returns (index, term, isLeader); isLeader is false on followers and on a
// node that has stopped.
func (rn *RaftNode) Propose(command []byte) (uint64, uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.role != RoleLeader || rn.stoppedLocked() {
		return 0, 0, false
	}
	entry := rn.log.Append(rn.currentTerm, command)
	rn.matchIndex[rn.cfg.NodeID] = entry.Index
	if !rn.persistLocked([]LogEntry{entry}) {
		return 0, 0, false
	}
	rn.broadcastAppendEntriesLocked()
	return entry.Index, entry.Term, true
}

// broadcastAppendEntriesLocked replicates to (or heartbeats) every follower.
func (rn *RaftNode) broadcastAppendEntriesLocked() {
	rn.forEachPeer(rn.replicateToLocked)
}

// replicateToLocked sends peer whatever it's missing: log entries from its
// nextIndex, or the snapshot if those entries were already compacted away.
func (rn *RaftNode) replicateToLocked(peer uint64) {
	next := rn.nextIndex[peer]
	if next <= rn.log.SnapshotIndex() {
		rn.sendSnapshotLocked(peer)
		return
	}
	prevIndex := next - 1
	prevTerm, _ := rn.log.TermAt(prevIndex) // prevIndex >= SnapshotIndex, so known
	args := &AppendEntriesArgs{
		Term:         rn.currentTerm,
		LeaderID:     rn.cfg.NodeID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      rn.log.SliceN(next, maxEntriesPerAppend),
		LeaderCommit: rn.commitIndex,
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout)
		defer cancel()
		reply, err := rn.transport.SendAppendEntries(ctx, peer, args)
		if err != nil {
			return
		}
		rn.mu.Lock()
		defer rn.mu.Unlock()
		rn.handleAppendEntriesReplyLocked(peer, args, reply)
	}()
}

func (rn *RaftNode) handleAppendEntriesReplyLocked(peer uint64, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rn.stepDownIfStaleLocked(reply.Term) {
		return
	}
	if rn.role != RoleLeader || args.Term != rn.currentTerm {
		return // reply to a term we're no longer leading
	}

	if reply.Success {
		// Replies can arrive out of order, so progress only moves forward.
		match := args.PrevLogIndex + uint64(len(args.Entries))
		rn.matchIndex[peer] = max(rn.matchIndex[peer], match)
		rn.nextIndex[peer] = max(rn.nextIndex[peer], match+1)
		rn.advanceCommitIndexLocked()
		return
	}

	// Log mismatch: back up using the follower's conflict hint.
	next := reply.ConflictIndex
	if reply.ConflictTerm != 0 {
		if last := rn.log.LastIndexOfTerm(reply.ConflictTerm); last > 0 {
			next = last + 1 // we share that term: resume right after our last entry of it
		}
	}
	// Never back up past what the peer is known to hold, never beyond our log.
	next = max(next, rn.matchIndex[peer]+1, 1)
	rn.nextIndex[peer] = min(next, rn.log.LastIndex()+1)
}

// advanceCommitIndexLocked commits the highest index stored on a quorum -
// but only if it's from the current term (Raft §5.4.2, Figure 8): an entry
// from an earlier term can be on a majority and still be overwritten by a
// future leader, so it is only committed indirectly, by committing a later
// current-term entry (which is why new leaders append a no-op).
func (rn *RaftNode) advanceCommitIndexLocked() {
	for n := rn.log.LastIndex(); n > rn.commitIndex; n-- {
		if term, _ := rn.log.TermAt(n); term != rn.currentTerm {
			break // terms only decrease going back; nothing lower qualifies
		}
		replicas := 1 // the leader itself
		rn.forEachPeer(func(p uint64) {
			if rn.matchIndex[p] >= n {
				replicas++
			}
		})
		if replicas >= rn.quorum() {
			rn.setCommitIndexLocked(n)
			return
		}
	}
}

func (rn *RaftNode) setCommitIndexLocked(index uint64) {
	rn.commitIndex = index
	rn.commitSignal.broadcast()
	rn.scheduleApplyLocked()
}

// HandleAppendEntries processes replication and heartbeats from the leader.
func (rn *RaftNode) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	reply.Term = rn.currentTerm
	reply.Success = false
	if rn.stoppedLocked() || args.Term < rn.currentTerm {
		return
	}
	// A valid leader exists for args.Term: follow it (this also demotes a
	// candidate or pre-candidate of the same term) - persisted before replying.
	if !rn.becomeFollowerLocked(args.Term, args.LeaderID) {
		return
	}
	reply.Term = rn.currentTerm
	rn.lastHeartBeat = time.Now()

	if !rn.logMatchesLocked(args, reply) {
		return
	}

	// Persist only what TruncateAndAppend actually wrote, never the raw
	// args.Entries: Save replaces the durable log from entries[0].Index
	// onward, so a stale, reordered RPC whose entries we already hold would
	// otherwise truncate acknowledged entries on disk.
	written := rn.log.TruncateAndAppend(args.PrevLogIndex, args.Entries)
	if !rn.persistLocked(written) {
		return
	}
	reply.Success = true

	// Commit up to what the leader has committed, but only as far as this
	// RPC proves our log matches the leader's.
	lastNew := args.PrevLogIndex + uint64(len(args.Entries))
	if newCommit := min(args.LeaderCommit, lastNew); newCommit > rn.commitIndex {
		rn.setCommitIndexLocked(newCommit)
	}
}

// logMatchesLocked is the AppendEntries consistency check: do we hold the
// entry at PrevLogIndex with PrevLogTerm? On failure it fills in the
// conflict hint for the leader.
func (rn *RaftNode) logMatchesLocked(args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	if args.PrevLogIndex < rn.log.SnapshotIndex() {
		// Everything through our snapshot is committed and therefore matches;
		// ask the leader to resume just past it.
		reply.ConflictIndex = rn.log.SnapshotIndex() + 1
		return false
	}
	if args.PrevLogIndex > rn.log.LastIndex() {
		reply.ConflictIndex = rn.log.LastIndex() + 1
		return false
	}
	if term, _ := rn.log.TermAt(args.PrevLogIndex); term != args.PrevLogTerm {
		reply.ConflictTerm = term
		reply.ConflictIndex = rn.log.FirstIndexOfTerm(args.PrevLogIndex)
		return false
	}
	return true
}
