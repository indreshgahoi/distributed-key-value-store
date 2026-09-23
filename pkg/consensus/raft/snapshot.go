package raft

import (
	"context"
	"fmt"
	"time"
)

// Snapshot tells Raft the state machine has captured its full state as of
// appliedIndex, so the log up to that point can be discarded (Raft §7).
//
// appliedIndex must be one the state machine has reported via ReportApplied,
// and data must reflect exactly the entries up to it - the simplest way to
// guarantee both is to call Snapshot from the goroutine that applies entries.
func (rn *RaftNode) Snapshot(appliedIndex uint64, data []byte) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if appliedIndex <= rn.log.SnapshotIndex() {
		return ErrSnapshotOutOfDate
	}
	// Compacting past what the state machine has confirmed would discard
	// log entries whose effects the snapshot data doesn't contain.
	if appliedIndex > rn.lastApplied {
		return ErrSnapshotAheadOfApplied
	}
	term, ok := rn.log.TermAt(appliedIndex)
	if !ok {
		return fmt.Errorf("raft: no log entry at snapshot index %d", appliedIndex)
	}

	// Persist first, then compact memory: if the write fails, the in-memory
	// log still matches what's on disk.
	if err := rn.storage.CreateSnapshot(SnapshotMeta{LastIncludedIndex: appliedIndex, LastIncludedTerm: term}, data); err != nil {
		return fmt.Errorf("raft: persisting snapshot failed: %w", err)
	}
	rn.log.CompactLog(appliedIndex, term)
	rn.snapshotData = data
	return nil
}

// sendSnapshotLocked ships the latest snapshot to a follower that needs
// entries already compacted away. At most one is in flight per peer:
// heartbeats would otherwise launch a new multi-megabyte transfer every
// interval while the first was still on the wire.
func (rn *RaftNode) sendSnapshotLocked(peer uint64) {
	if rn.snapshotInFlight[peer] {
		return
	}
	rn.snapshotInFlight[peer] = true
	args := &InstallSnapshotArgs{
		Term:              rn.currentTerm,
		LeaderID:          rn.cfg.NodeID,
		LastIncludedIndex: rn.log.SnapshotIndex(),
		LastIncludedTerm:  rn.log.SnapshotTerm(),
		Data:              rn.snapshotData,
	}
	go func() {
		// Snapshots are large; allow more time than a normal RPC.
		ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout*5)
		defer cancel()
		reply, err := rn.transport.SendInstallSnapshot(ctx, peer, args)

		rn.mu.Lock()
		defer rn.mu.Unlock()
		delete(rn.snapshotInFlight, peer)
		if err != nil || rn.stepDownIfStaleLocked(reply.Term) {
			return
		}
		if rn.role != RoleLeader || args.Term != rn.currentTerm || !reply.Success {
			return
		}
		rn.matchIndex[peer] = max(rn.matchIndex[peer], args.LastIncludedIndex)
		rn.nextIndex[peer] = max(rn.nextIndex[peer], args.LastIncludedIndex+1)
		rn.advanceCommitIndexLocked()
	}()
}

// HandleInstallSnapshot replaces a lagging follower's state with the leader's snapshot.
func (rn *RaftNode) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	reply.Term = rn.currentTerm
	reply.Success = false
	if rn.stoppedLocked() || args.Term < rn.currentTerm {
		return
	}
	if !rn.becomeFollowerLocked(args.Term, args.LeaderID) {
		return
	}
	reply.Term = rn.currentTerm
	rn.lastHeartBeat = time.Now()

	// Already have this state (applied, or committed and on its way to the
	// state machine): installing it would only roll us backwards.
	if args.LastIncludedIndex <= rn.commitIndex {
		reply.Success = true
		return
	}

	meta := SnapshotMeta{LastIncludedIndex: args.LastIncludedIndex, LastIncludedTerm: args.LastIncludedTerm}
	if err := rn.storage.ApplySnapshot(meta, args.Data); err != nil {
		rn.haltLocked(fmt.Errorf("raft: installing snapshot failed: %w", err))
		return
	}

	// Raft §7: if we hold the snapshot's last entry, entries after it are
	// still valid; otherwise our log diverges from the leader's history
	// there and must be discarded wholesale. Storage makes the same call.
	if term, ok := rn.log.TermAt(meta.LastIncludedIndex); ok && term == meta.LastIncludedTerm {
		rn.log.CompactLog(meta.LastIncludedIndex, meta.LastIncludedTerm)
	} else {
		rn.log.ResetToSnapshot(meta.LastIncludedIndex, meta.LastIncludedTerm)
	}
	rn.snapshotData = args.Data
	rn.commitIndex = meta.LastIncludedIndex
	rn.commitSignal.broadcast()

	// Delivered through the same ordered queue as commands, after anything
	// already queued, so it can never be overtaken by (or overtake) them.
	rn.lastDispatched = meta.LastIncludedIndex
	rn.enqueueApplyLocked(applyItem{msg: ApplyMsg{
		CommandValid: false,
		CommandIndex: meta.LastIncludedIndex,
		CommandTerm:  meta.LastIncludedTerm,
		Command:      args.Data,
	}})
	reply.Success = true
}
