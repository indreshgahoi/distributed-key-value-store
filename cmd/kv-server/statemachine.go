package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

// compactAtUsage is the store memory fraction that triggers a compaction.
const compactAtUsage = 0.75

// stateMachine applies committed Raft entries to the MVCC store.
//
// Run is the store's only writer. That single-writer design is what makes
// the rest simple and correct: a snapshot taken here reflects exactly the
// entries applied so far (no apply can interleave), and compaction can't
// race a write.
//
// Timestamps: each command's MVCC version is its Raft log index. The index
// is already a total order agreed by every replica, so versions are
// identical everywhere without any clock. (Milestone 4 replaces this with a
// Hybrid Logical Clock for cross-range transactions.)
type stateMachine struct {
	store         *mvcc.Store
	node          *raft.RaftNode
	proposals     *ProposalTracker
	snapshotEvery uint64
	lastSnapshot  uint64
	// liveAfterCompact is the store size right after the last compaction.
	// The next one waits until usage has grown well past it, so a store whose
	// live data alone is near the threshold doesn't recompact on every write.
	liveAfterCompact uint64
}

// Run applies messages until the node stops. A non-nil error means the state
// machine could not apply a committed entry and the process must stop:
// skipping it would make this replica diverge from the others.
func (sm *stateMachine) Run(applyCh <-chan raft.ApplyMsg) error {
	for {
		var msg raft.ApplyMsg
		select {
		case msg = <-applyCh:
		case <-sm.node.Done():
			return nil
		}
		if err := sm.apply(msg); err != nil {
			return fmt.Errorf("applying index %d: %w", msg.CommandIndex, err)
		}
		// Report only once the store reflects the entry: this gates
		// linearizable reads (WaitApplied) and log compaction (Snapshot).
		sm.node.ReportApplied(msg.CommandIndex)
		sm.maybeSnapshot(msg.CommandIndex)
	}
}

func (sm *stateMachine) apply(msg raft.ApplyMsg) error {
	if !msg.CommandValid {
		// A snapshot from the leader (or our own, on restart) replaces all state.
		if err := sm.store.RestoreSnapshot(bytes.NewReader(msg.Command)); err != nil {
			return fmt.Errorf("restoring snapshot: %w", err)
		}
		sm.lastSnapshot = msg.CommandIndex
		sm.liveAfterCompact, _, _ = sm.store.MemoryUsage() // a restore is a fresh, compact engine
		log.Printf("[state machine] restored snapshot at index %d", msg.CommandIndex)
		return nil
	}

	cmd, err := DecodeCommand(msg.Command)
	if err != nil {
		// Deterministic: every replica rejects the same bytes the same way.
		sm.proposals.Notify(msg.CommandIndex, msg.CommandTerm, err)
		return nil
	}
	err = sm.execute(cmd, msg.CommandIndex)
	if errors.Is(err, raw.ErrArenaFull) {
		// Reclaim dead versions, then retry once.
		if cerr := sm.compact(msg.CommandIndex - 1); cerr != nil {
			return cerr
		}
		err = sm.execute(cmd, msg.CommandIndex)
	}
	if err != nil {
		return err // store genuinely full (live data exceeds --memtable-bytes) or broken
	}
	sm.proposals.Notify(msg.CommandIndex, msg.CommandTerm, nil)

	if sm.shouldCompact() {
		return sm.compact(msg.CommandIndex)
	}
	return nil
}

// shouldCompact: usage is past the threshold and at least half of the space
// left after the previous compaction has since been consumed.
func (sm *stateMachine) shouldCompact() bool {
	used, capacity, ok := sm.store.MemoryUsage()
	if !ok || used <= sm.liveAfterCompact || float64(used) <= compactAtUsage*float64(capacity) {
		return false
	}
	return used-sm.liveAfterCompact > (capacity-sm.liveAfterCompact)/2
}

func (sm *stateMachine) execute(cmd Command, index uint64) error {
	switch cmd.Op {
	case OpPut:
		return sm.store.Put([]byte(cmd.Key), []byte(cmd.Value), index)
	case OpDelete:
		return sm.store.Delete([]byte(cmd.Key), index)
	}
	return fmt.Errorf("unknown op %q", cmd.Op)
}

// compact rebuilds the store keeping only what's visible at watermark (reads
// are always served at the latest applied index, so older versions are dead).
func (sm *stateMachine) compact(watermark uint64) error {
	before, capacity, _ := sm.store.MemoryUsage()
	if err := sm.store.Compact(watermark); err != nil {
		return fmt.Errorf("compacting store: %w", err)
	}
	after, _, _ := sm.store.MemoryUsage()
	sm.liveAfterCompact = after
	log.Printf("[state machine] compacted store: %d -> %d of %d bytes", before, after, capacity)
	return nil
}

// maybeSnapshot hands Raft a snapshot every snapshotEvery entries so the log
// (and restart replay time) stays bounded. Failure is not fatal: the log is
// simply kept until the next attempt.
func (sm *stateMachine) maybeSnapshot(index uint64) {
	if index-sm.lastSnapshot < sm.snapshotEvery {
		return
	}
	var buf bytes.Buffer
	if err := sm.store.ExportSnapshot(&buf, index); err != nil {
		log.Printf("[state machine] snapshot export at %d failed: %v", index, err)
		return
	}
	if err := sm.node.Snapshot(index, buf.Bytes()); err != nil && !errors.Is(err, raft.ErrSnapshotOutOfDate) {
		log.Printf("[state machine] snapshot at %d failed: %v", index, err)
		return
	}
	sm.lastSnapshot = index
}
