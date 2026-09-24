// Package replica is the key-value state machine driven by one Raft group:
// the replicated command format, the single-writer applier (with snapshots
// and store compaction), and the tracker that tells a writer when its
// command was applied. cmd/kv-server runs one replica; a multi-range node
// (pkg/sharding) runs one per range.
package replica

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

// ErrKeyOutOfRange rejects a command for a key this replica doesn't own - a
// write that was routed with a stale routing table.
var ErrKeyOutOfRange = errors.New("replica: key is outside this replica's range")

// Config configures a StateMachine.
type Config struct {
	Store     *mvcc.Store
	Node      *raft.RaftNode
	Proposals *ProposalTracker

	// SnapshotEvery hands Raft a snapshot after this many applied entries.
	SnapshotEvery uint64

	// Owns reports whether a key belongs to this replica. Commands for other
	// keys are rejected at apply time - deterministically, on every replica.
	// Nil accepts every key.
	Owns func(key []byte) bool

	// Name labels log lines (e.g. "range 3"); optional.
	Name string
}

// StateMachine applies committed Raft entries to the MVCC store.
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
type StateMachine struct {
	cfg          Config
	lastSnapshot uint64
	// liveAfterCompact is the store size right after the last compaction.
	// The next one waits until usage has grown well past it, so a store whose
	// live data alone is near the threshold doesn't recompact on every write.
	liveAfterCompact uint64
}

// New creates a state machine; call Run to start applying.
func New(cfg Config) *StateMachine {
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = 10000
	}
	return &StateMachine{cfg: cfg}
}

// Run applies messages until the node stops. A non-nil error means the state
// machine could not apply a committed entry and the replica must stop:
// skipping it would make this replica diverge from the others.
func (sm *StateMachine) Run(applyCh <-chan raft.ApplyMsg) error {
	for {
		var msg raft.ApplyMsg
		select {
		case msg = <-applyCh:
		case <-sm.cfg.Node.Done():
			return nil
		}
		if err := sm.apply(msg); err != nil {
			return fmt.Errorf("%sapplying index %d: %w", sm.prefix(), msg.CommandIndex, err)
		}
		// Report only once the store reflects the entry: this gates
		// linearizable reads (WaitApplied) and log compaction (Snapshot).
		sm.cfg.Node.ReportApplied(msg.CommandIndex)
		sm.maybeSnapshot(msg.CommandIndex)
	}
}

func (sm *StateMachine) prefix() string {
	if sm.cfg.Name == "" {
		return ""
	}
	return sm.cfg.Name + ": "
}

func (sm *StateMachine) apply(msg raft.ApplyMsg) error {
	store := sm.cfg.Store
	if !msg.CommandValid {
		// A snapshot from the leader (or our own, on restart) replaces all state.
		if err := store.RestoreSnapshot(bytes.NewReader(msg.Command)); err != nil {
			return fmt.Errorf("restoring snapshot: %w", err)
		}
		sm.lastSnapshot = msg.CommandIndex
		sm.liveAfterCompact, _, _ = store.MemoryUsage() // a restore is a fresh, compact engine
		log.Printf("[state machine] %srestored snapshot at index %d", sm.prefix(), msg.CommandIndex)
		return nil
	}

	cmd, err := DecodeCommand(msg.Command)
	if err == nil && sm.cfg.Owns != nil && !sm.cfg.Owns([]byte(cmd.Key)) {
		err = fmt.Errorf("%w: %q", ErrKeyOutOfRange, cmd.Key)
	}
	if err != nil {
		// Deterministic: every replica rejects the same bytes the same way.
		sm.cfg.Proposals.Notify(msg.CommandIndex, msg.CommandTerm, err)
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
		return err // store genuinely full (live data exceeds its budget) or broken
	}
	sm.cfg.Proposals.Notify(msg.CommandIndex, msg.CommandTerm, nil)

	if sm.shouldCompact() {
		return sm.compact(msg.CommandIndex)
	}
	return nil
}

func (sm *StateMachine) execute(cmd Command, index uint64) error {
	switch cmd.Op {
	case OpPut:
		return sm.cfg.Store.Put([]byte(cmd.Key), []byte(cmd.Value), index)
	case OpDelete:
		return sm.cfg.Store.Delete([]byte(cmd.Key), index)
	}
	return fmt.Errorf("unknown op %q", cmd.Op)
}

// shouldCompact: usage is past the threshold and at least half of the space
// left after the previous compaction has since been consumed.
func (sm *StateMachine) shouldCompact() bool {
	used, capacity, ok := sm.cfg.Store.MemoryUsage()
	if !ok || used <= sm.liveAfterCompact || float64(used) <= compactAtUsage*float64(capacity) {
		return false
	}
	return used-sm.liveAfterCompact > (capacity-sm.liveAfterCompact)/2
}

// compact rebuilds the store keeping only what's visible at watermark (reads
// are always served at the latest applied index, so older versions are dead).
func (sm *StateMachine) compact(watermark uint64) error {
	before, capacity, _ := sm.cfg.Store.MemoryUsage()
	if err := sm.cfg.Store.Compact(watermark); err != nil {
		return fmt.Errorf("compacting store: %w", err)
	}
	after, _, _ := sm.cfg.Store.MemoryUsage()
	sm.liveAfterCompact = after
	log.Printf("[state machine] %scompacted store: %d -> %d of %d bytes", sm.prefix(), before, after, capacity)
	return nil
}

// maybeSnapshot hands Raft a snapshot every SnapshotEvery entries so the log
// (and restart replay time) stays bounded. Failure is not fatal: the log is
// simply kept until the next attempt.
func (sm *StateMachine) maybeSnapshot(index uint64) {
	if index-sm.lastSnapshot < sm.cfg.SnapshotEvery {
		return
	}
	var buf bytes.Buffer
	if err := sm.cfg.Store.ExportSnapshot(&buf, index); err != nil {
		log.Printf("[state machine] %ssnapshot export at %d failed: %v", sm.prefix(), index, err)
		return
	}
	if err := sm.cfg.Node.Snapshot(index, buf.Bytes()); err != nil && !errors.Is(err, raft.ErrSnapshotOutOfDate) {
		log.Printf("[state machine] %ssnapshot at %d failed: %v", sm.prefix(), index, err)
		return
	}
	sm.lastSnapshot = index
}
