package raft

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

func newKVStorage(t *testing.T) *KVStorage {
	t.Helper()
	s, err := NewKVStorage(raw.NewSkipListEngine(8 << 20))
	if err != nil {
		t.Fatalf("NewKVStorage: %v", err)
	}
	return s
}

// TestRaft_NewLeaderCommitsNoOpBeforeServingReads checks Raft §8: a new
// leader appends a no-op in its own term, commits it, and only then serves
// ReadIndex - and the state machine never sees the no-op.
func TestRaft_NewLeaderCommitsNoOpBeforeServingReads(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, term, err := tc.FindLeader(2 * time.Second)
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}
	leader := tc.nodes[leaderID]

	leader.mu.Lock()
	firstTerm, _ := leader.log.TermAt(1)
	firstType := leader.log.entries[1].Type
	leader.mu.Unlock()
	if firstType != EntryNoOp || firstTerm != term {
		t.Fatalf("expected index 1 to be a no-op from term %d, got type=%d term=%d", term, firstType, firstTerm)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	readIndex, err := leader.ReadIndex(ctx)
	if err != nil {
		t.Fatalf("ReadIndex on a fresh leader: %v", err)
	}
	if readIndex < 1 {
		t.Fatalf("ReadIndex must cover the committed no-op, got %d", readIndex)
	}
	// The no-op is applied by Raft itself, so lastApplied advances without
	// the state machine ever receiving it.
	if err := leader.WaitApplied(ctx, readIndex); err != nil {
		t.Fatalf("no-op was never marked applied: %v", err)
	}
}

// TestRaft_FollowerReturnsConflictHints checks the follower side of fast
// log backtracking (Raft §5.3).
func TestRaft_FollowerReturnsConflictHints(t *testing.T) {
	node, err := NewRaftNode(quietFollowerConfig(), NewSimulatedNetwork(), newKVStorage(t), make(chan ApplyMsg, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	// Log: indices 1-3 in term 1, 4-6 in term 2.
	node.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: makeEntries(1, 3, 1)}, &AppendEntriesReply{})
	node.HandleAppendEntries(&AppendEntriesArgs{Term: 2, LeaderID: 1, PrevLogIndex: 3, PrevLogTerm: 1, Entries: makeEntries(4, 6, 2)}, &AppendEntriesReply{})

	// Term mismatch at 6: hint is the first index of our conflicting term.
	reply := &AppendEntriesReply{}
	node.HandleAppendEntries(&AppendEntriesArgs{Term: 3, LeaderID: 3, PrevLogIndex: 6, PrevLogTerm: 3}, reply)
	if reply.Success || reply.ConflictTerm != 2 || reply.ConflictIndex != 4 {
		t.Fatalf("term mismatch: expected ConflictTerm=2 ConflictIndex=4, got %+v", reply)
	}

	// Leader probing past our tail: hint is our next index.
	reply = &AppendEntriesReply{}
	node.HandleAppendEntries(&AppendEntriesArgs{Term: 3, LeaderID: 3, PrevLogIndex: 20, PrevLogTerm: 3}, reply)
	if reply.Success || reply.ConflictTerm != 0 || reply.ConflictIndex != 7 {
		t.Fatalf("missing entries: expected ConflictTerm=0 ConflictIndex=7, got %+v", reply)
	}
}

// TestRaft_LeaderUsesConflictHints checks the leader side: it skips a whole
// term per rejection instead of one entry.
func TestRaft_LeaderUsesConflictHints(t *testing.T) {
	node, err := NewRaftNode(quietFollowerConfig(), NewSimulatedNetwork(), newKVStorage(t), make(chan ApplyMsg, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	node.mu.Lock()
	defer node.mu.Unlock()
	// Leader log: 1-3 in term 1, 4-8 in term 3.
	node.log.TruncateAndAppend(0, append(makeEntries(1, 3, 1), makeEntries(4, 8, 3)...))
	node.role, node.currentTerm = RoleLeader, 3
	args := &AppendEntriesArgs{Term: 3, PrevLogIndex: 8, PrevLogTerm: 3}

	// Follower's conflicting term 2 isn't in our log: jump to its first index.
	node.nextIndex[1] = 9
	node.handleAppendEntriesReplyLocked(1, args, &AppendEntriesReply{Term: 3, ConflictTerm: 2, ConflictIndex: 4})
	if got := node.nextIndex[1]; got != 4 {
		t.Fatalf("unknown conflict term: expected nextIndex 4, got %d", got)
	}

	// Follower's conflicting term 1 is in our log: resume after our last term-1 entry.
	node.nextIndex[1] = 9
	node.handleAppendEntriesReplyLocked(1, args, &AppendEntriesReply{Term: 3, ConflictTerm: 1, ConflictIndex: 2})
	if got := node.nextIndex[1]; got != 4 {
		t.Fatalf("shared conflict term: expected nextIndex 4, got %d", got)
	}
}

// failingStorage makes Save fail on demand.
type failingStorage struct {
	*KVStorage
	fail atomic.Bool
}

func (f *failingStorage) Save(hs HardState, entries []LogEntry) error {
	if f.fail.Load() {
		return errors.New("injected disk failure")
	}
	return f.KVStorage.Save(hs, entries)
}

// TestRaft_StorageFailureHaltsNode checks fail-stop: a node that can't
// persist must stop acknowledging anything rather than carry on with state
// its disk doesn't reflect.
func TestRaft_StorageFailureHaltsNode(t *testing.T) {
	storage := &failingStorage{KVStorage: newKVStorage(t)}
	node, err := NewRaftNode(quietFollowerConfig(), NewSimulatedNetwork(), storage, make(chan ApplyMsg, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	storage.fail.Store(true)
	reply := &AppendEntriesReply{}
	node.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: makeEntries(1, 2, 1)}, reply)
	if reply.Success {
		t.Fatalf("follower acknowledged entries it failed to persist")
	}
	select {
	case <-node.Done():
	case <-time.After(time.Second):
		t.Fatalf("node did not halt after a storage failure")
	}
	if node.Err() == nil {
		t.Fatalf("expected Err() to report the storage failure")
	}

	// Once halted it stays silent, even after the disk "recovers".
	storage.fail.Store(false)
	reply = &AppendEntriesReply{}
	node.HandleAppendEntries(&AppendEntriesArgs{Term: 1, LeaderID: 1, Entries: makeEntries(1, 2, 1)}, reply)
	if reply.Success {
		t.Fatalf("halted node acknowledged entries")
	}
}

// TestRaft_BootDeliversSnapshotThenCommittedEntries checks recovery order: a
// restarted node rebuilds an empty state machine by delivering the stored
// snapshot first, then the committed entries after it.
func TestRaft_BootDeliversSnapshotThenCommittedEntries(t *testing.T) {
	storage := newKVStorage(t)
	if err := storage.Save(HardState{Term: 1, Commit: 5}, makeEntries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	if err := storage.CreateSnapshot(SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 1}, []byte("state@3")); err != nil {
		t.Fatal(err)
	}

	applyCh := make(chan ApplyMsg, 16)
	node, err := NewRaftNode(quietFollowerConfig(), NewSimulatedNetwork(), storage, applyCh)
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	if msg := recvApply(t, applyCh); msg.CommandValid || msg.CommandIndex != 3 || string(msg.Command) != "state@3" {
		t.Fatalf("expected snapshot at 3 first, got %+v", msg)
	}
	for want := uint64(4); want <= 5; want++ {
		if msg := recvApply(t, applyCh); !msg.CommandValid || msg.CommandIndex != want {
			t.Fatalf("expected command %d, got %+v", want, msg)
		}
	}
	// Nothing counts as applied until the state machine says so.
	if got := node.LastApplied(); got != 0 {
		t.Fatalf("expected LastApplied 0 before any report, got %d", got)
	}
}

// TestRaft_EarlierTermEntryNotCommittedByCounting pins Raft §5.4.2 (Figure 8)
// deterministically - the interleaving that breaks it is too rare for the
// randomized chaos test to hit reliably. A leader in term 4 sees an entry
// from term 2 stored on a majority; that alone must not commit it, because a
// later leader whose log ends in term 3 could still be elected and overwrite
// it. Only a quorum on a current-term entry commits it (indirectly).
func TestRaft_EarlierTermEntryNotCommittedByCounting(t *testing.T) {
	cfg := quietFollowerConfig()
	cfg.NodeID, cfg.Peers = 1, []uint64{1, 2, 3, 4, 5}
	node, err := NewRaftNode(cfg, NewSimulatedNetwork(), newKVStorage(t), make(chan ApplyMsg, 16))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Stop()

	node.mu.Lock()
	defer node.mu.Unlock()
	// Log: 1 (term 1), 2 (term 2), 3 (term 4: this leader's no-op).
	node.log.TruncateAndAppend(0, []LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 4, Type: EntryNoOp}})
	node.role, node.currentTerm, node.commitIndex = RoleLeader, 4, 1
	node.matchIndex = map[uint64]uint64{1: 3, 2: 2, 3: 2, 4: 0, 5: 0} // index 2 on a majority (1,2,3)

	node.advanceCommitIndexLocked()
	if node.commitIndex != 1 {
		t.Fatalf("committed index %d by counting replicas of an earlier-term entry (Figure 8)", node.commitIndex)
	}

	// Once the current-term no-op reaches a majority, both commit.
	node.matchIndex[2] = 3
	node.matchIndex[3] = 3
	node.advanceCommitIndexLocked()
	if node.commitIndex != 3 {
		t.Fatalf("expected commit to reach 3 once the no-op is on a majority, got %d", node.commitIndex)
	}
}
