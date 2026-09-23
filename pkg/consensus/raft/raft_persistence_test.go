package raft

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestRaft_NodeReplaysLogFromStorageOnRestart is the integration-level
// counterpart to TestStorage_TidwallStorage_CrashReplayAndCompaction: that
// test proves TidwallStorage itself correctly persists and returns entries
// after a reopen, but never exercises whether RaftNode actually consults
// storage.Entries()/storage.LastIndex() when booting. This test catches the
// gap where RaftNode's in-memory log silently started empty past the
// snapshot boundary on every restart, discarding everything Save() had ever
// written, regardless of what storage still had on disk.
func TestRaft_NodeReplaysLogFromStorageOnRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft_persistence_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	cfg := Config{
		NodeID:             1,
		Peers:              []uint64{1, 2, 3}, // Validate() requires >= 3; 2 and 3 are never registered/used
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 200 * time.Millisecond, // long enough that no election fires during this test
		ElectionTimeoutMax: 400 * time.Millisecond,
		RPCTimeout:         20 * time.Millisecond,
	}

	// Session 1: a "previous process" writes HardState and 5 entries, then
	// dies (closes storage) without ever snapshotting.
	func() {
		storage, err := NewTidwallStorage(dir)
		if err != nil {
			t.Fatalf("failed to open storage: %v", err)
		}
		defer storage.Close()

		var entries []LogEntry
		for i := uint64(1); i <= 5; i++ {
			entries = append(entries, LogEntry{Index: i, Term: 1, Data: fmt.Appendf(nil, "cmd_%d", i)})
		}
		if err := storage.Save(HardState{Term: 1, Vote: 1, Commit: 5}, entries); err != nil {
			t.Fatalf("save failed: %v", err)
		}
	}()

	// Session 2: reopen the same directory (simulating the process restarting)
	// and boot a fresh RaftNode over it.
	storage2, err := NewTidwallStorage(dir)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}

	applyCh := make(chan ApplyMsg, 10)
	net := NewSimulatedNetwork() // peers 2 and 3 are never registered; harmless for this test
	node, err := NewRaftNode(cfg, net, storage2, applyCh)
	if err != nil {
		t.Fatalf("failed to boot RaftNode over recovered storage: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	lastIdx := node.log.LastIndex()
	term5, ok := node.log.TermAt(5)
	commitIdx := node.commitIndex
	node.mu.Unlock()

	if lastIdx != 5 {
		t.Fatalf("expected in-memory log to be replayed up to index 5 from storage, got LastIndex()=%d (log started empty past the snapshot boundary)", lastIdx)
	}
	if !ok || term5 != 1 {
		t.Fatalf("expected entry 5 replayed with term 1, got ok=%v term=%d", ok, term5)
	}
	if commitIdx != 5 {
		t.Fatalf("expected commitIndex restored from HardState to be 5, got %d", commitIdx)
	}

	t.Logf("PASS: RaftNode replayed %d entries from durable storage on restart", lastIdx)
}

// TestRaft_NodeReplaysLogFromStorageAfterSnapshotOnRestart is the persistence
// counterpart to TestRaft_ProposeAfterSnapshotCompactionUsesCorrectIndex: that
// test proves Append/TruncateAndAppend compute correct indices after an
// in-memory CompactLog on a still-running node. This test proves the same
// holds across a real process restart, where NewRaftNode has to stitch the
// snapshot boundary (via CompactLog) back together with whatever entries were
// durably written to storage after the snapshot (via storage.Entries()) -
// the exact on-disk shape (SnapMeta.last_included_index=1, plus later WAL
// entries) that the original bug report's metadata.json showed.
func TestRaft_NodeReplaysLogFromStorageAfterSnapshotOnRestart(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft_persistence_snap_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	// Session 1: write entry 1, snapshot through it, then write entries 2 and
	// 3 on top of the snapshot boundary, then die without ever restarting.
	func() {
		storage, err := NewTidwallStorage(dir)
		if err != nil {
			t.Fatalf("failed to open storage: %v", err)
		}
		defer storage.Close()

		if err := storage.Save(HardState{Term: 1, Vote: 1, Commit: 1}, []LogEntry{
			{Index: 1, Term: 1, Data: []byte("cmd_1")},
		}); err != nil {
			t.Fatalf("save entry 1 failed: %v", err)
		}
		if err := storage.CreateSnapshot(SnapshotMeta{LastIncludedIndex: 1, LastIncludedTerm: 1}, []byte("snap-at-1")); err != nil {
			t.Fatalf("snapshot failed: %v", err)
		}
		if err := storage.Save(HardState{Term: 1, Vote: 1, Commit: 3}, []LogEntry{
			{Index: 2, Term: 1, Data: []byte("cmd_2")},
			{Index: 3, Term: 1, Data: []byte("cmd_3")},
		}); err != nil {
			t.Fatalf("save entries 2,3 failed: %v", err)
		}
	}()

	// Session 2: reopen the same directory and boot a fresh RaftNode over it.
	storage2, err := NewTidwallStorage(dir)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}

	cfg := Config{
		NodeID:             1,
		Peers:              []uint64{1, 2, 3},
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 200 * time.Millisecond,
		ElectionTimeoutMax: 400 * time.Millisecond,
		RPCTimeout:         20 * time.Millisecond,
	}
	applyCh := make(chan ApplyMsg, 10)
	net := NewSimulatedNetwork()
	node, err := NewRaftNode(cfg, net, storage2, applyCh)
	if err != nil {
		t.Fatalf("failed to boot RaftNode over recovered storage: %v", err)
	}
	defer node.Stop()

	node.mu.Lock()
	lastIdx := node.log.LastIndex()
	term3, ok3 := node.log.TermAt(3)
	commitIdx := node.commitIndex
	// Force leadership so Propose() below doesn't just return isLeader=false.
	// Mirrors what startRealElectionLocked does on a real win: nextIndex and
	// matchIndex must be populated for every peer before anything calls
	// broadcastAppendEntriesLocked, or nextIndex[peer]-1 underflows.
	node.role = RoleLeader
	for _, p := range cfg.Peers {
		node.nextIndex[p] = node.log.LastIndex() + 1
		node.matchIndex[p] = 0
	}
	node.mu.Unlock()

	if lastIdx != 3 {
		t.Fatalf("expected in-memory log to be replayed to index 3 (1 from snapshot boundary + 2,3 from WAL past it), got LastIndex()=%d", lastIdx)
	}
	if !ok3 || term3 != 1 {
		t.Fatalf("expected entry 3 replayed with term 1, got ok=%v term=%d", ok3, term3)
	}
	if commitIdx != 3 {
		t.Fatalf("expected commitIndex restored to 3, got %d", commitIdx)
	}

	// The actual bug shape: propose immediately after restart and confirm the
	// next index accounts for everything hydrated, not just what's in the
	// fresh in-memory log's own append count.
	idx, _, isLeader := node.Propose([]byte("cmd_4"))
	if !isLeader {
		t.Fatalf("expected node forced into RoleLeader to accept the proposal")
	}
	if idx != 4 {
		t.Fatalf("expected Propose after restart to return index 4, got %d", idx)
	}

	t.Logf("PASS: RaftNode replayed through snapshot boundary and WAL to index %d, next Propose correctly returned %d", lastIdx, idx)
}
