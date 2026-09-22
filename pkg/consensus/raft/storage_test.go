package raft

import (
	"testing"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

func TestStorage_KVStorage_PersistenceAndTruncation(t *testing.T) {
	engine := raw.NewSkipListEngine(16 * 1024 * 1024)
	storage, err := NewKVStorage(engine)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// 1. Initial State Check
	hs, _, _ := storage.InitialState()
	if hs.Term != 0 || hs.Vote != 0 {
		t.Fatalf("expected zero HardState, got %+v", hs)
	}

	// 2. Persist Term, Vote, and entries 1..3
	hs1 := HardState{Term: 2, Vote: 1, Commit: 2}
	entries1 := []LogEntry{
		{Index: 1, Term: 1, Data: []byte("cmd1")},
		{Index: 2, Term: 2, Data: []byte("cmd2")},
		{Index: 3, Term: 2, Data: []byte("cmd3")},
	}
	if err := storage.Save(hs1, entries1); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	last, _ := storage.LastIndex()
	if last != 3 {
		t.Fatalf("expected last index 3, got %d", last)
	}

	// 3. Test Conflict Truncation: overwrite index 3 with term 3 entry
	entries2 := []LogEntry{
		{Index: 3, Term: 3, Data: []byte("cmd3_override")},
		{Index: 4, Term: 3, Data: []byte("cmd4")},
	}
	if err := storage.Save(HardState{Term: 3, Vote: 1, Commit: 2}, entries2); err != nil {
		t.Fatalf("Save conflict failed: %v", err)
	}

	last2, _ := storage.LastIndex()
	if last2 != 4 {
		t.Fatalf("expected last index 4 after append, got %d", last2)
	}

	termAt3, _ := storage.Term(3)
	if termAt3 != 3 {
		t.Fatalf("expected term 3 at index 3, got %d", termAt3)
	}

	// 4. Test Snapshot Compaction
	meta := SnapshotMeta{LastIncludedIndex: 2, LastIncludedTerm: 2}
	if err := storage.CreateSnapshot(meta, []byte("snap_state")); err != nil {
		t.Fatalf("CreateSnapshot failed: %v", err)
	}

	first, _ := storage.FirstIndex()
	if first != 3 {
		t.Fatalf("expected first index 3 after compaction of 1..2, got %d", first)
	}

	// Verify entries 1 and 2 now return ErrCompacted
	if _, err := storage.Entries(1, 2, 1024); err != ErrCompacted {
		t.Fatalf("expected ErrCompacted, got %v", err)
	}
}
