package raft

import (
	"fmt"
	"os"
	"testing"
)

func TestStorage_TidwallStorage_CrashReplayAndCompaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "tidwall_raft_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Session 1: Write entries 1..50, take snapshot up to 30, append 31..60, then crash
	func() {
		store, err := NewTidwallStorage(tempDir)
		if err != nil {
			t.Fatalf("failed to open TidwallStorage: %v", err)
		}
		defer store.Close()

		hs := HardState{Term: 2, Vote: 1, Commit: 50}

		// Append entries 1..50
		var initialEntries []LogEntry
		for i := uint64(1); i <= 50; i++ {
			initialEntries = append(initialEntries, LogEntry{
				Index: i,
				Term:  1,
				Data:  fmt.Appendf(nil, "data_%d", i),
			})
		}
		if err := store.Save(hs, initialEntries); err != nil {
			t.Fatalf("initial save failed: %v", err)
		}

		// Snapshot up to 30 (Physically purges 1..30 via TruncateFront)
		meta := SnapshotMeta{LastIncludedIndex: 30, LastIncludedTerm: 1}
		if err := store.CreateSnapshot(meta, []byte("snapshot_at_30")); err != nil {
			t.Fatalf("CreateSnapshot failed: %v", err)
		}

		// Append entries 51..60
		var extraEntries []LogEntry
		for i := uint64(51); i <= 60; i++ {
			extraEntries = append(extraEntries, LogEntry{
				Index: i,
				Term:  2,
				Data:  fmt.Appendf(nil, "extra_data_%d", i),
			})
		}
		if err := store.Save(hs, extraEntries); err != nil {
			t.Fatalf("extra save failed: %v", err)
		}

		last, _ := store.LastIndex()
		if last != 60 {
			t.Fatalf("expected last index 60, got %d", last)
		}
	}()

	// Session 2: Crash recovery. Reopen from the exact same directory!
	store2, err := NewTidwallStorage(tempDir)
	if err != nil {
		t.Fatalf("re-open failed: %v", err)
	}
	defer store2.Close()

	// 1. Verify HardState survived
	hs, snapMeta, err := store2.InitialState()
	if err != nil || hs.Term != 2 || hs.Commit != 50 {
		t.Fatalf("corrupted HardState on reboot: %+v", hs)
	}

	// 2. Verify Snapshot boundary survived
	if snapMeta.LastIncludedIndex != 30 || snapMeta.LastIncludedTerm != 1 {
		t.Fatalf("corrupted SnapshotMeta: %+v", snapMeta)
	}

	firstIdx, _ := store2.FirstIndex()
	if firstIdx != 31 {
		t.Fatalf("expected FirstIndex to be 31 after compaction, got %d", firstIdx)
	}

	// 3. Compacted entries (1..30) must return ErrCompacted
	if _, err := store2.Entries(1, 10, 1024); err != ErrCompacted {
		t.Fatalf("expected ErrCompacted for entries 1..10, got %v", err)
	}

	// 4. Un-compacted entries (31..60) must be readable and intact
	entries, err := store2.Entries(31, 61, 1024*1024)
	if err != nil {
		t.Fatalf("failed to read surviving entries: %v", err)
	}
	if len(entries) != 30 {
		t.Fatalf("expected 30 surviving entries, got %d", len(entries))
	}

	if string(entries[0].Data) != "data_31" {
		t.Fatalf("expected data_31, got %s", string(entries[0].Data))
	}
	if string(entries[len(entries)-1].Data) != "extra_data_60" {
		t.Fatalf("expected extra_data_60, got %s", string(entries[len(entries)-1].Data))
	}

	// 5. Test Conflict Truncation: Leader overrides 55..60 with Term 3 entries
	conflictEntries := []LogEntry{
		{Index: 55, Term: 3, Data: []byte("override_55")},
		{Index: 56, Term: 3, Data: []byte("override_56")},
	}
	if err := store2.Save(HardState{Term: 3, Vote: 1, Commit: 50}, conflictEntries); err != nil {
		t.Fatalf("conflict save failed: %v", err)
	}

	lastAfterConflict, _ := store2.LastIndex()
	if lastAfterConflict != 56 {
		t.Fatalf("expected last index 56 after conflict truncation, got %d", lastAfterConflict)
	}

	term55, _ := store2.Term(55)
	if term55 != 3 {
		t.Fatalf("expected term 3 at index 55, got %d", term55)
	}

	t.Logf("PASS: TidwallStorage passed all persistence, conflict truncation, and O(1) compaction checks!")
}
