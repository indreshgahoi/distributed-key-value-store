package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
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

// TestStorage_TidwallStorage_InstallSnapshotBeyondLog covers a follower
// installing a leader's snapshot past the end of its own log: the WAL is
// replaced by a fresh one based at the snapshot index, new entries continue
// from there, and all of it survives a reopen.
func TestStorage_TidwallStorage_InstallSnapshotBeyondLog(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTidwallStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(HardState{Term: 1}, makeEntries(1, 3, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplySnapshot(SnapshotMeta{LastIncludedIndex: 10, LastIncludedTerm: 2}, []byte("leader-state")); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}
	if first, _ := s.FirstIndex(); first != 11 {
		t.Fatalf("expected FirstIndex 11, got %d", first)
	}
	if last, _ := s.LastIndex(); last != 10 {
		t.Fatalf("expected LastIndex 10 (the snapshot), got %d", last)
	}
	if _, err := s.Entries(2, 3, ^uint64(0)); !errors.Is(err, ErrCompacted) {
		t.Fatalf("pre-snapshot entries must be gone, got %v", err)
	}
	// This write used to be impossible: tidwall/wal requires an empty log to
	// start at index 1.
	if err := s.Save(HardState{Term: 2}, makeEntries(11, 12, 2)); err != nil {
		t.Fatalf("appending after the snapshot: %v", err)
	}
	s.Close()

	s, err = NewTidwallStorage(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	if last, _ := s.LastIndex(); last != 12 {
		t.Fatalf("after reopen: expected LastIndex 12, got %d", last)
	}
	if term, err := s.Term(10); err != nil || term != 2 {
		t.Fatalf("after reopen: snapshot boundary term expected 2, got %d (%v)", term, err)
	}
	if data, err := s.SnapshotData(); err != nil || string(data) != "leader-state" {
		t.Fatalf("after reopen: snapshot data %q (%v)", data, err)
	}
	entries, err := s.Entries(11, 13, ^uint64(0))
	if err != nil || len(entries) != 2 || entries[1].Index != 12 {
		t.Fatalf("after reopen: entries 11-12 unreadable: %v %+v", err, entries)
	}
	names, _ := os.ReadDir(dir)
	if len(names) != 3 {
		var got []string
		for _, n := range names {
			got = append(got, n.Name())
		}
		t.Fatalf("expected only metadata, one snapshot, one WAL dir; found %v", got)
	}
}

// TestStorage_TidwallStorage_InstallSnapshotKeepsMatchingSuffix: when the log
// agrees with the snapshot at its boundary, the entries after it are kept.
func TestStorage_TidwallStorage_InstallSnapshotKeepsMatchingSuffix(t *testing.T) {
	s, err := NewTidwallStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Save(HardState{Term: 1}, makeEntries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.ApplySnapshot(SnapshotMeta{LastIncludedIndex: 3, LastIncludedTerm: 1}, []byte("s")); err != nil {
		t.Fatal(err)
	}
	entries, err := s.Entries(4, 6, ^uint64(0))
	if err != nil || len(entries) != 2 {
		t.Fatalf("suffix after a matching snapshot must be kept: %v %+v", err, entries)
	}
}

// TestStorage_TidwallStorage_RejectsIncompatibleFormat: a directory written
// by an older layout fails fast instead of being misread.
func TestStorage_TidwallStorage_RejectsIncompatibleFormat(t *testing.T) {
	dir := t.TempDir()
	legacy := `{"HS":{"term":3,"vote":1,"commit":2},"SnapMeta":{"last_included_index":0,"last_included_term":0}}`
	if err := os.WriteFile(filepath.Join(dir, "metadata.json"), []byte(legacy), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := NewTidwallStorage(dir); !errors.Is(err, ErrIncompatibleFormat) {
		t.Fatalf("expected ErrIncompatibleFormat, got %v", err)
	}
}

// TestStorage_TidwallStorage_UnchangedHardStateIsNotRewritten: heartbeats
// re-save the same HardState constantly; that must not cost an fsync.
func TestStorage_TidwallStorage_UnchangedHardStateIsNotRewritten(t *testing.T) {
	dir := t.TempDir()
	s, err := NewTidwallStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	hs := HardState{Term: 4, Vote: 2, Commit: 1}
	if err := s.Save(hs, nil); err != nil {
		t.Fatal(err)
	}
	meta := filepath.Join(dir, "metadata.json")
	before, _ := os.Stat(meta)
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < 5; i++ {
		if err := s.Save(hs, nil); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := os.Stat(meta)
	if !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("metadata.json rewritten for an unchanged HardState")
	}
}
