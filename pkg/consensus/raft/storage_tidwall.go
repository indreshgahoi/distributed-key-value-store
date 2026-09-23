package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/tidwall/wal"
)

// TidwallStorage implements the raft.Storage interface backed by tidwall/wal
// rolling segmented log files and atomic JSON metadata state.
type TidwallStorage struct {
	mu sync.RWMutex

	dir      string
	metaFile string
	snapFile string
	walLog   *wal.Log

	hs       HardState
	snapMeta SnapshotMeta
}

// NewTidwallStorage initializes or recovers a durable segmented Raft storage directory.
func NewTidwallStorage(dir string) (*TidwallStorage, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	metaPath := filepath.Join(dir, "metadata.json")
	snapPath := filepath.Join(dir, "state.snap")
	walDir := filepath.Join(dir, "segments")

	// Open tidwall/wal segmented engine (rolls at 20MB per segment by default).
	// AllowEmpty: without it, TruncateFront/TruncateBack refuse to leave the
	// log with zero entries - which blocks the legitimate case of compacting
	// all the way to the current tip right after a snapshot with nothing new
	// proposed since, and the edge case of truncating a conflict at index 1.
	l, err := wal.Open(walDir, &wal.Options{
		NoSync:     false, // Ensure physical fsync on writes
		AllowEmpty: true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open tidwall/wal at %s: %w", walDir, err)
	}

	ts := &TidwallStorage{
		dir:      dir,
		metaFile: metaPath,
		snapFile: snapPath,
		walLog:   l,
	}

	// Recover metadata (term, vote, commit, snapshot bounds)
	if err := ts.recoverMetadata(); err != nil {
		_ = l.Close()
		return nil, err
	}

	return ts, nil
}

func (ts *TidwallStorage) recoverMetadata() error {
	data, err := os.ReadFile(ts.metaFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Fresh directory
		}
		return err
	}

	var state struct {
		HS       HardState
		SnapMeta SnapshotMeta
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	ts.hs = state.HS
	ts.snapMeta = state.SnapMeta
	return nil
}

func (ts *TidwallStorage) saveMetadataLocked() error {
	state := struct {
		HS       HardState
		SnapMeta SnapshotMeta
	}{
		HS:       ts.hs,
		SnapMeta: ts.snapMeta,
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	// os.WriteFile does not fsync - Close() only flushes userspace buffers via
	// write(2), not the OS page cache to stable storage. HardState is exactly
	// what makes the double-voting bug (docs/milestoneTwo.md §7) possible if
	// lost, so this needs the same fsync-before-rename durability the WAL
	// entries already get (wal.Options.NoSync: false), matching the pattern
	// already used by SaveSnapshotToFile (pkg/storage/mvcc/snapshot.go).
	tmpFile := ts.metaFile + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmpFile, ts.metaFile)
}

func (ts *TidwallStorage) InitialState() (HardState, SnapshotMeta, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.hs, ts.snapMeta, nil
}

// Save atomically writes HardState and appends log entries with conflict resolution.
func (ts *TidwallStorage) Save(hs HardState, entries []LogEntry) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	// 1. Update HardState if modified
	if hs.Term != 0 {
		ts.hs = hs
		if err := ts.saveMetadataLocked(); err != nil {
			return err
		}
	}

	if len(entries) == 0 {
		return nil
	}

	firstNew := entries[0].Index
	lastIdx, _ := ts.walLog.LastIndex()

	// 2. Conflict Truncation: Discard conflicting uncommitted entries from the tail.
	// TruncateBack(index) KEEPS `index` as the new last entry and removes only
	// what comes after it (see tidwall/wal docs). Since firstNew is the first
	// index the new entries are about to occupy, truncate to firstNew-1 so the
	// old entry at firstNew is discarded too - otherwise the batch write below
	// collides with it and tidwall/wal rejects the whole write as "out of order".
	if lastIdx >= firstNew {
		if err := ts.walLog.TruncateBack(firstNew - 1); err != nil {
			return fmt.Errorf("failed to truncate back at %d: %w", firstNew-1, err)
		}
	}

	// 3. Fast binary append: [Term (8B)][Command Bytes]
	batch := new(wal.Batch)
	for _, entry := range entries {
		payload := make([]byte, 8+len(entry.Data))
		binary.BigEndian.PutUint64(payload[:8], entry.Term)
		copy(payload[8:], entry.Data)

		batch.Write(entry.Index, payload)
	}

	return ts.walLog.WriteBatch(batch)
}

func (ts *TidwallStorage) Entries(low, high uint64, maxBytes uint64) ([]LogEntry, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	firstIdx, err := ts.FirstIndexLocked()
	if err != nil {
		return nil, err
	}
	if low < firstIdx {
		return nil, ErrCompacted
	}

	lastIdx, _ := ts.LastIndexLocked()
	if high > lastIdx+1 {
		return nil, ErrUnavailable
	}

	var results []LogEntry
	var totalBytes uint64

	for idx := low; idx < high; idx++ {
		data, err := ts.walLog.Read(idx)
		if err != nil {
			if errors.Is(err, wal.ErrNotFound) {
				return nil, ErrCompacted
			}
			return nil, err
		}

		if len(data) < 8 {
			return nil, errors.New("corrupted WAL entry: shorter than 8 bytes")
		}

		term := binary.BigEndian.Uint64(data[:8])
		cmd := data[8:]

		entry := LogEntry{
			Index: idx,
			Term:  term,
			Data:  cmd,
		}

		entrySize := uint64(len(cmd)) + 16
		if len(results) > 0 && totalBytes+entrySize > maxBytes {
			break
		}

		results = append(results, entry)
		totalBytes += entrySize
	}

	return results, nil
}

func (ts *TidwallStorage) Term(index uint64) (uint64, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	// Horizon boundary check
	if index == ts.snapMeta.LastIncludedIndex {
		return ts.snapMeta.LastIncludedTerm, nil
	}
	if index < ts.snapMeta.LastIncludedIndex {
		return 0, ErrCompacted
	}

	data, err := ts.walLog.Read(index)
	if err != nil {
		if errors.Is(err, wal.ErrNotFound) {
			return 0, ErrUnavailable
		}
		return 0, err
	}

	if len(data) < 8 {
		return 0, errors.New("corrupted WAL entry")
	}

	return binary.BigEndian.Uint64(data[:8]), nil
}

func (ts *TidwallStorage) FirstIndex() (uint64, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.FirstIndexLocked()
}

func (ts *TidwallStorage) FirstIndexLocked() (uint64, error) {
	if ts.snapMeta.LastIncludedIndex > 0 {
		return ts.snapMeta.LastIncludedIndex + 1, nil
	}
	firstIdx, err := ts.walLog.FirstIndex()
	if err != nil {
		return 1, nil
	}
	return firstIdx, nil
}

func (ts *TidwallStorage) LastIndex() (uint64, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.LastIndexLocked()
}

func (ts *TidwallStorage) LastIndexLocked() (uint64, error) {
	lastIdx, err := ts.walLog.LastIndex()
	if err != nil || lastIdx == 0 {
		return ts.snapMeta.LastIncludedIndex, nil
	}
	if lastIdx < ts.snapMeta.LastIncludedIndex {
		return ts.snapMeta.LastIncludedIndex, nil
	}
	return lastIdx, nil
}

// CreateSnapshot stores state machine data and physically deletes old segment files.
func (ts *TidwallStorage) CreateSnapshot(meta SnapshotMeta, stateMachineData []byte) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	firstIdx, _ := ts.FirstIndexLocked()
	// firstIdx-1 underflows uint64 to ~1.8e19 if firstIdx is ever 0, which
	// would silently disable this safety check instead of erroring. Guard it
	// explicitly rather than relying on FirstIndexLocked() never returning 0.
	if firstIdx > 0 && meta.LastIncludedIndex < firstIdx-1 {
		return ErrSnapshotOutOfDate
	}

	// 1. Save durable snapshot file
	tmpPath := ts.snapFile + ".tmp"
	if err := os.WriteFile(tmpPath, stateMachineData, 0644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, ts.snapFile); err != nil {
		return err
	}

	// 2. Persist updated metadata
	ts.snapMeta = meta
	if err := ts.saveMetadataLocked(); err != nil {
		return err
	}

	// 3. INSTANT DISK RECLAMATION (O(1) delete of obsolete segment files):
	// Deletes all closed segments whose entries are <= LastIncludedIndex
	return ts.walLog.TruncateFront(meta.LastIncludedIndex)
}

func (ts *TidwallStorage) ApplySnapshot(meta SnapshotMeta, stateMachineData []byte) error {
	return ts.CreateSnapshot(meta, stateMachineData)
}

func (ts *TidwallStorage) Close() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.walLog.Close()
}
