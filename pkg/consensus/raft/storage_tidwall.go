package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/tidwall/wal"
)

// tidwallFormatVersion is bumped whenever the on-disk layout changes
// incompatibly; opening a directory written with another version fails fast
// rather than misreading it.
const tidwallFormatVersion = 2

// TidwallStorage implements Storage on disk:
//
//	<dir>/
//	├── metadata.json        HardState, snapshot bounds, and which WAL/snapshot files are live
//	├── snap-<index>.dat     payload of the latest snapshot
//	└── wal-<base>/          tidwall/wal segments; WAL index i holds Raft index i+base
//
// metadata.json is the single commit point for every multi-file change: it
// is replaced atomically (write temp, fsync, rename, fsync dir), and new
// files are always fully written before the metadata that references them.
// A crash therefore leaves either the old state or the new one, plus
// possibly some unreferenced files that the next open deletes.
//
// Why a base offset: tidwall/wal requires an empty log to start at index 1.
// When a follower installs a snapshot at index S that its log doesn't
// contain, it starts a fresh WAL with base S, so Raft index S+1 is WAL index 1.
type TidwallStorage struct {
	mu   sync.RWMutex
	dir  string
	meta tidwallMeta
	wal  *wal.Log
}

// tidwallMeta is the content of metadata.json.
type tidwallMeta struct {
	FormatVersion int          `json:"format_version"`
	HardState     HardState    `json:"hard_state"`
	Snapshot      SnapshotMeta `json:"snapshot"`
	SnapshotFile  string       `json:"snapshot_file,omitempty"`
	WALDir        string       `json:"wal_dir"`
	WALBase       uint64       `json:"wal_base"`
}

// NewTidwallStorage opens or creates a storage directory.
func NewTidwallStorage(dir string) (*TidwallStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ts := &TidwallStorage{dir: dir}

	fresh, err := ts.loadMeta()
	if err != nil {
		return nil, err
	}
	if ts.wal, err = openWAL(ts.path(ts.meta.WALDir)); err != nil {
		return nil, err
	}
	if fresh {
		if err := ts.saveMetaLocked(); err != nil {
			ts.wal.Close()
			return nil, err
		}
	}
	if err := ts.removeUnreferencedFiles(); err != nil {
		ts.wal.Close()
		return nil, err
	}
	// A crash between recording a snapshot and truncating the WAL leaves
	// covered entries behind; finish the job.
	if err := ts.truncateCoveredLocked(); err != nil {
		ts.wal.Close()
		return nil, err
	}
	return ts, nil
}

func openWAL(path string) (*wal.Log, error) {
	// AllowEmpty lets the log be truncated to zero entries (compacting right
	// up to the tip, or a conflict at the very first entry).
	l, err := wal.Open(path, &wal.Options{NoSync: false, AllowEmpty: true})
	if err != nil {
		return nil, fmt.Errorf("raft/storage: failed to open WAL at %s: %w", path, err)
	}
	return l, nil
}

func (ts *TidwallStorage) path(name string) string { return filepath.Join(ts.dir, name) }

// loadMeta reads metadata.json, reporting whether the directory is new.
func (ts *TidwallStorage) loadMeta() (fresh bool, err error) {
	data, err := os.ReadFile(ts.path("metadata.json"))
	if os.IsNotExist(err) {
		ts.meta = tidwallMeta{FormatVersion: tidwallFormatVersion, WALDir: walDirName(0)}
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, &ts.meta); err != nil {
		return false, fmt.Errorf("raft/storage: corrupt metadata.json: %w", err)
	}
	if ts.meta.FormatVersion != tidwallFormatVersion {
		return false, fmt.Errorf("%w: %s has version %d, this build reads %d (move the directory aside to start fresh)",
			ErrIncompatibleFormat, ts.dir, ts.meta.FormatVersion, tidwallFormatVersion)
	}
	return false, nil
}

func (ts *TidwallStorage) saveMetaLocked() error {
	data, err := json.Marshal(ts.meta)
	if err != nil {
		return err
	}
	return writeFileAtomic(ts.path("metadata.json"), data)
}

func walDirName(base uint64) string        { return fmt.Sprintf("wal-%020d", base) }
func snapshotFileName(index uint64) string { return fmt.Sprintf("snap-%020d.dat", index) }

// removeUnreferencedFiles deletes WAL dirs, snapshots, and temp files left
// behind by a crash mid-change.
func (ts *TidwallStorage) removeUnreferencedFiles() error {
	names, err := os.ReadDir(ts.dir)
	if err != nil {
		return err
	}
	for _, e := range names {
		name := e.Name()
		stale := strings.HasSuffix(name, ".tmp") ||
			(strings.HasPrefix(name, "wal-") && name != ts.meta.WALDir) ||
			(strings.HasPrefix(name, "snap-") && name != ts.meta.SnapshotFile)
		if stale {
			if err := os.RemoveAll(ts.path(name)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Index translation between Raft and the current WAL.
func (ts *TidwallStorage) walIndex(raftIndex uint64) uint64 { return raftIndex - ts.meta.WALBase }

func (ts *TidwallStorage) lastIndexLocked() (uint64, error) {
	walLast, err := ts.wal.LastIndex()
	if err != nil {
		return 0, err
	}
	return max(ts.meta.Snapshot.LastIncludedIndex, walLast+ts.meta.WALBase), nil
}

func (ts *TidwallStorage) InitialState() (HardState, SnapshotMeta, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.meta.HardState, ts.meta.Snapshot, nil
}

// Save persists HardState first, then entries: HardState changes (a new
// term) always precede the entries that depend on them.
func (ts *TidwallStorage) Save(hs HardState, entries []LogEntry) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if hs.Term != 0 && hs != ts.meta.HardState {
		prev := ts.meta.HardState
		ts.meta.HardState = hs
		if err := ts.saveMetaLocked(); err != nil {
			ts.meta.HardState = prev
			return err
		}
	}
	if len(entries) == 0 {
		return nil
	}

	first := entries[0].Index
	last, err := ts.lastIndexLocked()
	if err != nil {
		return err
	}
	if first <= ts.meta.Snapshot.LastIncludedIndex || first > last+1 {
		return fmt.Errorf("raft/storage: cannot write entries from %d (snapshot %d, last %d)",
			first, ts.meta.Snapshot.LastIncludedIndex, last)
	}
	// Replace everything from `first` on. TruncateBack(i) keeps i.
	if first <= last {
		if err := ts.wal.TruncateBack(ts.walIndex(first - 1)); err != nil {
			return fmt.Errorf("raft/storage: truncating WAL after %d: %w", first-1, err)
		}
	}
	batch := new(wal.Batch)
	for _, e := range entries {
		batch.Write(ts.walIndex(e.Index), encodeWALEntry(e))
	}
	return ts.wal.WriteBatch(batch)
}

// WAL entry payload: [term: 8 bytes][type: 1 byte][command].
func encodeWALEntry(e LogEntry) []byte {
	buf := make([]byte, 9+len(e.Data))
	binary.BigEndian.PutUint64(buf[:8], e.Term)
	buf[8] = byte(e.Type)
	copy(buf[9:], e.Data)
	return buf
}

func decodeWALEntry(index uint64, payload []byte) (LogEntry, error) {
	if len(payload) < 9 {
		return LogEntry{}, fmt.Errorf("raft/storage: corrupt WAL entry %d (%d bytes)", index, len(payload))
	}
	return LogEntry{
		Index: index,
		Term:  binary.BigEndian.Uint64(payload[:8]),
		Type:  EntryType(payload[8]),
		Data:  payload[9:],
	}, nil
}

func (ts *TidwallStorage) readLocked(index uint64) (LogEntry, error) {
	payload, err := ts.wal.Read(ts.walIndex(index))
	if errors.Is(err, wal.ErrNotFound) {
		return LogEntry{}, ErrUnavailable
	}
	if err != nil {
		return LogEntry{}, err
	}
	return decodeWALEntry(index, payload)
}

func (ts *TidwallStorage) Entries(low, high uint64, maxBytes uint64) ([]LogEntry, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	if low <= ts.meta.Snapshot.LastIncludedIndex {
		return nil, ErrCompacted
	}
	last, err := ts.lastIndexLocked()
	if err != nil {
		return nil, err
	}
	if high > last+1 {
		return nil, ErrUnavailable
	}

	var results []LogEntry
	var totalBytes uint64
	for idx := low; idx < high; idx++ {
		e, err := ts.readLocked(idx)
		if err != nil {
			return nil, err
		}
		size := uint64(len(e.Data)) + 16
		if len(results) > 0 && totalBytes+size > maxBytes {
			break
		}
		results = append(results, e)
		totalBytes += size
	}
	return results, nil
}

func (ts *TidwallStorage) Term(index uint64) (uint64, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.termLocked(index)
}

func (ts *TidwallStorage) termLocked(index uint64) (uint64, error) {
	snap := ts.meta.Snapshot
	switch {
	case index == snap.LastIncludedIndex:
		return snap.LastIncludedTerm, nil
	case index < snap.LastIncludedIndex:
		return 0, ErrCompacted
	}
	last, err := ts.lastIndexLocked()
	if err != nil {
		return 0, err
	}
	if index > last {
		return 0, ErrUnavailable
	}
	e, err := ts.readLocked(index)
	return e.Term, err
}

func (ts *TidwallStorage) FirstIndex() (uint64, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.meta.Snapshot.LastIncludedIndex + 1, nil
}

func (ts *TidwallStorage) LastIndex() (uint64, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.lastIndexLocked()
}

// CreateSnapshot records a locally taken snapshot and reclaims the WAL
// segments it covers.
func (ts *TidwallStorage) CreateSnapshot(meta SnapshotMeta, data []byte) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if meta.LastIncludedIndex <= ts.meta.Snapshot.LastIncludedIndex {
		return ErrSnapshotOutOfDate
	}
	last, err := ts.lastIndexLocked()
	if err != nil {
		return err
	}
	if meta.LastIncludedIndex > last {
		return fmt.Errorf("raft/storage: snapshot index %d is beyond the log (last %d)", meta.LastIncludedIndex, last)
	}
	return ts.recordSnapshotLocked(meta, data, ts.meta.WALDir, ts.meta.WALBase, nil)
}

// ApplySnapshot installs a leader's snapshot (see Storage.ApplySnapshot).
func (ts *TidwallStorage) ApplySnapshot(meta SnapshotMeta, data []byte) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if meta.LastIncludedIndex <= ts.meta.Snapshot.LastIncludedIndex {
		return ErrSnapshotOutOfDate
	}
	if term, err := ts.termLocked(meta.LastIncludedIndex); err == nil && term == meta.LastIncludedTerm {
		// Our log agrees with the snapshot at its boundary: keep the suffix.
		return ts.recordSnapshotLocked(meta, data, ts.meta.WALDir, ts.meta.WALBase, nil)
	}

	// Discard the whole log: start a fresh, empty WAL positioned right
	// after the snapshot. It only becomes live once metadata says so.
	newDir := walDirName(meta.LastIncludedIndex)
	if err := os.RemoveAll(ts.path(newDir)); err != nil {
		return err
	}
	newWAL, err := openWAL(ts.path(newDir))
	if err != nil {
		return err
	}
	if err := syncDir(ts.dir); err != nil {
		newWAL.Close()
		return err
	}
	return ts.recordSnapshotLocked(meta, data, newDir, meta.LastIncludedIndex, newWAL)
}

// recordSnapshotLocked writes the snapshot file, commits metadata pointing
// at it (and at walDir/walBase), then cleans up what the change replaced.
// newWAL is non-nil when walDir is a freshly created log that replaces the
// current one.
func (ts *TidwallStorage) recordSnapshotLocked(meta SnapshotMeta, data []byte, walDir string, walBase uint64, newWAL *wal.Log) error {
	abort := func(err error) error {
		if newWAL != nil {
			newWAL.Close()
			os.RemoveAll(ts.path(walDir))
		}
		return err
	}
	snapFile := snapshotFileName(meta.LastIncludedIndex)
	if err := writeFileAtomic(ts.path(snapFile), data); err != nil {
		return abort(err)
	}

	prev := ts.meta
	ts.meta.Snapshot, ts.meta.SnapshotFile = meta, snapFile
	ts.meta.WALDir, ts.meta.WALBase = walDir, walBase
	if err := ts.saveMetaLocked(); err != nil { // commit point
		ts.meta = prev
		return abort(err)
	}

	// Committed. Everything below is cleanup; failures leave only garbage
	// that the next open removes.
	if prev.SnapshotFile != "" && prev.SnapshotFile != snapFile {
		os.Remove(ts.path(prev.SnapshotFile))
	}
	if newWAL != nil {
		ts.wal.Close()
		os.RemoveAll(ts.path(prev.WALDir))
		ts.wal = newWAL
	}
	return ts.truncateCoveredLocked()
}

// truncateCoveredLocked drops WAL entries at or below the snapshot index.
func (ts *TidwallStorage) truncateCoveredLocked() error {
	snap := ts.meta.Snapshot.LastIncludedIndex
	if snap <= ts.meta.WALBase {
		return nil // a fresh WAL starts after the snapshot
	}
	walFirst, err := ts.wal.FirstIndex()
	if err != nil {
		return err
	}
	if keepFrom := ts.walIndex(snap) + 1; keepFrom > walFirst {
		return ts.wal.TruncateFront(keepFrom)
	}
	return nil
}

func (ts *TidwallStorage) SnapshotData() ([]byte, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	if ts.meta.SnapshotFile == "" {
		return nil, nil
	}
	return os.ReadFile(ts.path(ts.meta.SnapshotFile))
}

func (ts *TidwallStorage) Close() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.wal.Close()
}
