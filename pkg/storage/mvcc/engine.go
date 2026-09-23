package mvcc

import (
	"errors"
	"io"
)

var (
	// ErrKeyNotFound is returned when a key does not exist or was deleted before readTS.
	ErrKeyNotFound = errors.New("mvcc: key not found or deleted at snapshot timestamp")
	// ErrEngineClosed is returned when operations are attempted on a stopped engine.
	ErrEngineClosed = errors.New("mvcc: storage engine is closed")
	// ErrCorruptSnapshot is returned when a snapshot stream fails validation.
	ErrCorruptSnapshot = errors.New("mvcc: snapshot is corrupt or truncated")
	// ErrCannotRebuild is returned by Compact/RestoreSnapshot when the store
	// has no way to create a fresh engine to rebuild into.
	ErrCannotRebuild = errors.New("mvcc: engine does not support rebuilding (no EmptyCloner)")
)

// KeyValue represents a logical user entry visible at a specific snapshot.
type KeyValue struct {
	Key       []byte
	Value     []byte
	Timestamp uint64
}

// MVCCStore defines the API contract for the Multi-Version Concurrency Control engine.
type MVCCStore interface {
	// Put writes a new version of key at commitTS.
	Put(key, value []byte, commitTS uint64) error

	// Delete writes a tombstone version for key at commitTS.
	Delete(key []byte, commitTS uint64) error

	// Get retrieves the newest visible value for key at or before readTS.
	Get(key []byte, readTS uint64) ([]byte, error)

	// Scan returns all active key-values in [startKey, endKey) visible at readTS.
	// If limit <= 0, all visible records are returned.
	Scan(startKey, endKey []byte, readTS uint64, limit int) ([]KeyValue, error)

	// Close shuts down the underlying engine.
	Close() error

	// ExportSnapshot streams the store's state as of safeWatermarkTS (plus
	// every version newer than it) to w.
	ExportSnapshot(w io.Writer, safeWatermarkTS uint64) error

	// RestoreSnapshot replaces the store's entire contents with a snapshot.
	RestoreSnapshot(r io.Reader) error
}
