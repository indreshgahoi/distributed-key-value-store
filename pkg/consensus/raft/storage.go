package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"sync"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

var (
	// ErrCompacted is returned when requesting log entries that were pruned by a snapshot.
	ErrCompacted = errors.New("raft/storage: requested entry has been compacted by snapshot")

	// ErrUnavailable is returned when requesting log entries past the latest known index.
	ErrUnavailable = errors.New("raft/storage: requested entry is not yet available in log")

	// ErrSnapshotOutOfDate is returned when attempting to apply a snapshot older than current state.
	ErrSnapshotOutOfDate = errors.New("raft/storage: snapshot is older than current storage watermark")

	// ErrNotLeader is returned by leader-only operations (e.g. ReadIndex) when
	// called against a node that is not currently the Leader.
	ErrNotLeader = errors.New("raft: node is not the leader")

	// ErrQuorumUnreachable is returned by ReadIndex when a quorum of peers
	// could not be confirmed to still recognize this node as leader before
	// the caller's context expired - e.g. this node is the Leader of a
	// minority partition and doesn't know it yet.
	ErrQuorumUnreachable = errors.New("raft: could not confirm leadership with a quorum before deadline")
)

// HardState represents the non-volatile consensus variables that MUST be persisted
// before responding to any RPCs (Raft §5.2).
type HardState struct {
	Term   uint64 `json:"term"`
	Vote   uint64 `json:"vote"`
	Commit uint64 `json:"commit"`
}

// SnapshotMeta holds boundaries for log compaction.
type SnapshotMeta struct {
	LastIncludedIndex uint64 `json:"last_included_index"`
	LastIncludedTerm  uint64 `json:"last_included_term"`
}

// Storage is the pluggable interface that any Raft storage engine must implement.
type Storage interface {
	// InitialState returns the persisted HardState and current snapshot bounds upon reboot.
	InitialState() (HardState, SnapshotMeta, error)

	// Save atomically persists HardState and appends new log entries in a single atomic commit.
	// entries replace the durable log from entries[0].Index onward: any existing entry at or
	// after that index is discarded, regardless of its term. Conflict detection is the
	// caller's job (RaftLog.TruncateAndAppend returns exactly the suffix to pass here) -
	// passing an already-matching prefix would truncate acknowledged entries.
	Save(hs HardState, entries []LogEntry) error

	// Entries returns a continuous slice of log entries in the range [low, high).
	// maxBytes bounds the total memory footprint of returned entries to prevent OOMs.
	Entries(low, high uint64, maxBytes uint64) ([]LogEntry, error)

	// Term returns the term of the entry at index.
	Term(index uint64) (uint64, error)

	// FirstIndex returns the first available log index (the horizon after snapshot compaction).
	FirstIndex() (uint64, error)

	// LastIndex returns the highest log index present in the engine.
	LastIndex() (uint64, error)

	// CreateSnapshot compacts the log through lastIncludedIndex and stores state machine bytes.
	CreateSnapshot(meta SnapshotMeta, stateMachineData []byte) error

	// ApplySnapshot restores storage directly from an incoming leader snapshot.
	ApplySnapshot(meta SnapshotMeta, stateMachineData []byte) error

	// Close safely flushes buffers and closes underlying file descriptors.
	Close() error
}

var (
	prefixHardState = []byte("!raft:hs")
	prefixLogEntry  = []byte("!raft:log:")
	prefixSnapshot  = []byte("!raft:snap")
)

// KVStorage implements Storage backed by a Layer 0 ByteEngine (Pebble/SkipList pattern).
type KVStorage struct {
	mu         sync.RWMutex
	engine     raw.ByteEngine
	firstIndex uint64
	lastIndex  uint64
}

// NewKVStorage instantiates a durable Raft storage adapter over Layer 0.
func NewKVStorage(engine raw.ByteEngine) (*KVStorage, error) {
	s := &KVStorage{
		engine:     engine,
		firstIndex: 1,
		lastIndex:  0,
	}

	it := engine.NewIterator()
	defer it.Close()

	it.Seek(prefixLogEntry)
	if it.Valid() && bytes.HasPrefix(it.Key(), prefixLogEntry) {
		s.firstIndex = decodeLogKey(it.Key())
	}
	for it.Valid() && bytes.HasPrefix(it.Key(), prefixLogEntry) {
		s.lastIndex = decodeLogKey(it.Key())
		it.Next()
	}
	return s, nil
}

func encodeLogKey(index uint64) []byte {
	buf := make([]byte, len(prefixLogEntry)+8)
	copy(buf, prefixLogEntry)
	binary.BigEndian.PutUint64(buf[len(prefixLogEntry):], index)
	return buf
}

func decodeLogKey(rawKey []byte) uint64 {
	return binary.BigEndian.Uint64(rawKey[len(prefixLogEntry):])
}

func (k *KVStorage) InitialState() (HardState, SnapshotMeta, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	var hs HardState
	val, err := k.engine.Get(prefixHardState)
	if err == nil && len(val) > 0 {
		_ = json.Unmarshal(val, &hs)
	}

	var meta SnapshotMeta
	snapVal, err := k.engine.Get(prefixSnapshot)
	if err == nil && len(snapVal) > 0 {
		_ = json.Unmarshal(snapVal, &meta)
	}

	return hs, meta, nil
}

func (k *KVStorage) Save(hs HardState, entries []LogEntry) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if hs.Term != 0 {
		hsBytes, _ := json.Marshal(hs)
		if err := k.engine.Put(prefixHardState, hsBytes); err != nil {
			return err
		}
	}

	if len(entries) == 0 {
		return nil
	}

	// Truncate conflicts if any entry overlaps
	firstNew := entries[0].Index
	if k.lastIndex >= firstNew {
		for i := firstNew; i <= k.lastIndex; i++ {
			_ = k.engine.Delete(encodeLogKey(i))
		}
		k.lastIndex = firstNew - 1
	}

	// Write new entries sequentially
	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		if err := k.engine.Put(encodeLogKey(entry.Index), data); err != nil {
			return err
		}
		k.lastIndex = entry.Index
	}
	return nil
}

func (k *KVStorage) Entries(low, high uint64, maxBytes uint64) ([]LogEntry, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if low < k.firstIndex {
		return nil, ErrCompacted
	}
	if high > k.lastIndex+1 {
		return nil, ErrUnavailable
	}

	var results []LogEntry
	var totalBytes uint64

	for idx := low; idx < high; idx++ {
		rawVal, err := k.engine.Get(encodeLogKey(idx))
		if err != nil {
			break
		}
		var entry LogEntry
		if err := json.Unmarshal(rawVal, &entry); err != nil {
			return nil, err
		}
		entrySize := uint64(len(entry.Data)) + 16
		if len(results) > 0 && totalBytes+entrySize > maxBytes {
			break
		}
		results = append(results, entry)
		totalBytes += entrySize
	}
	return results, nil
}

func (k *KVStorage) Term(index uint64) (uint64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	if index < k.firstIndex {
		return 0, ErrCompacted
	}
	if index > k.lastIndex {
		return 0, ErrUnavailable
	}

	rawVal, err := k.engine.Get(encodeLogKey(index))
	if err != nil {
		return 0, ErrUnavailable
	}

	var entry LogEntry
	if err := json.Unmarshal(rawVal, &entry); err != nil {
		return 0, err
	}
	return entry.Term, nil
}

func (k *KVStorage) FirstIndex() (uint64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.firstIndex, nil
}

func (k *KVStorage) LastIndex() (uint64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.lastIndex, nil
}

func (k *KVStorage) CreateSnapshot(meta SnapshotMeta, data []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if meta.LastIncludedIndex < k.firstIndex {
		return ErrSnapshotOutOfDate
	}

	for i := k.firstIndex; i <= meta.LastIncludedIndex; i++ {
		_ = k.engine.Delete(encodeLogKey(i))
	}

	k.firstIndex = meta.LastIncludedIndex + 1
	if k.lastIndex < meta.LastIncludedIndex {
		k.lastIndex = meta.LastIncludedIndex
	}

	snapBytes, _ := json.Marshal(meta)
	return k.engine.Put(prefixSnapshot, snapBytes)
}

func (k *KVStorage) ApplySnapshot(meta SnapshotMeta, data []byte) error {
	return k.CreateSnapshot(meta, data)
}

func (k *KVStorage) Close() error {
	return k.engine.Close()
}
