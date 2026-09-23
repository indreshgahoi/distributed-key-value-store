package raft

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

var (
	keyHardState    = []byte("!raft:hs")
	keySnapshotMeta = []byte("!raft:snap")
	keySnapshotData = []byte("!raft:snapdata")
	prefixLogEntry  = []byte("!raft:log:")
)

// KVStorage implements Storage over a Layer 0 ByteEngine. It is exactly as
// durable as that engine - with the in-memory SkipListEngine, state survives
// a RaftNode restart (the engine object is reused) but not a process crash,
// which makes it the storage of choice for tests that simulate restarts.
type KVStorage struct {
	mu        sync.RWMutex
	engine    raw.ByteEngine
	hs        HardState
	snap      SnapshotMeta
	lastIndex uint64
}

// NewKVStorage opens (or recovers) Raft storage over engine.
func NewKVStorage(engine raw.ByteEngine) (*KVStorage, error) {
	k := &KVStorage{engine: engine}
	if err := k.getJSON(keyHardState, &k.hs); err != nil {
		return nil, err
	}
	if err := k.getJSON(keySnapshotMeta, &k.snap); err != nil {
		return nil, err
	}

	// Recover the log tail. Layer 0 deletes leave a key with an empty value
	// behind, so only non-empty values count as live entries.
	k.lastIndex = k.snap.LastIncludedIndex
	it := engine.NewIterator()
	defer it.Close()
	for it.Seek(prefixLogEntry); it.Valid() && bytes.HasPrefix(it.Key(), prefixLogEntry); it.Next() {
		if idx := decodeLogKey(it.Key()); len(it.Value()) > 0 && idx > k.lastIndex {
			k.lastIndex = idx
		}
	}
	return k, it.Error()
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

// getJSON decodes key into v, leaving v untouched if the key is absent.
func (k *KVStorage) getJSON(key []byte, v any) error {
	val, err := k.engine.Get(key)
	if err == raw.ErrNotFound || (err == nil && len(val) == 0) {
		return nil
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(val, v)
}

func (k *KVStorage) putJSON(key []byte, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return k.engine.Put(key, data)
}

func (k *KVStorage) InitialState() (HardState, SnapshotMeta, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.hs, k.snap, nil
}

func (k *KVStorage) Save(hs HardState, entries []LogEntry) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if hs.Term != 0 && hs != k.hs {
		if err := k.putJSON(keyHardState, hs); err != nil {
			return err
		}
		k.hs = hs
	}
	if len(entries) == 0 {
		return nil
	}

	first := entries[0].Index
	if first <= k.snap.LastIncludedIndex || first > k.lastIndex+1 {
		return fmt.Errorf("raft/storage: cannot write entries from %d (snapshot %d, last %d)",
			first, k.snap.LastIncludedIndex, k.lastIndex)
	}
	if err := k.deleteRange(first, k.lastIndex); err != nil {
		return err
	}
	for _, e := range entries {
		if err := k.putJSON(encodeLogKey(e.Index), e); err != nil {
			return err
		}
	}
	k.lastIndex = entries[len(entries)-1].Index
	return nil
}

// deleteRange removes log entries [from, to].
func (k *KVStorage) deleteRange(from, to uint64) error {
	for i := from; i <= to; i++ {
		if err := k.engine.Delete(encodeLogKey(i)); err != nil {
			return err
		}
	}
	return nil
}

func (k *KVStorage) Entries(low, high uint64, maxBytes uint64) ([]LogEntry, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if low <= k.snap.LastIncludedIndex {
		return nil, ErrCompacted
	}
	if high > k.lastIndex+1 {
		return nil, ErrUnavailable
	}

	var results []LogEntry
	var totalBytes uint64
	for idx := low; idx < high; idx++ {
		e, err := k.entryLocked(idx)
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

func (k *KVStorage) entryLocked(index uint64) (LogEntry, error) {
	var e LogEntry
	val, err := k.engine.Get(encodeLogKey(index))
	if err == raw.ErrNotFound || (err == nil && len(val) == 0) {
		return e, ErrUnavailable
	}
	if err != nil {
		return e, err
	}
	return e, json.Unmarshal(val, &e)
}

func (k *KVStorage) Term(index uint64) (uint64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.termLocked(index)
}

func (k *KVStorage) termLocked(index uint64) (uint64, error) {
	switch {
	case index == k.snap.LastIncludedIndex:
		return k.snap.LastIncludedTerm, nil
	case index < k.snap.LastIncludedIndex:
		return 0, ErrCompacted
	case index > k.lastIndex:
		return 0, ErrUnavailable
	}
	e, err := k.entryLocked(index)
	return e.Term, err
}

func (k *KVStorage) FirstIndex() (uint64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.snap.LastIncludedIndex + 1, nil
}

func (k *KVStorage) LastIndex() (uint64, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.lastIndex, nil
}

func (k *KVStorage) CreateSnapshot(meta SnapshotMeta, data []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if meta.LastIncludedIndex <= k.snap.LastIncludedIndex {
		return ErrSnapshotOutOfDate
	}
	if meta.LastIncludedIndex > k.lastIndex {
		return fmt.Errorf("raft/storage: snapshot index %d is beyond the log (last %d)", meta.LastIncludedIndex, k.lastIndex)
	}
	return k.installLocked(meta, data, true)
}

func (k *KVStorage) ApplySnapshot(meta SnapshotMeta, data []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if meta.LastIncludedIndex <= k.snap.LastIncludedIndex {
		return ErrSnapshotOutOfDate
	}
	term, err := k.termLocked(meta.LastIncludedIndex)
	return k.installLocked(meta, data, err == nil && term == meta.LastIncludedTerm)
}

// installLocked records the snapshot, then drops the log it covers - or the
// whole log, if the entries after it can't be kept.
func (k *KVStorage) installLocked(meta SnapshotMeta, data []byte, keepSuffix bool) error {
	if err := k.engine.Put(keySnapshotData, data); err != nil {
		return err
	}
	if err := k.putJSON(keySnapshotMeta, meta); err != nil {
		return err
	}
	oldLast := k.lastIndex
	dropTo := meta.LastIncludedIndex // compaction: drop through the snapshot
	if !keepSuffix {
		dropTo = oldLast // divergent history: drop everything
	}
	if err := k.deleteRange(k.snap.LastIncludedIndex+1, min(dropTo, oldLast)); err != nil {
		return err
	}
	k.snap = meta
	if keepSuffix {
		k.lastIndex = oldLast
	} else {
		k.lastIndex = meta.LastIncludedIndex
	}
	return nil
}

func (k *KVStorage) SnapshotData() ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.snap.LastIncludedIndex == 0 {
		return nil, nil
	}
	val, err := k.engine.Get(keySnapshotData)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(val), nil
}

func (k *KVStorage) Close() error {
	return k.engine.Close()
}
