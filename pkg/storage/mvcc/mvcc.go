package mvcc

import (
	"bytes"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/codec"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

// Store is Layer 2: a multi-version key-value store over a Layer 0 engine.
// Every write adds a version at a commit timestamp; reads see the newest
// version at or before their read timestamp.
//
// The underlying engine can be replaced atomically (RestoreSnapshot,
// Compact). Readers never block: they use whichever engine was current when
// they started. Writers are paused only while a replacement is being built,
// so no write can land in an engine that is about to be discarded.
type Store struct {
	engine  atomic.Pointer[engineRef]
	rebuild sync.RWMutex // held shared by writers, exclusively by a rebuild
}

type engineRef struct{ raw.ByteEngine }

// NewStore initializes a Layer 2 MVCC store backed by a Layer 0 ByteEngine.
func NewStore(engine raw.ByteEngine) *Store {
	s := &Store{}
	s.engine.Store(&engineRef{engine})
	return s
}

func (s *Store) current() raw.ByteEngine { return s.engine.Load().ByteEngine }

// Put encodes the user key with commitTS and writes an OpTypePut record.
func (s *Store) Put(key, value []byte, commitTS uint64) error {
	s.rebuild.RLock()
	defer s.rebuild.RUnlock()
	return putVersion(s.current(), key, commitTS, codec.OpTypePut, value)
}

// Delete appends a tombstone version at commitTS.
func (s *Store) Delete(key []byte, commitTS uint64) error {
	s.rebuild.RLock()
	defer s.rebuild.RUnlock()
	return putVersion(s.current(), key, commitTS, codec.OpTypeDelete, nil)
}

// putVersion writes one physical version record into engine.
func putVersion(engine raw.ByteEngine, key []byte, ts uint64, op codec.OpType, value []byte) error {
	physKey := codec.EncodeKeyAppend(make([]byte, 0, codec.EncodedKeyLen(key)), key, ts)
	physVal := codec.EncodeValueAppend(make([]byte, 0, 1+len(value)), op, value)
	return engine.Put(physKey, physVal)
}

// Get finds the latest version visible at or before readTS.
func (s *Store) Get(key []byte, readTS uint64) ([]byte, error) {
	iter := s.current().NewIterator()
	defer iter.Close()

	// Versions of a key sort newest-first (timestamps are stored inverted),
	// so seeking to (key, readTS) lands on the newest version <= readTS.
	iter.Seek(codec.EncodeKeyAppend(make([]byte, 0, codec.EncodedKeyLen(key)), key, readTS))
	if err := iter.Error(); err != nil {
		return nil, err
	}
	var scratchKey []byte
	for ; iter.Valid(); iter.Next() {
		userKey, versionTS, err := codec.DecodeKey(scratchKey, iter.Key())
		if err != nil {
			return nil, err
		}
		scratchKey = userKey
		if !bytes.Equal(userKey, key) {
			break // moved past this key's versions
		}
		if versionTS > readTS {
			continue
		}
		op, payload, err := codec.DecodeValue(iter.Value())
		if err != nil {
			return nil, err
		}
		if op == codec.OpTypeDelete {
			return nil, ErrKeyNotFound
		}
		return slices.Clone(payload), nil // never hand out engine memory
	}
	return nil, ErrKeyNotFound
}

// Scan traverses [startKey, endKey) at snapshot readTS, skipping shadows and tombstones.
func (s *Store) Scan(startKey, endKey []byte, readTS uint64, limit int) ([]KeyValue, error) {
	iter := s.current().NewIterator()
	defer iter.Close()

	iter.Seek(codec.EncodeKeyAppend(make([]byte, 0, codec.EncodedKeyLen(startKey)), startKey, readTS))
	if err := iter.Error(); err != nil {
		return nil, err
	}

	var results []KeyValue
	var lastVisitedKey, scratchKey []byte
	for ; iter.Valid(); iter.Next() {
		if limit > 0 && len(results) >= limit {
			break
		}
		userKey, versionTS, err := codec.DecodeKey(scratchKey, iter.Key())
		if err != nil {
			return nil, err
		}
		scratchKey = userKey
		if len(endKey) > 0 && bytes.Compare(userKey, endKey) >= 0 {
			break
		}
		// Already resolved this key's visible version: skip older shadows.
		if lastVisitedKey != nil && bytes.Equal(userKey, lastVisitedKey) {
			continue
		}
		if versionTS > readTS {
			continue
		}
		lastVisitedKey = append(lastVisitedKey[:0], userKey...)

		op, payload, err := codec.DecodeValue(iter.Value())
		if err != nil {
			return nil, err
		}
		if op == codec.OpTypePut {
			results = append(results, KeyValue{
				Key:       slices.Clone(userKey),
				Value:     slices.Clone(payload),
				Timestamp: versionTS,
			})
		}
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return results, nil
}

// MemoryUsage reports the current engine's memory use, if it has a budget.
func (s *Store) MemoryUsage() (used, capacity uint64, ok bool) {
	if r, isReporter := s.current().(raw.MemoryReporter); isReporter {
		used, capacity = r.MemoryUsage()
		return used, capacity, true
	}
	return 0, 0, false
}

func (s *Store) Close() error {
	return s.current().Close()
}
