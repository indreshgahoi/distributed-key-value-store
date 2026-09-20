package mvcc

import (
	"bytes"
	"slices"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/codec"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

type Store struct {
	raw raw.ByteEngine
}

// NewStore initializes a Layer 2 MVCC store backed by a Layer 0 ByteEngine.
func NewStore(raw raw.ByteEngine) *Store {
	return &Store{raw: raw}
}

// Put encodes the user key with commitTS and writes an OpTypePut record.
func (s *Store) Put(key, value []byte, commitTS uint64) error {
	// 1. Allocate physical key buffer without extra heap esacpe
	encKeyLen := codec.EncodedKeyLen(key)
	kBuf := make([]byte, 0, encKeyLen)
	physKey := codec.EncodeKeyAppend(kBuf, key, commitTS)

	// 2. Encode physical value payload with OptypePut header
	vBuf := make([]byte, 0, 1+len(value))
	physVal := codec.EncodeValueAppend(vBuf, codec.OpTypePut, value)
	// 3. Write into layer 0
	return s.raw.Put(physKey, physVal)
}

// Delete appends a tombstone version at commitTS.
func (s *Store) Delete(key []byte, commitTS uint64) error {
	encKeyLen := codec.EncodedKeyLen(key)
	kBuf := make([]byte, 0, encKeyLen)
	physKey := codec.EncodeKeyAppend(kBuf, key, commitTS)

	// Write OpTypeDelete with empty payload
	vBuf := make([]byte, 0, 1)
	physVal := codec.EncodeValueAppend(vBuf, codec.OpTypeDelete, nil)

	return s.raw.Put(physKey, physVal)
}

// Get finds the latest version visible at or before readTS.
func (s *Store) Get(key []byte, readTS uint64) ([]byte, error) {
	iter := s.raw.NewIterator()
	defer iter.Close()
	// Target the highest verison visibile at or before readTS
	// In descending order, ^readTS is the smallest inverted timestamp <= readTS
	encKeyLen := codec.EncodedKeyLen(key)
	seekBuf := make([]byte, 0, encKeyLen)
	seekTarget := codec.EncodeKeyAppend(seekBuf, key, readTS)

	iter.Seek(seekTarget)
	if err := iter.Error(); err != nil {
		return nil, err
	}
	var scratchKey []byte
	for iter.Valid() {
		physcKey := iter.Key()
		var err error
		scratchKey, versionTS, err := codec.DecodeKey(scratchKey, physcKey)
		if err != nil {
			return nil, err
		}
		// Check if the iterator moved past the requested key
		if !bytes.Equal(scratchKey, key) {
			break
		}
		// Checkd the visibility invriant: versionTS <= readTS
		if versionTS <= readTS {
			op, payload, err := codec.DecodeValue(iter.Value())
			if err != nil {
				return nil, err
			}
			if op == codec.OpTypeDelete {
				return nil, ErrKeyNotFound
			}
			// Return cloned slice to protech storage engine memory
			return slices.Clone(payload), nil
		}

		iter.Next()
	}
	return nil, ErrKeyNotFound
}

// Scan traverses [startKey, endKey) at snapshot readTS, skipping shadows and tombstones.
func (s *Store) Scan(startKey, endKey []byte, readTS uint64, limit int) ([]KeyValue, error) {
	iter := s.raw.NewIterator()
	defer iter.Close()

	// Initial seek
	encKeyLen := codec.EncodedKeyLen(startKey)
	seekBuf := make([]byte, 0, encKeyLen)
	seekTarget := codec.EncodeKeyAppend(seekBuf, startKey, readTS)

	iter.Seek(seekTarget)
	if err := iter.Error(); err != nil {
		return nil, err
	}

	var results []KeyValue
	var lastVisitedKey []byte
	var scratchKey []byte

	for iter.Valid() {
		if limit > 0 && len(results) >= limit {
			break
		}

		physKey := iter.Key()
		var err error
		scratchKey, versionTS, err := codec.DecodeKey(scratchKey, physKey)
		if err != nil {
			return nil, err
		}

		// Stop if cursor reached or passed endKey (if endKey is specified)
		if len(endKey) > 0 && bytes.Compare(scratchKey, endKey) >= 0 {
			break
		}

		// If we already collected or evaluated the latest version for this user key,
		// skip older historical versions (shadows).
		if bytes.Equal(scratchKey, lastVisitedKey) {
			iter.Next()
			continue
		}

		// Candidate version found
		if versionTS <= readTS {
			// Mark this user key as visited
			lastVisitedKey = append(lastVisitedKey[:0], scratchKey...)

			op, payload, err := codec.DecodeValue(iter.Value())
			if err != nil {
				return nil, err
			}

			// If it's a Put, include in results; if Delete (tombstone), omit it.
			if op == codec.OpTypePut {
				results = append(results, KeyValue{
					Key:       slices.Clone(scratchKey),
					Value:     slices.Clone(payload),
					Timestamp: versionTS,
				})
			}
		}

		iter.Next()
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return results, nil
}

func (s *Store) Close() error {
	return s.raw.Close()
}
