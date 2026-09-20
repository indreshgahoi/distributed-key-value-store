package mvcc

import (
	"bytes"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/codec"
)

// GCStats records the outcome of a garbage collection sweep.
type GCStats struct {
	KeysInspected   int
	VersionsPurged  int
	TombstonesFreed int
}

// CompactBelowWatermark iterates through the storage engine and purges all versions
// that are shadowed and older than safeWatermarkTS.
func (s *Store) CompactBelowWatermark(startKey, endKey []byte, safeWatermarkTS uint64) (GCStats, error) {
	iter := s.raw.NewIterator()
	defer iter.Close()

	// Initial seek at the start of the key range
	seekTarget := codec.EncodeKeyAppend(nil, startKey, ^uint64(0)) // Highest possible TS
	iter.Seek(seekTarget)
	if err := iter.Error(); err != nil {
		return GCStats{}, err
	}

	var (
		stats               GCStats
		lastVisitedUserKey  []byte
		keptBaselineBelowWM bool
		keysToPurge         [][]byte
		scratchKey          []byte
	)

	for iter.Valid() {
		physKey := iter.Key()
		var err error
		scratchKey, versionTS, err := codec.DecodeKey(scratchKey, physKey)
		if err != nil {
			return stats, err
		}

		// Boundary check: stop if we stepped past endKey
		if len(endKey) > 0 && bytes.Compare(scratchKey, endKey) >= 0 {
			break
		}

		stats.KeysInspected++

		// Detected transition to a new user key
		if !bytes.Equal(scratchKey, lastVisitedUserKey) {
			lastVisitedUserKey = append(lastVisitedUserKey[:0], scratchKey...)
			keptBaselineBelowWM = false
		}

		op, _, err := codec.DecodeValue(iter.Value())
		if err != nil {
			return stats, err
		}

		// Rule 1: Any version strictly above the watermark must be kept intact.
		if versionTS > safeWatermarkTS {
			iter.Next()
			continue
		}

		// Rule 2: The FIRST version we encounter <= safeWatermarkTS is our baseline.
		if !keptBaselineBelowWM {
			keptBaselineBelowWM = true

			// If the baseline itself is a Tombstone, and it's below the watermark,
			// it means the key was deleted before any active transaction started.
			// Mark this tombstone for final physical eviction.
			if op == codec.OpTypeDelete {
				keysToPurge = append(keysToPurge, bytes.Clone(physKey))
				stats.TombstonesFreed++
			}
			iter.Next()
			continue
		}

		// Rule 3: Any SUBSEQUENT version <= safeWatermarkTS is shadowed and dead.
		keysToPurge = append(keysToPurge, bytes.Clone(physKey))
		stats.VersionsPurged++

		iter.Next()
	}

	if err := iter.Error(); err != nil {
		return stats, err
	}

	// Overwrite each purged physical slot with a valid, self-describing tombstone
	// rather than raw.Delete: Layer 0 has no real space reclamation, and
	// raw.Delete just writes a 0-byte value, which later codec.DecodeValue calls
	// cannot parse. Writing an explicit OpTypeDelete keeps the slot decodable and
	// correctly invisible to Get/Scan.
	purgedValue := codec.EncodeValueAppend(nil, codec.OpTypeDelete, nil)
	for _, deadKey := range keysToPurge {
		if err := s.raw.Put(deadKey, purgedValue); err != nil {
			return stats, err
		}
	}

	return stats, nil
}
