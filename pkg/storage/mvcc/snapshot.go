package mvcc

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/codec"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

// Snapshot stream format (all integers big-endian):
//
//	magic   "MVCCSNP1"
//	record* [keyLen u32][key][ts u64][op u8][valLen u32][value]
//	end     [0xFFFFFFFF][crc32c u32 of everything before it]
//
// The end marker and checksum make truncation and corruption detectable, so
// a damaged snapshot is rejected instead of silently restoring partial state.
var snapshotMagic = []byte("MVCCSNP1")

const endOfRecords = ^uint32(0)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ExportSnapshot streams the store's state as of safeWatermarkTS: for each
// key, every version newer than the watermark plus the newest version at or
// below it (dropped if that is a tombstone). Older versions can no longer be
// observed by any reader at or above the watermark, so they are omitted -
// which makes export-then-restore a compaction as well as a copy.
func (s *Store) ExportSnapshot(w io.Writer, safeWatermarkTS uint64) error {
	return exportEngine(s.current(), w, safeWatermarkTS)
}

func exportEngine(engine raw.ByteEngine, w io.Writer, watermark uint64) error {
	crc := crc32.New(crcTable)
	bw := bufio.NewWriter(io.MultiWriter(w, crc))
	if _, err := bw.Write(snapshotMagic); err != nil {
		return err
	}

	iter := engine.NewIterator()
	defer iter.Close()
	var (
		scratch, currentKey []byte
		haveBaseline        bool // emitted (or dropped) this key's version <= watermark
	)
	for iter.First(); iter.Valid(); iter.Next() {
		userKey, ts, err := codec.DecodeKey(scratch, iter.Key())
		if err != nil {
			return err
		}
		scratch = userKey
		if currentKey == nil || !bytes.Equal(userKey, currentKey) {
			currentKey = append(currentKey[:0], userKey...)
			haveBaseline = false
		}
		if ts <= watermark {
			if haveBaseline {
				continue // shadowed by a newer version at or below the watermark
			}
			haveBaseline = true
		}
		op, payload, err := codec.DecodeValue(iter.Value())
		if err != nil {
			return err
		}
		if op == codec.OpTypeDelete && ts <= watermark {
			continue // a baseline tombstone just means "absent"
		}
		if err := writeRecord(bw, userKey, ts, op, payload); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}

	if err := binary.Write(bw, binary.BigEndian, endOfRecords); err != nil {
		return err
	}
	if err := bw.Flush(); err != nil { // everything so far is now in crc
		return err
	}
	return binary.Write(w, binary.BigEndian, crc.Sum32())
}

func writeRecord(w io.Writer, key []byte, ts uint64, op codec.OpType, value []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(key)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.Write(key); err != nil {
		return err
	}
	var meta [13]byte
	binary.BigEndian.PutUint64(meta[:8], ts)
	meta[8] = byte(op)
	binary.BigEndian.PutUint32(meta[9:], uint32(len(value)))
	if _, err := w.Write(meta[:]); err != nil {
		return err
	}
	_, err := w.Write(value)
	return err
}

// RestoreSnapshot replaces the store's entire contents with the snapshot in
// r. The snapshot is loaded into a fresh engine and verified before it
// replaces the current one, so a corrupt snapshot leaves the store unchanged
// - and keys absent from the snapshot are really gone afterwards (merging
// into the existing engine would resurrect keys deleted before the snapshot).
func (s *Store) RestoreSnapshot(r io.Reader) error {
	return s.rebuildFrom(func(fresh raw.ByteEngine) error { return loadInto(fresh, r) })
}

// Compact rebuilds the store into a fresh engine holding only what is still
// observable at or above safeWatermarkTS, reclaiming the memory of shadowed
// versions and tombstones. The arena-based engine can't free individual
// entries, so this copy is how its memory is recovered.
func (s *Store) Compact(safeWatermarkTS uint64) error {
	return s.rebuildFrom(func(fresh raw.ByteEngine) error {
		pr, pw := io.Pipe()
		go func() { pw.CloseWithError(exportEngine(s.current(), pw, safeWatermarkTS)) }()
		err := loadInto(fresh, pr)
		pr.CloseWithError(err) // unblock the exporter if loading failed early
		return err
	})
}

// rebuildFrom fills a fresh engine via fill and, on success, swaps it in.
// Writers are paused for the duration; readers are not.
func (s *Store) rebuildFrom(fill func(fresh raw.ByteEngine) error) error {
	s.rebuild.Lock()
	defer s.rebuild.Unlock()

	cloner, ok := s.current().(raw.EmptyCloner)
	if !ok {
		return ErrCannotRebuild
	}
	fresh := cloner.NewEmpty()
	if err := fill(fresh); err != nil {
		fresh.Close()
		return err
	}
	// The old engine is not closed: in-flight readers may still be using it.
	// It is garbage once they finish.
	s.engine.Store(&engineRef{fresh})
	return nil
}

// loadInto reads and verifies a snapshot stream, writing its records into engine.
func loadInto(engine raw.ByteEngine, r io.Reader) error {
	crc := crc32.New(crcTable)
	br := &checksumReader{r: bufio.NewReader(r), crc: crc}

	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(br, magic); err != nil || !bytes.Equal(magic, snapshotMagic) {
		return fmt.Errorf("%w: missing header", ErrCorruptSnapshot)
	}
	for {
		var keyLen uint32
		if err := binary.Read(br, binary.BigEndian, &keyLen); err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
		}
		if keyLen == endOfRecords {
			break
		}
		key := make([]byte, keyLen)
		var meta [13]byte
		if _, err := io.ReadFull(br, key); err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
		}
		if _, err := io.ReadFull(br, meta[:]); err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
		}
		value := make([]byte, binary.BigEndian.Uint32(meta[9:]))
		if _, err := io.ReadFull(br, value); err != nil {
			return fmt.Errorf("%w: %v", ErrCorruptSnapshot, err)
		}
		op := codec.OpType(meta[8])
		if op != codec.OpTypePut && op != codec.OpTypeDelete {
			return fmt.Errorf("%w: unknown op %d", ErrCorruptSnapshot, op)
		}
		if err := putVersion(engine, key, binary.BigEndian.Uint64(meta[:8]), op, value); err != nil {
			return err
		}
	}

	want := crc.Sum32()
	br.crc = nil // the checksum itself is not part of the checksummed data
	var got uint32
	if err := binary.Read(br, binary.BigEndian, &got); err != nil || got != want {
		return fmt.Errorf("%w: checksum mismatch", ErrCorruptSnapshot)
	}
	return nil
}

// checksumReader feeds everything it reads into crc (while crc is non-nil).
type checksumReader struct {
	r   io.Reader
	crc hash.Hash32
}

func (c *checksumReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	if c.crc != nil {
		c.crc.Write(p[:n])
	}
	return n, err
}
