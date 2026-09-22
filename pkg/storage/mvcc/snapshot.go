package mvcc

import (
	"encoding/binary"
	"io"
	"os"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/codec"
)

// ExportSnapshot streams all active, non-shadowed records <= safeWatermarkTS.
func (s *Store) ExportSnapshot(w io.Writer, safeWatermarkTS uint64) error {
	iter := s.raw.NewIterator()
	defer iter.Close()

	seekTarget := codec.EncodeKeyAppend(nil, nil, ^uint64(0))
	iter.Seek(seekTarget)

	var scratchKey []byte
	var lastVisitedKey []byte

	for iter.Valid() {
		physKey := iter.Key()
		var err error
		scratchKey, versionTS, err := codec.DecodeKey(scratchKey, physKey)
		if err != nil {
			return err
		}

		// Skip older shadowed versions of the same user key
		if string(scratchKey) == string(lastVisitedKey) {
			iter.Next()
			continue
		}

		if versionTS <= safeWatermarkTS {
			lastVisitedKey = append(lastVisitedKey[:0], scratchKey...)

			op, payload, err := codec.DecodeValue(iter.Value())
			if err != nil {
				return err
			}

			// Only write live records (skip tombstones)
			if op == codec.OpTypePut {
				// [KeyLen: 4B][Key][TS: 8B][ValLen: 4B][Val]
				if err := binary.Write(w, binary.BigEndian, uint32(len(scratchKey))); err != nil {
					return err
				}
				if _, err := w.Write(scratchKey); err != nil {
					return err
				}
				if err := binary.Write(w, binary.BigEndian, versionTS); err != nil {
					return err
				}
				if err := binary.Write(w, binary.BigEndian, uint32(len(payload))); err != nil {
					return err
				}
				if _, err := w.Write(payload); err != nil {
					return err
				}
			}
		}
		iter.Next()
	}
	return iter.Error()
}

// RestoreSnapshot reads binary records and inserts them back into Layer 0.
func (s *Store) RestoreSnapshot(r io.Reader) error {
	for {
		var keyLen uint32
		err := binary.Read(r, binary.BigEndian, &keyLen)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		userKey := make([]byte, keyLen)
		if _, err := io.ReadFull(r, userKey); err != nil {
			return err
		}

		var versionTS uint64
		if err := binary.Read(r, binary.BigEndian, &versionTS); err != nil {
			return err
		}

		var valLen uint32
		if err := binary.Read(r, binary.BigEndian, &valLen); err != nil {
			return err
		}

		value := make([]byte, valLen)
		if _, err := io.ReadFull(r, value); err != nil {
			return err
		}

		if err := s.Put(userKey, value, versionTS); err != nil {
			return err
		}
	}
	return nil
}

// SaveSnapshotToFile writes the snapshot to disk using atomic rename.
func (s *Store) SaveSnapshotToFile(filePath string, safeWatermarkTS uint64) error {
	tmpFile := filePath + ".tmp"
	file, err := os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if err := s.ExportSnapshot(file, safeWatermarkTS); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	file.Close()

	return os.Rename(tmpFile, filePath)
}

// LoadSnapshotFromFile restores Layer 0 from a file on disk upon reboot.
func (s *Store) LoadSnapshotFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // First boot: start with empty store
		}
		return err
	}
	defer file.Close()
	return s.RestoreSnapshot(file)
}
