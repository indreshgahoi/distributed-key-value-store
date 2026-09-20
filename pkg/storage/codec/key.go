package codec

import (
	"encoding/binary"
	"errors"
)

var (
	// ErrMalformedKey is returned when the byte stream cannot be decoded as an MVCC key.
	ErrMalformedKey = errors.New("codec: malformed physical key encoding")
)

const (
	escapeByte byte = 0x00
	escapedFF  byte = 0xFF
	terminator byte = 0x01
)

// EncodedKeyLen computes the exact length required for an encoded MVCC key.
// It allows the caller to pre-allocate buffers on the stack or from a sync.Pool.
func EncodedKeyLen(userKey []byte) int {
	escapedLen := 0
	for _, b := range userKey {
		if b == escapeByte {
			escapedLen += 2
		} else {
			escapedLen++
		}
	}
	// escapedLen + 2 bytes terminator (0x00, 0x01) + 8 bytes inverted uint64 timestamp
	return escapedLen + 2 + 8
}

// EncodeKeyAppend encodes an MVCC key into dst without heap allocations.
// Physical Key Layout: [Escaped User Key] [0x00 0x01] [^Timestamp (uint64 BigEndian)]
func EncodeKeyAppend(dst []byte, userKey []byte, ts uint64) []byte {
	// 1. Escape the user key
	for _, b := range userKey {
		if b == escapeByte {
			dst = append(dst, escapeByte, escapedFF)
		} else {
			dst = append(dst, b)
		}
	}

	// 2. Append terminator
	dst = append(dst, escapeByte, terminator)

	// 3. Append bitwise-inverted timestamp (big-endian) for descending sort order
	var tsBuf [8]byte
	binary.BigEndian.PutUint64(tsBuf[:], ^ts)
	return append(dst, tsBuf[:]...)
}

// DecodeKey decodes a physical key into the original user key and timestamp.
// If dst is provided with sufficient capacity, it unpacks into dst without allocating.
func DecodeKey(dst []byte, physicalKey []byte) ([]byte, uint64, error) {
	// At minimum: 2 bytes terminator (0x00, 0x01) + 8 bytes timestamp = 10 bytes
	if len(physicalKey) < 10 {
		return nil, 0, ErrMalformedKey
	}

	// 1. Extract inverted timestamp from tail
	tsStart := len(physicalKey) - 8
	invertedTS := binary.BigEndian.Uint64(physicalKey[tsStart:])
	ts := ^invertedTS

	rawKeyPart := physicalKey[:tsStart]
	if len(rawKeyPart) < 2 {
		return nil, 0, ErrMalformedKey
	}

	// 2. Validate terminator bytes [0x00, 0x01]
	if rawKeyPart[len(rawKeyPart)-2] != escapeByte || rawKeyPart[len(rawKeyPart)-1] != terminator {
		return nil, 0, ErrMalformedKey
	}
	escapedData := rawKeyPart[:len(rawKeyPart)-2]

	// 3. Unescape user key into dst buffer
	if dst == nil {
		dst = make([]byte, 0, len(escapedData))
	} else {
		dst = dst[:0]
	}

	for i := 0; i < len(escapedData); i++ {
		if escapedData[i] == escapeByte {
			if i+1 >= len(escapedData) || escapedData[i+1] != escapedFF {
				return nil, 0, ErrMalformedKey
			}
			dst = append(dst, escapeByte)
			i++ // skip escapedFF
		} else {
			dst = append(dst, escapedData[i])
		}
	}

	return dst, ts, nil
}
