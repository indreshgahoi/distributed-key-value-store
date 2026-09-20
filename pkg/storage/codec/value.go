package codec

import "errors"

// OpType represents the state of an MVCC record (Put vs Tombstone Delete).
type OpType byte

const (
	OpTypePut    OpType = 1
	OpTypeDelete OpType = 2
)

var (
	// ErrMalformedValue is returned when decoding an empty or invalid value payload.
	ErrMalformedValue = errors.New("codec: malformed physical value payload")
)

// EncodeValueAppend appends a 1-byte OpType header followed by raw user value.
// It avoids heap allocation when dst has capacity.
func EncodeValueAppend(dst []byte, op OpType, val []byte) []byte {
	dst = append(dst, byte(op))
	return append(dst, val...)
}

// DecodeValue unpacks the OpType and returns a zero-copy subslice of the payload.
func DecodeValue(raw []byte) (OpType, []byte, error) {
	if len(raw) < 1 {
		return 0, nil, ErrMalformedValue
	}
	return OpType(raw[0]), raw[1:], nil
}
