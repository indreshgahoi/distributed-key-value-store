package raw

import "errors"

var (
	ErrNotFound = errors.New("raw: key not found")
	ErrClosed   = errors.New("raw: engine is closed")
)

// Layer 0 Byte Engine
type ByteEngine interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error

	NewIterator() Iterator
	Close() error
}

type Iterator interface {
	Seek(targetKey []byte)
	First()
	Valid() bool
	Next()
	Key() []byte
	Value() []byte
	Error() error
	Close() error
}
