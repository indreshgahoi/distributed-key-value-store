package raw

import "errors"

var (
	ErrNotFound = errors.New("raw: key not found")
	ErrClosed   = errors.New("raw: engine is closed")

	// ErrArenaFull is returned by a write that doesn't fit in a fixed-size
	// engine. The engine stays readable; reclaim space by rebuilding into a
	// fresh engine (see mvcc.Store.Compact).
	ErrArenaFull = errors.New("raw: arena is full")
)

// ByteEngine is Layer 0: an ordered map from byte keys to byte values.
type ByteEngine interface {
	Get(key []byte) ([]byte, error)
	Put(key []byte, value []byte) error
	Delete(key []byte) error

	NewIterator() Iterator
	Close() error
}

// EmptyCloner is implemented by engines that can create a new, empty engine
// with the same configuration - what a rebuild (compaction or snapshot
// restore) writes into before atomically replacing the old engine.
type EmptyCloner interface {
	NewEmpty() ByteEngine
}

// MemoryReporter is implemented by engines with a fixed memory budget.
type MemoryReporter interface {
	// MemoryUsage returns bytes used and total capacity.
	MemoryUsage() (used, capacity uint64)
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
