package raw

import (
	"bytes"
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const (
	maxHeight = 20
	branching = 4 // p = 1/4
	nodeAlign = 8 // nodes hold 64-bit atomics, which must be 8-byte aligned
)

// node is stored inside the arena. All fields that change after the node is
// published are read and written atomically.
type node struct {
	keyOffset uint32
	keyLen    uint32
	// value packs the value's arena offset (high 32 bits) and length (low 32
	// bits) into one word, so a concurrent reader always sees a matching
	// pair - never a new length with an old offset.
	value        uint64
	height       uint16
	_            [6]byte
	nextPointers [maxHeight]uint64
}

var nodeSize = uint32(unsafe.Sizeof(node{}))

func packValue(offset, length uint32) uint64 { return uint64(offset)<<32 | uint64(length) }

func unpackValue(v uint64) (offset, length uint32) { return uint32(v >> 32), uint32(v) }

// Arena is a fixed-size bump allocator: nodes, keys and values are carved
// out of one []byte, so the skiplist creates no per-entry garbage for the
// Go GC to trace. Memory is never freed individually; the whole arena is
// discarded when the engine is rebuilt.
type Arena struct {
	buf []byte
	off uint32
}

func newArena(capacity uint32) *Arena {
	return &Arena{
		buf: make([]byte, capacity),
		off: nodeAlign, // offset 0 is the null pointer
	}
}

// alloc reserves size bytes aligned to align, or reports false if the arena
// can't fit them. Lock-free: concurrent writers race with CAS.
func (a *Arena) alloc(size, align uint32) (uint32, bool) {
	for {
		old := atomic.LoadUint32(&a.off)
		start := (uint64(old) + uint64(align) - 1) &^ (uint64(align) - 1)
		end := start + uint64(size)
		if end > uint64(len(a.buf)) {
			return 0, false
		}
		if atomic.CompareAndSwapUint32(&a.off, old, uint32(end)) {
			return uint32(start), true
		}
	}
}

// allocBytes copies b into the arena and returns its offset.
func (a *Arena) allocBytes(b []byte) (uint32, bool) {
	if len(b) == 0 {
		return 0, true
	}
	off, ok := a.alloc(uint32(len(b)), 1)
	if ok {
		copy(a.buf[off:], b)
	}
	return off, ok
}

func (a *Arena) getBytes(offset, length uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+length]
}

// SkipListEngine implements Layer 0 as a lock-free concurrent skip list over
// an arena. Writers never block readers or each other; they publish with
// compare-and-swap and retry on contention.
type SkipListEngine struct {
	arena  *Arena
	head   uint32
	height int32
	closed uint32
}

// NewSkipListEngine creates an engine with a fixed memory budget.
func NewSkipListEngine(arenaSize uint32) *SkipListEngine {
	arena := newArena(arenaSize)
	head, ok := arena.alloc(nodeSize, nodeAlign)
	if !ok {
		panic("raw: arena too small for the head node")
	}
	*(*node)(unsafe.Pointer(&arena.buf[head])) = node{height: maxHeight}
	return &SkipListEngine{arena: arena, head: head, height: 1}
}

// NewEmpty implements EmptyCloner.
func (s *SkipListEngine) NewEmpty() ByteEngine {
	return NewSkipListEngine(uint32(len(s.arena.buf)))
}

// MemoryUsage implements MemoryReporter.
func (s *SkipListEngine) MemoryUsage() (used, capacity uint64) {
	return uint64(atomic.LoadUint32(&s.arena.off)), uint64(len(s.arena.buf))
}

func (s *SkipListEngine) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.arena.buf[offset]))
}

func (s *SkipListEngine) keyOf(n *node) []byte { return s.arena.getBytes(n.keyOffset, n.keyLen) }

func (s *SkipListEngine) valueOf(n *node) []byte {
	off, length := unpackValue(atomic.LoadUint64(&n.value))
	return s.arena.getBytes(off, length)
}

func (s *SkipListEngine) randomHeight() uint16 {
	h := uint16(1)
	for h < maxHeight && rand.Intn(branching) == 0 {
		h++
	}
	return h
}

// findSplice fills pre/next with, at every level, the last node before key
// and the first node at or after it.
func (s *SkipListEngine) findSplice(key []byte, pre, next *[maxHeight]uint32) {
	curr := s.head
	for h := int(atomic.LoadInt32(&s.height)) - 1; h >= 0; h-- {
		for {
			nextOffset := uint32(atomic.LoadUint64(&s.getNode(curr).nextPointers[h]))
			if nextOffset == 0 || bytes.Compare(s.keyOf(s.getNode(nextOffset)), key) >= 0 {
				break
			}
			curr = nextOffset
		}
		pre[h] = curr
		next[h] = uint32(atomic.LoadUint64(&s.getNode(curr).nextPointers[h]))
	}
}

// Put inserts key or replaces its value.
func (s *SkipListEngine) Put(key, value []byte) error {
	if atomic.LoadUint32(&s.closed) == 1 {
		return ErrClosed
	}
	valOffset, ok := s.arena.allocBytes(value)
	if !ok {
		return ErrArenaFull
	}
	packed := packValue(valOffset, uint32(len(value)))

	var pre, next [maxHeight]uint32
	s.findSplice(key, &pre, &next)
	if s.updateIfPresent(next[0], key, packed) {
		return nil
	}

	keyOffset, ok := s.arena.allocBytes(key)
	if !ok {
		return ErrArenaFull
	}
	nodeOffset, ok := s.arena.alloc(nodeSize, nodeAlign)
	if !ok {
		return ErrArenaFull
	}
	height := s.randomHeight()
	nd := s.getNode(nodeOffset)
	*nd = node{keyOffset: keyOffset, keyLen: uint32(len(key)), value: packed, height: height}

	for {
		currH := atomic.LoadInt32(&s.height)
		if int32(height) <= currH || atomic.CompareAndSwapInt32(&s.height, currH, int32(height)) {
			break
		}
	}

	// Link bottom-up: once linked at level 0 the node is visible to readers;
	// higher levels are only shortcuts.
	for h := uint16(0); h < height; h++ {
		for {
			p := pre[h]
			if p == 0 {
				p = s.head
			}
			atomic.StoreUint64(&nd.nextPointers[h], uint64(next[h]))
			if atomic.CompareAndSwapUint64(&s.getNode(p).nextPointers[h], uint64(next[h]), uint64(nodeOffset)) {
				break
			}
			// Lost a race: recompute the splice. If a concurrent writer
			// inserted this same key first, update its node instead of
			// linking a duplicate (only possible before level 0 is linked).
			s.findSplice(key, &pre, &next)
			if h == 0 && s.updateIfPresent(next[0], key, packed) {
				return nil
			}
		}
	}
	return nil
}

// updateIfPresent stores packed as the value of candidate if it holds key.
func (s *SkipListEngine) updateIfPresent(candidate uint32, key []byte, packed uint64) bool {
	if candidate == 0 {
		return false
	}
	n := s.getNode(candidate)
	if !bytes.Equal(s.keyOf(n), key) {
		return false
	}
	atomic.StoreUint64(&n.value, packed)
	return true
}

func (s *SkipListEngine) Get(key []byte) ([]byte, error) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return nil, ErrClosed
	}
	// Hand-rolled rather than findSplice: a hit can return from whatever
	// level it's found on instead of descending to level 0 - the hot path.
	curr := s.head
	for h := int(atomic.LoadInt32(&s.height)) - 1; h >= 0; h-- {
		for {
			nextOffset := uint32(atomic.LoadUint64(&s.getNode(curr).nextPointers[h]))
			if nextOffset == 0 {
				break
			}
			n := s.getNode(nextOffset)
			cmp := bytes.Compare(s.keyOf(n), key)
			if cmp == 0 {
				return s.valueOf(n), nil
			}
			if cmp > 0 {
				break
			}
			curr = nextOffset
		}
	}
	return nil, ErrNotFound
}

// Delete clears key's value. Layer 0 has no physical removal (the node stays
// linked with an empty value); MVCC deletes are tombstone versions instead.
func (s *SkipListEngine) Delete(key []byte) error {
	return s.Put(key, nil)
}

func (s *SkipListEngine) Close() error {
	atomic.StoreUint32(&s.closed, 1)
	return nil
}

func (s *SkipListEngine) NewIterator() Iterator {
	return &skiplistIterator{engine: s}
}

type skiplistIterator struct {
	engine *SkipListEngine
	curr   uint32
	err    error
}

func (it *skiplistIterator) checkOpen() bool {
	if it.err != nil {
		return false
	}
	if atomic.LoadUint32(&it.engine.closed) == 1 {
		it.err = ErrClosed
		it.curr = 0
		return false
	}
	return true
}

// Seek positions the cursor at the first key >= target.
func (it *skiplistIterator) Seek(target []byte) {
	if !it.checkOpen() {
		return
	}
	var pre, next [maxHeight]uint32
	it.engine.findSplice(target, &pre, &next)
	it.curr = next[0]
}

// First positions the cursor at the smallest key.
func (it *skiplistIterator) First() {
	if !it.checkOpen() {
		return
	}
	it.curr = uint32(atomic.LoadUint64(&it.engine.getNode(it.engine.head).nextPointers[0]))
}

func (it *skiplistIterator) Next() {
	if it.curr != 0 {
		it.curr = uint32(atomic.LoadUint64(&it.engine.getNode(it.curr).nextPointers[0]))
	}
}

func (it *skiplistIterator) Valid() bool   { return it.curr != 0 && it.err == nil }
func (it *skiplistIterator) Key() []byte   { return it.engine.keyOf(it.engine.getNode(it.curr)) }
func (it *skiplistIterator) Value() []byte { return it.engine.valueOf(it.engine.getNode(it.curr)) }
func (it *skiplistIterator) Error() error  { return it.err }

func (it *skiplistIterator) Close() error {
	it.curr = 0
	it.err = nil
	return nil
}
