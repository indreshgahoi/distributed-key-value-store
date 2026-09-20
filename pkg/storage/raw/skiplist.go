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
)

type node struct {
	keyOffset    uint32
	keyLen       uint32
	valOffset    uint32
	valLen       uint32
	height       uint16
	_            [6]byte // padding to align next pointers
	nextPointers [maxHeight]uint64
}

type Arena struct {
	buf []byte
	off uint32
}

func newArena(capacity uint32) *Arena {
	return &Arena{
		buf: make([]byte, capacity),
		off: 1, // Offset 0 is null pointer
	}
}

func (a *Arena) alloc(size uint32) uint32 {
	offset := atomic.AddUint32(&a.off, size)
	if offset > uint32(len(a.buf)) {
		panic("raw: arena out of memory")
	}
	return offset - size
}

func (a *Arena) getBytes(offset, length uint32) []byte {
	if offset == 0 {
		return nil
	}
	return a.buf[offset : offset+length]
}

// SkipListEngine implements Layer 0 Raw Engine
type SkipListEngine struct {
	arena  *Arena
	head   uint32
	height int32
	closed uint32
}

func NewSkipListEngine(arenaSize uint32) *SkipListEngine {
	arena := newArena(arenaSize)
	// allocate head node
	headNode := &node{height: maxHeight}
	headOffset := arena.alloc(uint32(unsafe.Sizeof(*headNode)))
	*(*node)(unsafe.Pointer(&arena.buf[headOffset])) = *headNode
	return &SkipListEngine{
		arena:  arena,
		head:   headOffset,
		height: 1,
	}
}

func (s *SkipListEngine) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&s.arena.buf[offset]))
}

func (s *SkipListEngine) randomHeight() uint16 {
	h := uint16(1)
	for h < maxHeight && rand.Intn(branching) == 0 {
		h++
	}
	return h
}

// findSplice requires pointer references so caller arrays receive modifications
func (s *SkipListEngine) findSplice(key []byte, pre *[maxHeight]uint32, next *[maxHeight]uint32) {
	curr := s.head
	currH := int(atomic.LoadInt32(&s.height)) - 1

	for h := currH; h >= 0; h-- {
		for {
			currNode := s.getNode(curr)
			nextOffSet := atomic.LoadUint64(&currNode.nextPointers[h])
			if nextOffSet == 0 {
				break
			}
			nextNode := s.getNode(uint32(nextOffSet))
			nextKey := s.arena.getBytes(nextNode.keyOffset, nextNode.keyLen)

			if bytes.Compare(nextKey, key) >= 0 {
				break
			}
			curr = uint32(nextOffSet)
		}
		pre[h] = curr
		next[h] = uint32(atomic.LoadUint64(&s.getNode(curr).nextPointers[h]))
	}
}

func (s *SkipListEngine) Put(key, value []byte) error {
	if atomic.LoadUint32(&s.closed) == 1 {
		return ErrClosed
	}

	var pre, next [maxHeight]uint32
	s.findSplice(key, &pre, &next)

	// Check if Key exists at Base Level
	if next[0] != 0 {
		nextNode := s.getNode(next[0])
		existingKey := s.arena.getBytes(nextNode.keyOffset, nextNode.keyLen)

		if bytes.Equal(existingKey, key) {
			// Append-only value update to ensure concurrent readers never observe torn reads.
			valOffset := s.arena.alloc(uint32(len(value)))
			copy(s.arena.buf[valOffset:], value)
			// Publish the len and offset atomically
			atomic.StoreUint32(&nextNode.valLen, uint32(len(value)))
			atomic.StoreUint32(&nextNode.valOffset, valOffset)
			return nil
		}
	}

	height := s.randomHeight()
	for {
		currH := atomic.LoadInt32(&s.height)
		if int32(height) <= currH || atomic.CompareAndSwapInt32(&s.height, currH, int32(height)) {
			break
		}
	}

	// Allocate new Node in Arena
	keyOffset := s.arena.alloc(uint32(len(key)))
	copy(s.arena.buf[keyOffset:], key)
	valueOffset := s.arena.alloc(uint32(len(value)))
	copy(s.arena.buf[valueOffset:], value)

	newNode := &node{
		keyOffset: keyOffset,
		keyLen:    uint32(len(key)),
		valOffset: valueOffset,
		valLen:    uint32(len(value)),
		height:    height,
	}
	newNodeOffset := s.arena.alloc(uint32(unsafe.Sizeof(*newNode)))
	*(*node)(unsafe.Pointer(&s.arena.buf[newNodeOffset])) = *newNode

	// Link Pointer bottom up
	for h := uint16(0); h < height; h++ {
		for {
			p := pre[h]
			n := next[h]
			if p == 0 {
				p = s.head
			}
			pNode := s.getNode(p)
			newNodeInArena := s.getNode(newNodeOffset)
			atomic.StoreUint64(&newNodeInArena.nextPointers[h], uint64(n))
			if atomic.CompareAndSwapUint64(&pNode.nextPointers[h], uint64(n), uint64(newNodeOffset)) {
				break
			}
			// try again
			s.findSplice(key, &pre, &next)
		}
	}

	return nil
}

func (s *SkipListEngine) Get(key []byte) ([]byte, error) {
	if atomic.LoadUint32(&s.closed) == 1 {
		return nil, ErrClosed
	}
	curr := s.head
	currH := int(atomic.LoadInt32(&s.height)) - 1

	// top to bottom search
	for h := currH; h >= 0; h-- {
		for {
			currNode := s.getNode(curr)
			nextOffSet := atomic.LoadUint64(&currNode.nextPointers[h])
			if nextOffSet == 0 {
				break
			}
			nextNode := s.getNode(uint32(nextOffSet))
			nextKey := s.arena.getBytes(nextNode.keyOffset, nextNode.keyLen)

			cmp := bytes.Compare(nextKey, key)
			if cmp == 0 {
				return s.arena.getBytes(atomic.LoadUint32(&nextNode.valOffset),
					atomic.LoadUint32(&nextNode.valLen)), nil
			}
			if cmp > 0 {
				break
			}
			curr = uint32(nextOffSet)
		}
	}
	return nil, ErrNotFound
}

func (s *SkipListEngine) Delete(key []byte) error {
	// Layer 0 Delete removes physical key. Layer 2 MVCC uses tombstone
	return s.Put(key, nil)
}

func (s *SkipListEngine) Close() error {
	atomic.StoreUint32(&s.closed, 1)
	return nil
}

func (s *SkipListEngine) NewIterator() Iterator {
	return &skiplistIterator{engine: s, curr: 0, err: nil}
}

type skiplistIterator struct {
	engine *SkipListEngine
	curr   uint32
	err    error
}

func (it *skiplistIterator) Seek(target []byte) {
	if it.err != nil {
		return
	}
	if atomic.LoadUint32(&it.engine.closed) == 1 {
		it.err = ErrClosed
		it.curr = 0
		return
	}

	curr := it.engine.head
	currH := int(atomic.LoadInt32(&it.engine.height)) - 1

	// top to bottom search
	for h := currH; h >= 0; h-- {
		for {
			currNode := it.engine.getNode(curr)
			nextOffSet := atomic.LoadUint64(&currNode.nextPointers[h])
			if nextOffSet == 0 {
				break
			}
			nextNode := it.engine.getNode(uint32(nextOffSet))
			nextKey := it.engine.arena.getBytes(nextNode.keyOffset, nextNode.keyLen)

			cmp := bytes.Compare(nextKey, target)
			if cmp >= 0 {
				break
			}

			curr = uint32(nextOffSet)
		}
	}
	it.curr = uint32(atomic.LoadUint64(&it.engine.getNode(curr).nextPointers[0]))
}

func (it *skiplistIterator) Next() {
	if it.curr != 0 {
		it.curr = uint32(atomic.LoadUint64(&it.engine.getNode(it.curr).nextPointers[0]))
	}
}

func (it *skiplistIterator) Valid() bool {
	return it.curr != 0 && it.err == nil
}

func (it *skiplistIterator) Key() []byte {
	n := it.engine.getNode(it.curr)
	return it.engine.arena.getBytes(n.keyOffset, n.keyLen)
}

func (it *skiplistIterator) Value() []byte {
	n := it.engine.getNode(it.curr)
	return it.engine.arena.getBytes(atomic.LoadUint32(&n.valOffset), atomic.LoadUint32(&n.valLen))
}

// First positions the cursor at the first (smallest) key in the SkipList.
func (it *skiplistIterator) First() {
	if it.err != nil {
		return
	}
	if atomic.LoadUint32(&it.engine.closed) == 1 {
		it.err = ErrClosed
		it.curr = 0
		return
	}

	headNode := it.engine.getNode(it.engine.head)
	if headNode == nil {
		it.curr = 0
		return
	}

	firstOffset := atomic.LoadUint64(&headNode.nextPointers[0])
	it.curr = uint32(firstOffset)
}

func (it *skiplistIterator) Error() error {
	return it.err
}

func (it *skiplistIterator) Close() error {
	it.curr = 0
	it.err = nil
	return nil
}
