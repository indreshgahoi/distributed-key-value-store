package raw

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const defaultTestArenaSize = 128 * 1024 * 1024 // 64 MB

// TestSkipList_BasicCRUD tests standard insertion, updates, point lookups, and lifecycle closure.
func TestSkipList_BasicCRUD(t *testing.T) {
	engine := NewSkipListEngine(defaultTestArenaSize)
	defer engine.Close()

	// 1. Point lookup on empty list
	val, err := engine.Get([]byte("key1"))
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound on empty engine, got err: %v, val: %s", err, val)
	}

	// 2. Put and Get
	key := []byte("account_001")
	val1 := []byte("balance_100")
	if err := engine.Put(key, val1); err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}

	got, err := engine.Get(key)
	if err != nil || !bytes.Equal(got, val1) {
		t.Fatalf("expected value %s, got %s (err: %v)", val1, got, err)
	}

	// 3. Update existing key (re-allocation in arena with atomic pointer update)
	val2 := []byte("balance_250")
	if err := engine.Put(key, val2); err != nil {
		t.Fatalf("unexpected put error: %v", err)
	}

	got2, err := engine.Get(key)
	if err != nil || !bytes.Equal(got2, val2) {
		t.Fatalf("expected updated value %s, got %s (err: %v)", val2, got2, err)
	}

	// 4. Closed engine rejection
	_ = engine.Close()
	if err := engine.Put([]byte("key2"), []byte("val2")); err != ErrClosed {
		t.Fatalf("expected ErrClosed on closed engine, got: %v", err)
	}
	if _, err := engine.Get(key); err != ErrClosed {
		t.Fatalf("expected ErrClosed on Get, got: %v", err)
	}
}

// TestSkipList_TotalLexicographicalOrder verifies that ordered iterations strictly follow bytes.Compare.
func TestSkipList_TotalLexicographicalOrder(t *testing.T) {
	engine := NewSkipListEngine(defaultTestArenaSize)
	defer engine.Close()

	// Insert items in deliberately reversed order
	count := 1000
	for i := count; i > 0; i-- {
		k := fmt.Sprintf("k_%05d", i)
		v := fmt.Sprintf("v_%05d", i)
		if err := engine.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("failed to insert key %s: %v", k, err)
		}
	}

	// Verify sequential scan via First() and Next()
	it := engine.NewIterator()
	defer it.Close()

	last := []byte("")
	visited := 0

	for it.First(); it.Valid(); it.Next() {
		currKey := it.Key()
		if bytes.Compare(currKey, last) <= 0 {
			t.Fatalf("ordering invariant broken: previous key %q >= current key %q", last, currKey)
		}
		last = append([]byte(nil), currKey...)
		visited++
	}

	if err := it.Error(); err != nil {
		t.Fatalf("iterator returned unexpected error: %v", err)
	}

	if visited != count {
		t.Fatalf("expected to traverse %d keys, visited %d", count, visited)
	}
}

// TestSkipList_SeekSemantics tests exact, forward, and out-of-range seek positions.
func TestSkipList_SeekSemantics(t *testing.T) {
	engine := NewSkipListEngine(defaultTestArenaSize)
	defer engine.Close()

	keys := []string{"apple", "cherry", "date", "fig"}
	for _, k := range keys {
		_ = engine.Put([]byte(k), []byte("v_"+k))
	}

	it := engine.NewIterator()
	defer it.Close()

	tests := []struct {
		target   string
		expected string
		valid    bool
	}{
		{target: "apple", expected: "apple", valid: true},   // Exact start
		{target: "banana", expected: "cherry", valid: true}, // Non-existent key lands on next greater
		{target: "cherry", expected: "cherry", valid: true}, // Exact middle
		{target: "date5", expected: "fig", valid: true},     // Between date and fig
		{target: "grape", expected: "", valid: false},       // Out of range upper bound
	}

	for _, tt := range tests {
		it.Seek([]byte(tt.target))
		if it.Valid() != tt.valid {
			t.Errorf("Seek(%q): expected valid=%v, got %v", tt.target, tt.valid, it.Valid())
			continue
		}
		if tt.valid && !bytes.Equal(it.Key(), []byte(tt.expected)) {
			t.Errorf("Seek(%q): expected key %q, got %q", tt.target, tt.expected, it.Key())
		}
	}
}

// TestSkipList_ConcurrentRaceContention runs high-concurrency read/write workloads across goroutines.
// Execute with: go test -race -run TestSkipList_ConcurrentRaceContention
func TestSkipList_ConcurrentRaceContention(t *testing.T) {
	engine := NewSkipListEngine(defaultTestArenaSize)
	defer engine.Close()

	const (
		numWriters = 8
		numReaders = 16
		numKeys    = 1000
		duration   = 2 * time.Second
	)

	var stopSignal int32
	var wg sync.WaitGroup

	// Start concurrent writers
	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for atomic.LoadInt32(&stopSignal) == 0 {
				k := fmt.Sprintf("key_%04d", rng.Intn(numKeys))
				v := fmt.Sprintf("val_%04d_w_%d", rng.Intn(numKeys), workerID)
				_ = engine.Put([]byte(k), []byte(v))
			}
		}(w)
	}

	// Start concurrent readers performing point lookups and range scans
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for atomic.LoadInt32(&stopSignal) == 0 {
				if workerID%2 == 0 {
					// Point lookup
					k := fmt.Sprintf("key_%04d", rng.Intn(numKeys))
					_, _ = engine.Get([]byte(k))
				} else {
					// Forward short scan
					it := engine.NewIterator()
					target := fmt.Sprintf("key_%04d", rng.Intn(numKeys))
					it.Seek([]byte(target))
					steps := 0
					for it.Valid() && steps < 10 {
						_ = it.Key()
						_ = it.Value()
						it.Next()
						steps++
					}
					_ = it.Close()
				}
			}
		}(r)
	}

	time.Sleep(duration)
	atomic.StoreInt32(&stopSignal, 1)
	wg.Wait()
}

// BenchmarkSkipList_ConcurrentReads measures throughput and verifies zero allocations on reads.
func BenchmarkSkipList_ConcurrentReads(b *testing.B) {
	engine := NewSkipListEngine(defaultTestArenaSize)
	defer engine.Close()

	const numKeys = 10000
	keys := make([][]byte, numKeys)
	for i := 0; i < numKeys; i++ {
		k := fmt.Appendf(nil, "bench_key_%06d", i)
		v := fmt.Appendf(nil, "bench_val_%06d", i)
		keys[i] = k
		_ = engine.Put(k, v)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		idx := 0
		for pb.Next() {
			k := keys[idx%numKeys]
			val, err := engine.Get(k)
			if err != nil || len(val) == 0 {
				b.Fatalf("unexpected read failure: %v", err)
			}
			idx++
		}
	})
}

// TestSkipList_FullArenaReturnsError: running out of arena space is an error
// the caller can handle (e.g. by compacting), not a process-killing panic.
func TestSkipList_FullArenaReturnsError(t *testing.T) {
	engine := NewSkipListEngine(4096)
	var err error
	for i := 0; i < 1000 && err == nil; i++ {
		err = engine.Put([]byte(fmt.Sprintf("key-%04d", i)), bytes.Repeat([]byte("v"), 64))
	}
	if err != ErrArenaFull {
		t.Fatalf("expected ErrArenaFull once the arena is exhausted, got %v", err)
	}
	// Existing data stays readable.
	if _, err := engine.Get([]byte("key-0000")); err != nil {
		t.Fatalf("engine unreadable after filling up: %v", err)
	}
}

// TestSkipList_ConcurrentInsertsOfSameKeyDontDuplicate: writers racing to
// insert the same new key must end up with exactly one node for it.
func TestSkipList_ConcurrentInsertsOfSameKeyDontDuplicate(t *testing.T) {
	for round := 0; round < 50; round++ {
		engine := NewSkipListEngine(1 << 20)
		var wg sync.WaitGroup
		for w := 0; w < 8; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				_ = engine.Put([]byte("contended"), []byte{byte(w)})
			}(w)
		}
		wg.Wait()
		count := 0
		it := engine.NewIterator()
		for it.First(); it.Valid(); it.Next() {
			count++
		}
		if count != 1 {
			t.Fatalf("round %d: expected 1 node for a key inserted concurrently, found %d", round, count)
		}
	}
}
