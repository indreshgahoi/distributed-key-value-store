package sharding

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestRangeRouter_ExactBoundaries verifies point lookups across continuous intervals:
// Range 1: ["", "d")
// Range 2: ["d", "m")
// Range 3: ["m", "t")
// Range 4: ["t", "") [+Infinity]
func TestRangeRouter_ExactBoundaries(t *testing.T) {
	router := NewRangeRouter()

	descs := []RangeDescriptor{
		{RangeID: 1, StartKey: []byte(""), EndKey: []byte("d"), Peers: []uint64{1, 2, 3}},
		{RangeID: 2, StartKey: []byte("d"), EndKey: []byte("m"), Peers: []uint64{1, 2, 3}},
		{RangeID: 3, StartKey: []byte("m"), EndKey: []byte("t"), Peers: []uint64{1, 2, 3}},
		{RangeID: 4, StartKey: []byte("t"), EndKey: []byte(""), Peers: []uint64{1, 2, 3}},
	}

	if err := router.UpdateTable(descs); err != nil {
		t.Fatalf("failed to install valid routing table: %v", err)
	}

	tests := []struct {
		key         string
		expectedID  uint64
		expectFound bool
	}{
		// Range 1 ["", "d")
		{key: "", expectedID: 1, expectFound: true},
		{key: "a", expectedID: 1, expectFound: true},
		{key: "apple", expectedID: 1, expectFound: true},
		{key: "c\xff\xff", expectedID: 1, expectFound: true},

		// Boundary "d" belongs to Range 2 ["d", "m")
		{key: "d", expectedID: 2, expectFound: true},
		{key: "date", expectedID: 2, expectFound: true},
		{key: "lemon", expectedID: 2, expectFound: true},

		// Boundary "m" belongs to Range 3 ["m", "t")
		{key: "m", expectedID: 3, expectFound: true},
		{key: "mango", expectedID: 3, expectFound: true},
		{key: "sam", expectedID: 3, expectFound: true},

		// Boundary "t" belongs to Range 4 ["t", +Infinity)
		{key: "t", expectedID: 4, expectFound: true},
		{key: "tiger", expectedID: 4, expectFound: true},
		{key: "zebra", expectedID: 4, expectFound: true},
		{key: "\xff\xff\xff", expectedID: 4, expectFound: true},
	}

	for _, tt := range tests {
		desc, err := router.FindRange([]byte(tt.key))
		if (err == nil) != tt.expectFound {
			t.Errorf("FindRange(%q): expected found=%v, got err=%v", tt.key, tt.expectFound, err)
			continue
		}
		if tt.expectFound && desc.RangeID != tt.expectedID {
			t.Errorf("FindRange(%q): expected RangeID %d, got %d [%q, %q)",
				tt.key, tt.expectedID, desc.RangeID, desc.StartKey, desc.EndKey)
		}
	}
}

// TestRangeRouter_ContinuityInvariants asserts that gaps or overlapping bounds are rejected.
func TestRangeRouter_ContinuityInvariants(t *testing.T) {
	router := NewRangeRouter()

	// 1. Rejects if first range does not start at ""
	gapAtStart := []RangeDescriptor{
		{RangeID: 1, StartKey: []byte("a"), EndKey: []byte(""), Peers: []uint64{1}},
	}
	if err := router.UpdateTable(gapAtStart); err == nil {
		t.Fatalf("expected error for table not starting at \"\"")
	}

	// 2. Rejects if last range does not end at "" (+Infinity)
	gapAtEnd := []RangeDescriptor{
		{RangeID: 1, StartKey: []byte(""), EndKey: []byte("z"), Peers: []uint64{1}},
	}
	if err := router.UpdateTable(gapAtEnd); err == nil {
		t.Fatalf("expected error for table not ending at +Infinity")
	}

	// 3. Rejects internal gap between ranges
	// Range 1: ["", "m")
	// Range 2: ["p", "") -> Missing ["m", "p")!
	gapMiddle := []RangeDescriptor{
		{RangeID: 1, StartKey: []byte(""), EndKey: []byte("m"), Peers: []uint64{1}},
		{RangeID: 2, StartKey: []byte("p"), EndKey: []byte(""), Peers: []uint64{1}},
	}
	if err := router.UpdateTable(gapMiddle); err == nil {
		t.Fatalf("expected error for missing keyspace gap [\"m\", \"p\")")
	}

	// 4. Rejects overlapping ranges
	// Range 1: ["", "p")
	// Range 2: ["m", "") -> Overlap in ["m", "p")!
	overlap := []RangeDescriptor{
		{RangeID: 1, StartKey: []byte(""), EndKey: []byte("p"), Peers: []uint64{1}},
		{RangeID: 2, StartKey: []byte("m"), EndKey: []byte(""), Peers: []uint64{1}},
	}
	if err := router.UpdateTable(overlap); err == nil {
		t.Fatalf("expected error for overlapping ranges")
	}
}

// TestRangeRouter_ConcurrentReadsAndUpdates verifies thread safety under high concurrency.
// Run with: go test -race -run TestRangeRouter_ConcurrentReadsAndUpdates
func TestRangeRouter_ConcurrentReadsAndUpdates(t *testing.T) {
	router := NewRangeRouter()

	initialDescs := []RangeDescriptor{
		{RangeID: 1, StartKey: []byte(""), EndKey: []byte("m"), Peers: []uint64{1}},
		{RangeID: 2, StartKey: []byte("m"), EndKey: []byte(""), Peers: []uint64{1}},
	}
	_ = router.UpdateTable(initialDescs)

	const (
		numReaders = 8
		duration   = 1 * time.Second
	)

	var stopSignal int32
	var wg sync.WaitGroup

	// Reader goroutines performing continuous point lookups
	for r := 0; r < numReaders; r++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			keys := [][]byte{[]byte("apple"), []byte("mango"), []byte("zebra")}
			i := 0
			for atomic.LoadInt32(&stopSignal) == 0 {
				k := keys[i%len(keys)]
				_, err := router.FindRange(k)
				if err != nil {
					t.Errorf("read failed during concurrent updates: %v", err)
					return
				}
				i++
			}
		}(r)
	}

	// Writer goroutine alternating routing tables
	wg.Add(1)
	go func() {
		defer wg.Done()
		descs3Ranges := []RangeDescriptor{
			{RangeID: 1, StartKey: []byte(""), EndKey: []byte("g"), Peers: []uint64{1}},
			{RangeID: 2, StartKey: []byte("g"), EndKey: []byte("p"), Peers: []uint64{1}},
			{RangeID: 3, StartKey: []byte("p"), EndKey: []byte(""), Peers: []uint64{1}},
		}

		for atomic.LoadInt32(&stopSignal) == 0 {
			_ = router.UpdateTable(descs3Ranges)
			router.UpdateLeader(2, uint64(rand.Intn(3)+1))
			time.Sleep(5 * time.Millisecond)
			_ = router.UpdateTable(initialDescs)
			time.Sleep(5 * time.Millisecond)
		}
	}()

	time.Sleep(duration)
	atomic.StoreInt32(&stopSignal, 1)
	wg.Wait()
}
