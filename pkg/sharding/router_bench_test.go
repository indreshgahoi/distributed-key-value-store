package sharding

import (
	"fmt"
	"testing"
)

// BenchmarkRangeRouter_FindRange_1000Ranges measures O(log N) lookup across 1,000 ranges.
func BenchmarkRangeRouter_FindRange_1000Ranges(b *testing.B) {
	benchmarkRouterWithRangeCount(b, 1000)
}

// BenchmarkRangeRouter_FindRange_10000Ranges measures O(log N) lookup across 10,000 ranges (Tier-1 scale).
func BenchmarkRangeRouter_FindRange_10000Ranges(b *testing.B) {
	benchmarkRouterWithRangeCount(b, 10000)
}

func benchmarkRouterWithRangeCount(b *testing.B, rangeCount int) {
	router := NewRangeRouter()

	descs := make([]RangeDescriptor, rangeCount)
	for i := 0; i < rangeCount; i++ {
		start := fmt.Sprintf("k_%06d", i*10)
		end := fmt.Sprintf("k_%06d", (i+1)*10)
		if i == 0 {
			start = "" // -Infinity
		}
		if i == rangeCount-1 {
			end = "" // +Infinity
		}
		descs[i] = RangeDescriptor{
			RangeID:  uint64(i + 1),
			StartKey: []byte(start),
			EndKey:   []byte(end),
			Peers:    []uint64{1, 2, 3},
		}
	}

	if err := router.UpdateTable(descs); err != nil {
		b.Fatalf("failed setup: %v", err)
	}

	targetKey := []byte(fmt.Sprintf("k_%06d", (rangeCount/2)*10+5))

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			desc, err := router.FindRange(targetKey)
			if err != nil || desc.RangeID == 0 {
				b.Fatalf("lookup failed")
			}
		}
	})
}
