package mvcc

import (
	"testing"
)

func TestMVCC_WatermarkCompaction(t *testing.T) {
	store := newTestMVCCStore(t)
	defer store.Close()

	key := []byte("account:Ram")

	// Write timeline:
	// T=50:  Put 50
	// T=100: Put 100
	// T=200: Put 200
	// T=300: Delete
	_ = store.Put(key, []byte("50"), 50)
	_ = store.Put(key, []byte("100"), 100)
	_ = store.Put(key, []byte("200"), 200)
	_ = store.Delete(key, 300)

	// Set Safe Watermark = 150
	// - TS 300: Kept (> 150)
	// - TS 200: Kept (> 150)
	// - TS 100: Kept (first version <= 150)
	// - TS 50:  Purged (shadowed below 150)
	stats, err := store.CompactBelowWatermark([]byte("account:"), []byte("account:\xff"), 150)
	if err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	if stats.VersionsPurged != 1 {
		t.Fatalf("expected 1 version purged (TS 50), got %d", stats.VersionsPurged)
	}

	// Verify reads:
	// 1. TS 250 still reads 200 (unaffected)
	val250, err := store.Get(key, 250)
	if err != nil || string(val250) != "200" {
		t.Fatalf("expected 200 at TS 250, got %s", val250)
	}

	// 2. TS 100 still reads 100 (the preserved baseline)
	val100, err := store.Get(key, 100)
	if err != nil || string(val100) != "100" {
		t.Fatalf("expected 100 at TS 100, got %s", val100)
	}

	// 3. Historical read at TS 50 now returns ErrKeyNotFound (safely reclaimed)
	_, err = store.Get(key, 50)
	if err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound for reclaimed version at TS 50, got: %v", err)
	}
}
