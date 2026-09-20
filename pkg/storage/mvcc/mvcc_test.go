package mvcc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

func newTestMVCCStore(t *testing.T) *Store {
	rawEngine := raw.NewSkipListEngine(16 * 1024 * 1024)
	return NewStore(rawEngine)
}

func TestMVCC_PointGet_IsolationAndTombstones(t *testing.T) {
	store := newTestMVCCStore(t)
	defer store.Close()

	key := []byte("account:Ram")

	// 1. T=100: Ram sets balance to 100
	if err := store.Put(key, []byte("100"), 100); err != nil {
		t.Fatalf("failed to put at T=100: %v", err)
	}

	// 2. T=200: Ram updates balance to 250
	if err := store.Put(key, []byte("250"), 200); err != nil {
		t.Fatalf("failed to put at T=200: %v", err)
	}

	// 3. T=300: Account is closed (Delete / Tombstone)
	if err := store.Delete(key, 300); err != nil {
		t.Fatalf("failed to delete at T=300: %v", err)
	}

	// Invariant 1: Reader before account creation (T=50) sees ErrKeyNotFound
	if _, err := store.Get(key, 50); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound at T=50, got: %v", err)
	}

	// Invariant 2: Snapshot reader at T=150 MUST see 100 (isolated from write at T=200)
	val150, err := store.Get(key, 150)
	if err != nil || string(val150) != "100" {
		t.Fatalf("expected 100 at T=150, got %s (err: %v)", string(val150), err)
	}

	// Invariant 3: Snapshot reader at T=250 MUST see 250 (isolated from delete at T=300)
	val250, err := store.Get(key, 250)
	if err != nil || string(val250) != "250" {
		t.Fatalf("expected 250 at T=250, got %s (err: %v)", string(val250), err)
	}

	// Invariant 4: Reader at T=350 sees ErrKeyNotFound due to Tombstone
	if _, err := store.Get(key, 350); err != ErrKeyNotFound {
		t.Fatalf("expected ErrKeyNotFound at T=350, got: %v", err)
	}
}

func TestMVCC_Scan_FiltersShadowsAndTombstones(t *testing.T) {
	store := newTestMVCCStore(t)
	defer store.Close()

	// Initial dataset at T=100
	_ = store.Put([]byte("user:alice"), []byte("v1"), 100)
	_ = store.Put([]byte("user:bob"), []byte("v1"), 100)
	_ = store.Put([]byte("user:charlie"), []byte("v1"), 100)

	// Updates and deletions at T=200
	_ = store.Put([]byte("user:alice"), []byte("v2"), 200) // Updated
	_ = store.Delete([]byte("user:bob"), 200)              // Deleted
	_ = store.Put([]byte("user:david"), []byte("v1"), 200) // Newly added

	// Scan at T=150 (Snapshot 1)
	res150, err := store.Scan([]byte("user:"), []byte("user:\xff"), 150, 0)
	if err != nil {
		t.Fatalf("scan at T=150 failed: %v", err)
	}

	// Should see: alice(v1), bob(v1), charlie(v1)
	if len(res150) != 3 {
		t.Fatalf("expected 3 entries at T=150, got %d", len(res150))
	}
	if string(res150[0].Value) != "v1" || string(res150[1].Key) != "user:bob" {
		t.Fatalf("unexpected snapshot view at T=150: %+v", res150)
	}

	// Scan at T=250 (Snapshot 2)
	res250, err := store.Scan([]byte("user:"), []byte("user:\xff"), 250, 0)
	if err != nil {
		t.Fatalf("scan at T=250 failed: %v", err)
	}

	// Should see: alice(v2), charlie(v1), david(v1) (bob is omitted due to tombstone)
	if len(res250) != 3 {
		t.Fatalf("expected 3 entries at T=250, got %d", len(res250))
	}
	expectedKeys := []string{"user:alice", "user:charlie", "user:david"}
	for i, k := range expectedKeys {
		if !bytes.Equal(res250[i].Key, []byte(k)) {
			t.Fatalf("at index %d, expected key %s, got %s", i, k, res250[i].Key)
		}
	}
	if string(res250[0].Value) != "v2" {
		t.Fatalf("expected alice to be updated to v2, got %s", res250[0].Value)
	}
}

func BenchmarkMVCC_SnapshotPointGet(b *testing.B) {
	rawEngine := raw.NewSkipListEngine(64 * 1024 * 1024)
	store := NewStore(rawEngine)
	defer store.Close()

	key := []byte("benchmark_user_key")
	// Insert 10 versions of the same key
	for ts := uint64(1); ts <= 10; ts++ {
		_ = store.Put(key, []byte(fmt.Sprintf("val_%d", ts)), ts*100)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Read at snapshot T=550 (should resolve to version at T=500)
		_, err := store.Get(key, 550)
		if err != nil {
			b.Fatalf("read failed: %v", err)
		}
	}
}
