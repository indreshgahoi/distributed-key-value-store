package mvcc

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

func newTestStore(size uint32) *Store { return NewStore(raw.NewSkipListEngine(size)) }

func mustGet(t *testing.T, s *Store, key string, ts uint64) string {
	t.Helper()
	v, err := s.Get([]byte(key), ts)
	if err != nil {
		t.Fatalf("Get(%q, %d): %v", key, ts, err)
	}
	return string(v)
}

func mustBeAbsent(t *testing.T, s *Store, key string, ts uint64) {
	t.Helper()
	if v, err := s.Get([]byte(key), ts); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("Get(%q, %d): expected not found, got %q (%v)", key, ts, v, err)
	}
}

// TestSnapshot_RestoreReplacesState is a regression test: RestoreSnapshot
// used to merge into the existing engine, so a follower installing a
// snapshot kept keys that had been deleted before it.
func TestSnapshot_RestoreReplacesState(t *testing.T) {
	leader := newTestStore(1 << 20)
	_ = leader.Put([]byte("k1"), []byte("v1"), 1)
	_ = leader.Put([]byte("k2"), []byte("v2"), 2)
	_ = leader.Delete([]byte("k1"), 3)
	var snap bytes.Buffer
	if err := leader.ExportSnapshot(&snap, 10); err != nil {
		t.Fatal(err)
	}

	follower := newTestStore(1 << 20)
	_ = follower.Put([]byte("k1"), []byte("v1"), 1) // applied before the delete
	_ = follower.Put([]byte("stale"), []byte("x"), 1)
	if err := follower.RestoreSnapshot(bytes.NewReader(snap.Bytes())); err != nil {
		t.Fatal(err)
	}
	mustBeAbsent(t, follower, "k1", 100)
	mustBeAbsent(t, follower, "stale", 100)
	if got := mustGet(t, follower, "k2", 100); got != "v2" {
		t.Fatalf("k2: got %q", got)
	}
}

// TestSnapshot_CorruptionIsRejectedAndStoreUnchanged: a damaged or
// truncated snapshot must fail loudly and leave the store as it was.
func TestSnapshot_CorruptionIsRejectedAndStoreUnchanged(t *testing.T) {
	src := newTestStore(1 << 20)
	for i := 0; i < 20; i++ {
		_ = src.Put([]byte(fmt.Sprintf("key-%02d", i)), []byte("value"), uint64(i+1))
	}
	var snap bytes.Buffer
	if err := src.ExportSnapshot(&snap, 100); err != nil {
		t.Fatal(err)
	}
	good := snap.Bytes()

	flipped := bytes.Clone(good)
	flipped[len(flipped)/2] ^= 0xFF
	cases := map[string][]byte{
		"bit flip":  flipped,
		"truncated": good[:len(good)-10],
		"empty":     nil,
	}
	for name, data := range cases {
		dst := newTestStore(1 << 20)
		_ = dst.Put([]byte("mine"), []byte("keep"), 1)
		if err := dst.RestoreSnapshot(bytes.NewReader(data)); !errors.Is(err, ErrCorruptSnapshot) {
			t.Fatalf("%s: expected ErrCorruptSnapshot, got %v", name, err)
		}
		if got := mustGet(t, dst, "mine", 10); got != "keep" {
			t.Fatalf("%s: failed restore changed the store", name)
		}
	}
}

// TestSnapshot_KeepsVersionsAboveWatermark: export keeps every version a
// reader at or above the watermark could still see, and nothing older.
func TestSnapshot_KeepsVersionsAboveWatermark(t *testing.T) {
	src := newTestStore(1 << 20)
	_ = src.Put([]byte("k"), []byte("ancient"), 1)
	_ = src.Put([]byte("k"), []byte("old"), 5)
	_ = src.Put([]byte("k"), []byte("new"), 20)
	_ = src.Put([]byte("gone"), []byte("x"), 2)
	_ = src.Delete([]byte("gone"), 4)

	var snap bytes.Buffer
	if err := src.ExportSnapshot(&snap, 10); err != nil {
		t.Fatal(err)
	}
	dst := newTestStore(1 << 20)
	if err := dst.RestoreSnapshot(&snap); err != nil {
		t.Fatal(err)
	}
	if got := mustGet(t, dst, "k", 10); got != "old" {
		t.Fatalf("at the watermark: got %q, want old", got)
	}
	if got := mustGet(t, dst, "k", 25); got != "new" {
		t.Fatalf("above the watermark: got %q, want new", got)
	}
	mustBeAbsent(t, dst, "k", 2)     // shadowed below the watermark: compacted away
	mustBeAbsent(t, dst, "gone", 50) // deleted below the watermark
}

// TestStore_CompactReclaimsMemory: repeatedly overwriting keys fills the
// arena with dead versions; Compact copies only live data into a fresh
// engine, which is how the arena's memory is recovered.
func TestStore_CompactReclaimsMemory(t *testing.T) {
	s := newTestStore(4 << 20)
	var ts uint64
	for round := 0; round < 50; round++ {
		for k := 0; k < 100; k++ {
			ts++
			if err := s.Put([]byte(fmt.Sprintf("key-%03d", k)), bytes.Repeat([]byte{byte(round)}, 100), ts); err != nil {
				t.Fatalf("put: %v", err)
			}
		}
	}
	before, _, _ := s.MemoryUsage()
	if err := s.Compact(ts); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after, _, _ := s.MemoryUsage()
	if after*10 > before {
		t.Fatalf("expected compaction to reclaim most memory: %d -> %d bytes", before, after)
	}
	if got := mustGet(t, s, "key-042", ts); got != string(bytes.Repeat([]byte{49}, 100)) {
		t.Fatalf("latest value lost by compaction")
	}
}
