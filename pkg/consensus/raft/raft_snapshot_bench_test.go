package raft

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

// BenchmarkMVCC_SnapshotExport measures how fast Layer 2 exports 10,000 keys to a snapshot stream.
func BenchmarkMVCC_SnapshotExport(b *testing.B) {
	rawEngine := raw.NewSkipListEngine(64 * 1024 * 1024)
	store := mvcc.NewStore(rawEngine)
	defer store.Close()

	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		k := fmt.Appendf(nil, "account:%06d", i)
		v := fmt.Appendf(nil, "balance:%06d", i*10)
		_ = store.Put(k, v, 100)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		if err := store.ExportSnapshot(&buf, 150); err != nil {
			b.Fatalf("export failed: %v", err)
		}
	}
}

// BenchmarkMVCC_SnapshotRestore measures ingestion speed of a 10,000-key snapshot into Layer 0.
func BenchmarkMVCC_SnapshotRestore(b *testing.B) {
	rawEngine := raw.NewSkipListEngine(64 * 1024 * 1024)
	store := mvcc.NewStore(rawEngine)
	defer store.Close()

	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		k := fmt.Appendf(nil, "account:%06d", i)
		v := fmt.Appendf(nil, "balance:%06d", i*10)
		_ = store.Put(k, v, 100)
	}

	var snapBuf bytes.Buffer
	_ = store.ExportSnapshot(&snapBuf, 150)
	snapshotBytes := snapBuf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		destEngine := raw.NewSkipListEngine(64 * 1024 * 1024)
		destStore := mvcc.NewStore(destEngine)
		r := bytes.NewReader(snapshotBytes)
		if err := destStore.RestoreSnapshot(r); err != nil {
			b.Fatalf("restore failed: %v", err)
		}
		_ = destStore.Close()
	}
}
