package raft

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkTidwallStorage_Append(b *testing.B) {
	tempDir, _ := os.MkdirTemp("", "tidwall_bench_*")
	defer os.RemoveAll(tempDir)

	store, err := NewTidwallStorage(tempDir)
	if err != nil {
		b.Fatalf("failed to open: %v", err)
	}
	defer store.Close()

	hs := HardState{Term: 1, Vote: 1}
	payload := []byte("account:Ram:balance=500")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		entry := []LogEntry{{
			Index: uint64(i + 1),
			Term:  1,
			Data:  payload,
		}}
		_ = store.Save(hs, entry)
	}
}

func BenchmarkTidwallStorage_SequentialRead(b *testing.B) {
	tempDir, _ := os.MkdirTemp("", "tidwall_bench_read_*")
	defer os.RemoveAll(tempDir)

	store, _ := NewTidwallStorage(tempDir)
	defer store.Close()

	const count = 10000
	hs := HardState{Term: 1, Vote: 1}
	var entries []LogEntry
	for i := 1; i <= count; i++ {
		entries = append(entries, LogEntry{
			Index: uint64(i),
			Term:  1,
			Data:  fmt.Appendf(nil, "payload_%d", i),
		})
	}
	_ = store.Save(hs, entries)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		idx := uint64(1)
		for pb.Next() {
			target := (idx % (count - 10)) + 1
			_, _ = store.Entries(target, target+10, 64*1024)
			idx++
		}
	})
}
