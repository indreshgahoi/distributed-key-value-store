package raft

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkRaft_LogAppend measures raw in-memory replicated log append throughput.
func BenchmarkRaft_LogAppend(b *testing.B) {
	rlog := NewRaftLog()
	payload := []byte("account:bench:val_1000")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = rlog.Append(1, payload)
	}
}

// BenchmarkRaft_SequentialProposals measures end-to-end proposal, quorum replication,
// and commit latency on a 3-node cluster.
func BenchmarkRaft_SequentialProposals(b *testing.B) {
	tc := NewTestCluster(nil, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(2 * time.Second)
	if err != nil {
		b.Fatalf("failed to find leader: %v", err)
	}

	payload := []byte("account:bench_seq:100")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		idx, _, isLeader := tc.nodes[leaderID].Propose(payload)
		if !isLeader {
			b.Fatalf("leader lost during benchmark")
		}

		// Wait until entry applied to state machine
		applied := false
		for attempt := 0; attempt < 50; attempt++ {
			if tc.nodes[leaderID].LastApplied() >= idx {
				applied = true
			}
			if applied {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
	}
}

// BenchmarkRaft_ConcurrentProposals measures throughput under concurrent goroutine write pressure.
func BenchmarkRaft_ConcurrentProposals(b *testing.B) {
	tc := NewTestCluster(nil, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(2 * time.Second)
	if err != nil {
		b.Fatalf("failed to find leader: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		workerID := time.Now().UnixNano() % 10000
		i := 0
		for pb.Next() {
			cmd := fmt.Appendf(nil, "bench:w%d_%d:val_%d", workerID, i, i)
			tc.nodes[leaderID].Propose(cmd)
			i++
		}
	})
}
