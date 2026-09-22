package raft

import (
	"testing"
	"time"
)

// BenchmarkRaft_LeaderElection measures time-to-first-leader on a fresh
// 3-node cluster. Pre-Vote adds a full extra RPC round-trip (the trial
// election) before every real election, so this is the benchmark that
// actually reflects its cost - none of the other benchmarks in this package
// exercise the election path at all, only steady-state proposing after a
// leader already exists. Cluster construction/teardown overhead is included
// in each iteration since a leader can only be elected once per cluster.
func BenchmarkRaft_LeaderElection(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tc := NewTestCluster(nil, 3)
		if _, _, err := tc.FindLeader(2 * time.Second); err != nil {
			b.Fatalf("failed to elect leader: %v", err)
		}
		tc.Shutdown()
	}
}
