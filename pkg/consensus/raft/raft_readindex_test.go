package raft

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRaft_ReadIndex_HealthyLeaderSucceeds is the baseline case: a leader
// with a healthy quorum should be able to confirm its own leadership and
// return a usable read index promptly.
func TestRaft_ReadIndex_HealthyLeaderSucceeds(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}

	idx, _, isLeader := tc.nodes[leaderID].Propose([]byte("account:alice:100"))
	if !isLeader {
		t.Fatalf("expected Propose to succeed on the leader")
	}
	time.Sleep(150 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	readIndex, err := tc.nodes[leaderID].ReadIndex(ctx)
	if err != nil {
		t.Fatalf("expected ReadIndex to succeed on a healthy leader, got %v", err)
	}
	if readIndex < idx {
		t.Fatalf("expected readIndex (%d) to be at least the last proposed index (%d)", readIndex, idx)
	}

	t.Logf("PASS: ReadIndex on healthy leader returned %d", readIndex)
}

// TestRaft_ReadIndex_NonLeaderRejectedImmediately ensures a follower never
// pretends it can serve a linearizable read.
func TestRaft_ReadIndex_NonLeaderRejectedImmediately(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}

	var follower uint64
	for _, id := range tc.peers {
		if id != leaderID {
			follower = id
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = tc.nodes[follower].ReadIndex(ctx)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected ErrNotLeader from a follower, got %v", err)
	}
}

// TestRaft_ReadIndex_PartitionedLeaderCannotConfirmQuorum is the direct
// regression test for the stale-read hazard: a leader isolated into a
// minority island has no way to learn on its own that it's been deposed,
// and would happily keep serving reads from its local (increasingly stale)
// state machine forever if nothing forced it to check in with the rest of
// the cluster first. ReadIndex must fail here rather than return a value -
// this is exactly the scenario where naively reading `store.Get` with
// time.Now() as the read timestamp (the code this test was written to
// replace) would have served client requests wrong.
func TestRaft_ReadIndex_PartitionedLeaderCannotConfirmQuorum(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}

	var others []uint64
	for _, id := range tc.peers {
		if id != leaderID {
			others = append(others, id)
		}
	}

	// Isolate the leader alone; the other two can still talk to each other
	// and will elect a new leader among themselves.
	tc.net.Partition([]uint64{leaderID}, others)
	t.Logf("partitioned leader %d away from the rest of the cluster", leaderID)

	// Give the majority side time to notice the silence and elect a new
	// leader - proving the old leader really is stale, not just slow.
	newLeaderID, _, err := tc.FindLeader(2*time.Second, leaderID)
	if err != nil {
		t.Fatalf("majority side failed to elect a new leader after partition: %v", err)
	}
	if newLeaderID == leaderID {
		t.Fatalf("expected a different node to become leader on the majority side")
	}

	// The old leader still believes it's the leader - it has no way to know
	// otherwise while partitioned. This is exactly the hazard: GetState()
	// alone would wrongly say it's safe to serve a local read.
	if _, isLeader := tc.nodes[leaderID].GetState(); !isLeader {
		t.Fatalf("test setup invariant broken: partitioned old leader should still (wrongly) believe it's leader")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err = tc.nodes[leaderID].ReadIndex(ctx)
	if err == nil {
		t.Fatalf("expected ReadIndex to fail on a partitioned minority leader, but it succeeded - stale reads are not blocked")
	}

	t.Logf("PASS: ReadIndex correctly refused to confirm quorum for a partitioned leader (%v)", err)
}
