package raft

import (
	"testing"
	"time"
)

// TestRaft_PreVoteBlocksTermInflationWhileIsolated verifies the core Pre-Vote
// guarantee: a node cut off from the cluster must NOT advance currentTerm no
// matter how many election timeouts elapse while it's isolated, because
// startRealElectionLocked (the only place currentTerm++ happens) is only
// reached after winning a Pre-Vote quorum — which an isolated node can never
// do. Before Pre-Vote existed, every failed candidacy attempt incremented
// currentTerm unconditionally, so a long enough isolation window produced a
// large term jump.
func TestRaft_PreVoteBlocksTermInflationWhileIsolated(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("initial election failed: %v", err)
	}

	var isolatedID uint64
	for _, id := range tc.peers {
		if id != leaderID {
			isolatedID = id
			break
		}
	}

	termBefore, _ := tc.nodes[isolatedID].GetState()

	tc.net.Disconnect(isolatedID)
	t.Logf("Isolated node %d at term %d", isolatedID, termBefore)

	// Long enough for several election-timeout cycles (60-120ms configured
	// in NewTestCluster) to elapse while cut off from everyone.
	time.Sleep(600 * time.Millisecond)

	termAfter, isLeaderWhileIsolated := tc.nodes[isolatedID].GetState()
	if isLeaderWhileIsolated {
		t.Fatalf("isolated node %d should never become Leader while it cannot reach a quorum", isolatedID)
	}
	if termAfter != termBefore {
		t.Fatalf("Pre-Vote should block term inflation while isolated: term was %d before disconnect, %d after 600ms cut off",
			termBefore, termAfter)
	}
	t.Logf("PASS: isolated node's term stayed at %d through 600ms of isolation", termAfter)

	tc.net.Reconnect(isolatedID)
}

// TestRaft_PreVoteProtectsStableLeaderOnReconnect verifies the other half of
// the guarantee: even setting term inflation aside, a node reconnecting after
// a long isolation window must not force the current, healthy Leader to step
// down. Before Pre-Vote, HandleRequestVote adopted any higher term
// unconditionally regardless of whether the vote itself would be granted, so
// a reconnecting node's stale RequestVote could depose a perfectly healthy
// Leader. This test is the scenario that originally made
// TestRaft_InstallSnapshotToLaggingFollower flaky (~20% failure rate).
func TestRaft_PreVoteProtectsStableLeaderOnReconnect(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, leaderTermBefore, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("initial election failed: %v", err)
	}

	var isolatedID uint64
	for _, id := range tc.peers {
		if id != leaderID {
			isolatedID = id
			break
		}
	}

	tc.net.Disconnect(isolatedID)
	t.Logf("Isolated node %d while %d leads at term %d", isolatedID, leaderID, leaderTermBefore)

	// Isolate through several election-timeout cycles - the exact scenario
	// that used to let a reconnecting node's inflated term depose the leader.
	time.Sleep(600 * time.Millisecond)

	tc.net.Reconnect(isolatedID)
	t.Logf("Reconnected node %d", isolatedID)

	// Poll for a window well past a full heartbeat/election-timeout cycle and
	// assert the ORIGINAL leader never wavers: still Leader, term unchanged.
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		term, isLeader := tc.nodes[leaderID].GetState()
		if !isLeader {
			t.Fatalf("original leader %d stepped down after node %d reconnected (Pre-Vote should have prevented this)", leaderID, isolatedID)
		}
		if term != leaderTermBefore {
			t.Fatalf("leader %d's term changed from %d to %d after node %d reconnected (Pre-Vote should have prevented this)",
				leaderID, leaderTermBefore, term, isolatedID)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("PASS: leader %d stayed Leader at term %d through reconnect of isolated node %d", leaderID, leaderTermBefore, isolatedID)
}
