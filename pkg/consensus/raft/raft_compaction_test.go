package raft

import (
	"testing"
	"time"
)

// TestRaft_ProposeAfterSnapshotCompactionUsesCorrectIndex is a regression
// test for a bug found via live manual testing (not by any existing
// automated test): once a snapshot has compacted the log, RaftLog.Append
// computed a new entry's index as len(l.entries), which is only correct
// while entries[0].Index == 0 (nothing ever compacted). After CompactLog
// repositions entries[0] to the snapshot boundary, that formula silently
// returns an index that's too low by entries[0].Index - e.g. Propose()
// returning the same index twice in a row for two different commands
// immediately after the first snapshot. TruncateAndAppend (the follower-side
// replication path) had the identical class of bug.
//
// No existing test caught this because none of them propose anything after
// a real compaction has happened - TestStorage_TidwallStorage_CrashReplayAndCompaction
// and TestRaft_InstallSnapshotToLaggingFollower both compact, but neither
// then calls Propose again on the compacted node afterward.
func TestRaft_ProposeAfterSnapshotCompactionUsesCorrectIndex(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("election failed: %v", err)
	}

	idx1, _, isLeader := tc.nodes[leaderID].Propose([]byte("account:carol:verified"))
	if !isLeader || idx1 != 1 {
		t.Fatalf("expected first Propose to return index 1, got idx=%d isLeader=%v", idx1, isLeader)
	}
	time.Sleep(150 * time.Millisecond) // let it commit, apply, and replicate

	// Compact the log through index 1 - the exact trigger for this bug.
	if err := tc.nodes[leaderID].Snapshot(1, []byte("snapshot-at-1")); err != nil {
		t.Fatalf("snapshot failed: %v", err)
	}

	// This is exactly what returned index 1 again (instead of 2) before the fix.
	idx2, _, isLeader2 := tc.nodes[leaderID].Propose([]byte("account:carol1:verified1"))
	if !isLeader2 {
		t.Fatalf("leader lost leadership unexpectedly")
	}
	if idx2 != 2 {
		t.Fatalf("expected second Propose (after compaction) to return index 2, got %d", idx2)
	}
	if idx2 == idx1 {
		t.Fatalf("second Propose returned the same index as the first (%d) - this is the exact bug reported from live testing", idx2)
	}

	time.Sleep(150 * time.Millisecond)

	// Also verify replication to followers landed correctly past the
	// compaction boundary - exercises the TruncateAndAppend fix.
	for _, id := range tc.peers {
		tc.nodes[id].mu.Lock()
		lastIdx := tc.nodes[id].log.LastIndex()
		tc.nodes[id].mu.Unlock()
		if lastIdx != 2 {
			t.Fatalf("node %d: expected log to reach index 2 after post-compaction replication, got %d", id, lastIdx)
		}
	}

	t.Logf("PASS: Propose after compaction correctly returned index 2, and replicated to all followers")
}
