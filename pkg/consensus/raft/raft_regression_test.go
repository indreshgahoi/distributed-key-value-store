package raft

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// quietFollowerConfig returns a config whose election timeout is long enough
// that the node under test stays a passive Follower for the whole test, so
// the only state changes are the ones the test drives via RPC handlers.
func quietFollowerConfig() Config {
	return Config{
		NodeID:             2,
		Peers:              []uint64{1, 2, 3},
		HeartbeatInterval:  50 * time.Millisecond,
		ElectionTimeoutMin: 10 * time.Second,
		ElectionTimeoutMax: 20 * time.Second,
		RPCTimeout:         50 * time.Millisecond,
	}
}

func makeEntries(from, to, term uint64) []LogEntry {
	var out []LogEntry
	for i := from; i <= to; i++ {
		out = append(out, LogEntry{Index: i, Term: term, Data: fmt.Appendf(nil, "cmd_%d", i)})
	}
	return out
}

// TestRaft_StaleAppendEntriesDoesNotTruncateDurableLog is a regression test
// for a durability/safety bug: HandleAppendEntries used to hand the leader's
// raw args.Entries to Storage.Save, which replaces everything from
// entries[0].Index onward. The in-memory log correctly ignored a delayed,
// reordered AppendEntries whose entries were an already-matching prefix, but
// the WAL was truncated back to that prefix - silently discarding entries
// this follower had already acknowledged (and the leader may have counted
// toward a commit quorum). After a crash, those committed entries were gone.
func TestRaft_StaleAppendEntriesDoesNotTruncateDurableLog(t *testing.T) {
	dir, err := os.MkdirTemp("", "raft_stale_ae_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)

	storage, err := NewTidwallStorage(dir)
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	applyCh := make(chan ApplyMsg, 100)
	node, err := NewRaftNode(quietFollowerConfig(), NewSimulatedNetwork(), storage, applyCh)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}

	// 1. Leader replicates 1..5; follower acknowledges.
	reply := &AppendEntriesReply{}
	node.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: makeEntries(1, 5, 1),
	}, reply)
	if !reply.Success {
		t.Fatalf("expected initial AppendEntries to succeed")
	}

	// 2. An older in-flight RPC carrying only 1..2 arrives late (reordered).
	reply = &AppendEntriesReply{}
	node.HandleAppendEntries(&AppendEntriesArgs{
		Term: 1, LeaderID: 1, PrevLogIndex: 0, PrevLogTerm: 0, Entries: makeEntries(1, 2, 1),
	}, reply)
	if !reply.Success {
		t.Fatalf("expected stale-but-consistent AppendEntries to succeed")
	}

	if got := node.log.LastIndex(); got != 5 {
		t.Fatalf("in-memory log: expected LastIndex 5, got %d", got)
	}
	if got, _ := storage.LastIndex(); got != 5 {
		t.Fatalf("durable log truncated by a stale AppendEntries: expected LastIndex 5, got %d", got)
	}

	// 3. A genuine conflict must still truncate durably: a new leader at term
	// 2 overwrites index 4 onward.
	reply = &AppendEntriesReply{}
	node.HandleAppendEntries(&AppendEntriesArgs{
		Term: 2, LeaderID: 3, PrevLogIndex: 3, PrevLogTerm: 1, Entries: makeEntries(4, 4, 2),
	}, reply)
	if !reply.Success {
		t.Fatalf("expected conflicting AppendEntries to succeed")
	}
	node.Stop()
	_ = storage.Close()

	// 4. Crash + reopen: disk must reflect exactly the in-memory log.
	reopened, err := NewTidwallStorage(dir)
	if err != nil {
		t.Fatalf("failed to reopen storage: %v", err)
	}
	defer reopened.Close()
	if got, _ := reopened.LastIndex(); got != 4 {
		t.Fatalf("after conflict + restart: expected durable LastIndex 4, got %d", got)
	}
	for idx, want := range map[uint64]uint64{1: 1, 3: 1, 4: 2} {
		if term, err := reopened.Term(idx); err != nil || term != want {
			t.Fatalf("after restart: entry %d expected term %d, got term=%d err=%v", idx, want, term, err)
		}
	}
}

// TestRaft_AppliesCommittedEntriesInOrder is a regression test for
// scheduleApplyLocked launching one goroutine per commit advance: with
// several batches in flight, the Go scheduler decided which reached applyCh
// first, so a state machine could observe index 7 before index 6.
func TestRaft_AppliesCommittedEntriesInOrder(t *testing.T) {
	storage, err := NewTidwallStorage(t.TempDir())
	if err != nil {
		t.Fatalf("failed to open storage: %v", err)
	}
	defer storage.Close()

	// Unbuffered, and nobody reads until every batch is queued, so all
	// batches are pending at once - the worst case for ordering.
	applyCh := make(chan ApplyMsg)
	node, err := NewRaftNode(quietFollowerConfig(), NewSimulatedNetwork(), storage, applyCh)
	if err != nil {
		t.Fatalf("failed to create node: %v", err)
	}
	defer node.Stop()

	const n = 200
	for i := uint64(1); i <= n; i++ {
		reply := &AppendEntriesReply{}
		// One entry per RPC, committed immediately: every call is its own
		// commit advance, and therefore its own apply batch.
		node.HandleAppendEntries(&AppendEntriesArgs{
			Term: 1, LeaderID: 1, PrevLogIndex: i - 1, PrevLogTerm: termOf(i - 1),
			Entries: makeEntries(i, i, 1), LeaderCommit: i,
		}, reply)
		if !reply.Success {
			t.Fatalf("AppendEntries for index %d failed", i)
		}
	}

	// A snapshot installed after those commits must also be delivered after
	// them, never interleaved ahead of commands it supersedes.
	snapReply := &InstallSnapshotReply{}
	node.HandleInstallSnapshot(&InstallSnapshotArgs{
		Term: 1, LeaderID: 1, LastIncludedIndex: n + 50, LastIncludedTerm: 1, Data: []byte("snap"),
	}, snapReply)

	for want := uint64(1); want <= n; want++ {
		msg := recvApply(t, applyCh)
		if !msg.CommandValid || msg.CommandIndex != want {
			t.Fatalf("out-of-order apply: expected command at index %d, got index=%d commandValid=%v",
				want, msg.CommandIndex, msg.CommandValid)
		}
	}
	if msg := recvApply(t, applyCh); msg.CommandValid || msg.CommandIndex != n+50 {
		t.Fatalf("expected snapshot at index %d last, got index=%d commandValid=%v",
			n+50, msg.CommandIndex, msg.CommandValid)
	}
}

func termOf(index uint64) uint64 {
	if index == 0 {
		return 0
	}
	return 1
}

func recvApply(t *testing.T, ch <-chan ApplyMsg) ApplyMsg {
	t.Helper()
	select {
	case msg := <-ch:
		return msg
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for ApplyMsg")
		return ApplyMsg{}
	}
}
