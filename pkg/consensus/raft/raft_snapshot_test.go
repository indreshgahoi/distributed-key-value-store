package raft

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

func TestRaft_InstallSnapshotToLaggingFollower(t *testing.T) {
	peerIDs := []uint64{1, 2, 3}
	net := NewSimulatedNetwork()

	stores := make(map[uint64]*mvcc.Store)
	rafts := make(map[uint64]*RaftNode)

	for _, id := range peerIDs {
		cfg := Config{
			NodeID:             id,
			Peers:              peerIDs,
			HeartbeatInterval:  20 * time.Millisecond,
			ElectionTimeoutMin: 60 * time.Millisecond,
			ElectionTimeoutMax: 120 * time.Millisecond,
			RPCTimeout:         30 * time.Millisecond,
		}

		applyCh := make(chan ApplyMsg, 1000)
		rawEngine := raw.NewSkipListEngine(16 * 1024 * 1024)
		store := mvcc.NewStore(rawEngine)
		stores[id] = store

		storage, _ := NewKVStorage(raw.NewSkipListEngine(16 * 1024 * 1024))
		node, err := NewRaftNode(cfg, net, storage, applyCh)
		if err != nil {
			t.Fatalf("failed to boot node %d: %v", id, err)
		}
		rafts[id] = node
		net.Register(id, node)

		go func(nid uint64, ch chan ApplyMsg, s *mvcc.Store) {
			for msg := range ch {
				if msg.CommandValid {
					sep := bytes.LastIndexByte(msg.Command, ':')
					if sep >= 0 {
						_ = s.Put(msg.Command[:sep], msg.Command[sep+1:], msg.CommandIndex*10)
					}
				} else {
					// Snapshot restoration
					_ = s.RestoreSnapshot(bytes.NewReader(msg.Command))
				}
			}
		}(id, applyCh, store)
	}

	defer func() {
		for _, r := range rafts {
			r.Stop()
		}
	}()

	// 1. Elect Leader
	time.Sleep(300 * time.Millisecond)
	var leaderID uint64
	for id, r := range rafts {
		if _, isLeader := r.GetState(); isLeader {
			leaderID = id
			break
		}
	}
	if leaderID == 0 {
		t.Fatalf("no leader elected")
	}

	// 2. Disconnect Follower 3
	var followerID uint64 = 3
	if leaderID == 3 {
		followerID = 2
	}
	net.Disconnect(followerID)
	t.Logf("Disconnected Follower %d", followerID)

	// 3. Leader commits 20 writes while Follower is offline
	for i := 1; i <= 20; i++ {
		cmd := fmt.Appendf(nil, "key_%02d:val_%02d", i, i)
		rafts[leaderID].Propose(cmd)
	}
	time.Sleep(200 * time.Millisecond)

	// 4. Leader triggers Log Compaction at Index 15
	var snapBuf bytes.Buffer
	if err := stores[leaderID].ExportSnapshot(&snapBuf, uint64(time.Now().UnixNano())); err != nil {
		t.Fatalf("snapshot export failed: %v", err)
	}

	if err := rafts[leaderID].Snapshot(15, snapBuf.Bytes()); err != nil {
		t.Fatalf("raft snapshot failed: %v", err)
	}
	t.Logf("Leader successfully compacted log up to Index 15")

	// 5. Reconnect lagging Follower
	net.Reconnect(followerID)
	t.Logf("Reconnected Follower %d. Waiting for InstallSnapshot...", followerID)

	// 6. Verify Follower received and restored the snapshot.
	//
	// Poll instead of a single fixed sleep. Pre-Vote (see raft_prevote_test.go)
	// now stops a reconnecting isolated node from inflating its term and
	// disrupting the healthy leader, but this still polls with a generous
	// timeout rather than a single fixed wait — general hygiene for
	// distributed timing, not a workaround for a specific known race.
	deadline := time.Now().Add(3 * time.Second)
	var val []byte
	var err error
	for time.Now().Before(deadline) {
		val, err = stores[followerID].Get([]byte("key_10"), 1000000)
		if err == nil && string(val) == "val_10" {
			t.Logf("PASS: Lagging follower successfully fast-forwarded via InstallSnapshot")
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("Follower %d failed to restore state from snapshot within 3s. Got %q, err: %v", followerID, string(val), err)
}
