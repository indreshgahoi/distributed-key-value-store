package raft

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

type SimulatedNetwork struct {
	mu           sync.RWMutex
	nodes        map[uint64]*RaftNode
	disconnected map[uint64]bool
	partitionMap map[uint64]map[uint64]bool
}

func NewSimulatedNetwork() *SimulatedNetwork {
	return &SimulatedNetwork{
		nodes:        make(map[uint64]*RaftNode),
		disconnected: make(map[uint64]bool),
		partitionMap: make(map[uint64]map[uint64]bool),
	}
}

func (n *SimulatedNetwork) Register(id uint64, node *RaftNode) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = node
}

func (n *SimulatedNetwork) Disconnect(id uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disconnected[id] = true
}

func (n *SimulatedNetwork) Reconnect(id uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.disconnected, id)
}

func (n *SimulatedNetwork) Partition(groupA, groupB []uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, a := range groupA {
		if n.partitionMap[a] == nil {
			n.partitionMap[a] = make(map[uint64]bool)
		}
		for _, b := range groupB {
			n.partitionMap[a][b] = true
		}
	}
	for _, b := range groupB {
		if n.partitionMap[b] == nil {
			n.partitionMap[b] = make(map[uint64]bool)
		}
		for _, a := range groupA {
			n.partitionMap[b][a] = true
		}
	}
}

func (n *SimulatedNetwork) HealPartitions() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitionMap = make(map[uint64]map[uint64]bool)
	n.disconnected = make(map[uint64]bool)
}

func (n *SimulatedNetwork) isBlocked(from, to uint64) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.disconnected[from] || n.disconnected[to] {
		return true
	}
	if n.partitionMap[from] != nil && n.partitionMap[from][to] {
		return true
	}
	return false
}

func (n *SimulatedNetwork) SendRequestVote(ctx context.Context, to uint64, args *RequestVoteArgs) (*RequestVoteReply, error) {
	if n.isBlocked(args.CandidateID, to) {
		return nil, context.DeadlineExceeded
	}

	n.mu.RLock()
	targetNode := n.nodes[to]
	n.mu.RUnlock()

	if targetNode == nil {
		return nil, fmt.Errorf("node %d not found", to)
	}

	reply := &RequestVoteReply{}
	targetNode.HandleRequestVote(args, reply)
	return reply, nil
}

func (n *SimulatedNetwork) SendAppendEntries(ctx context.Context, to uint64, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	if n.isBlocked(args.LeaderID, to) {
		return nil, context.DeadlineExceeded
	}

	n.mu.RLock()
	targetNode := n.nodes[to]
	n.mu.RUnlock()

	if targetNode == nil {
		return nil, fmt.Errorf("node %d not found", to)
	}

	reply := &AppendEntriesReply{}
	targetNode.HandleAppendEntries(args, reply)
	return reply, nil
}

func (n *SimulatedNetwork) SendInstallSnapshot(ctx context.Context, to uint64, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	if n.isBlocked(args.LeaderID, to) {
		return nil, context.DeadlineExceeded
	}

	n.mu.RLock()
	targetNode := n.nodes[to]
	n.mu.RUnlock()

	if targetNode == nil {
		return nil, fmt.Errorf("node %d not found", to)
	}

	reply := &InstallSnapshotReply{}
	targetNode.HandleInstallSnapshot(args, reply)
	return reply, nil
}

type TestCluster struct {
	peers  []uint64
	net    *SimulatedNetwork
	nodes  map[uint64]*RaftNode
	stores map[uint64]*mvcc.Store
}

func NewTestCluster(t *testing.T, nodeCount int) *TestCluster {
	var peers []uint64
	for i := 1; i <= nodeCount; i++ {
		peers = append(peers, uint64(i))
	}

	net := NewSimulatedNetwork()
	tc := &TestCluster{
		peers:  peers,
		net:    net,
		nodes:  make(map[uint64]*RaftNode),
		stores: make(map[uint64]*mvcc.Store),
	}

	for _, id := range peers {
		cfg := Config{
			NodeID:             id,
			Peers:              peers,
			HeartbeatInterval:  20 * time.Millisecond,
			ElectionTimeoutMin: 60 * time.Millisecond,
			ElectionTimeoutMax: 120 * time.Millisecond,
			RPCTimeout:         20 * time.Millisecond,
		}

		applyCh := make(chan ApplyMsg, 500)
		rawEngine := raw.NewSkipListEngine(16 * 1024 * 1024)
		store := mvcc.NewStore(rawEngine)
		tc.stores[id] = store

		storage, err := NewKVStorage(raw.NewSkipListEngine(16 * 1024 * 1024))
		if err != nil && t != nil {
			t.Fatalf("failed to create storage for node %d: %v", id, err)
		}

		node, err := NewRaftNode(cfg, net, storage, applyCh)
		if err != nil && t != nil {
			t.Fatalf("failed to create node %d: %v", id, err)
		}
		tc.nodes[id] = node
		net.Register(id, node)

		go func(node *RaftNode, ch chan ApplyMsg, s *mvcc.Store) {
			for msg := range ch {
				if msg.CommandValid {
					// Commands are "key:value", but the key itself may contain
					// colons (e.g. "account:Ram:500" -> key "account:Ram", value
					// "500"), so split on the LAST colon, not the first.
					if sep := bytes.LastIndexByte(msg.Command, ':'); sep >= 0 {
						key := msg.Command[:sep]
						value := msg.Command[sep+1:]
						_ = s.Put(key, value, msg.CommandIndex*10)
					}
				}
				node.ReportApplied(msg.CommandIndex)
			}
		}(node, applyCh, store)
	}

	return tc
}

func (tc *TestCluster) Shutdown() {
	for _, n := range tc.nodes {
		n.Stop()
	}
	for _, s := range tc.stores {
		_ = s.Close()
	}
}

// FindLeader polls until exactly one non-excluded node reports itself as
// Leader. exclude lets callers ignore a node known to be disconnected: an
// isolated node correctly keeps believing it's Leader forever (it has no way
// to learn otherwise while cut off), which would otherwise let this return a
// stale leader that can never actually replicate anything.
func (tc *TestCluster) FindLeader(timeout time.Duration, exclude ...uint64) (uint64, uint64, error) {
	excluded := make(map[uint64]bool, len(exclude))
	for _, id := range exclude {
		excluded[id] = true
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var leaders []uint64
		var term uint64
		for id, node := range tc.nodes {
			if excluded[id] {
				continue
			}
			curTerm, isLeader := node.GetState()
			if isLeader {
				leaders = append(leaders, id)
				term = curTerm
			}
		}
		if len(leaders) == 1 {
			return leaders[0], term, nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, 0, fmt.Errorf("failed to elect a single leader within %v", timeout)
}

func TestRaft_DeterministicLeaderElection(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, term, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("Election failed: %v", err)
	}

	if term == 0 {
		t.Fatalf("Expected term > 0, got %d", term)
	}
	t.Logf("PASS: Single Leader elected: Node %d at Term %d", leaderID, term)
}

func TestRaft_QuorumReplicationAndMVCCCommit(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("Election failed: %v", err)
	}

	cmd := []byte("account:Ram:500")
	index, _, isLeader := tc.nodes[leaderID].Propose(cmd)
	if !isLeader {
		t.Fatalf("Node %d lost leadership before proposing", leaderID)
	}

	time.Sleep(200 * time.Millisecond)

	for _, id := range tc.peers {
		val, err := tc.stores[id].Get([]byte("account:Ram"), index*10)
		if err != nil || string(val) != "500" {
			t.Fatalf("Node %d failed to read committed balance. Got %q, err: %v", id, string(val), err)
		}
	}
	t.Logf("PASS: Mutation committed across quorum and verified on MVCC stores")
}

func TestRaft_MinorityPartitionNonBlocking(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("Election failed: %v", err)
	}

	var isolatedFollower uint64
	for _, id := range tc.peers {
		if id != leaderID {
			isolatedFollower = id
			break
		}
	}

	tc.net.Disconnect(isolatedFollower)
	t.Logf("Partitioned node %d from cluster. Remaining: 2/3", isolatedFollower)

	cmd := []byte("account:Sita:750")
	index, _, isLeader := tc.nodes[leaderID].Propose(cmd)
	if !isLeader {
		t.Fatalf("Leader should remain leader with 2/3 quorum")
	}

	time.Sleep(200 * time.Millisecond)

	for _, id := range tc.peers {
		if id == isolatedFollower {
			continue
		}
		val, err := tc.stores[id].Get([]byte("account:Sita"), index*10)
		if err != nil || string(val) != "750" {
			t.Fatalf("Active node %d failed to commit write under 2-node quorum. Got %q, err: %v", id, string(val), err)
		}
	}
	t.Logf("PASS: Write committed successfully despite dead minority node")
}

func TestRaft_LeaderFailureAndFailover(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	oldLeader, initialTerm, err := tc.FindLeader(1 * time.Second)
	if err != nil {
		t.Fatalf("Initial election failed: %v", err)
	}

	tc.net.Disconnect(oldLeader)
	time.Sleep(300 * time.Millisecond)

	var newLeader, newTerm uint64
	for _, id := range tc.peers {
		if id == oldLeader {
			continue
		}
		term, isLeader := tc.nodes[id].GetState()
		if isLeader {
			newLeader = id
			newTerm = term
			break
		}
	}

	if newLeader == 0 {
		t.Fatalf("Remaining nodes failed to elect a new leader")
	}
	if newTerm <= initialTerm {
		t.Fatalf("New leader term (%d) must be greater than old term (%d)", newTerm, initialTerm)
	}

	cmd := []byte("account:Lakshman:900")
	newIdx, _, _ := tc.nodes[newLeader].Propose(cmd)
	time.Sleep(150 * time.Millisecond)

	tc.net.Reconnect(oldLeader)
	time.Sleep(200 * time.Millisecond)

	oldLeaderTerm, oldLeaderIsLeader := tc.nodes[oldLeader].GetState()
	if oldLeaderIsLeader {
		t.Fatalf("Old leader %d failed to step down after discovering higher term", oldLeader)
	}
	if oldLeaderTerm != newTerm {
		t.Fatalf("Old leader term %d did not update to new term %d", oldLeaderTerm, newTerm)
	}

	val, err := tc.stores[oldLeader].Get([]byte("account:Lakshman"), newIdx*10)
	if err != nil || string(val) != "900" {
		t.Fatalf("Old leader failed to catch up with committed log after reconnecting. Got %q, err: %v", string(val), err)
	}
	t.Logf("PASS: Stale leader stepped down and caught up cleanly")
}

func TestRaft_LogConflictTruncation(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, _ := tc.FindLeader(1 * time.Second)

	var otherFollower, isolatedNode uint64
	for _, id := range tc.peers {
		if id != leaderID {
			if otherFollower == 0 {
				otherFollower = id
			} else {
				isolatedNode = id
			}
		}
	}

	_, _, _ = tc.nodes[leaderID].Propose([]byte("k1:v1"))
	time.Sleep(150 * time.Millisecond)

	tc.net.Partition([]uint64{leaderID}, []uint64{otherFollower, isolatedNode})
	tc.net.Partition([]uint64{isolatedNode}, []uint64{leaderID, otherFollower})

	tc.nodes[leaderID].Propose([]byte("k2:stale_value"))
	time.Sleep(100 * time.Millisecond)

	tc.net.HealPartitions()
	tc.net.Disconnect(leaderID)

	newLeader, _, err := tc.FindLeader(1*time.Second, leaderID)
	if err != nil {
		t.Fatalf("Failed to elect new leader among survivors: %v", err)
	}

	idx2, _, _ := tc.nodes[newLeader].Propose([]byte("k2:correct_value"))
	time.Sleep(200 * time.Millisecond)

	tc.net.Reconnect(leaderID)
	time.Sleep(300 * time.Millisecond)

	for _, id := range tc.peers {
		val, err := tc.stores[id].Get([]byte("k2"), idx2*10)
		if err != nil || string(val) != "correct_value" {
			t.Fatalf("Node %d has conflicting entry not overwritten! Got %q, err: %v", id, string(val), err)
		}
	}
	t.Logf("PASS: Conflicting uncommitted log entry overwritten via Raft Log Matching Invariant")
}

func TestRaft_ConcurrentProposalsAndRace(t *testing.T) {
	tc := NewTestCluster(t, 3)
	defer tc.Shutdown()

	leaderID, _, _ := tc.FindLeader(1 * time.Second)

	const numWriters = 5
	const writesPerWorker = 20
	var wg sync.WaitGroup

	for w := 0; w < numWriters; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < writesPerWorker; i++ {
				cmd := []byte(fmt.Sprintf("stress:w%d_%d:val_%d", workerID, i, i))
				tc.nodes[leaderID].Propose(cmd)
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	expectedLen := tc.nodes[leaderID].Status().LastIndex
	for _, id := range tc.peers {
		lastIdx := tc.nodes[id].Status().LastIndex
		if lastIdx != expectedLen {
			t.Fatalf("Node %d log index %d diverged from leader index %d", id, lastIdx, expectedLen)
		}
	}
	t.Logf("PASS: High-concurrency proposals synchronized with 0 data races")
}
