package sharding

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/replica"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

// --- simulated network -------------------------------------------------------

// SimulatedMultiRaftNetwork routes range RPCs in-process, with per-node
// disconnection for fault injection.
type SimulatedMultiRaftNetwork struct {
	mu           sync.RWMutex
	nodes        map[uint64]*MultiRaftNode
	disconnected map[uint64]bool
}

func NewSimulatedMultiRaftNetwork() *SimulatedMultiRaftNetwork {
	return &SimulatedMultiRaftNetwork{
		nodes:        make(map[uint64]*MultiRaftNode),
		disconnected: make(map[uint64]bool),
	}
}

func (n *SimulatedMultiRaftNetwork) Register(id uint64, node *MultiRaftNode) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.nodes[id] = node
}

func (n *SimulatedMultiRaftNetwork) Disconnect(id uint64) { n.setDisconnected(id, true) }
func (n *SimulatedMultiRaftNetwork) Reconnect(id uint64)  { n.setDisconnected(id, false) }

func (n *SimulatedMultiRaftNetwork) setDisconnected(id uint64, v bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.disconnected[id] = v
}

// route finds the target range's RaftNode, or fails like a network would.
func (n *SimulatedMultiRaftNetwork) route(from, to, rangeID uint64) (*raft.RaftNode, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.disconnected[from] || n.disconnected[to] {
		return nil, context.DeadlineExceeded
	}
	target, ok := n.nodes[to]
	if !ok {
		return nil, fmt.Errorf("target node %d not found", to)
	}
	node, ok := target.GetRange(rangeID)
	if !ok {
		return nil, fmt.Errorf("range %d not found on node %d", rangeID, to)
	}
	return node, nil
}

func (n *SimulatedMultiRaftNetwork) SendRangeRequestVote(_ context.Context, to, rangeID uint64, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	node, err := n.route(args.CandidateID, to, rangeID)
	if err != nil {
		return nil, err
	}
	reply := &raft.RequestVoteReply{}
	node.HandleRequestVote(args, reply)
	return reply, nil
}

func (n *SimulatedMultiRaftNetwork) SendRangeAppendEntries(_ context.Context, to, rangeID uint64, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	node, err := n.route(args.LeaderID, to, rangeID)
	if err != nil {
		return nil, err
	}
	reply := &raft.AppendEntriesReply{}
	node.HandleAppendEntries(args, reply)
	return reply, nil
}

func (n *SimulatedMultiRaftNetwork) SendRangeInstallSnapshot(_ context.Context, to, rangeID uint64, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	node, err := n.route(args.LeaderID, to, rangeID)
	if err != nil {
		return nil, err
	}
	reply := &raft.InstallSnapshotReply{}
	node.HandleInstallSnapshot(args, reply)
	return reply, nil
}

// --- test cluster ------------------------------------------------------------

var (
	testPeers = []uint64{1, 2, 3}
	// Two ranges splitting the keyspace at "m".
	rangeLow  = RangeDescriptor{RangeID: 1, StartKey: []byte(""), EndKey: []byte("m"), Peers: testPeers}
	rangeHigh = RangeDescriptor{RangeID: 2, StartKey: []byte("m"), EndKey: []byte(""), Peers: testPeers}
)

type testCluster struct {
	t      *testing.T
	router *RangeRouter
	nodes  map[uint64]*MultiRaftNode
}

func raftConfig(id uint64) raft.Config {
	return raft.Config{
		NodeID:             id,
		Peers:              testPeers,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 60 * time.Millisecond,
		ElectionTimeoutMax: 120 * time.Millisecond,
		RPCTimeout:         30 * time.Millisecond,
	}
}

// newTestCluster boots 3 nodes hosting both ranges; transportFor supplies
// each node's transport, register is called with each node once created.
func newTestCluster(t *testing.T, opts Options, transportFor func(id uint64) MultiRaftTransport, register func(id uint64, n *MultiRaftNode)) *testCluster {
	t.Helper()
	c := &testCluster{t: t, router: NewRangeRouter(), nodes: make(map[uint64]*MultiRaftNode)}
	if err := c.router.UpdateTable([]RangeDescriptor{rangeLow, rangeHigh}); err != nil {
		t.Fatalf("routing table: %v", err)
	}
	if opts.NewStore == nil {
		opts.NewStore = func(RangeDescriptor) *mvcc.Store { return mvcc.NewStore(raw.NewSkipListEngine(8 << 20)) }
	}
	for _, id := range testPeers {
		node := NewMultiRaftNode(id, c.router, transportFor(id), opts)
		c.nodes[id] = node
		register(id, node)
		for _, desc := range []RangeDescriptor{rangeLow, rangeHigh} {
			storage, err := raft.NewKVStorage(raw.NewSkipListEngine(8 << 20))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := node.CreateRange(desc, raftConfig(id), storage); err != nil {
				t.Fatalf("node %d, range %d: %v", id, desc.RangeID, err)
			}
		}
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func newSimCluster(t *testing.T, opts Options) (*testCluster, *SimulatedMultiRaftNetwork) {
	net := NewSimulatedMultiRaftNetwork()
	c := newTestCluster(t, opts, func(uint64) MultiRaftTransport { return net }, net.Register)
	return c, net
}

// eventually polls cond until it returns nil or timeout passes.
func eventually(t *testing.T, timeout time.Duration, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		err := cond()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("condition not met within %v: %v", timeout, err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// leader waits for rangeID to elect a leader and returns it.
func (c *testCluster) leader(rangeID uint64) uint64 {
	c.t.Helper()
	var leader uint64
	eventually(c.t, 3*time.Second, func() error {
		for id, n := range c.nodes {
			node, _ := n.GetRange(rangeID)
			if _, isLeader := node.GetState(); isLeader {
				leader = id
				return nil
			}
		}
		return fmt.Errorf("range %d has no leader", rangeID)
	})
	return leader
}

// put writes through whichever node leads key's range, retrying across
// leadership changes, and returns the range it landed in.
func (c *testCluster) put(key, value string) uint64 {
	c.t.Helper()
	var rangeID uint64
	eventually(c.t, 3*time.Second, func() error {
		desc, err := c.router.FindRange([]byte(key))
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		rangeID, _, err = c.nodes[c.leader(desc.RangeID)].Put(ctx, []byte(key), []byte(value))
		return err
	})
	return rangeID
}

// waitReplicated waits until key reads value on every listed node.
func (c *testCluster) waitReplicated(key, value string, ids ...uint64) {
	c.t.Helper()
	if len(ids) == 0 {
		ids = testPeers
	}
	for _, id := range ids {
		eventually(c.t, 3*time.Second, func() error {
			got, err := c.nodes[id].LocalGet([]byte(key))
			if err != nil || string(got) != value {
				return fmt.Errorf("node %d: %s = %q (%v), want %q", id, key, got, err, value)
			}
			return nil
		})
	}
}

// --- tests -------------------------------------------------------------------

func TestMultiRaft_ParallelConsensusAcrossRanges(t *testing.T) {
	c, _ := newSimCluster(t, Options{})
	t.Logf("range 1 leader: %d, range 2 leader: %d", c.leader(1), c.leader(2))

	if id := c.put("apple", "red"); id != 1 {
		t.Fatalf("apple routed to range %d, want 1", id)
	}
	if id := c.put("zebra", "stripes"); id != 2 {
		t.Fatalf("zebra routed to range %d, want 2", id)
	}
	c.waitReplicated("apple", "red")
	c.waitReplicated("zebra", "stripes")

	// Linearizable reads through each range's leader.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for key, want := range map[string]string{"apple": "red", "zebra": "stripes"} {
		desc, _ := c.router.FindRange([]byte(key))
		got, err := c.nodes[c.leader(desc.RangeID)].Get(ctx, []byte(key))
		if err != nil || string(got) != want {
			t.Fatalf("Get(%s) = %q, %v; want %q", key, got, err, want)
		}
	}
}

// TestMultiRaft_RangeSnapshotRestoreIsIsolated is a regression test: all
// ranges on a node used to share one store, and a snapshot restore replaces
// the whole store - so installing range 1's snapshot erased range 2's data.
func TestMultiRaft_RangeSnapshotRestoreIsIsolated(t *testing.T) {
	c, _ := newSimCluster(t, Options{})
	c.put("apple", "red")
	c.put("zebra", "stripes")
	c.waitReplicated("apple", "red")
	c.waitReplicated("zebra", "stripes")

	// Restore node 1's range-1 replica from an empty snapshot.
	var empty bytes.Buffer
	if err := mvcc.NewStore(raw.NewSkipListEngine(1<<20)).ExportSnapshot(&empty, 0); err != nil {
		t.Fatal(err)
	}
	r1, _ := c.nodes[1].replicaByID(1)
	if err := r1.store.RestoreSnapshot(&empty); err != nil {
		t.Fatal(err)
	}

	if _, err := c.nodes[1].LocalGet([]byte("apple")); !errors.Is(err, mvcc.ErrKeyNotFound) {
		t.Fatalf("range 1 not restored: apple still readable (%v)", err)
	}
	if got, err := c.nodes[1].LocalGet([]byte("zebra")); err != nil || string(got) != "stripes" {
		t.Fatalf("restoring range 1 changed range 2: zebra = %q (%v)", got, err)
	}
}

// TestMultiRaft_LaggingNodeCatchesUpViaSnapshot drives InstallSnapshot
// end to end through the multiplexed transport: a node misses enough of
// range 1 that the leader has compacted the entries it needs.
func TestMultiRaft_LaggingNodeCatchesUpViaSnapshot(t *testing.T) {
	c, net := newSimCluster(t, Options{SnapshotEvery: 5})
	c.put("zebra", "stripes")
	c.waitReplicated("zebra", "stripes")

	lagger := c.leader(1)%3 + 1 // any node other than range 1's current leader
	net.Disconnect(lagger)
	for i := 0; i < 20; i++ {
		c.put(fmt.Sprintf("a%02d", i), fmt.Sprint(i))
	}
	leaderNode, _ := c.nodes[c.leader(1)].GetRange(1)
	eventually(t, 3*time.Second, func() error {
		if s := leaderNode.Status().Snapshot; s < 10 {
			return fmt.Errorf("range 1 leader snapshot index %d", s)
		}
		return nil
	})

	net.Reconnect(lagger)
	c.waitReplicated("a19", "19", lagger)
	laggerRange1, _ := c.nodes[lagger].GetRange(1)
	if laggerRange1.Status().Snapshot == 0 {
		t.Fatalf("node %d caught up without installing a snapshot", lagger)
	}
	// Installing range 1's snapshot left range 2 untouched.
	c.waitReplicated("zebra", "stripes", lagger)
}

// TestMultiRaft_ValuesWithColonsRoundTrip is a regression test: commands
// used to be encoded as "key:value" and split on the last colon, so
// Put("a", "b:c") stored key "a:b" with value "c".
func TestMultiRaft_ValuesWithColonsRoundTrip(t *testing.T) {
	c, _ := newSimCluster(t, Options{})
	c.put("a", "b:c")
	c.waitReplicated("a", "b:c")
	if got, err := c.nodes[1].LocalGet([]byte("a:b")); err == nil {
		t.Fatalf("value was split into a phantom key: a:b = %q", got)
	}
}

// TestMultiRaft_MisroutedCommandIsRejected: a write that reaches the wrong
// range (a stale routing table) is rejected at apply time on every replica,
// never applied to a range that doesn't own the key.
func TestMultiRaft_MisroutedCommandIsRejected(t *testing.T) {
	c, _ := newSimCluster(t, Options{})
	r1, _ := c.nodes[c.leader(1)].replicaByID(1)

	_, result, isLeader := r1.proposals.Propose(r1.node, replica.Command{Op: replica.OpPut, Key: "zebra", Value: "misrouted"}.Encode())
	if !isLeader {
		t.Fatal("lost leadership")
	}
	select {
	case err := <-result:
		if !errors.Is(err, replica.ErrKeyOutOfRange) {
			t.Fatalf("expected ErrKeyOutOfRange, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("misrouted command never resolved")
	}
	if got, err := r1.store.Get([]byte("zebra"), latestTS); err == nil {
		t.Fatalf("range 1 applied a key it doesn't own: %q", got)
	}
}

func TestMultiRaft_StopIsIdempotent(t *testing.T) {
	c, _ := newSimCluster(t, Options{})
	c.nodes[1].Stop()
	c.nodes[1].Stop() // used to panic: close of closed channel
}

// TestMultiRaft_OverTCP runs both ranges over the real TCP transport - one
// listener and one connection per node pair, shared by every range.
func TestMultiRaft_OverTCP(t *testing.T) {
	listeners := make(map[uint64]net.Listener)
	addrs := make(map[uint64]string)
	for _, id := range testPeers {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { l.Close() })
		listeners[id], addrs[id] = l, l.Addr().String()
	}
	transports := make(map[uint64]*MultiRaftTCPTransport)
	c := newTestCluster(t, Options{},
		func(id uint64) MultiRaftTransport {
			transports[id] = NewMultiRaftTCPTransport(addrs)
			return transports[id]
		},
		func(id uint64, n *MultiRaftNode) {
			if err := ServeMultiRaft(listeners[id], n); err != nil {
				t.Fatal(err)
			}
		})

	c.put("apple", "red")
	c.put("zebra", "stripes")
	c.waitReplicated("apple", "red")
	c.waitReplicated("zebra", "stripes")

	// An RPC for a range the peer doesn't host fails cleanly...
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := transports[1].SendRangeAppendEntries(ctx, 2, 99, &raft.AppendEntriesArgs{})
	if err == nil {
		t.Fatal("expected an error for an unhosted range")
	}
	// ...and the shared connection keeps carrying the real ranges.
	c.put("mango", "yellow")
	c.waitReplicated("mango", "yellow")
}
