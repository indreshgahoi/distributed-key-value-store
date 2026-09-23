package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

// TestRaft_Chaos is a randomized fault-injection test. A 5-node cluster runs
// under a network that drops, delays, duplicates and reorders messages and
// loses replies, while a nemesis crashes/restarts nodes and partitions the
// cluster, and clients keep proposing. Throughout, it checks Raft's safety
// properties:
//
//   - State Machine Safety: no two nodes ever apply different commands at the
//     same index, and no command is applied at two different indexes.
//   - Election Safety: at most one leader per term.
//   - Durability: every write acknowledged to a client is, after the cluster
//     heals, applied on every node at the index it was acknowledged at.
//
// Seeds make the fault schedule reproducible (goroutine interleavings are
// still up to the scheduler). Tune with RAFT_CHAOS_SEEDS / RAFT_CHAOS_SECONDS;
// a failure prints the seed to rerun with RAFT_CHAOS_SEED.
func TestRaft_Chaos(t *testing.T) {
	seeds := envInt("RAFT_CHAOS_SEEDS", 3)
	duration := time.Duration(envInt("RAFT_CHAOS_SECONDS", 3)) * time.Second
	if testing.Short() {
		seeds, duration = 1, time.Second
	}
	if s, ok := os.LookupEnv("RAFT_CHAOS_SEED"); ok {
		seed, _ := strconv.ParseInt(s, 10, 64)
		runChaos(t, seed, duration)
		return
	}
	for i := 0; i < seeds; i++ {
		seed := time.Now().UnixNano() + int64(i)
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) { runChaos(t, seed, duration) })
	}
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

func runChaos(t *testing.T, seed int64, duration time.Duration) {
	t.Logf("seed %d (rerun with RAFT_CHAOS_SEED=%d)", seed, seed)
	c := newChaosCluster(t, 5, seed)
	defer c.shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); c.runNemesis(ctx) }()
	go func() { defer wg.Done(); c.runElectionSafetyChecker(ctx) }()
	go func() { defer wg.Done(); c.runClients(ctx, 4) }()
	wg.Wait()

	// Heal everything and check the cluster converges.
	c.net.heal()
	for id := range c.nodes {
		if c.net.isDown(id) {
			c.restart(id)
		}
	}
	marker := c.proposeUntilCommitted(t, "final-marker", 10*time.Second)
	c.waitAllApplied(t, marker, 10*time.Second)

	c.history.check(t)
	c.checkAcknowledgedWritesDurable(t)
	t.Logf("seed %d: %d writes acknowledged, %d crashes, %d partitions; all safety checks passed",
		seed, len(c.acked), c.crashes, c.partitions)
}

// --- network ---------------------------------------------------------------

// chaosNet routes RPCs between nodes, injecting faults.
type chaosNet struct {
	mu      sync.Mutex
	rng     *rand.Rand
	nodes   map[uint64]*RaftNode
	down    map[uint64]bool
	cut     map[[2]uint64]bool
	lossy   bool
	maxWait time.Duration
}

func (n *chaosNet) endpoint(from uint64) NetworkTransport { return &chaosEndpoint{net: n, from: from} }

func (n *chaosNet) isDown(id uint64) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.down[id]
}

func (n *chaosNet) heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cut = make(map[[2]uint64]bool)
	n.lossy = false
}

// deliveryPlan is what the network decides to do with one message.
type deliveryPlan struct {
	target      *RaftNode
	delay       time.Duration
	duplicate   bool
	loseReply   bool
	undelivered bool
}

func (n *chaosNet) plan(from, to uint64) deliveryPlan {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.down[from] || n.down[to] || n.cut[[2]uint64{from, to}] || n.nodes[to] == nil {
		return deliveryPlan{undelivered: true}
	}
	p := deliveryPlan{target: n.nodes[to]}
	if n.lossy {
		p.undelivered = n.rng.Float64() < 0.05
		p.delay = time.Duration(n.rng.Int63n(int64(n.maxWait)))
		p.duplicate = n.rng.Float64() < 0.05
		p.loseReply = n.rng.Float64() < 0.05
	}
	return p
}

// deliver invokes handle on the target according to the network's plan.
// A message that isn't delivered (or whose reply is lost) looks exactly like
// a real network timeout to the sender.
func deliver[R any](ctx context.Context, n *chaosNet, from, to uint64, handle func(*RaftNode) R) (R, error) {
	var zero R
	p := n.plan(from, to)
	if p.undelivered {
		<-ctx.Done()
		return zero, ctx.Err()
	}
	select {
	case <-time.After(p.delay):
	case <-ctx.Done():
		return zero, ctx.Err()
	}
	reply := handle(p.target)
	if p.duplicate {
		go func() {
			time.Sleep(p.delay)
			handle(p.target) // a late duplicate; its reply goes nowhere
		}()
	}
	if p.loseReply {
		<-ctx.Done()
		return zero, ctx.Err()
	}
	return reply, nil
}

type chaosEndpoint struct {
	net  *chaosNet
	from uint64
}

func (e *chaosEndpoint) SendRequestVote(ctx context.Context, to uint64, args *RequestVoteArgs) (*RequestVoteReply, error) {
	return deliver(ctx, e.net, e.from, to, func(n *RaftNode) *RequestVoteReply {
		r := &RequestVoteReply{}
		n.HandleRequestVote(args, r)
		return r
	})
}

func (e *chaosEndpoint) SendAppendEntries(ctx context.Context, to uint64, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	return deliver(ctx, e.net, e.from, to, func(n *RaftNode) *AppendEntriesReply {
		r := &AppendEntriesReply{}
		n.HandleAppendEntries(args, r)
		return r
	})
}

func (e *chaosEndpoint) SendInstallSnapshot(ctx context.Context, to uint64, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	return deliver(ctx, e.net, e.from, to, func(n *RaftNode) *InstallSnapshotReply {
		r := &InstallSnapshotReply{}
		n.HandleInstallSnapshot(args, r)
		return r
	})
}

// --- cluster ---------------------------------------------------------------

type chaosCluster struct {
	t       *testing.T
	cfgs    map[uint64]Config
	net     *chaosNet
	nodes   map[uint64]*chaosNode
	history *appliedHistory
	rng     *rand.Rand // nemesis decisions (single goroutine)

	mu         sync.Mutex
	acked      map[string]uint64 // command -> index acknowledged at
	crashes    int
	partitions int
}

type chaosNode struct {
	disk *raw.SkipListEngine // survives crashes; everything else is rebuilt

	mu   sync.Mutex
	node *RaftNode
	sm   *chaosSM
}

// chaosSM is the replicated state machine: it simply remembers which
// command it applied at each index, which is exactly what the safety checks
// need. Its snapshot is that whole map.
type chaosSM struct {
	mu      sync.Mutex
	applied map[uint64]string
	last    uint64
}

func (s *chaosSM) get(index uint64) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd, ok := s.applied[index]
	return cmd, ok
}

func newChaosCluster(t *testing.T, size int, seed int64) *chaosCluster {
	c := &chaosCluster{
		t:       t,
		cfgs:    make(map[uint64]Config),
		nodes:   make(map[uint64]*chaosNode),
		history: newAppliedHistory(),
		rng:     rand.New(rand.NewSource(seed)),
		acked:   make(map[string]uint64),
		net: &chaosNet{
			rng:     rand.New(rand.NewSource(seed + 1)),
			nodes:   make(map[uint64]*RaftNode),
			down:    make(map[uint64]bool),
			cut:     make(map[[2]uint64]bool),
			maxWait: 5 * time.Millisecond,
		},
	}
	var peers []uint64
	for i := 1; i <= size; i++ {
		peers = append(peers, uint64(i))
	}
	for _, id := range peers {
		c.cfgs[id] = Config{
			NodeID:             id,
			Peers:              peers,
			HeartbeatInterval:  10 * time.Millisecond,
			ElectionTimeoutMin: 50 * time.Millisecond,
			ElectionTimeoutMax: 100 * time.Millisecond,
			RPCTimeout:         25 * time.Millisecond,
		}
		c.nodes[id] = &chaosNode{disk: raw.NewSkipListEngine(64 << 20)}
		c.start(id)
	}
	return c
}

// start boots a node incarnation over its surviving disk, with an empty
// state machine that Raft must rebuild (snapshot + replay).
func (c *chaosCluster) start(id uint64) {
	cn := c.nodes[id]
	storage, err := NewKVStorage(cn.disk)
	if err != nil {
		c.t.Fatalf("node %d: reopening storage: %v", id, err)
	}
	applyCh := make(chan ApplyMsg, 64)
	node, err := NewRaftNode(c.cfgs[id], c.net.endpoint(id), storage, applyCh)
	if err != nil {
		c.t.Fatalf("node %d: boot: %v", id, err)
	}
	sm := &chaosSM{applied: make(map[uint64]string)}

	cn.mu.Lock()
	cn.node, cn.sm = node, sm
	cn.mu.Unlock()
	c.net.mu.Lock()
	c.net.nodes[id] = node
	c.net.down[id] = false
	c.net.mu.Unlock()

	go c.applyLoop(id, node, sm, applyCh)
}

func (c *chaosCluster) applyLoop(id uint64, node *RaftNode, sm *chaosSM, applyCh <-chan ApplyMsg) {
	for {
		var msg ApplyMsg
		select {
		case msg = <-applyCh:
		case <-node.Done():
			return
		}

		sm.mu.Lock()
		if msg.CommandIndex <= sm.last {
			c.history.violation("node %d: apply went backwards: %d after %d", id, msg.CommandIndex, sm.last)
		}
		if msg.CommandValid {
			sm.applied[msg.CommandIndex] = string(msg.Command)
			c.history.record(id, msg.CommandIndex, string(msg.Command))
		} else {
			restored := make(map[uint64]string)
			if err := json.Unmarshal(msg.Command, &restored); err != nil {
				c.history.violation("node %d: corrupt snapshot at %d: %v", id, msg.CommandIndex, err)
			}
			sm.applied = restored
			for idx, cmd := range restored {
				c.history.record(id, idx, cmd)
			}
		}
		sm.last = msg.CommandIndex
		var snapshot []byte
		if msg.CommandIndex%25 == 0 { // compact regularly, to exercise InstallSnapshot
			snapshot, _ = json.Marshal(sm.applied)
		}
		sm.mu.Unlock()

		node.ReportApplied(msg.CommandIndex)
		if snapshot != nil {
			_ = node.Snapshot(msg.CommandIndex, snapshot)
		}
	}
}

func (c *chaosCluster) crash(id uint64) {
	cn := c.nodes[id]
	cn.mu.Lock()
	node := cn.node
	cn.mu.Unlock()
	c.net.mu.Lock()
	c.net.down[id] = true
	c.net.mu.Unlock()
	node.Stop()
	c.mu.Lock()
	c.crashes++
	c.mu.Unlock()
}

func (c *chaosCluster) restart(id uint64) { c.start(id) }

func (c *chaosCluster) shutdown() {
	for _, cn := range c.nodes {
		cn.mu.Lock()
		cn.node.Stop()
		cn.mu.Unlock()
	}
}

func (c *chaosCluster) node(id uint64) (*RaftNode, *chaosSM) {
	cn := c.nodes[id]
	cn.mu.Lock()
	defer cn.mu.Unlock()
	return cn.node, cn.sm
}

// runNemesis injects one fault every 20-120ms. Beyond random crashes and
// partitions it targets the leader - crashing it, or cutting it off with a
// single follower - because leadership changes mid-replication are what
// produce divergent logs, the situation Raft's subtler rules (the Election
// Restriction, Figure 8) exist for. It never takes down a majority, so the
// cluster must keep making progress.
func (c *chaosCluster) runNemesis(ctx context.Context) {
	ids := make([]uint64, 0, len(c.nodes))
	for id := range c.nodes {
		ids = append(ids, id)
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(20+c.rng.Intn(100)) * time.Millisecond):
		}
		switch c.rng.Intn(7) {
		case 0: // crash a random node
			c.crashIfMajorityRemains(c.upNodes(ids), ids)
		case 1: // crash the leader
			if leader, ok := c.currentLeader(ids); ok {
				c.crashIfMajorityRemains([]uint64{leader}, ids)
			}
		case 2: // restart a crashed node
			for _, id := range ids {
				if c.net.isDown(id) {
					c.restart(id)
					break
				}
			}
		case 3: // random two-way partition
			c.rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
			split := 1 + c.rng.Intn(len(ids)-1)
			c.partition(ids[:split], ids[split:])
		case 4: // isolate the leader with one follower: it keeps accepting writes it can't commit
			if leader, ok := c.currentLeader(ids); ok {
				minority := []uint64{leader}
				var rest []uint64
				for _, id := range ids {
					if id != leader {
						rest = append(rest, id)
					}
				}
				c.rng.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
				c.partition(append(minority, rest[0]), rest[1:])
			}
		case 5:
			c.partition(nil, nil) // heal
		case 6:
			c.net.mu.Lock()
			c.net.lossy = !c.net.lossy
			c.net.mu.Unlock()
		}
	}
}

func (c *chaosCluster) upNodes(ids []uint64) []uint64 {
	var up []uint64
	for _, id := range ids {
		if !c.net.isDown(id) {
			up = append(up, id)
		}
	}
	return up
}

// crashIfMajorityRemains crashes one of candidates if a majority stays up.
func (c *chaosCluster) crashIfMajorityRemains(candidates, all []uint64) {
	if len(candidates) == 0 || len(c.upNodes(all))-1 < len(all)/2+1 {
		return
	}
	c.crash(candidates[c.rng.Intn(len(candidates))])
}

func (c *chaosCluster) currentLeader(ids []uint64) (uint64, bool) {
	for _, id := range c.upNodes(ids) {
		node, _ := c.node(id)
		if _, isLeader := node.GetState(); isLeader {
			return id, true
		}
	}
	return 0, false
}

// partition cuts every link between groups a and b (and heals all others).
func (c *chaosCluster) partition(a, b []uint64) {
	c.net.mu.Lock()
	c.net.cut = make(map[[2]uint64]bool)
	for _, x := range a {
		for _, y := range b {
			c.net.cut[[2]uint64{x, y}] = true
			c.net.cut[[2]uint64{y, x}] = true
		}
	}
	c.net.mu.Unlock()
	if len(a) > 0 && len(b) > 0 {
		c.mu.Lock()
		c.partitions++
		c.mu.Unlock()
	}
}

// runElectionSafetyChecker samples every node's view and flags two leaders
// in the same term.
func (c *chaosCluster) runElectionSafetyChecker(ctx context.Context) {
	leaders := make(map[uint64]uint64)
	for ctx.Err() == nil {
		for id := range c.nodes {
			node, _ := c.node(id)
			if term, isLeader := node.GetState(); isLeader {
				if prev, ok := leaders[term]; ok && prev != id {
					c.history.violation("two leaders in term %d: %d and %d", term, prev, id)
				}
				leaders[term] = id
			}
		}
		time.Sleep(time.Millisecond)
	}
}

// runClients proposes unique commands to whichever node claims leadership.
// A write counts as acknowledged exactly when kv-server would return success:
// the proposing leader applied that command at the index Propose returned.
func (c *chaosCluster) runClients(ctx context.Context, n int) {
	var wg sync.WaitGroup
	for client := 0; client < n; client++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seq := 0; ctx.Err() == nil; seq++ {
				cmd := fmt.Sprintf("client%d-op%d", client, seq)
				c.tryWrite(ctx, cmd, 300*time.Millisecond)
			}
		}()
	}
	wg.Wait()
}

// tryWrite proposes cmd to a leader and waits for it to be applied there.
func (c *chaosCluster) tryWrite(ctx context.Context, cmd string, wait time.Duration) (uint64, bool) {
	for id := range c.nodes {
		node, sm := c.node(id)
		idx, _, ok := node.Propose([]byte(cmd))
		if !ok {
			continue
		}
		deadline := time.Now().Add(wait)
		for time.Now().Before(deadline) && ctx.Err() == nil {
			if got, ok := sm.get(idx); ok {
				if got != cmd {
					return 0, false // superseded by a leadership change: not acknowledged
				}
				c.mu.Lock()
				c.acked[cmd] = idx
				c.mu.Unlock()
				return idx, true
			}
			time.Sleep(2 * time.Millisecond)
		}
		return 0, false
	}
	time.Sleep(5 * time.Millisecond) // no leader right now
	return 0, false
}

func (c *chaosCluster) proposeUntilCommitted(t *testing.T, cmd string, timeout time.Duration) uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for ctx.Err() == nil {
		if idx, ok := c.tryWrite(ctx, cmd, time.Second); ok {
			return idx
		}
	}
	t.Fatalf("cluster did not commit a write within %v after healing (liveness)", timeout)
	return 0
}

func (c *chaosCluster) waitAllApplied(t *testing.T, index uint64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for id := range c.nodes {
		for {
			_, sm := c.node(id)
			sm.mu.Lock()
			last := sm.last
			sm.mu.Unlock()
			if last >= index {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("node %d applied only up to %d, expected %d after healing", id, last, index)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (c *chaosCluster) checkAcknowledgedWritesDurable(t *testing.T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for cmd, idx := range c.acked {
		for id := range c.nodes {
			_, sm := c.node(id)
			if got, _ := sm.get(idx); got != cmd {
				t.Errorf("acknowledged write %q at index %d lost on node %d (found %q)", cmd, idx, id, got)
			}
		}
	}
}

// --- history ---------------------------------------------------------------

// appliedHistory is the cluster-wide record of what was applied where.
type appliedHistory struct {
	mu         sync.Mutex
	byIndex    map[uint64]string
	byCommand  map[string]uint64
	violations []string
}

func newAppliedHistory() *appliedHistory {
	return &appliedHistory{byIndex: make(map[uint64]string), byCommand: make(map[string]uint64)}
}

func (h *appliedHistory) record(node, index uint64, cmd string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if prev, ok := h.byIndex[index]; ok && prev != cmd {
		h.violations = append(h.violations, fmt.Sprintf("index %d: node %d applied %q but %q was applied there before", index, node, cmd, prev))
	}
	if prev, ok := h.byCommand[cmd]; ok && prev != index {
		h.violations = append(h.violations, fmt.Sprintf("command %q applied at both %d and %d", cmd, prev, index))
	}
	h.byIndex[index], h.byCommand[cmd] = cmd, index
}

func (h *appliedHistory) violation(format string, args ...any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.violations = append(h.violations, fmt.Sprintf(format, args...))
}

func (h *appliedHistory) check(t *testing.T) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, v := range h.violations {
		t.Errorf("SAFETY VIOLATION: %s", v)
	}
}
