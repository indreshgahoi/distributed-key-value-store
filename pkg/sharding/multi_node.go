package sharding

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/replica"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

var (
	ErrNotRangeLeader = errors.New("sharding: node is not the leader for targeted range")
	ErrRangeExists    = errors.New("sharding: range already exists on this node")
	ErrRangeNotHosted = errors.New("sharding: this node hosts no replica of the range")
)

// latestTS reads the newest version of a key.
const latestTS = ^uint64(0)

// Options configures a MultiRaftNode.
type Options struct {
	// NewStore creates the store for one range replica. Each range needs its
	// own store: a range's snapshot replaces its store's entire contents,
	// which must never touch another range's data. Default: an in-memory
	// store with a 64 MiB budget.
	NewStore func(desc RangeDescriptor) *mvcc.Store

	// SnapshotEvery is passed to every range's state machine (0 = default).
	SnapshotEvery uint64
}

// MultiRaftNode hosts many independent range replicas on one physical node.
//
// Each range is a complete replica - its own Raft group, store, state
// machine and proposal tracker (pkg/replica) - so ranges share nothing but
// the node's network transport. Requests are routed by key through the
// RangeRouter.
type MultiRaftNode struct {
	nodeID    uint64
	router    *RangeRouter
	transport MultiRaftTransport
	opts      Options

	mu       sync.RWMutex
	ranges   map[uint64]*rangeReplica
	stopOnce sync.Once
}

// rangeReplica is one range's replica on this node.
type rangeReplica struct {
	desc      RangeDescriptor
	node      *raft.RaftNode
	store     *mvcc.Store
	proposals *replica.ProposalTracker
}

// NewMultiRaftNode initializes the physical node container.
func NewMultiRaftNode(nodeID uint64, router *RangeRouter, transport MultiRaftTransport, opts Options) *MultiRaftNode {
	if opts.NewStore == nil {
		opts.NewStore = func(RangeDescriptor) *mvcc.Store {
			return mvcc.NewStore(raw.NewSkipListEngine(64 << 20))
		}
	}
	return &MultiRaftNode{
		nodeID:    nodeID,
		router:    router,
		transport: transport,
		opts:      opts,
		ranges:    make(map[uint64]*rangeReplica),
	}
}

// CreateRange starts this node's replica of a range: an independent Raft
// group over storage, applying into a store of its own.
func (m *MultiRaftNode) CreateRange(desc RangeDescriptor, cfg raft.Config, storage raft.Storage) (*raft.RaftNode, error) {
	if err := desc.Validate(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.ranges[desc.RangeID]; exists {
		return nil, fmt.Errorf("%w: range %d", ErrRangeExists, desc.RangeID)
	}

	// Each RaftNode believes it's a normal single-group node; the adapter
	// tags its RPCs with the range ID on the shared transport.
	applyCh := make(chan raft.ApplyMsg, 1024)
	node, err := raft.NewRaftNode(cfg, newRangeTransportAdapter(desc.RangeID, m.transport), storage, applyCh)
	if err != nil {
		return nil, fmt.Errorf("failed to boot Raft group for range %d: %w", desc.RangeID, err)
	}

	desc = desc.Clone()
	r := &rangeReplica{
		desc:      desc,
		node:      node,
		store:     m.opts.NewStore(desc),
		proposals: replica.NewProposalTracker(),
	}
	sm := replica.New(replica.Config{
		Store:         r.store,
		Node:          node,
		Proposals:     r.proposals,
		SnapshotEvery: m.opts.SnapshotEvery,
		Owns:          desc.Contains, // rejects writes routed with a stale table
		Name:          fmt.Sprintf("range %d", desc.RangeID),
	})
	go func() {
		// Fail-stop per range: a replica that can't apply a committed entry
		// must stop participating rather than diverge. Other ranges carry on.
		if err := sm.Run(applyCh); err != nil {
			log.Printf("[node %d] range %d halted: %v", m.nodeID, desc.RangeID, err)
			node.Stop()
		}
	}()

	m.ranges[desc.RangeID] = r
	return node, nil
}

// GetRange returns the local RaftNode of a range.
func (m *MultiRaftNode) GetRange(rangeID uint64) (*raft.RaftNode, bool) {
	r, ok := m.replicaByID(rangeID)
	if !ok {
		return nil, false
	}
	return r.node, true
}

func (m *MultiRaftNode) replicaByID(rangeID uint64) (*rangeReplica, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.ranges[rangeID]
	return r, ok
}

// replicaFor routes key to its range and returns this node's replica of it.
func (m *MultiRaftNode) replicaFor(key []byte) (*rangeReplica, error) {
	desc, err := m.router.FindRange(key)
	if err != nil {
		return nil, err
	}
	r, ok := m.replicaByID(desc.RangeID)
	if !ok {
		return nil, fmt.Errorf("%w: node %d, range %d", ErrRangeNotHosted, m.nodeID, desc.RangeID)
	}
	return r, nil
}

// Put writes key=value and returns once the write is applied on this node
// (the range's leader). Returns the range and log index it committed at.
func (m *MultiRaftNode) Put(ctx context.Context, key, value []byte) (rangeID, index uint64, err error) {
	return m.replicate(ctx, replica.Command{Op: replica.OpPut, Key: string(key), Value: string(value)})
}

// Delete removes key, returning once the delete is applied.
func (m *MultiRaftNode) Delete(ctx context.Context, key []byte) (rangeID, index uint64, err error) {
	return m.replicate(ctx, replica.Command{Op: replica.OpDelete, Key: string(key)})
}

// replicate proposes cmd to the owning range and waits until it is applied:
// until then a leadership change could still discard it. On ctx expiry the
// outcome is unknown (the command may still commit).
func (m *MultiRaftNode) replicate(ctx context.Context, cmd replica.Command) (uint64, uint64, error) {
	r, err := m.replicaFor([]byte(cmd.Key))
	if err != nil {
		return 0, 0, err
	}
	id := r.desc.RangeID
	index, result, isLeader := r.proposals.Propose(r.node, cmd.Encode())
	if !isLeader {
		leader := r.node.Leader()
		if leader != 0 {
			m.router.UpdateLeader(id, leader) // steer the caller's retry
		}
		return id, 0, fmt.Errorf("%w: range %d (leader hint: %d)", ErrNotRangeLeader, id, leader)
	}
	m.router.UpdateLeader(id, m.nodeID)

	select {
	case err := <-result:
		return id, index, err
	case <-ctx.Done():
		r.proposals.Cancel(index)
		return id, index, ctx.Err()
	}
}

// Get is a linearizable read of key, served by the range's leader via Read
// Index: confirm leadership with a quorum, wait for this replica to apply
// everything committed before the read, then read locally.
func (m *MultiRaftNode) Get(ctx context.Context, key []byte) ([]byte, error) {
	r, err := m.replicaFor(key)
	if err != nil {
		return nil, err
	}
	readIndex, err := r.node.ReadIndex(ctx)
	if errors.Is(err, raft.ErrNotLeader) {
		return nil, fmt.Errorf("%w: range %d", ErrNotRangeLeader, r.desc.RangeID)
	}
	if err != nil {
		return nil, err
	}
	if err := r.node.WaitApplied(ctx, readIndex); err != nil {
		return nil, err
	}
	return r.store.Get(key, latestTS)
}

// LocalGet reads this node's replica of key's range without any
// coordination. It works on followers but may be arbitrarily stale.
func (m *MultiRaftNode) LocalGet(key []byte) ([]byte, error) {
	r, err := m.replicaFor(key)
	if err != nil {
		return nil, err
	}
	return r.store.Get(key, latestTS)
}

// Stop stops every hosted range. Safe to call more than once.
func (m *MultiRaftNode) Stop() {
	m.stopOnce.Do(func() {
		m.mu.RLock()
		defer m.mu.RUnlock()
		for _, r := range m.ranges {
			r.node.Stop()
		}
	})
}
