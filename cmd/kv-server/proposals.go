package main

import (
	"errors"
	"sync"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
)

// ErrProposalSuperseded is returned to a writer whose log index ended up
// holding a different entry than the one it proposed - a leadership change
// overwrote it before it committed. Its fate is unknown to the client, which
// must retry rather than be told it succeeded.
var ErrProposalSuperseded = errors.New("proposal was superseded by a leadership change before it committed")

// ProposalTracker lets a client request wait until its specific proposal is
// applied to the state machine - not merely appended to the leader's log,
// where a leadership change could still discard it.
type ProposalTracker struct {
	mu      sync.Mutex
	waiters map[uint64]proposalWaiter
}

// proposalWaiter pairs a result channel with the term the entry was proposed
// in: the only way to tell "my command committed" from "another command
// ended up at this index".
type proposalWaiter struct {
	term   uint64
	result chan error
}

func NewProposalTracker() *ProposalTracker {
	return &ProposalTracker{waiters: make(map[uint64]proposalWaiter)}
}

// Propose submits cmd through node and registers a waiter for it. The
// tracker's lock is held across both steps, so the applier's Notify for this
// index can't run in between and be missed (a fast commit would otherwise
// leave the client waiting until timeout for a write that succeeded).
func (pt *ProposalTracker) Propose(node *raft.RaftNode, cmd []byte) (index uint64, result <-chan error, isLeader bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	index, term, isLeader := node.Propose(cmd)
	if !isLeader {
		return 0, nil, false
	}
	if old, ok := pt.waiters[index]; ok {
		// An earlier leader's proposal at this index was overwritten.
		old.result <- ErrProposalSuperseded
	}
	ch := make(chan error, 1)
	pt.waiters[index] = proposalWaiter{term: term, result: ch}
	return index, ch, true
}

// Cancel drops a waiter whose client gave up.
func (pt *ProposalTracker) Cancel(index uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	delete(pt.waiters, index)
}

// Notify resolves the waiter for index, if any, with the outcome of applying
// the entry that actually committed there (proposed in actualTerm).
func (pt *ProposalTracker) Notify(index, actualTerm uint64, applyErr error) {
	pt.mu.Lock()
	w, ok := pt.waiters[index]
	delete(pt.waiters, index)
	pt.mu.Unlock()
	if !ok {
		return
	}
	if w.term != actualTerm {
		applyErr = ErrProposalSuperseded
	}
	w.result <- applyErr
}
