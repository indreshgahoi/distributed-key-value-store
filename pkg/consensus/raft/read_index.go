package raft

import "context"

// ReadIndex implements Raft's Read Index protocol (§6.4): a linearizable read
// served from the local state machine without appending to the log.
//
// It returns an index such that, once LastApplied() >= index (see
// WaitApplied), reading local state is linearizable. Two things must hold:
//
//  1. The leader's commitIndex must be current. A brand-new leader may not
//     yet know which earlier entries are committed, so it first waits for
//     its election no-op - an entry from its own term - to commit.
//  2. It must still be the leader. One cut off in a minority partition has
//     no way to learn it was deposed, so it confirms leadership with a fresh
//     round trip to a quorum before trusting its commitIndex.
func (rn *RaftNode) ReadIndex(ctx context.Context) (uint64, error) {
	readIndex, term, err := rn.waitCurrentTermCommit(ctx)
	if err != nil {
		return 0, err
	}
	if err := rn.confirmLeadership(ctx, term); err != nil {
		return 0, err
	}
	return readIndex, nil
}

// waitCurrentTermCommit blocks until this leader has committed an entry from
// its current term, then returns (commitIndex, term).
func (rn *RaftNode) waitCurrentTermCommit(ctx context.Context) (uint64, uint64, error) {
	for {
		rn.mu.Lock()
		if rn.role != RoleLeader {
			rn.mu.Unlock()
			return 0, 0, ErrNotLeader
		}
		if t, _ := rn.log.TermAt(rn.commitIndex); t == rn.currentTerm {
			index, term := rn.commitIndex, rn.currentTerm
			rn.mu.Unlock()
			return index, term, nil
		}
		wait := rn.commitSignal.wait()
		rn.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-rn.stopCh:
			return 0, 0, ErrStopped
		}
	}
}

// confirmLeadership sends an empty AppendEntries to every peer and returns
// once a quorum (counting ourselves) has accepted us as leader for term.
func (rn *RaftNode) confirmLeadership(ctx context.Context, term uint64) error {
	acks := make(chan bool, len(rn.cfg.Peers))
	rn.forEachPeer(func(peer uint64) {
		go func() { acks <- rn.probeLeadership(ctx, peer, term) }()
	})

	acked, pending := 1, len(rn.cfg.Peers)-1
	for acked < rn.quorum() {
		if pending == 0 {
			return ErrQuorumUnreachable
		}
		select {
		case ok := <-acks:
			pending--
			if ok {
				acked++
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.role != RoleLeader || rn.currentTerm != term {
		return ErrNotLeader
	}
	return nil
}

// probeLeadership reports whether peer still accepts us as leader for term.
func (rn *RaftNode) probeLeadership(ctx context.Context, peer, term uint64) bool {
	rn.mu.Lock()
	if rn.role != RoleLeader || rn.currentTerm != term {
		rn.mu.Unlock()
		return false
	}
	prevIndex := rn.nextIndex[peer] - 1
	prevTerm, _ := rn.log.TermAt(prevIndex)
	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rn.cfg.NodeID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: rn.commitIndex,
		// No entries: this round trip only confirms leadership.
	}
	rn.mu.Unlock()

	rpcCtx, cancel := context.WithTimeout(ctx, rn.cfg.RPCTimeout)
	defer cancel()
	reply, err := rn.transport.SendAppendEntries(rpcCtx, peer, args)
	if err != nil {
		return false
	}
	if reply.Term > term {
		rn.mu.Lock()
		rn.stepDownIfStaleLocked(reply.Term) // we've been deposed - for real, not just for this read
		rn.mu.Unlock()
		return false
	}
	// Any reply at our term - Success or not - proves the peer accepts RPCs
	// from us as leader. A false Success only signals replication lag.
	return true
}
