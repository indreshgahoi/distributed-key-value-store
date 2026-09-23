package raft

import (
	"context"
	"time"
)

// startElection runs when the election timer fires on a non-leader.
//
// Elections are two-phase (Pre-Vote, Raft §9.6): first a trial round that
// changes no durable state, asking "would you vote for me?". Only if a
// quorum says yes does the node increment its term and run a real election.
// A node cut off from the cluster therefore can't inflate its term and
// depose a healthy leader when it reconnects.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.role == RoleLeader || rn.stoppedLocked() {
		return
	}
	rn.role = RolePreCandidate
	rn.leaderID = 0
	rn.resetElectionTimerLocked() // if this round stalls, the next timeout retries
	rn.requestVotesLocked(true)
}

// startRealElectionLocked is phase two, entered once a Pre-Vote quorum is won.
func (rn *RaftNode) startRealElectionLocked() {
	rn.role = RoleCandidate
	rn.currentTerm++
	rn.votedFor = rn.cfg.NodeID
	// Our own vote must be durable before we ask for anyone else's.
	if !rn.persistLocked(nil) {
		return
	}
	rn.resetElectionTimerLocked()
	rn.requestVotesLocked(false)
}

// requestVotesLocked broadcasts one round of (pre-)vote requests and tallies
// the replies, advancing to the next phase once a quorum grants.
func (rn *RaftNode) requestVotesLocked(preVote bool) {
	roundTerm, roundRole := rn.currentTerm, rn.role
	args := RequestVoteArgs{
		Term:         rn.currentTerm,
		CandidateID:  rn.cfg.NodeID,
		LastLogIndex: rn.log.LastIndex(),
		LastLogTerm:  rn.log.LastTerm(),
		IsPreVote:    preVote,
	}
	if preVote {
		args.Term++ // the term we would campaign in
	}

	votes := 1 // our own; guarded by rn.mu like everything else
	rn.forEachPeer(func(peer uint64) {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout)
			defer cancel()
			reqArgs := args // each RPC gets its own copy
			reply, err := rn.transport.SendRequestVote(ctx, peer, &reqArgs)
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()
			if rn.stepDownIfStaleLocked(reply.Term) {
				return
			}
			// Ignore replies to a round that is no longer the current one.
			if rn.role != roundRole || rn.currentTerm != roundTerm || !reply.VoteGranted {
				return
			}
			votes++
			if votes != rn.quorum() { // == so each phase advances exactly once
				return
			}
			if preVote {
				rn.startRealElectionLocked()
			} else {
				rn.becomeLeaderLocked()
			}
		}()
	})
}

// becomeLeaderLocked takes office after winning a real election.
func (rn *RaftNode) becomeLeaderLocked() {
	rn.role = RoleLeader
	rn.leaderID = rn.cfg.NodeID
	rn.electionTimer.Stop() // leaders don't time out; becomeFollowerLocked re-arms it

	for _, p := range rn.cfg.Peers {
		rn.nextIndex[p] = rn.log.LastIndex() + 1
		rn.matchIndex[p] = 0
		delete(rn.snapshotInFlight, p)
	}

	// Raft §8: a new leader doesn't know which earlier-term entries are
	// committed (it may only commit by counting replicas for current-term
	// entries - Figure 8). Committing a no-op from its own term settles
	// that immediately, which ReadIndex depends on, and unblocks any
	// earlier-term entries still waiting to commit.
	noop := rn.log.AppendNoOp(rn.currentTerm)
	rn.matchIndex[rn.cfg.NodeID] = noop.Index
	if !rn.persistLocked([]LogEntry{noop}) {
		return
	}
	rn.broadcastAppendEntriesLocked()
}

// HandleRequestVote processes incoming (pre-)vote requests from candidates.
func (rn *RaftNode) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	reply.Term = rn.currentTerm
	reply.VoteGranted = false
	if rn.stoppedLocked() {
		return
	}

	if args.IsPreVote {
		reply.VoteGranted = rn.grantPreVoteLocked(args)
		return
	}

	if args.Term > rn.currentTerm && !rn.becomeFollowerLocked(args.Term, 0) {
		return
	}
	reply.Term = rn.currentTerm
	if args.Term < rn.currentTerm {
		return
	}
	alreadyVoted := rn.votedFor != 0 && rn.votedFor != args.CandidateID
	if alreadyVoted || !rn.candidateLogUpToDateLocked(args) {
		return
	}
	rn.votedFor = args.CandidateID
	// The vote must be durable before it's granted: forgetting it after a
	// crash would let this node vote twice in one term (two leaders).
	if !rn.persistLocked(nil) {
		return
	}
	rn.resetElectionTimerLocked()
	reply.VoteGranted = true
}

// grantPreVoteLocked decides a Pre-Vote. It never changes local state.
func (rn *RaftNode) grantPreVoteLocked(args *RequestVoteArgs) bool {
	// Leader stickiness: while we hear from a live leader (or are one), the
	// cluster is healthy and a would-be candidate is the one that's cut off.
	if rn.role == RoleLeader || time.Since(rn.lastHeartBeat) < rn.cfg.ElectionTimeoutMin {
		return false
	}
	return args.Term > rn.currentTerm && rn.candidateLogUpToDateLocked(args)
}

// candidateLogUpToDateLocked is the Election Restriction (Raft §5.4.1): only
// vote for a candidate whose log is at least as up to date as ours, so every
// elected leader already holds every committed entry.
func (rn *RaftNode) candidateLogUpToDateLocked(args *RequestVoteArgs) bool {
	lastTerm, lastIndex := rn.log.LastTerm(), rn.log.LastIndex()
	return args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)
}
