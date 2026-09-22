package raft

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"
)

// RaftNode implements a single consensus replica executing the Raft protocol.
type RaftNode struct {
	mu  sync.Mutex
	cfg Config

	transport NetworkTransport

	// Persistent state on all nodes (1-indexed log)
	currentTerm uint64
	votedFor    uint64
	log         *RaftLog
	// Volatile State on all node
	commitIndex uint64
	lastApplied uint64
	role        NodeRole
	// Volatile State leader Only
	nextIndex  map[uint64]uint64
	matchIndex map[uint64]uint64

	// Channles & Timers
	applyCh        chan<- ApplyMsg
	lastHeartBeat  time.Time
	electionTimer  *time.Timer
	heartBeatTimer *time.Ticker
	stopCh         chan struct{}
}

// NewRaftNode creates, validates, and boots a consensus node.
func NewRaftNode(
	cfg Config,
	transport NetworkTransport,
	applyCh chan<- ApplyMsg,
) (*RaftNode, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	rn := &RaftNode{
		cfg:            cfg,
		transport:      transport,
		currentTerm:    0,
		votedFor:       0,
		log:            NewRaftLog(),
		commitIndex:    0,
		lastApplied:    0,
		nextIndex:      make(map[uint64]uint64),
		matchIndex:     make(map[uint64]uint64),
		applyCh:        applyCh,
		lastHeartBeat:  time.Now(),
		heartBeatTimer: time.NewTicker(cfg.HeartbeatInterval),
		stopCh:         make(chan struct{}),
	}
	rn.resetElectionTimerLocked()
	go rn.eventLoop()
	return rn, nil

}

// resetElectionTimerLocked sets a randomized timeout in [cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax].
// Invariant: Caller must hold rn.mu.
func (rn *RaftNode) resetElectionTimerLocked() {
	delta := int64(rn.cfg.ElectionTimeoutMax - rn.cfg.ElectionTimeoutMin)
	jitter := time.Duration(rand.Int63n(delta))
	timeout := rn.cfg.ElectionTimeoutMin + jitter

	if rn.electionTimer == nil {
		rn.electionTimer = time.NewTimer(timeout)
	} else {
		// Proper Go timer draining idiom: stop and drain channel if unread
		if !rn.electionTimer.Stop() {
			select {
			case <-rn.electionTimer.C:
			default:
			}
		}
		rn.electionTimer.Reset(timeout)
	}
}
func (rn *RaftNode) eventLoop() {
	for {
		select {
		case <-rn.stopCh:
			return

		case <-rn.electionTimer.C:
			rn.mu.Lock()
			isLeader := (rn.role == RoleLeader)
			if isLeader {
				// A Timer only fires once; a Leader must re-arm its own
				// (otherwise stale, leftover-from-candidacy) timer here so
				// it isn't left permanently dead once this node steps back
				// down to Follower and actually needs it to detect a future
				// leader's silence.
				rn.resetElectionTimerLocked()
			}
			rn.mu.Unlock()
			// Only Followers and Candidates start elections
			if !isLeader {
				rn.startElectoin()
			}

		case <-rn.heartBeatTimer.C:
			rn.mu.Lock()
			if rn.role == RoleLeader {
				rn.broadcastAppendEntriesLocked()
			}
			rn.mu.Unlock()
		}
	}
}

func (rn *RaftNode) startElectoin() {

	rn.mu.Lock()
	// Guard: never start election if already Leader
	if rn.role == RoleLeader {
		rn.mu.Unlock()
		return
	}
	rn.role = RoleCandidate
	rn.currentTerm++

	rn.votedFor = rn.cfg.NodeID
	term := rn.currentTerm
	lastIndex := rn.log.LastIndex()
	lastTerm := rn.log.LastTerm()

	rn.resetElectionTimerLocked()
	rn.mu.Unlock()

	votesReceived := 1 // Vote for self
	var voteMu sync.Mutex

	for _, peer := range rn.cfg.Peers {
		if peer == rn.cfg.NodeID {
			continue
		}
		go func(target uint64) {
			requestArgs := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rn.cfg.NodeID,
				LastLogTerm:  lastTerm,
				LastLogIndex: lastIndex,
			}
			ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout)
			defer cancel()
			reply, err := rn.transport.SendRequestVote(ctx, target, requestArgs)
			if err != nil {
				// Log the failure instead of swallowing it
				log.Printf("[Node %d] RequestVote to Node %d failed: %v", rn.cfg.NodeID, target, err)
				return
			}
			rn.mu.Lock()
			defer rn.mu.Unlock()

			if reply.Term > rn.currentTerm {
				// adopt higher term
				rn.currentTerm = reply.Term
				rn.role = RoleFollower
				rn.votedFor = 0
				rn.resetElectionTimerLocked()
				return
			}

			if rn.role == RoleCandidate && reply.VoteGranted && reply.Term == rn.currentTerm {
				voteMu.Lock()
				votesReceived++
				// Majority Quorun Check
				if votesReceived > len(rn.cfg.Peers)/2 {
					rn.role = RoleLeader
					for _, p := range rn.cfg.Peers {
						rn.nextIndex[p] = rn.log.LastIndex() + 1
						rn.matchIndex[p] = 0
					}
					rn.broadcastAppendEntriesLocked()
				}
				voteMu.Unlock()
			}
		}(peer)
	}
}

// HandleRequestVote processes incoming vote requests from candidate peers.
func (rn *RaftNode) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	// respect the highest term
	if args.Term > rn.currentTerm {
		rn.currentTerm = args.Term
		rn.role = RoleFollower
		rn.votedFor = 0
	}
	reply.Term = rn.currentTerm
	reply.VoteGranted = false
	// igone the old leader
	if args.Term < rn.currentTerm {
		return
	}
	// Election Safety: check if candidate's log is at least as up-to-date as receiver's
	lastIndex := rn.log.LastIndex()
	lastTerm := rn.log.LastTerm()
	logOk := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	if (rn.votedFor == 0 || rn.votedFor == args.CandidateID) && logOk {
		reply.VoteGranted = true
		rn.votedFor = args.CandidateID
		rn.resetElectionTimerLocked()
	}
}

func (rn *RaftNode) broadcastAppendEntriesLocked() {
	for _, peer := range rn.cfg.Peers {
		if peer == rn.cfg.NodeID {
			continue
		}

		prevIndex := rn.nextIndex[peer] - 1
		prevTerm, _ := rn.log.TermAt(prevIndex)
		entries := rn.log.Slice(rn.nextIndex[peer])

		args := &AppendEntriesArgs{
			Term:         rn.currentTerm,
			LeaderID:     rn.cfg.NodeID,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rn.commitIndex,
		}
		go func(target uint64, args *AppendEntriesArgs) {
			ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout)
			defer cancel()
			reply, err := rn.transport.SendAppendEntries(ctx, target, args)

			if err != nil {
				return
			}
			rn.mu.Lock()
			defer rn.mu.Unlock()
			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = RoleFollower
				rn.votedFor = 0
				rn.resetElectionTimerLocked()
				return
			}
			if rn.role != RoleLeader || args.Term != rn.currentTerm {
				return
			}
			if reply.Success {
				rn.nextIndex[target] = args.PrevLogIndex + uint64(len(args.Entries)) + 1
				rn.matchIndex[target] = rn.nextIndex[target] - 1
				rn.checkAdvanceCommitIndexLocked()
			} else {
				if rn.nextIndex[target] > 1 {
					rn.nextIndex[target]--
				}
			}
		}(peer, args)

	}
}

// HandleAppendEntries processes replication and heartbeats from the Leader.
func (rn *RaftNode) HandleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	reply.Success = false
	reply.Term = rn.currentTerm
	if args.Term < rn.currentTerm {
		return
	}
	if args.Term > rn.currentTerm || rn.role == RoleCandidate {
		rn.currentTerm = args.Term
		rn.role = RoleFollower
		rn.votedFor = 0
	}
	rn.resetElectionTimerLocked()
	if args.PrevLogIndex > rn.log.LastIndex() {
		return
	}
	if term, ok := rn.log.TermAt(args.PrevLogIndex); ok && term != args.PrevLogTerm {
		return
	}
	rn.log.TruncateAndAppend(args.PrevLogIndex, args.Entries)
	reply.Success = true
	if args.LeaderCommit > rn.commitIndex {
		lastNewIndex := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNewIndex {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
		rn.scheduleApplyLocked()
	}
}

// checkAdvanceCommitIndexLocked enforces Raft Figure 8:
// Leaders only commit entries from the CURRENT term directly by counting replicas.
func (rn *RaftNode) checkAdvanceCommitIndexLocked() {
	for N := rn.log.LastIndex(); N > rn.commitIndex; N-- {
		term, _ := rn.log.TermAt(N)
		if term != rn.currentTerm {
			continue
		}
		matchCount := 1
		for _, peer := range rn.cfg.Peers {
			if peer == rn.cfg.NodeID {
				continue
			}
			if rn.matchIndex[peer] >= N {
				matchCount++
			}
		}
		if matchCount > len(rn.cfg.Peers)/2 {
			rn.commitIndex = N
			rn.scheduleApplyLocked()
			break
		}

	}
}

// scheduleApplyLocked extracts committed entries under lock and pushes them
// to applyCh asynchronously to avoid blocking the consensus engine on disk/state machine operations.
func (rn *RaftNode) scheduleApplyLocked() {
	var toApply []ApplyMsg
	for rn.commitIndex > rn.lastApplied {
		rn.lastApplied++
		term, _ := rn.log.TermAt(rn.lastApplied)
		slice := rn.log.Slice(rn.lastApplied)
		if len(slice) > 0 {
			toApply = append(toApply, ApplyMsg{
				CommandValid: true,
				CommandIndex: rn.lastApplied,
				CommandTerm:  term,
				Command:      slice[0].Data,
			})
		}
	}

	if len(toApply) > 0 {
		go func(msgs []ApplyMsg) {
			for _, m := range msgs {
				rn.applyCh <- m
			}
		}(toApply)
	}
}
