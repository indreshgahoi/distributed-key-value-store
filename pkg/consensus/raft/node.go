package raft

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// RaftNode implements a single consensus replica executing the Raft protocol.
type RaftNode struct {
	mu  sync.Mutex
	cfg Config

	transport NetworkTransport
	storage   Storage

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

	// SnapShot state
	lastSnapshotIndex uint64
	lastSnapshotTerm  uint64
	snapshotData      []byte

	// Apply pipeline: applyQueue (guarded by mu) is drained, in order, by the
	// single applyLoop goroutine - the only writer to applyCh. applyNotify
	// (cap 1) wakes it without ever blocking the consensus path.
	applyQueue  []ApplyMsg
	applyNotify chan struct{}

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
	storage Storage,
	applyCh chan<- ApplyMsg,
) (*RaftNode, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	hs, snapMeta, err := storage.InitialState()
	if err != nil {
		return nil, err
	}
	rn := &RaftNode{
		cfg:               cfg,
		transport:         transport,
		storage:           storage,
		currentTerm:       hs.Term,
		votedFor:          hs.Vote,
		log:               NewRaftLog(),
		commitIndex:       hs.Commit,
		lastApplied:       snapMeta.LastIncludedIndex,
		lastSnapshotIndex: snapMeta.LastIncludedIndex,
		lastSnapshotTerm:  snapMeta.LastIncludedTerm,
		nextIndex:         make(map[uint64]uint64),
		matchIndex:        make(map[uint64]uint64),
		applyCh:           applyCh,
		applyNotify:       make(chan struct{}, 1),
		lastHeartBeat:     time.Now(),
		heartBeatTimer:    time.NewTicker(cfg.HeartbeatInterval),
		stopCh:            make(chan struct{}),
	}

	// Initialize log compaction horizon if prior snapshot exists
	if snapMeta.LastIncludedIndex > 0 {
		rn.log.CompactLog(snapMeta.LastIncludedIndex, snapMeta.LastIncludedTerm)
	}

	rn.mu.Lock()

	// Replay entries durably persisted beyond the snapshot boundary back into
	// the in-memory log. Without this, rn.log starts empty past the
	// compaction horizon on every restart regardless of what's actually on
	// disk - silently discarding every entry Save() ever wrote since the
	// last snapshot, which defeats the point of a durable Storage backend.
	storageLast, err := storage.LastIndex()
	if err != nil {
		rn.mu.Unlock()
		return nil, fmt.Errorf("failed to read last index from storage: %w", err)
	}
	if storageLast > rn.log.LastIndex() {
		entries, err := storage.Entries(rn.log.LastIndex()+1, storageLast+1, ^uint64(0))
		if err != nil {
			rn.mu.Unlock()
			return nil, fmt.Errorf("failed to replay log entries from storage: %w", err)
		}
		rn.log.TruncateAndAppend(rn.log.LastIndex(), entries)
	}

	// Re-apply anything already committed (per the restored HardState) but
	// not yet reflected in the state machine. The state machine (Layer 2
	// MVCC) has no persistence of its own beyond an independent, infrequent
	// snapshot ticker in cmd/kv-server/main.go - replaying into rn.log alone
	// fixes Raft's own bookkeeping but silently leaves the actual data
	// missing after a restart unless this also re-drives applyCh.
	if rn.commitIndex > rn.lastApplied {
		rn.scheduleApplyLocked()
	}

	rn.resetElectionTimerLocked()
	rn.mu.Unlock()
	go rn.applyLoop()
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
				rn.startElection()
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

// startElection initiates the two-phase election sequence.
// It executes Phase 1 (Pre-Vote) first to confirm cluster connectivity without
// perturbing the current term or deposing a healthy leader.
func (rn *RaftNode) startElection() {
	rn.mu.Lock()
	// Invariant: Leaders never run elections
	if rn.role == RoleLeader {
		rn.mu.Unlock()
		return
	}

	// ------------------------------------------------------------------
	// PHASE 1: PRE-VOTE (TRIAL ELECTION)
	// ------------------------------------------------------------------
	rn.role = RolePreCandidate
	preVoteTerm := rn.currentTerm + 1
	lastIndex := rn.log.LastIndex()
	lastTerm := rn.log.LastTerm()

	// Re-arm timer so that if Pre-Vote stalls, we retry
	rn.resetElectionTimerLocked()
	rn.mu.Unlock()

	preVotesReceived := 1 // Vote for self tentatively
	var preVoteMu sync.Mutex

	for _, peer := range rn.cfg.Peers {
		if peer == rn.cfg.NodeID {
			continue
		}

		go func(target uint64) {
			args := &RequestVoteArgs{
				Term:         preVoteTerm,
				CandidateID:  rn.cfg.NodeID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
				IsPreVote:    true,
			}

			ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout)
			defer cancel()

			reply, err := rn.transport.SendRequestVote(ctx, target, args)
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// If a higher term is discovered during Pre-Vote, step down immediately
			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = RoleFollower
				rn.votedFor = 0
				_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: 0, Commit: rn.commitIndex}, nil)
				rn.resetElectionTimerLocked()
				return
			}

			// Validate election context hasn't changed
			if rn.role != RolePreCandidate || preVoteTerm != rn.currentTerm+1 {
				return
			}

			if reply.VoteGranted {
				preVoteMu.Lock()
				preVotesReceived++
				wonPreVote := preVotesReceived > len(rn.cfg.Peers)/2
				preVoteMu.Unlock()

				if wonPreVote {
					// Pre-Vote passed! Cluster connectivity & up-to-date log confirmed.
					// Proceed to Phase 2: Real Election.
					rn.startRealElectionLocked()
				}
			}
		}(peer)
	}
}

// startRealElectionLocked executes Phase 2 of the election.
// Invariant: Caller MUST hold rn.mu.
func (rn *RaftNode) startRealElectionLocked() {
	// Transition from PreCandidate to Candidate
	rn.role = RoleCandidate
	rn.currentTerm++
	rn.votedFor = rn.cfg.NodeID

	term := rn.currentTerm
	lastIndex := rn.log.LastIndex()
	lastTerm := rn.log.LastTerm()

	// Persist updated HardState to disk before broadcasting real votes (Raft §5.2)
	_ = rn.storage.Save(HardState{
		Term:   rn.currentTerm,
		Vote:   rn.votedFor,
		Commit: rn.commitIndex,
	}, nil)

	rn.resetElectionTimerLocked()

	realVotesReceived := 1 // Vote for self
	var realVoteMu sync.Mutex

	for _, peer := range rn.cfg.Peers {
		if peer == rn.cfg.NodeID {
			continue
		}

		go func(target uint64) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rn.cfg.NodeID,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
				IsPreVote:    false, // Official binding vote
			}

			ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout)
			defer cancel()

			reply, err := rn.transport.SendRequestVote(ctx, target, args)
			if err != nil {
				return
			}

			rn.mu.Lock()
			defer rn.mu.Unlock()

			// Higher term discovered: revert to Follower
			if reply.Term > rn.currentTerm {
				rn.currentTerm = reply.Term
				rn.role = RoleFollower
				rn.votedFor = 0
				_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: 0, Commit: rn.commitIndex}, nil)
				rn.resetElectionTimerLocked()
				return
			}

			if rn.role != RoleCandidate || term != rn.currentTerm {
				return
			}

			if reply.VoteGranted {
				realVoteMu.Lock()
				realVotesReceived++
				wonRealElection := realVotesReceived > len(rn.cfg.Peers)/2
				realVoteMu.Unlock()

				if wonRealElection && rn.role == RoleCandidate {
					rn.role = RoleLeader

					// Invariant: Leaders stop their election timer!
					if rn.electionTimer != nil {
						rn.electionTimer.Stop()
					}

					// Initialize volatile leader state
					for _, p := range rn.cfg.Peers {
						rn.nextIndex[p] = rn.log.LastIndex() + 1
						rn.matchIndex[p] = 0
					}

					// Immediately broadcast heartbeats to assert authority
					rn.broadcastAppendEntriesLocked()
				}
			}
		}(peer)
	}
}

// HandleRequestVote processes incoming vote requests from candidate peers.
func (rn *RaftNode) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// 1. LEADER STICKINESS / LEASE PROTECTION (Raft §9.6)
	// If this node recently heard from a legitimate leader within ElectionTimeoutMin,
	// reject Pre-Votes immediately. This prevents an isolated, partitioned follower
	// from disrupting a healthy cluster when it reconnects.
	//
	// A node that IS the Leader never receives AppendEntries (it only sends
	// them), so its own lastHeartBeat never advances past construction time.
	// It doesn't need a timer to know it's active - role == RoleLeader alone
	// is proof, and must reject Pre-Votes unconditionally while true.
	leaderIsActive := rn.role == RoleLeader || time.Since(rn.lastHeartBeat) < rn.cfg.ElectionTimeoutMin
	if args.IsPreVote && leaderIsActive {
		reply.Term = rn.currentTerm
		reply.VoteGranted = false
		return
	}

	// 2. Discovering Higher Terms
	if args.Term > rn.currentTerm {
		// Invariant: Pre-Votes NEVER cause a receiver to adopt a higher term or step down!
		// Only real binding votes (IsPreVote == false) update the term.
		if !args.IsPreVote {
			rn.currentTerm = args.Term
			rn.role = RoleFollower
			rn.votedFor = 0
			_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: 0, Commit: rn.commitIndex}, nil)
			rn.resetElectionTimerLocked()
		}
	}

	reply.Term = rn.currentTerm
	reply.VoteGranted = false

	if args.Term < rn.currentTerm {
		return
	}

	// 3. ELECTION SAFETY: Check if candidate's log is at least as up-to-date as ours
	lastIndex := rn.log.LastIndex()
	lastTerm := rn.log.LastTerm()
	logOk := args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)

	// Can we grant the vote?
	// For Pre-Vote: Do not consume votedFor.
	// For Real Vote: votedFor must be 0 or candidateID.
	canVote := (rn.votedFor == 0 || rn.votedFor == args.CandidateID) || args.IsPreVote

	if canVote && logOk {
		reply.VoteGranted = true
		if !args.IsPreVote {
			rn.votedFor = args.CandidateID
			_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: rn.votedFor, Commit: rn.commitIndex}, nil)
			rn.resetElectionTimerLocked()
		}
	}
}

func (rn *RaftNode) broadcastAppendEntriesLocked() {
	for _, peer := range rn.cfg.Peers {
		if peer == rn.cfg.NodeID {
			continue
		}

		prevIndex := rn.nextIndex[peer] - 1
		firstAvailable := rn.log.FirstIndex()
		// FOLLOWER COMPACTION CHECK:
		// If peer needs an entry that has already been discarded, send InstallSnapshot!
		if prevIndex < firstAvailable-1 {
			go rn.sendSnapshotAsync(peer)
			continue
		}
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
				_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: 0, Commit: rn.commitIndex}, nil)
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

// sendSnapshotAsync sends InstallSnapshot to a lagging follower. Despite
// being launched via `go` from broadcastAppendEntriesLocked while the
// caller still holds rn.mu, this is safe and not reentrant: it acquires
// rn.mu itself (in its own goroutine, blocking harmlessly until the caller
// releases it) only to snapshot the fields it needs, then releases it
// before the network round-trip. Named _Async rather than _Locked (unlike
// every other ...Locked method here, which requires the CALLER to already
// hold rn.mu) since it manages its own locking and must never be called
// while already holding the lock on the same goroutine.
func (rn *RaftNode) sendSnapshotAsync(target uint64) {
	rn.mu.Lock()
	args := &InstallSnapshotArgs{
		Term:              rn.currentTerm,
		LeaderID:          rn.cfg.NodeID,
		LastIncludedIndex: rn.lastSnapshotIndex,
		LastIncludedTerm:  rn.lastSnapshotTerm,
		Data:              rn.snapshotData,
	}
	rn.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rn.cfg.RPCTimeout*5)
	defer cancel()

	reply, err := rn.transport.SendInstallSnapshot(ctx, target, args)
	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if reply.Term > rn.currentTerm {
		rn.currentTerm = reply.Term
		rn.role = RoleFollower
		rn.votedFor = 0
		_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: 0, Commit: rn.commitIndex}, nil)
		rn.resetElectionTimerLocked()
		return
	}

	if rn.role == RoleLeader && args.Term == rn.currentTerm {
		// Advance follower past the snapshot horizon
		rn.nextIndex[target] = args.LastIncludedIndex + 1
		rn.matchIndex[target] = args.LastIncludedIndex
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
	// Mark that we're actively hearing from a legitimate leader at args.Term,
	// so HandleRequestVote's Pre-Vote stickiness check (Raft §9.6) reflects
	// reality instead of only the moment this node booted.
	rn.lastHeartBeat = time.Now()
	rn.resetElectionTimerLocked()
	if args.PrevLogIndex > rn.log.LastIndex() {
		return
	}
	if term, ok := rn.log.TermAt(args.PrevLogIndex); ok && term != args.PrevLogTerm {
		return
	}
	// Persist only what TruncateAndAppend actually wrote, never the raw
	// args.Entries: Save replaces the durable log from entries[0].Index
	// onward, so a stale, reordered RPC whose entries we already hold would
	// otherwise truncate acknowledged entries on disk.
	written := rn.log.TruncateAndAppend(args.PrevLogIndex, args.Entries)
	_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: rn.votedFor, Commit: rn.commitIndex}, written)
	reply.Success = true
	if args.LeaderCommit > rn.commitIndex {
		lastNewIndex := args.PrevLogIndex + uint64(len(args.Entries))
		if args.LeaderCommit < lastNewIndex {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
		_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: rn.votedFor, Commit: rn.commitIndex}, nil)
		rn.scheduleApplyLocked()
	}
}

func (rn *RaftNode) HandleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

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

	// If our snapshot is already ahead, no-op
	if args.LastIncludedIndex <= rn.log.entries[0].Index {
		return
	}

	rn.log.CompactLog(args.LastIncludedIndex, args.LastIncludedTerm)

	if args.LastIncludedIndex > rn.commitIndex {
		rn.commitIndex = args.LastIncludedIndex
	}
	if args.LastIncludedIndex > rn.lastApplied {
		rn.lastApplied = args.LastIncludedIndex
	}

	// Deliver snapshot to the state machine through the same ordered queue as
	// commands, so it can never overtake entries committed before it.
	rn.enqueueApplyLocked(ApplyMsg{
		CommandValid: false, // Snapshot marker
		CommandIndex: args.LastIncludedIndex,
		CommandTerm:  args.LastIncludedTerm,
		Command:      args.Data,
	})
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
			_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: rn.votedFor, Commit: rn.commitIndex}, nil)
			rn.scheduleApplyLocked()
			break
		}

	}
}

// scheduleApplyLocked extracts committed entries under lock and queues them
// for applyLoop, so the consensus engine never blocks on a slow state machine.
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

	rn.enqueueApplyLocked(toApply...)
}

// enqueueApplyLocked appends msgs to the apply queue in log order and wakes
// applyLoop. It used to spawn a goroutine per batch, which let the Go
// scheduler reorder batches (and InstallSnapshot deliveries) on applyCh.
// Invariant: Caller must hold rn.mu.
func (rn *RaftNode) enqueueApplyLocked(msgs ...ApplyMsg) {
	if len(msgs) == 0 {
		return
	}
	rn.applyQueue = append(rn.applyQueue, msgs...)
	select {
	case rn.applyNotify <- struct{}{}:
	default: // a wakeup is already pending; applyLoop will see these msgs too
	}
}

// applyLoop is the sole sender on applyCh, delivering queued messages
// strictly in the order they were enqueued.
func (rn *RaftNode) applyLoop() {
	for {
		select {
		case <-rn.stopCh:
			return
		case <-rn.applyNotify:
		}

		rn.mu.Lock()
		msgs := rn.applyQueue
		rn.applyQueue = nil
		rn.mu.Unlock()

		for _, m := range msgs {
			select {
			case rn.applyCh <- m:
			case <-rn.stopCh:
				return
			}
		}
	}
}

// Snapshot is called by the State Machine / Compactor when Layer 2 has taken
// a snapshot up to appliedIndex. It compacts the log and persists the snapshot.
func (rn *RaftNode) Snapshot(appliedIndex uint64, stateMachineData []byte) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if appliedIndex <= rn.lastSnapshotIndex {
		return ErrSnapshotOutOfDate
	}

	term, _ := rn.log.TermAt(appliedIndex)
	rn.lastSnapshotIndex = appliedIndex
	rn.lastSnapshotTerm = term
	rn.snapshotData = stateMachineData

	// 1. Compact in-memory log
	rn.log.CompactLog(appliedIndex, term)

	// 2. Persist to pluggable Storage engine
	meta := SnapshotMeta{
		LastIncludedIndex: appliedIndex,
		LastIncludedTerm:  term,
	}
	return rn.storage.CreateSnapshot(meta, stateMachineData)
}

// Propose submits a client mutation command to the leader's replicated log.
// Returns (index, term, isLeader).
func (rn *RaftNode) Propose(command []byte) (uint64, uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != RoleLeader {
		return 0, 0, false
	}

	entry := rn.log.Append(rn.currentTerm, command)
	rn.matchIndex[rn.cfg.NodeID] = entry.Index
	rn.nextIndex[rn.cfg.NodeID] = entry.Index + 1
	_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: rn.votedFor, Commit: rn.commitIndex}, []LogEntry{entry})
	rn.broadcastAppendEntriesLocked()
	return entry.Index, entry.Term, true
}

// ReadIndex implements Raft's Read Index protocol (§6.4) so a client can
// serve a linearizable read straight from the local state machine without
// paying the cost of a full log append. A leader that's been partitioned
// into a minority island has no way to learn it was deposed on its own -
// nothing tells it to step down while it's cut off - so serving a read
// straight from local state the instant a request arrives could return
// data that's already stale by the time it reaches the client. Before
// trusting its own state, the leader must first get a quorum of peers to
// freshly confirm, via a real round-trip, that they still recognize it as
// leader in the current term.
//
// On success, ReadIndex returns the commitIndex at the moment the quorum
// check began. Once the caller observes LastApplied() >= that index, any
// subsequent local read against the state machine is linearizable.
func (rn *RaftNode) ReadIndex(ctx context.Context) (uint64, error) {
	rn.mu.Lock()
	if rn.role != RoleLeader {
		rn.mu.Unlock()
		return 0, ErrNotLeader
	}
	readIndex := rn.commitIndex
	term := rn.currentTerm
	peers := rn.cfg.Peers
	rn.mu.Unlock()

	quorum := len(peers)/2 + 1
	if quorum <= 1 {
		// Single-node "cluster": no peer can ever contradict us.
		return readIndex, nil
	}

	acks := make(chan bool, len(peers)-1)
	for _, peer := range peers {
		if peer == rn.cfg.NodeID {
			continue
		}
		go func(target uint64) {
			rn.mu.Lock()
			if rn.role != RoleLeader || rn.currentTerm != term {
				rn.mu.Unlock()
				acks <- false
				return
			}
			prevIndex := rn.nextIndex[target] - 1
			prevTerm, _ := rn.log.TermAt(prevIndex)
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     rn.cfg.NodeID,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  prevTerm,
				Entries:      nil, // probe-only: this round trip exists to confirm leadership, not to replicate
				LeaderCommit: rn.commitIndex,
			}
			rn.mu.Unlock()

			rpcCtx, cancel := context.WithTimeout(ctx, rn.cfg.RPCTimeout)
			reply, err := rn.transport.SendAppendEntries(rpcCtx, target, args)
			cancel()
			if err != nil {
				acks <- false
				return
			}
			if reply.Term > term {
				// A higher term means we've been deposed - step down for
				// real, not just for the purposes of this read.
				rn.mu.Lock()
				if reply.Term > rn.currentTerm {
					rn.currentTerm = reply.Term
					rn.role = RoleFollower
					rn.votedFor = 0
					_ = rn.storage.Save(HardState{Term: rn.currentTerm, Vote: 0, Commit: rn.commitIndex}, nil)
					rn.resetElectionTimerLocked()
				}
				rn.mu.Unlock()
				acks <- false
				return
			}
			// Any reply at our term or lower - Success or not - proves this
			// peer still accepts RPCs from us as leader. A false Success
			// only signals a log-matching gap (replication lag), which is
			// orthogonal to whether our leadership is still recognized.
			acks <- true
		}(peer)
	}

	acked := 1 // self
	for i := 0; i < len(peers)-1; i++ {
		select {
		case ok := <-acks:
			if ok {
				acked++
				if acked >= quorum {
					rn.mu.Lock()
					stillLeader := rn.role == RoleLeader && rn.currentTerm == term
					rn.mu.Unlock()
					if !stillLeader {
						return 0, ErrNotLeader
					}
					return readIndex, nil
				}
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return 0, ErrQuorumUnreachable
}

// WaitApplied blocks until LastApplied() reaches at least index, or ctx is
// done. Callers use this after ReadIndex to know when it's safe to read the
// local state machine. Implemented as a short poll rather than a broadcast
// channel - simple and correct, at the cost of up to pollInterval of added
// latency; worth revisiting if that ever shows up in a latency budget.
func (rn *RaftNode) WaitApplied(ctx context.Context, index uint64) error {
	const pollInterval = 1 * time.Millisecond
	if rn.LastApplied() >= index {
		return nil
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if rn.LastApplied() >= index {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (rn *RaftNode) LastApplied() uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.lastApplied
}

// GetState returns current term and leadership boolean.
func (rn *RaftNode) GetState() (uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm, rn.role == RoleLeader
}

// Stop cleanly terminates timers and background routines.
func (rn *RaftNode) Stop() {
	close(rn.stopCh)
}
