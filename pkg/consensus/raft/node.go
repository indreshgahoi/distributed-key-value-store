package raft

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// RaftNode implements a single consensus replica executing the Raft protocol.
//
// The protocol logic is split by concern across files:
//
//	node.go        lifecycle, event loop, shared state transitions, persistence
//	election.go    Pre-Vote, real elections, RequestVote handling, becoming leader
//	replication.go Propose, AppendEntries send/handle, commit advancement
//	snapshot.go    Snapshot (compaction) and InstallSnapshot send/handle
//	apply.go       the ordered apply pipeline to the state machine
//	read_index.go  linearizable reads (Read Index)
//
// Concurrency model: one mutex (mu) guards all protocol state. Network I/O
// always happens on separate goroutines with mu released; replies re-acquire
// mu and re-validate (role, term) before acting, because the world may have
// changed during the round trip. Methods named ...Locked require mu held.
type RaftNode struct {
	mu  sync.Mutex
	cfg Config

	transport NetworkTransport
	storage   Storage

	// Persistent state (Raft Figure 2) - persisted before any RPC reply.
	currentTerm uint64
	votedFor    uint64
	log         *RaftLog
	// persistedHS is the HardState last written, so unchanged state isn't
	// rewritten (and fsync'd) on every heartbeat.
	persistedHS HardState

	// Volatile state on all nodes.
	role          NodeRole
	leaderID      uint64 // best-known leader, for client redirects; 0 if unknown
	commitIndex   uint64
	commitSignal  notifier // broadcast whenever commitIndex advances
	lastHeartBeat time.Time

	// Apply pipeline (apply.go). lastDispatched is the highest index queued
	// for the state machine; lastApplied is the highest index the state
	// machine has confirmed via ReportApplied. Only lastApplied may gate
	// reads (WaitApplied) or log compaction (Snapshot).
	lastDispatched uint64
	lastApplied    uint64
	appliedSignal  notifier
	applyQueue     []applyItem
	applyNotify    chan struct{} // cap 1: wakes applyLoop without blocking
	applyCh        chan<- ApplyMsg

	// Volatile state on leaders, reinitialized after each election.
	nextIndex        map[uint64]uint64
	matchIndex       map[uint64]uint64
	snapshotInFlight map[uint64]bool // at most one InstallSnapshot per peer at a time

	// Latest snapshot, kept in memory to send to lagging followers.
	snapshotData []byte

	// Lifecycle.
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker
	stopCh         chan struct{}
	stopOnce       sync.Once
	fatalErr       error // set when the node halts on a storage failure
	wg             sync.WaitGroup
}

// NewRaftNode recovers durable state from storage and boots a consensus node.
//
// Recovery order matters: the latest snapshot is delivered to applyCh first,
// followed by every committed entry after it, so a state machine that starts
// empty is rebuilt through exactly the same ordered path used at runtime.
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
		cfg:              cfg,
		transport:        transport,
		storage:          storage,
		currentTerm:      hs.Term,
		votedFor:         hs.Vote,
		persistedHS:      hs,
		log:              NewRaftLog(),
		commitIndex:      max(hs.Commit, snapMeta.LastIncludedIndex),
		commitSignal:     newNotifier(),
		lastHeartBeat:    time.Now(),
		lastDispatched:   snapMeta.LastIncludedIndex,
		appliedSignal:    newNotifier(),
		applyNotify:      make(chan struct{}, 1),
		applyCh:          applyCh,
		nextIndex:        make(map[uint64]uint64),
		matchIndex:       make(map[uint64]uint64),
		snapshotInFlight: make(map[uint64]bool),
		heartbeatTimer:   time.NewTicker(cfg.HeartbeatInterval),
		stopCh:           make(chan struct{}),
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if snapMeta.LastIncludedIndex > 0 {
		rn.log.CompactLog(snapMeta.LastIncludedIndex, snapMeta.LastIncludedTerm)
		data, err := storage.SnapshotData()
		if err != nil {
			return nil, fmt.Errorf("raft: failed to load snapshot data: %w", err)
		}
		rn.snapshotData = data
		rn.enqueueApplyLocked(applyItem{msg: ApplyMsg{
			CommandValid: false,
			CommandIndex: snapMeta.LastIncludedIndex,
			CommandTerm:  snapMeta.LastIncludedTerm,
			Command:      data,
		}})
	}

	// Replay every entry durably persisted after the snapshot boundary.
	storageLast, err := storage.LastIndex()
	if err != nil {
		return nil, fmt.Errorf("raft: failed to read last index from storage: %w", err)
	}
	if storageLast > rn.log.LastIndex() {
		entries, err := storage.Entries(rn.log.LastIndex()+1, storageLast+1, ^uint64(0))
		if err != nil {
			return nil, fmt.Errorf("raft: failed to replay log entries from storage: %w", err)
		}
		rn.log.TruncateAndAppend(rn.log.LastIndex(), entries)
	}

	// The persisted commit index is only a lower-bound hint (it is refreshed
	// opportunistically, never on its own), so it can't exceed the log.
	rn.commitIndex = min(rn.commitIndex, rn.log.LastIndex())
	rn.scheduleApplyLocked()

	rn.resetElectionTimerLocked()
	rn.wg.Add(2)
	go rn.applyLoop()
	go rn.eventLoop()
	return rn, nil
}

// eventLoop drives the two timers: election timeouts (followers and
// candidates) and heartbeats (leaders).
func (rn *RaftNode) eventLoop() {
	defer rn.wg.Done()
	for {
		select {
		case <-rn.stopCh:
			return

		case <-rn.electionTimer.C:
			rn.startElection()

		case <-rn.heartbeatTimer.C:
			rn.mu.Lock()
			if rn.role == RoleLeader {
				rn.broadcastAppendEntriesLocked()
			}
			rn.mu.Unlock()
		}
	}
}

// resetElectionTimerLocked arms the election timer with a randomized timeout
// in [ElectionTimeoutMin, ElectionTimeoutMax), which makes split votes rare.
func (rn *RaftNode) resetElectionTimerLocked() {
	delta := int64(rn.cfg.ElectionTimeoutMax - rn.cfg.ElectionTimeoutMin)
	timeout := rn.cfg.ElectionTimeoutMin + time.Duration(rand.Int63n(delta))

	if rn.electionTimer == nil {
		rn.electionTimer = time.NewTimer(timeout)
		return
	}
	if !rn.electionTimer.Stop() {
		select {
		case <-rn.electionTimer.C: // drain a fire that was never consumed
		default:
		}
	}
	rn.electionTimer.Reset(timeout)
}

// becomeFollowerLocked adopts term (if newer) and follows leaderID (0 if
// unknown). A newer term clears votedFor. The change is persisted before the
// caller replies to anyone; false means persistence failed and the node halted.
func (rn *RaftNode) becomeFollowerLocked(term, leaderID uint64) bool {
	if term > rn.currentTerm {
		rn.currentTerm = term
		rn.votedFor = 0
	}
	rn.role = RoleFollower
	rn.leaderID = leaderID
	rn.resetElectionTimerLocked()
	return rn.persistLocked(nil)
}

// stepDownIfStaleLocked handles the universal Raft rule: any message carrying
// a higher term means we are out of date and must revert to follower.
// It reports whether the caller should abandon what it was doing.
func (rn *RaftNode) stepDownIfStaleLocked(term uint64) bool {
	if term <= rn.currentTerm {
		return false
	}
	rn.becomeFollowerLocked(term, 0)
	return true
}

// persistLocked durably writes entries plus the HardState, if it changed.
//
// Term and vote must be durable before the node acts on them (Raft §5.2).
// Commit is only an optimization hint for recovery, so it is refreshed
// whenever the HardState is written anyway but never triggers a write alone.
//
// A storage failure is fatal: a node that can't persist can't safely vote
// or acknowledge entries, so it halts (fail-stop) instead of carrying on
// with state its disk doesn't reflect. See Err and Done.
func (rn *RaftNode) persistLocked(entries []LogEntry) bool {
	// A stopped node must never write again: a late RPC reply arriving after
	// Stop would otherwise persist into storage the process has handed off.
	if rn.fatalErr != nil || rn.stoppedLocked() {
		return false
	}
	hs := rn.persistedHS
	if hs.Term != rn.currentTerm || hs.Vote != rn.votedFor {
		hs = HardState{Term: rn.currentTerm, Vote: rn.votedFor, Commit: rn.commitIndex}
	}
	if len(entries) == 0 && hs == rn.persistedHS {
		return true
	}
	if err := rn.storage.Save(hs, entries); err != nil {
		rn.haltLocked(fmt.Errorf("raft: persisting state failed: %w", err))
		return false
	}
	rn.persistedHS = hs
	return true
}

// haltLocked stops the node permanently after an unrecoverable error.
func (rn *RaftNode) haltLocked(err error) {
	if rn.fatalErr != nil {
		return
	}
	rn.fatalErr = err
	rn.role = RoleFollower
	log.Printf("[raft %d] halting: %v", rn.cfg.NodeID, err)
	rn.stopOnce.Do(func() { close(rn.stopCh) })
}

// stoppedLocked reports whether Stop was called or the node halted.
func (rn *RaftNode) stoppedLocked() bool {
	select {
	case <-rn.stopCh:
		return true
	default:
		return false
	}
}

// Stop terminates the node's background goroutines and waits for them to
// exit. Pending apply messages are dropped. Safe to call more than once.
func (rn *RaftNode) Stop() {
	rn.stopOnce.Do(func() { close(rn.stopCh) })
	rn.wg.Wait()
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.heartbeatTimer.Stop()
	rn.electionTimer.Stop()
}

// Done is closed when the node stops, including when it halts on a storage
// failure - in which case Err returns the cause.
func (rn *RaftNode) Done() <-chan struct{} { return rn.stopCh }

// Err returns the error that halted the node, or nil.
func (rn *RaftNode) Err() error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.fatalErr
}

// GetState returns current term and whether this node believes it is leader.
func (rn *RaftNode) GetState() (uint64, bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.currentTerm, rn.role == RoleLeader
}

// Leader returns the best-known current leader's ID, or 0 if unknown.
func (rn *RaftNode) Leader() uint64 {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.leaderID
}

// Status is a point-in-time view of a node, for observability.
type Status struct {
	ID          uint64
	Term        uint64
	Role        NodeRole
	Leader      uint64
	CommitIndex uint64
	LastApplied uint64
	LastIndex   uint64
	Snapshot    uint64
}

// Status returns a consistent snapshot of the node's protocol state.
func (rn *RaftNode) Status() Status {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return Status{
		ID:          rn.cfg.NodeID,
		Term:        rn.currentTerm,
		Role:        rn.role,
		Leader:      rn.leaderID,
		CommitIndex: rn.commitIndex,
		LastApplied: rn.lastApplied,
		LastIndex:   rn.log.LastIndex(),
		Snapshot:    rn.log.SnapshotIndex(),
	}
}

// quorum is the number of votes (or replicas) that form a majority.
func (rn *RaftNode) quorum() int { return len(rn.cfg.Peers)/2 + 1 }

// forEachPeer calls fn for every peer except this node.
func (rn *RaftNode) forEachPeer(fn func(peer uint64)) {
	for _, p := range rn.cfg.Peers {
		if p != rn.cfg.NodeID {
			fn(p)
		}
	}
}
