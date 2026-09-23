package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

// clientRequestTimeout bounds how long a /put waits for its proposal to
// commit+apply, and how long a /get waits to confirm leadership + catch up
// to a linearizable read index, before failing the HTTP request rather than
// hanging forever on a stalled quorum.
const clientRequestTimeout = 2 * time.Second

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type CommandPayload struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ErrProposalSuperseded is returned to a /put waiter when the log index it
// registered against ended up holding a different entry than the one it
// proposed - i.e. a leadership change overwrote it before it committed. The
// original command's fate is unknown (it may never have been seen by a
// majority); the client must retry rather than be told it succeeded.
var ErrProposalSuperseded = errors.New("proposal was superseded by a leadership change before it committed")

// proposalWaiter pairs a notification channel with the term the proposer
// expected to see reflected back - the only way to tell "my command
// committed" apart from "a different command ended up at this index" once a
// leadership change is possible.
type proposalWaiter struct {
	term uint64
	ch   chan error
}

// ProposalTracker lets the /put HTTP handler block until its specific
// proposal is actually applied to the state machine (not just accepted
// locally), instead of returning success the instant Propose() appends to
// the local log - before quorum replication or application has happened.
type ProposalTracker struct {
	mu      sync.Mutex
	waiters map[uint64]proposalWaiter
}

func NewProposalTracker() *ProposalTracker {
	return &ProposalTracker{waiters: make(map[uint64]proposalWaiter)}
}

// Register records interest in index committing at the given term and
// returns a channel that receives exactly one value once Notify(index, ...)
// fires for it.
func (pt *ProposalTracker) Register(index, term uint64) chan error {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	ch := make(chan error, 1)
	pt.waiters[index] = proposalWaiter{term: term, ch: ch}
	return ch
}

// Cancel removes a waiter without notifying it - used when the HTTP request
// times out first, so a proposal that never commits doesn't leak its
// channel in the map forever.
func (pt *ProposalTracker) Cancel(index uint64) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	delete(pt.waiters, index)
}

// Notify resolves the waiter registered for index, if any. actualTerm is
// the term of whatever entry actually ended up applied at that index; if it
// doesn't match what Register saw, this index's original proposal was
// overwritten by a leadership change before it committed, and the waiter is
// told that explicitly instead of being handed an unrelated command's
// success/failure.
func (pt *ProposalTracker) Notify(index, actualTerm uint64, err error) {
	pt.mu.Lock()
	w, ok := pt.waiters[index]
	if !ok {
		pt.mu.Unlock()
		return
	}
	delete(pt.waiters, index)
	pt.mu.Unlock()

	if w.term != actualTerm {
		w.ch <- ErrProposalSuperseded
	} else {
		w.ch <- err
	}
	close(w.ch)
}

// On-disk layout for a node's persistent state, rooted at --data-dir
// (default "data") or overridden entirely via --node-dir:
//
//	data/
//	├── node_1/                     <- nodeDir: data/node_<id>
//	│   ├── mvcc.snap               <- Layer 2 MVCC state machine snapshot (mvcc/snapshot.go)
//	│   └── raft_wal/               <- Raft's durable storage (TidwallStorage)
//	│       ├── metadata.json       <- HardState{Term, Vote, Commit} + SnapshotMeta, fsync'd on every change
//	│       ├── state.snap          <- last snapshot payload CreateSnapshot() wrote (not read back on
//	│       │                          boot today - InitialState() only returns SnapshotMeta, not these
//	│       │                          bytes; Layer 2 restores from mvcc.snap independently instead)
//	│       └── segments/           <- rolling WAL segment files (tidwall/wal)
//	├── node_2/
//	└── node_3/
func main() {
	// 1. Command-line flags - all declared before the single Parse() call below.
	nodeID := flag.Uint64("id", 1, "Unique Node ID (e.g. 1, 2, 3)")
	dataDir := flag.String("data-dir", "data", "Base directory for all cluster node data")
	customNodeDir := flag.String("node-dir", "", "Explicit directory for this node (overrides data-dir/node_<id>)")
	raftAddr := flag.String("raft-addr", "127.0.0.1:8001", "Internal Raft TCP bind address")
	httpAddr := flag.String("http-addr", ":9001", "Public Client HTTP address")
	peersFlag := flag.String("peers", "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003", "Peer topology: id=host:port,...")
	heartbeat := flag.Duration("heartbeat-interval", 50*time.Millisecond, "Leader heartbeat broadcast interval")
	electionMin := flag.Duration("election-timeout-min", 150*time.Millisecond, "Minimum randomized election timeout")
	electionMax := flag.Duration("election-timeout-max", 300*time.Millisecond, "Maximum randomized election timeout")
	rpcTimeout := flag.Duration("rpc-timeout", 500*time.Millisecond, "Outbound RPC timeout deadline")
	flag.Parse()

	// 2. Resolve and create this node's data directory.
	nodeDir := *customNodeDir
	if nodeDir == "" {
		nodeDir = filepath.Join(*dataDir, fmt.Sprintf("node_%d", *nodeID))
	}
	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		log.Fatalf("[Node %d] Failed to create node directory %s: %v", *nodeID, nodeDir, err)
	}
	log.Printf("[Node %d] Using data directory: %s", *nodeID, nodeDir)

	// 3. Parse peer cluster topology.
	peerMap := make(map[uint64]string)
	var peerIDs []uint64
	for _, part := range strings.Split(*peersFlag, ",") {
		kv := strings.Split(part, "=")
		if len(kv) == 2 {
			id, _ := strconv.ParseUint(kv[0], 10, 64)
			peerMap[id] = kv[1]
			peerIDs = append(peerIDs, id)
		}
	}

	// 4. Build and validate the Raft configuration.
	cfg := raft.Config{
		NodeID:             *nodeID,
		Peers:              peerIDs,
		HeartbeatInterval:  *heartbeat,
		ElectionTimeoutMin: *electionMin,
		ElectionTimeoutMax: *electionMax,
		RPCTimeout:         *rpcTimeout,
	}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("[Node %d] Configuration validation error: %v", *nodeID, err)
	}

	// 5. Resolve on-disk paths for this node's persistent state.
	snapPath := filepath.Join(nodeDir, "mvcc.snap")
	raftDir := filepath.Join(nodeDir, "raft_wal")

	// 6. Initialize Layer 2: MVCC store (in-memory SkipList + periodic disk snapshot).
	mvccEngine := raw.NewSkipListEngine(32 * 1024 * 1024)
	store := mvcc.NewStore(mvccEngine)
	defer store.Close()

	// LoadSnapshotFromFile returns nil both when snapPath doesn't exist (fresh
	// start) and when it loads successfully - the two can't be told apart
	// from its error alone. Check existence first so the log line is
	// accurate, and so a real load failure (corruption, permissions) on a
	// file that does exist is treated as fatal instead of silently logged
	// as "starting fresh" and continuing with an empty store.
	if _, statErr := os.Stat(snapPath); statErr == nil {
		if err := store.LoadSnapshotFromFile(snapPath); err != nil {
			log.Fatalf("[Node %d] Failed to load existing snapshot from %s: %v", *nodeID, snapPath, err)
		}
		log.Printf("[Node %d] Successfully restored snapshot from %s", *nodeID, snapPath)
	} else {
		log.Printf("[Node %d] No existing snapshot found at %s. Starting fresh.", *nodeID, snapPath)
	}

	// 7. Initialize Raft's durable storage (tidwall/wal-backed segmented log).
	raftStorage, err := raft.NewTidwallStorage(raftDir)
	if err != nil {
		log.Fatalf("[Node %d] Failed to initialize TidwallStorage at %s: %v", *nodeID, raftDir, err)
	}
	defer raftStorage.Close()

	// 8. Initialize the Raft consensus node.
	applyCh := make(chan raft.ApplyMsg, 1000)
	transport := raft.NewTCPTransport(peerMap)
	node, err := raft.NewRaftNode(cfg, transport, raftStorage, applyCh)
	if err != nil {
		log.Fatalf("[Node %d] Failed to initialize Raft node: %v", *nodeID, err)
	}
	defer node.Stop()

	// 9. Start the internal Raft TCP RPC listener.
	if err := raft.StartServer(*raftAddr, node); err != nil {
		log.Fatalf("[Node %d] Failed to start Raft TCP listener: %v", *nodeID, err)
	}
	log.Printf("[Node %d] Consensus engine running on %s", *nodeID, *raftAddr)

	// 10. State-machine applier, which also owns snapshotting.
	//
	// This goroutine is the only writer to the store, so running the snapshot
	// here - rather than on an independent ticker goroutine - guarantees the
	// index handed to node.Snapshot matches the exported data exactly: no
	// apply can land between reading the index and exporting the state.
	proposalTracker := NewProposalTracker()
	applyMsg := func(msg raft.ApplyMsg) {
		// Snapshot installations from the Leader (InstallSnapshot RPC).
		if !msg.CommandValid {
			if err := store.RestoreSnapshot(bytes.NewReader(msg.Command)); err != nil {
				log.Printf("[State Machine] Failed to restore snapshot at index %d: %v", msg.CommandIndex, err)
			} else {
				log.Printf("[State Machine] Restored snapshot at Index %d (Term %d)", msg.CommandIndex, msg.CommandTerm)
			}
			return
		}

		// Regular committed client commands.
		var cmd CommandPayload
		if err := json.Unmarshal(msg.Command, &cmd); err != nil {
			log.Printf("Failed to unmarshal command at index %d: %v", msg.CommandIndex, err)
			proposalTracker.Notify(msg.CommandIndex, msg.CommandTerm, err)
			return
		}

		commitTS := msg.CommandIndex * 10
		var applyErr error
		switch cmd.Op {
		case "PUT":
			applyErr = store.Put([]byte(cmd.Key), []byte(cmd.Value), commitTS)
			log.Printf("[State Machine] Applied Index %d: PUT %s = %s (TS: %d)", msg.CommandIndex, cmd.Key, cmd.Value, commitTS)
		case "DELETE":
			applyErr = store.Delete([]byte(cmd.Key), commitTS)
			log.Printf("[State Machine] Applied Index %d: DELETE %s (TS: %d)", msg.CommandIndex, cmd.Key, commitTS)
		}
		// Wake up any /put handler blocked on this index - a no-op if
		// nobody's waiting (e.g. this entry arrived via normal
		// replication on a follower, or the HTTP request already timed
		// out and canceled its wait).
		proposalTracker.Notify(msg.CommandIndex, msg.CommandTerm, applyErr)
	}

	takeSnapshot := func() {
		appliedIndex := node.LastApplied()
		if appliedIndex == 0 {
			return
		}
		watermark := uint64(time.Now().UnixNano())

		// A. Persist Layer 2's snapshot file first. If this fails, Raft must
		// NOT compact: the log would then be the only copy of those entries.
		if err := store.SaveSnapshotToFile(snapPath, watermark); err != nil {
			log.Printf("[Snapshot] Failed to save %s, skipping log compaction: %v", snapPath, err)
			return
		}

		// B. Hand the same state to Raft so it can compact its own log.
		var snapBuf bytes.Buffer
		if err := store.ExportSnapshot(&snapBuf, watermark); err != nil {
			log.Printf("[Snapshot] Failed to export state at index %d: %v", appliedIndex, err)
			return
		}
		if err := node.Snapshot(appliedIndex, snapBuf.Bytes()); err != nil && !errors.Is(err, raft.ErrSnapshotOutOfDate) {
			log.Printf("[Snapshot] Raft log compaction at index %d failed: %v", appliedIndex, err)
		}
	}

	go func() {
		snapshotTicker := time.NewTicker(30 * time.Second)
		defer snapshotTicker.Stop()
		for {
			select {
			case msg, ok := <-applyCh:
				if !ok {
					return
				}
				applyMsg(msg)
				// Only now is the entry reflected in the store; this is what
				// gates linearizable reads (WaitApplied) and compaction.
				node.ReportApplied(msg.CommandIndex)
			case <-snapshotTicker.C:
				takeSnapshot()
			}
		}
	}()

	// 11. Public HTTP client API.
	http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, _ := io.ReadAll(r.Body)
		var req PutRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		cmdBytes, _ := json.Marshal(CommandPayload{Op: "PUT", Key: req.Key, Value: req.Value})
		idx, term, isLeader := node.Propose(cmdBytes)
		if !isLeader {
			http.Error(w, "Not leader. Redirect to leader.", http.StatusTemporaryRedirect)
			return
		}

		// Propose() only appends to this node's local log - it returns
		// immediately, well before quorum replication or state-machine
		// application. Returning success here would let a client observe a
		// "successful" write that a concurrent leadership change then
		// silently discards. Block until the applier actually processes
		// this exact index (proposalTracker.Notify, wired above) instead.
		waitCh := proposalTracker.Register(idx, term)
		ctx, cancel := context.WithTimeout(r.Context(), clientRequestTimeout)
		defer cancel()

		select {
		case applyErr := <-waitCh:
			if applyErr != nil {
				status := http.StatusInternalServerError
				if errors.Is(applyErr, ErrProposalSuperseded) {
					status = http.StatusConflict
				}
				http.Error(w, applyErr.Error(), status)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "committed",
				"index":  idx,
				"term":   term,
			})
		case <-ctx.Done():
			proposalTracker.Cancel(idx)
			http.Error(w, "commit timeout: quorum not reached in time", http.StatusGatewayTimeout)
		}
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key parameter is required", http.StatusBadRequest)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), clientRequestTimeout)
		defer cancel()

		// Reading straight from local state with time.Now() as the read
		// timestamp is unsafe: a leader partitioned into a minority island
		// has no way to learn it was deposed, and would keep serving
		// increasingly stale reads to any client that can still reach it -
		// a linearizability violation. ReadIndex (Raft §6.4) makes the
		// leader confirm, via a fresh quorum round-trip, that it's still
		// recognized as leader before trusting its own state machine;
		// WaitApplied then blocks until that state machine has actually
		// caught up to the confirmed index.
		readIndex, err := node.ReadIndex(ctx)
		if err != nil {
			if errors.Is(err, raft.ErrNotLeader) {
				http.Error(w, "Not leader. Redirect to leader.", http.StatusTemporaryRedirect)
			} else {
				http.Error(w, fmt.Sprintf("failed to confirm linearizable read: %v", err), http.StatusServiceUnavailable)
			}
			return
		}
		if err := node.WaitApplied(ctx, readIndex); err != nil {
			http.Error(w, "timed out waiting for state machine to catch up", http.StatusGatewayTimeout)
			return
		}

		readTS := uint64(time.Now().UnixNano())
		val, err := store.Get([]byte(key), readTS)
		if err != nil {
			http.Error(w, fmt.Sprintf("Key not found or error: %v", err), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"key":   key,
			"value": string(val),
		})
	})

	http.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		term, isLeader := node.GetState()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"node_id":   *nodeID,
			"term":      term,
			"is_leader": isLeader,
		})
	})

	log.Printf("[Node %d] Client HTTP API listening on %s", *nodeID, *httpAddr)
	log.Fatal(http.ListenAndServe(*httpAddr, nil))
}
