package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type CommandPayload struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value"`
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

	// 10. Background state-machine applier.
	go func() {
		for msg := range applyCh {
			// Snapshot installations from the Leader (InstallSnapshot RPC).
			if !msg.CommandValid {
				if err := store.RestoreSnapshot(bytes.NewReader(msg.Command)); err != nil {
					log.Printf("[State Machine] Failed to restore snapshot at index %d: %v", msg.CommandIndex, err)
				} else {
					log.Printf("[State Machine] Restored snapshot at Index %d (Term %d)", msg.CommandIndex, msg.CommandTerm)
				}
				continue
			}

			// Regular committed client commands.
			var cmd CommandPayload
			if err := json.Unmarshal(msg.Command, &cmd); err != nil {
				log.Printf("Failed to unmarshal command at index %d: %v", msg.CommandIndex, err)
				continue
			}

			commitTS := msg.CommandIndex * 10
			switch cmd.Op {
			case "PUT":
				_ = store.Put([]byte(cmd.Key), []byte(cmd.Value), commitTS)
				log.Printf("[State Machine] Applied Index %d: PUT %s = %s (TS: %d)", msg.CommandIndex, cmd.Key, cmd.Value, commitTS)
			case "DELETE":
				_ = store.Delete([]byte(cmd.Key), commitTS)
				log.Printf("[State Machine] Applied Index %d: DELETE %s (TS: %d)", msg.CommandIndex, cmd.Key, commitTS)
			}
		}
	}()

	// 11. Periodic state-machine snapshot & Raft log compaction loop.
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			watermark := uint64(time.Now().UnixNano())

			// A. Persist Layer 2's snapshot file to disk.
			_ = store.SaveSnapshotToFile(snapPath, watermark)

			// B. Hand the snapshot to Raft so it can compact its own log.
			lastApplied := node.LastApplied()
			if lastApplied > 0 {
				var snapBuf bytes.Buffer
				if err := store.ExportSnapshot(&snapBuf, watermark); err == nil {
					_ = node.Snapshot(lastApplied, snapBuf.Bytes())
				}
			}
		}
	}()

	// 12. Public HTTP client API.
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

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "proposed",
			"index":  idx,
			"term":   term,
			"leader": true,
		})
	})

	http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key parameter is required", http.StatusBadRequest)
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
