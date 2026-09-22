package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
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

func main() {
	nodeID := flag.Uint64("id", 1, "Node ID (e.g. 1, 2, 3)")
	raftAddr := flag.String("raft-addr", "127.0.0.1:8001", "Internal Raft TCP bind address")
	httpAddr := flag.String("http-addr", ":9001", "Public Client HTTP address")
	peersFlag := flag.String("peers", "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003", "Peer topology: id=host:port,...")

	heartbeat := flag.Duration("heartbeat-interval", 50*time.Millisecond, "Leader heartbeat broadcast interval")
	electionMin := flag.Duration("election-timeout-min", 150*time.Millisecond, "Minimum randomized election timeout")
	electionMax := flag.Duration("election-timeout-max", 300*time.Millisecond, "Maximum randomized election timeout")
	rpcTimeout := flag.Duration("rpc-timeout", 500*time.Millisecond, "Outbound RPC timeout deadline")

	flag.Parse()

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

	// 1. Initialize Milestone 1 MVCC Store
	rawEngine := raw.NewSkipListEngine(32 * 1024 * 1024)
	store := mvcc.NewStore(rawEngine)
	defer store.Close()

	// 2. Initialize Raft Consensus Engine
	applyCh := make(chan raft.ApplyMsg, 1000)
	transport := raft.NewTCPTransport(peerMap)
	node, err := raft.NewRaftNode(cfg, transport, applyCh)
	if err != nil {
		log.Fatalf("[Node %d] Failed to initialize Raft node: %v", *nodeID, err)
	}
	defer node.Stop()

	// 3. Start internal Raft TCP RPC Server
	if err := raft.StartServer(*raftAddr, node); err != nil {
		log.Fatalf("[Node %d] Failed to start Raft TCP listener: %v", *nodeID, err)
	}
	log.Printf("[Node %d] Consensus engine running on %s (Heartbeat: %v, Election: [%v, %v])",
		*nodeID, *raftAddr, cfg.HeartbeatInterval, cfg.ElectionTimeoutMin, cfg.ElectionTimeoutMax)

	// 4. Background State Machine Applier
	go func() {
		for msg := range applyCh {
			if !msg.CommandValid {
				continue
			}
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

	// 5. Public HTTP Client API
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
