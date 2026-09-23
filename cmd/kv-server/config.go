package main

import (
	"flag"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
)

// serverConfig is everything a node needs to start, parsed from flags.
type serverConfig struct {
	nodeID    uint64
	raftAddr  string
	httpAddr  string
	nodeDir   string
	raftPeers map[uint64]string // node ID -> Raft RPC address
	httpPeers map[uint64]string // node ID -> client HTTP address (for redirects; optional)

	raft          raft.Config
	snapshotEvery uint64 // take a snapshot after this many applied entries
	memtableBytes uint32 // in-memory store budget
}

func parseConfig(args []string) (serverConfig, error) {
	fs := flag.NewFlagSet("kv-server", flag.ContinueOnError)
	nodeID := fs.Uint64("id", 1, "unique node ID (> 0)")
	dataDir := fs.String("data-dir", "data", "base directory for node data (this node uses <data-dir>/node_<id>)")
	nodeDir := fs.String("node-dir", "", "explicit data directory for this node (overrides --data-dir)")
	raftAddr := fs.String("raft-addr", "127.0.0.1:8001", "Raft RPC listen address")
	httpAddr := fs.String("http-addr", ":9001", "client HTTP listen address")
	peers := fs.String("peers", "1=127.0.0.1:8001,2=127.0.0.1:8002,3=127.0.0.1:8003", "Raft peers: id=host:port,...")
	httpPeers := fs.String("http-peers", "", "client HTTP addresses, for redirecting to the leader: id=host:port,...")
	heartbeat := fs.Duration("heartbeat-interval", 50*time.Millisecond, "leader heartbeat interval")
	electionMin := fs.Duration("election-timeout-min", 150*time.Millisecond, "minimum randomized election timeout")
	electionMax := fs.Duration("election-timeout-max", 300*time.Millisecond, "maximum randomized election timeout")
	rpcTimeout := fs.Duration("rpc-timeout", 500*time.Millisecond, "Raft RPC timeout")
	snapshotEvery := fs.Uint64("snapshot-every", 10000, "snapshot and compact the log every N applied entries")
	memtable := fs.Uint("memtable-bytes", 64<<20, "memory budget of the in-memory store, in bytes")
	if err := fs.Parse(args); err != nil {
		return serverConfig{}, err
	}

	raftPeers, err := parsePeerList(*peers)
	if err != nil {
		return serverConfig{}, fmt.Errorf("--peers: %w", err)
	}
	httpPeerMap := map[uint64]string{}
	if *httpPeers != "" {
		if httpPeerMap, err = parsePeerList(*httpPeers); err != nil {
			return serverConfig{}, fmt.Errorf("--http-peers: %w", err)
		}
	}
	if _, ok := raftPeers[*nodeID]; !ok {
		return serverConfig{}, fmt.Errorf("--id %d is not listed in --peers", *nodeID)
	}
	if *snapshotEvery == 0 {
		return serverConfig{}, fmt.Errorf("--snapshot-every must be > 0")
	}
	if *memtable == 0 || *memtable > 1<<32-1 {
		return serverConfig{}, fmt.Errorf("--memtable-bytes must be in (0, 4GiB)")
	}

	ids := make([]uint64, 0, len(raftPeers))
	for id := range raftPeers {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	dir := *nodeDir
	if dir == "" {
		dir = filepath.Join(*dataDir, fmt.Sprintf("node_%d", *nodeID))
	}
	cfg := serverConfig{
		nodeID:        *nodeID,
		raftAddr:      *raftAddr,
		httpAddr:      *httpAddr,
		nodeDir:       dir,
		raftPeers:     raftPeers,
		httpPeers:     httpPeerMap,
		snapshotEvery: *snapshotEvery,
		memtableBytes: uint32(*memtable),
		raft: raft.Config{
			NodeID:             *nodeID,
			Peers:              ids,
			HeartbeatInterval:  *heartbeat,
			ElectionTimeoutMin: *electionMin,
			ElectionTimeoutMax: *electionMax,
			RPCTimeout:         *rpcTimeout,
		},
	}
	return cfg, cfg.raft.Validate()
}

// parsePeerList parses "id=host:port,id=host:port".
func parsePeerList(s string) (map[uint64]string, error) {
	peers := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		idStr, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || addr == "" {
			return nil, fmt.Errorf("malformed entry %q (want id=host:port)", part)
		}
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil || id == 0 {
			return nil, fmt.Errorf("invalid node ID %q", idStr)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("node ID %d listed twice", id)
		}
		peers[id] = addr
	}
	return peers, nil
}
