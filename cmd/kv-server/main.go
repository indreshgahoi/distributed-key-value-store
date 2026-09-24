// Command kv-server runs one node of the replicated key-value store: a Raft
// consensus node, the MVCC state machine it drives, and a client HTTP API.
//
// On-disk layout, per node (<data-dir>/node_<id>/ by default):
//
//	raft/                 Raft's durable state (see raft.TidwallStorage):
//	    metadata.json     term, vote, snapshot bounds - the atomic commit point
//	    snap-<index>.dat  latest state machine snapshot
//	    wal-<base>/       log entries after the snapshot
//
// The MVCC store itself is in memory. On restart it is rebuilt from the
// latest snapshot plus the log entries after it.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/replica"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/mvcc"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/storage/raw"
)

func main() {
	cfg, err := parseConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("configuration: %v", err)
	}
	log.SetPrefix(fmt.Sprintf("[node %d] ", cfg.nodeID))
	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

// run starts the node and blocks until it is signaled to stop or fails.
func run(cfg serverConfig) error {
	storage, err := raft.NewTidwallStorage(filepath.Join(cfg.nodeDir, "raft"))
	if err != nil {
		return fmt.Errorf("opening raft storage: %w", err)
	}
	defer storage.Close()

	store := mvcc.NewStore(raw.NewSkipListEngine(cfg.memtableBytes))
	defer store.Close()

	applyCh := make(chan raft.ApplyMsg, 1024)
	node, err := raft.NewRaftNode(cfg.raft, raft.NewTCPTransport(cfg.raftPeers), storage, applyCh)
	if err != nil {
		return fmt.Errorf("starting raft: %w", err)
	}
	defer node.Stop()
	if err := raft.StartServer(cfg.raftAddr, node); err != nil {
		return err
	}

	proposals := replica.NewProposalTracker()
	sm := replica.New(replica.Config{Store: store, Node: node, Proposals: proposals, SnapshotEvery: cfg.snapshotEvery})
	smErr := make(chan error, 1)
	go func() { smErr <- sm.Run(applyCh) }()

	server := &http.Server{
		Addr:              cfg.httpAddr,
		Handler:           (&api{node: node, store: store, proposals: proposals, httpPeers: cfg.httpPeers}).routes(),
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      requestTimeout + 5*time.Second,
	}
	httpErr := make(chan error, 1)
	go func() { httpErr <- server.ListenAndServe() }()
	log.Printf("raft on %s, clients on %s, data in %s", cfg.raftAddr, cfg.httpAddr, cfg.nodeDir)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// Fail-stop: any of these ends the process. A node that can't persist or
	// can't apply committed entries must not keep serving.
	var runErr error
	select {
	case sig := <-signals:
		log.Printf("received %v, shutting down", sig)
	case <-node.Done():
		runErr = fmt.Errorf("raft node halted: %w", node.Err())
	case err := <-smErr:
		runErr = fmt.Errorf("state machine failed: %w", err)
	case err := <-httpErr:
		runErr = fmt.Errorf("http server: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Printf("http shutdown: %v", err)
	}
	return runErr
}
