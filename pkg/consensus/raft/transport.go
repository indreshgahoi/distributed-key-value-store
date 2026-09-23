package raft

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// NetworkTransport defines the consensus RPC interface.
type NetworkTransport interface {
	SendRequestVote(ctx context.Context, to uint64, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, to uint64, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, to uint64, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

// RPCHandler bridges inbound network RPCs to the local RaftNode instance.
type RPCHandler struct {
	node *RaftNode
}

// RequestVote dispatches an incoming election vote request.
func (h *RPCHandler) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	h.node.HandleRequestVote(args, reply)
	return nil
}

// InstallSnapshot dispatches an incoming snapshot installation from the Leader.
func (h *RPCHandler) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) error {
	h.node.HandleInstallSnapshot(args, reply)
	return nil
}

// AppendEntries dispatches an incoming log replication or heartbeat request.
func (h *RPCHandler) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	h.node.HandleAppendEntries(args, reply)
	return nil
}

// StartServer binds a TCP listener and registers the Raft RPC service.
func StartServer(bindAddr string, node *RaftNode) error {
	server := rpc.NewServer()
	if err := server.RegisterName("Raft", &RPCHandler{node: node}); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", bindAddr, err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()
	return nil
}

// TCPTransport implements NetworkTransport over Go's net/rpc, with one
// lazily dialed, reused connection per peer.
type TCPTransport struct {
	peerMap map[uint64]string

	mu    sync.Mutex
	conns map[uint64]*peerConn
}

// peerConn serializes dialing per peer: a dead peer's dial timeout blocks
// only RPCs to that peer, never heartbeats to the healthy ones.
type peerConn struct {
	mu     sync.Mutex
	client *rpc.Client
}

// NewTCPTransport initializes the outgoing client manager.
func NewTCPTransport(peerMap map[uint64]string) *TCPTransport {
	return &TCPTransport{
		peerMap: peerMap,
		conns:   make(map[uint64]*peerConn),
	}
}

// SendRequestVote issues a RequestVote RPC.
func (t *TCPTransport) SendRequestVote(ctx context.Context, to uint64, args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	return reply, t.call(ctx, to, "Raft.RequestVote", args, reply)
}

// SendAppendEntries issues an AppendEntries RPC.
func (t *TCPTransport) SendAppendEntries(ctx context.Context, to uint64, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	return reply, t.call(ctx, to, "Raft.AppendEntries", args, reply)
}

// SendInstallSnapshot ships a snapshot to a lagging follower.
func (t *TCPTransport) SendInstallSnapshot(ctx context.Context, to uint64, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	reply := &InstallSnapshotReply{}
	return reply, t.call(ctx, to, "Raft.InstallSnapshot", args, reply)
}

// call performs one RPC, bounded by ctx.
func (t *TCPTransport) call(ctx context.Context, to uint64, method string, args, reply any) error {
	pc, client, err := t.client(ctx, to)
	if err != nil {
		return err
	}
	call := client.Go(method, args, reply, nil)
	select {
	case <-ctx.Done():
		// net/rpc can't cancel a call; drop the connection so a peer that
		// stopped responding is redialed instead of queuing behind it.
		pc.reset(client)
		return ctx.Err()
	case res := <-call.Done:
		if res.Error != nil {
			pc.reset(client)
			return res.Error
		}
		return nil
	}
}

// client returns the peer's connection, dialing it if needed.
func (t *TCPTransport) client(ctx context.Context, to uint64) (*peerConn, *rpc.Client, error) {
	addr, ok := t.peerMap[to]
	if !ok {
		return nil, nil, fmt.Errorf("raft: unknown peer ID %d", to)
	}
	t.mu.Lock()
	pc, ok := t.conns[to]
	if !ok {
		pc = &peerConn{}
		t.conns[to] = pc
	}
	t.mu.Unlock()

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.client != nil {
		return pc, pc.client, nil
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	pc.client = rpc.NewClient(conn)
	return pc, pc.client, nil
}

// reset closes client if it is still the peer's current connection (another
// goroutine may already have replaced it with a fresh one).
func (pc *peerConn) reset(client *rpc.Client) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.client == client {
		_ = client.Close()
		pc.client = nil
	}
}
