package raft

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
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

// TCPTransport implements NetworkTransport over Go standard net/rpc sockets.
type TCPTransport struct {
	mu      sync.Mutex
	peerMap map[uint64]string
	clients map[uint64]*rpc.Client
}

// NewTCPTransport initializes the outgoing client manager.
func NewTCPTransport(peerMap map[uint64]string) *TCPTransport {
	return &TCPTransport{
		peerMap: peerMap,
		clients: make(map[uint64]*rpc.Client),
	}
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

func (t *TCPTransport) getClient(to uint64) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if client, ok := t.clients[to]; ok {
		return client, nil
	}

	addr, ok := t.peerMap[to]
	if !ok {
		return nil, fmt.Errorf("unknown peer ID: %d", to)
	}

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}

	client := rpc.NewClient(conn)
	t.clients[to] = client
	return client, nil
}

func (t *TCPTransport) closeClient(to uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if client, ok := t.clients[to]; ok {
		_ = client.Close()
		delete(t.clients, to)
	}
}

// SendRequestVote issues a non-blocking RequestVote RPC over TCP.
func (t *TCPTransport) SendRequestVote(ctx context.Context, to uint64, args *RequestVoteArgs) (*RequestVoteReply, error) {
	client, err := t.getClient(to)
	if err != nil {
		return nil, err
	}

	reply := &RequestVoteReply{}
	call := client.Go("Raft.RequestVote", args, reply, nil)

	select {
	case <-ctx.Done():
		t.closeClient(to)
		return nil, ctx.Err()
	case res := <-call.Done:
		if res.Error != nil {
			t.closeClient(to)
			return nil, res.Error
		}
		return reply, nil
	}
}

// SendAppendEntries issues a non-blocking AppendEntries RPC over TCP.
func (t *TCPTransport) SendAppendEntries(ctx context.Context, to uint64, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	client, err := t.getClient(to)
	if err != nil {
		return nil, err
	}

	reply := &AppendEntriesReply{}
	call := client.Go("Raft.AppendEntries", args, reply, nil)

	select {
	case <-ctx.Done():
		t.closeClient(to)
		return nil, ctx.Err()
	case res := <-call.Done:
		if res.Error != nil {
			t.closeClient(to)
			return nil, res.Error
		}
		return reply, nil
	}
}

// SendInstallSnapshot streams a snapshot to a lagging follower.
func (t *TCPTransport) SendInstallSnapshot(ctx context.Context, to uint64, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	client, err := t.getClient(to)
	if err != nil {
		return nil, err
	}

	reply := &InstallSnapshotReply{}
	call := client.Go("Raft.InstallSnapshot", args, reply, nil)

	select {
	case <-ctx.Done():
		t.closeClient(to)
		return nil, ctx.Err()
	case res := <-call.Done:
		if res.Error != nil {
			t.closeClient(to)
			return nil, res.Error
		}
		return reply, nil
	}
}
