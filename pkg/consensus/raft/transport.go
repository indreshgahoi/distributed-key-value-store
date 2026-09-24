package raft

import (
	"context"
	"fmt"
	"net"
	"net/rpc"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/rpcclient"
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
	rpcclient.Serve(listener, server)
	return nil
}

// TCPTransport implements NetworkTransport over Go's net/rpc, with one
// reused connection per peer (see rpcclient.Pool).
type TCPTransport struct {
	pool *rpcclient.Pool
}

// NewTCPTransport initializes the outgoing client manager.
func NewTCPTransport(peerMap map[uint64]string) *TCPTransport {
	return &TCPTransport{pool: rpcclient.NewPool(peerMap)}
}

// SendRequestVote issues a RequestVote RPC.
func (t *TCPTransport) SendRequestVote(ctx context.Context, to uint64, args *RequestVoteArgs) (*RequestVoteReply, error) {
	reply := &RequestVoteReply{}
	return reply, t.pool.Call(ctx, to, "Raft.RequestVote", args, reply)
}

// SendAppendEntries issues an AppendEntries RPC.
func (t *TCPTransport) SendAppendEntries(ctx context.Context, to uint64, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	reply := &AppendEntriesReply{}
	return reply, t.pool.Call(ctx, to, "Raft.AppendEntries", args, reply)
}

// SendInstallSnapshot ships a snapshot to a lagging follower.
func (t *TCPTransport) SendInstallSnapshot(ctx context.Context, to uint64, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	reply := &InstallSnapshotReply{}
	return reply, t.pool.Call(ctx, to, "Raft.InstallSnapshot", args, reply)
}
