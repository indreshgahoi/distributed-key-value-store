package sharding

import (
	"context"
	"fmt"
	"net"
	"net/rpc"

	"github.com/indreshgahoi/distributed-key-value-store/pkg/consensus/raft"
	"github.com/indreshgahoi/distributed-key-value-store/pkg/rpcclient"
)

// A node hosting hundreds of ranges can't open a port per range. All of a
// node's Raft traffic shares one listener and one connection per peer: each
// outbound RPC is wrapped with its RangeID, and the receiving node routes it
// to that range's local RaftNode.

// Wire envelopes wrapping the single-group Raft RPCs with a target RangeID.

type RangeRequestVoteArgs struct {
	RangeID uint64
	Args    raft.RequestVoteArgs
}

type RangeAppendEntriesArgs struct {
	RangeID uint64
	Args    raft.AppendEntriesArgs
}

type RangeInstallSnapshotArgs struct {
	RangeID uint64
	Args    raft.InstallSnapshotArgs
}

// MultiRaftTransport carries Raft RPCs for any hosted range between nodes.
type MultiRaftTransport interface {
	SendRangeRequestVote(ctx context.Context, to uint64, rangeID uint64, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error)
	SendRangeAppendEntries(ctx context.Context, to uint64, rangeID uint64, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error)
	SendRangeInstallSnapshot(ctx context.Context, to uint64, rangeID uint64, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error)
}

// rangeTransportAdapter presents the node-wide transport to one range's
// RaftNode as a plain raft.NetworkTransport, so pkg/consensus/raft needs no
// knowledge of ranges at all.
type rangeTransportAdapter struct {
	rangeID   uint64
	transport MultiRaftTransport
}

func newRangeTransportAdapter(rangeID uint64, transport MultiRaftTransport) *rangeTransportAdapter {
	return &rangeTransportAdapter{rangeID: rangeID, transport: transport}
}

func (a *rangeTransportAdapter) SendRequestVote(ctx context.Context, to uint64, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return a.transport.SendRangeRequestVote(ctx, to, a.rangeID, args)
}

func (a *rangeTransportAdapter) SendAppendEntries(ctx context.Context, to uint64, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return a.transport.SendRangeAppendEntries(ctx, to, a.rangeID, args)
}

func (a *rangeTransportAdapter) SendInstallSnapshot(ctx context.Context, to uint64, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return a.transport.SendRangeInstallSnapshot(ctx, to, a.rangeID, args)
}

// MultiRaftRPCHandler receives wire calls and routes them to the target range.
//
// An RPC for a range this node doesn't host returns an error to the caller.
// That error travels as an rpc.ServerError, which rpcclient never treats as
// a broken connection - so it can't disturb other ranges sharing the link.
type MultiRaftRPCHandler struct {
	mNode *MultiRaftNode
}

func (h *MultiRaftRPCHandler) target(rangeID uint64) (*raft.RaftNode, error) {
	node, ok := h.mNode.GetRange(rangeID)
	if !ok {
		return nil, fmt.Errorf("%w: range %d on node %d", ErrRangeNotHosted, rangeID, h.mNode.nodeID)
	}
	return node, nil
}

func (h *MultiRaftRPCHandler) RequestVote(args *RangeRequestVoteArgs, reply *raft.RequestVoteReply) error {
	node, err := h.target(args.RangeID)
	if err == nil {
		node.HandleRequestVote(&args.Args, reply)
	}
	return err
}

func (h *MultiRaftRPCHandler) AppendEntries(args *RangeAppendEntriesArgs, reply *raft.AppendEntriesReply) error {
	node, err := h.target(args.RangeID)
	if err == nil {
		node.HandleAppendEntries(&args.Args, reply)
	}
	return err
}

func (h *MultiRaftRPCHandler) InstallSnapshot(args *RangeInstallSnapshotArgs, reply *raft.InstallSnapshotReply) error {
	node, err := h.target(args.RangeID)
	if err == nil {
		node.HandleInstallSnapshot(&args.Args, reply)
	}
	return err
}

// StartMultiRaftServer listens on bindAddr and serves every range hosted by mNode.
func StartMultiRaftServer(bindAddr string, mNode *MultiRaftNode) error {
	listener, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", bindAddr, err)
	}
	return ServeMultiRaft(listener, mNode)
}

// ServeMultiRaft serves every range hosted by mNode on an existing listener.
func ServeMultiRaft(listener net.Listener, mNode *MultiRaftNode) error {
	server := rpc.NewServer()
	if err := server.RegisterName("MultiRaft", &MultiRaftRPCHandler{mNode: mNode}); err != nil {
		return fmt.Errorf("failed to register MultiRaft RPC: %w", err)
	}
	rpcclient.Serve(listener, server)
	return nil
}

// MultiRaftTCPTransport sends every range's RPCs over one pooled connection
// per peer node.
type MultiRaftTCPTransport struct {
	pool *rpcclient.Pool
}

// NewMultiRaftTCPTransport creates a transport over node ID -> "host:port".
func NewMultiRaftTCPTransport(peerMap map[uint64]string) *MultiRaftTCPTransport {
	return &MultiRaftTCPTransport{pool: rpcclient.NewPool(peerMap)}
}

func (t *MultiRaftTCPTransport) SendRangeRequestVote(ctx context.Context, to, rangeID uint64, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	reply := &raft.RequestVoteReply{}
	return reply, t.pool.Call(ctx, to, "MultiRaft.RequestVote", &RangeRequestVoteArgs{RangeID: rangeID, Args: *args}, reply)
}

func (t *MultiRaftTCPTransport) SendRangeAppendEntries(ctx context.Context, to, rangeID uint64, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	reply := &raft.AppendEntriesReply{}
	return reply, t.pool.Call(ctx, to, "MultiRaft.AppendEntries", &RangeAppendEntriesArgs{RangeID: rangeID, Args: *args}, reply)
}

func (t *MultiRaftTCPTransport) SendRangeInstallSnapshot(ctx context.Context, to, rangeID uint64, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	reply := &raft.InstallSnapshotReply{}
	return reply, t.pool.Call(ctx, to, "MultiRaft.InstallSnapshot", &RangeInstallSnapshotArgs{RangeID: rangeID, Args: *args}, reply)
}
