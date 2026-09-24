// Package rpcclient keeps one reusable net/rpc connection per peer.
//
// It is shared by every transport in the system (single-group Raft and
// multi-range Raft), so connection handling is right in exactly one place.
package rpcclient

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// Pool dials peers lazily and reuses one connection per peer.
//
// Dialing is serialized per peer, never globally: a dead peer's dial timeout
// delays only calls to that peer, never heartbeats to healthy ones.
type Pool struct {
	addrs map[uint64]string

	mu    sync.Mutex
	conns map[uint64]*peerConn
}

type peerConn struct {
	mu     sync.Mutex
	client *rpc.Client
}

// NewPool creates a pool over peer ID -> "host:port".
func NewPool(addrs map[uint64]string) *Pool {
	return &Pool{addrs: addrs, conns: make(map[uint64]*peerConn)}
}

// Call performs one RPC to peer `to`, bounded by ctx.
//
// The connection is dropped (and redialed on the next call) only on
// transport failures: a timeout, since net/rpc can't cancel a call and a
// peer that stopped responding must not keep calls queued behind it, or a
// broken connection. An error the remote handler returned
// (rpc.ServerError) says nothing about the connection, which may be
// carrying other traffic - e.g. other ranges' Raft messages - so it is kept.
func (p *Pool) Call(ctx context.Context, to uint64, method string, args, reply any) error {
	pc, client, err := p.client(ctx, to)
	if err != nil {
		return err
	}
	call := client.Go(method, args, reply, nil)
	select {
	case <-ctx.Done():
		pc.reset(client)
		return ctx.Err()
	case res := <-call.Done:
		var serverErr rpc.ServerError
		if res.Error != nil && !errors.As(res.Error, &serverErr) {
			pc.reset(client)
		}
		return res.Error
	}
}

// client returns the peer's connection, dialing it if needed.
func (p *Pool) client(ctx context.Context, to uint64) (*peerConn, *rpc.Client, error) {
	addr, ok := p.addrs[to]
	if !ok {
		return nil, nil, fmt.Errorf("rpcclient: unknown peer ID %d", to)
	}
	p.mu.Lock()
	pc, ok := p.conns[to]
	if !ok {
		pc = &peerConn{}
		p.conns[to] = pc
	}
	p.mu.Unlock()

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

// Serve accepts connections on listener and serves them with server until
// the listener is closed.
func Serve(listener net.Listener, server *rpc.Server) {
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()
}
