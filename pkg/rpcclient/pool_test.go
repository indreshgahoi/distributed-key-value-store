package rpcclient

import (
	"context"
	"errors"
	"net"
	"net/rpc"
	"testing"
	"time"
)

type Svc struct{}

func (Svc) Echo(in string, out *string) error      { *out = in; return nil }
func (Svc) Fail(_ string, _ *string) error         { return errors.New("no such range") }
func (Svc) Sleep(d time.Duration, _ *string) error { time.Sleep(d); return nil }

func startServer(t *testing.T) string {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("Svc", Svc{}); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	Serve(l, server)
	return l.Addr().String()
}

func (p *Pool) currentClient(to uint64) *rpc.Client {
	p.mu.Lock()
	pc := p.conns[to]
	p.mu.Unlock()
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return pc.client
}

// TestPool_HandlerErrorKeepsConnection is a regression test: an error the
// remote handler returns (e.g. "unknown range") used to close the shared
// connection, cutting off every other range's traffic to that peer.
func TestPool_HandlerErrorKeepsConnection(t *testing.T) {
	p := NewPool(map[uint64]string{1: startServer(t)})
	var out string
	if err := p.Call(context.Background(), 1, "Svc.Echo", "hi", &out); err != nil || out != "hi" {
		t.Fatalf("echo: %q %v", out, err)
	}
	before := p.currentClient(1)

	err := p.Call(context.Background(), 1, "Svc.Fail", "", &out)
	var serverErr rpc.ServerError
	if !errors.As(err, &serverErr) {
		t.Fatalf("expected the handler's error, got %v", err)
	}
	if p.currentClient(1) != before {
		t.Fatalf("a handler error dropped the shared connection")
	}
}

// TestPool_TimeoutDropsConnection: net/rpc can't cancel a call, so a call
// that times out drops the connection and the next call redials.
func TestPool_TimeoutDropsConnection(t *testing.T) {
	p := NewPool(map[uint64]string{1: startServer(t)})
	var out string
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Call(ctx, 1, "Svc.Sleep", time.Second, &out); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected a timeout, got %v", err)
	}
	if p.currentClient(1) != nil {
		t.Fatalf("a timed-out connection was kept")
	}
	if err := p.Call(context.Background(), 1, "Svc.Echo", "again", &out); err != nil || out != "again" {
		t.Fatalf("redial after timeout: %q %v", out, err)
	}
}

// TestPool_DeadPeerDoesNotBlockOthers: dialing is per peer, so a peer whose
// dial hangs doesn't delay calls to a healthy one.
func TestPool_DeadPeerDoesNotBlockOthers(t *testing.T) {
	// 10.255.255.1 is unroutable: a dial there hangs until its context expires.
	p := NewPool(map[uint64]string{1: startServer(t), 2: "10.255.255.1:9"})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var out string
		_ = p.Call(ctx, 2, "Svc.Echo", "x", &out)
	}()
	time.Sleep(50 * time.Millisecond) // let the dead-peer dial start

	start := time.Now()
	var out string
	if err := p.Call(context.Background(), 1, "Svc.Echo", "fast", &out); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("healthy peer's call waited %v behind a dead peer's dial", elapsed)
	}
}
