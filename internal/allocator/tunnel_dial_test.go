package allocator

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/mytecor/r1s/internal/tunnel"
)

type portRuntime struct {
	*fakeRuntime
	id           string
	port         uint16
	peer         net.Conn
	beforeReturn func()
}

func (r *portRuntime) DialExecution(ctx context.Context, id string, port uint16) (net.Conn, error) {
	r.id, r.port = id, port
	conn, peer := net.Pipe()
	r.peer = peer
	if r.beforeReturn != nil {
		r.beforeReturn()
	}
	return conn, nil
}

func TestTunnelDialChecksSessionAndExecution(t *testing.T) {
	clock := newFakeClock()
	runtime := &portRuntime{fakeRuntime: newFakeRuntime()}
	a := newTunnelAllocator(t, clock, runtime.fakeRuntime)
	a.runtime = runtime
	id := assignRunning(t, a, clock, "a")
	ack := mustHandle(t, a, tunnelGrantEnvelope(clock.Now(), "grant", "a", id)).GetExecutionTunnelGrantAck()
	session, err := a.AcceptTunnel(id, ack.GetGrantId(), []byte(testPeerKey))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := a.DialTunnelTarget(ctx, &tunnel.Session{ExecutionID: id}, 8080); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("forged session: %v", err)
	}
	if _, err := a.DialTunnelTarget(ctx, session, 22); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("foreign port: %v", err)
	}
	if runtime.id != "" {
		t.Fatal("unauthorized call reached runtime")
	}
	conn, err := a.DialTunnelTarget(ctx, session, 8080)
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	runtime.peer.Close()
	if runtime.id != id || runtime.port != 8080 {
		t.Fatal("execution/port not forwarded to runtime")
	}
	runtime.beforeReturn = func() { a.tunnels.Invalidate(id) }
	if _, err := a.DialTunnelTarget(ctx, session, 8080); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("revocation during dial: %v", err)
	}
	defer runtime.peer.Close()
	if _, err := runtime.peer.Write([]byte("x")); err == nil {
		t.Fatal("connection survived revocation during dial")
	}
}
