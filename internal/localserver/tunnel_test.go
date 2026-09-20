package localserver

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/grpc/metadata"
)

// TestTunnelRelayOverMemory drives the full LocalTunnel bidirectional relay:
// a client connects through a real local API socket, the serve process's
// backend opens a tunnel (here over the in-memory overlay), and bytes, a
// half-close, and a teardown reason all traverse the relay in order. It proves
// the bridge contract end-to-end deterministically, without a network library.
func TestTunnelRelayOverMemory(t *testing.T) {
	broker := tunnel.NewMemoryBroker()
	allocatorKey := []byte("alloc-key-0000000000000000000000000")
	clientKey := []byte("client-key-0000000000000000000000000")
	allocatorListener := broker.Listen([]byte("ygg-addr"), nil)
	defer allocatorListener.Close()

	backend := &fakeBackend{
		executions: map[string]*r1sv1.ExecutionState{},
		tunnelConn: func(ctx context.Context, executionID, targetSlot string) (tunnel.Conn, string, error) {
			if executionID != "exec-1" {
				return nil, "", errors.New("tunnel: unknown execution")
			}
			conn, err := broker.Dial(ctx, tunnel.Endpoint{Address: []byte("ygg-addr"), PubKey: allocatorKey}, clientKey)
			return conn, "grant-1", err
		},
	}

	clientConn := r1sv1.NewLocalClientClient(dialConn(t, backend))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := clientConn.Tunnel(ctx)
	if err != nil {
		t.Fatalf("Tunnel: %v", err)
	}
	if err := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Open{Open: &r1sv1.LocalTunnelOpen{ExecutionId: "exec-1"}}}); err != nil {
		t.Fatalf("send open: %v", err)
	}

	// The serve backend dialed the memory mesh; claim the allocator edge.
	allocatorConn, err := allocatorListener.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer allocatorConn.Close()

	// Client -> serve -> allocator.
	if err := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Data{Data: []byte("ping")}}); err != nil {
		t.Fatalf("send data: %v", err)
	}
	got := make([]byte, 64)
	n, err := allocatorConn.Read(got)
	if err != nil || string(got[:n]) != "ping" {
		t.Fatalf("allocator read = %q, %v; want ping", got[:n], err)
	}

	// Allocator -> serve -> client.
	if _, err := allocatorConn.Write([]byte("pong")); err != nil {
		t.Fatalf("allocator write: %v", err)
	}
	msg, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv pong: %v", err)
	}
	if data := msg.GetData(); string(data) != "pong" {
		t.Fatalf("client got %q; want pong", data)
	}

	// Half-close: client's write side ends -> allocator sees EOF but can still
	// reply.
	if err := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Close{Close: &r1sv1.LocalTunnelClose{Half: true}}}); err != nil {
		t.Fatalf("send half-close: %v", err)
	}
	_, err = allocatorConn.Read(got)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("allocator read after half-close = %v; want EOF", err)
	}
	if _, err := allocatorConn.Write([]byte("after-eof")); err != nil {
		t.Fatalf("allocator write after client EOF: %v", err)
	}
	msg, err = stream.Recv()
	if err != nil || string(msg.GetData()) != "after-eof" {
		t.Fatalf("client got %q, %v; want after-eof", msg.GetData(), err)
	}

	// The allocator ends the session with a classified reason; the client's
	// read direction is closed with that reason surfaced.
	reasonCloser, ok := allocatorConn.(tunnel.ReasonCloser)
	if !ok {
		t.Fatal("allocator edge does not support reason close")
	}
	if err := reasonCloser.CloseWithReason(tunnel.ReasonExecutionEnded, "workload completed"); err != nil {
		t.Fatalf("CloseWithReason: %v", err)
	}
	msg, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv close: %v", err)
	}
	closeMsg := msg.GetClose()
	if closeMsg == nil {
		t.Fatalf("expected a close message, got %v", msg.GetPayload())
	}
	if closeMsg.GetReason() != string(tunnel.ReasonExecutionEnded) {
		t.Fatalf("reason = %q; want execution_ended", closeMsg.GetReason())
	}
	if closeMsg.GetDetail() != "workload completed" {
		t.Fatalf("detail = %q; want workload completed", closeMsg.GetDetail())
	}

	// The client fully closes; the serve handler returns cleanly.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
}

// TestTunnelTargetSlotPropagates verifies the named target slot in the open
// message reaches the serve backend unchanged, and stays only a slot reference:
// the serve process never resolves it to a raw endpoint.
func TestTunnelTargetSlotPropagates(t *testing.T) {
	var gotSlot string
	broker := tunnel.NewMemoryBroker()
	allocatorKey := []byte("alloc-key-0000000000000000000000000")
	clientKey := []byte("client-key-0000000000000000000000000")
	allocatorListener := broker.Listen([]byte("ygg-addr"), nil)
	defer allocatorListener.Close()

	backend := &fakeBackend{
		executions: map[string]*r1sv1.ExecutionState{},
		tunnelConn: func(ctx context.Context, executionID, targetSlot string) (tunnel.Conn, string, error) {
			gotSlot = targetSlot
			conn, err := broker.Dial(ctx, tunnel.Endpoint{Address: []byte("ygg-addr"), PubKey: allocatorKey}, clientKey)
			return conn, "grant-1", err
		},
	}

	clientConn := r1sv1.NewLocalClientClient(dialConn(t, backend))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := clientConn.Tunnel(ctx)
	if err != nil {
		t.Fatalf("Tunnel: %v", err)
	}
	if err := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Open{Open: &r1sv1.LocalTunnelOpen{ExecutionId: "exec-1", TargetSlot: "http"}}}); err != nil {
		t.Fatalf("send open: %v", err)
	}
	if _, err := allocatorListener.Accept(); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if gotSlot != "http" {
		t.Fatalf("backend target slot = %q; want http", gotSlot)
	}
}

// TestTunnelSetupFailureSurfacesAsError verifies that a backend tunnel failure
// (unknown execution, missing grant target) is surfaced as a stream error
// before any payload is relayed.
func TestTunnelSetupFailureSurfacesAsError(t *testing.T) {
	backend := &fakeBackend{
		executions: map[string]*r1sv1.ExecutionState{},
		tunnelConn: func(ctx context.Context, executionID, targetSlot string) (tunnel.Conn, string, error) {
			return nil, "", errors.New("tunnel: no grant for execution")
		},
	}
	clientConn := r1sv1.NewLocalClientClient(dialConn(t, backend))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := clientConn.Tunnel(ctx)
	if err != nil {
		t.Fatalf("Tunnel: %v", err)
	}
	if err := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Open{Open: &r1sv1.LocalTunnelOpen{ExecutionId: "exec-9"}}}); err != nil {
		t.Fatalf("send open: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("setup failure did not surface as an error")
	}
}

// TestTunnelRequiresOpenFirst verifies the first message must be an open.
func TestTunnelRequiresOpenFirst(t *testing.T) {
	backend := &fakeBackend{executions: map[string]*r1sv1.ExecutionState{}}
	clientConn := r1sv1.NewLocalClientClient(dialConn(t, backend))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := clientConn.Tunnel(ctx)
	if err != nil {
		t.Fatalf("Tunnel: %v", err)
	}
	if err := stream.Send(&r1sv1.LocalTunnelMessage{Payload: &r1sv1.LocalTunnelMessage_Data{Data: []byte("nope")}}); err != nil {
		t.Fatalf("send data first: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream without an open should fail")
	}

}

// tunnelStubConn is a minimal tunnel.Conn that yields a scripted sequence of
// Read results so pumpTunnel's teardown classification can be asserted without
// a transport.
type tunnelStubConn struct {
	peer []byte
	// readErr is returned on the first (and only) Read call after any data.
	readErr error
}

func (c *tunnelStubConn) Read(p []byte) (int, error) {
	if c.readErr == nil {
		return 0, io.EOF
	}
	return 0, c.readErr
}
func (c *tunnelStubConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *tunnelStubConn) PeerKey() []byte             { return append([]byte(nil), c.peer...) }
func (c *tunnelStubConn) Close() error                { return nil }
func (c *tunnelStubConn) CloseRead() error            { return nil }
func (c *tunnelStubConn) CloseWrite() error           { return nil }

// recordingStream is a minimal LocalClient_TunnelServer fake that records every
// message the pump sends.
type recordingStream struct {
	ctx  context.Context
	mu   sync.Mutex
	sent []*r1sv1.LocalTunnelMessage
	done chan struct{}
}

func (s *recordingStream) messages() []*r1sv1.LocalTunnelMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*r1sv1.LocalTunnelMessage(nil), s.sent...)
}

// pump sends exactly one terminal message; wait for it.
func (s *recordingStream) wait(t *testing.T) {
	t.Helper()
	select {
	case <-s.done:
	case <-time.After(2 * time.Second):
		t.Fatal("pumpTunnel did not finish")
	}
}

func (s *recordingStream) Recv() (*r1sv1.LocalTunnelMessage, error) { return nil, io.EOF }
func (s *recordingStream) Send(m *r1sv1.LocalTunnelMessage) error {
	s.mu.Lock()
	s.sent = append(s.sent, m)
	if m.GetClose() != nil {
		close(s.done)
	}
	s.mu.Unlock()
	return nil
}
func (s *recordingStream) Context() context.Context { return s.ctx }
func (s *recordingStream) SendMsg(m any) error      { return s.Send(m.(*r1sv1.LocalTunnelMessage)) }
func (s *recordingStream) RecvMsg(m any) error      { return io.EOF }
func (s *recordingStream) SetHeader(metadata.MD) error {
	return nil
}
func (s *recordingStream) SendHeader(metadata.MD) error { return nil }
func (s *recordingStream) SetTrailer(metadata.MD)       {}

func newRecordingStream() *recordingStream {
	return &recordingStream{ctx: context.Background(), done: make(chan struct{})}
}

// assertClassification runs pumpTunnel against a stub reading that error and
// checks the close message it emits. When wantMessage is false, no close
// message may be sent at all (the local-abort contract).
func assertClassification(t *testing.T, readErr error, wantReason tunnel.Reason, wantMessage bool) {
	t.Helper()
	server := &Server{}
	stream := newRecordingStream()
	conn := &tunnelStubConn{peer: []byte("key"), readErr: readErr}
	server.pumpTunnel(stream, conn)

	if wantMessage {
		stream.wait(t)
		msgs := stream.messages()
		if len(msgs) == 0 {
			t.Fatalf("pump sent no close message for %v", readErr)
		}
		last := msgs[len(msgs)-1].GetClose()
		if last == nil {
			t.Fatalf("last message is not a close: %v", msgs[len(msgs)-1].GetPayload())
		}
		if tunnel.Reason(last.GetReason()) != wantReason {
			t.Fatalf("close reason = %q; want %q (for %v)", last.GetReason(), wantReason, readErr)
		}
		return
	}

	// No message expected: give the pump a beat to run, then assert emptiness.
	select {
	case <-stream.done:
		t.Fatalf("pump emitted a close message for a local abort (%v)", readErr)
	case <-time.After(50 * time.Millisecond):
	}
	if msgs := stream.messages(); len(msgs) != 0 {
		t.Fatalf("pump sent %d messages for a local abort; want none: %v", len(msgs), msgs)
	}
}

// TestPumpTunnelNeverMisreportsLocalAbort is the regression test for the review
// finding: a locally aborted read side (client disconnected, relay closed the
// tunnel) must NOT emit grant_rejected — there is no grant rejection, the peer
// is simply gone. See the fix that introduced tunnel.ErrReadAborted and
// tunnel.ReasonSessionFailed.
func TestPumpTunnelNeverMisreportsLocalAbort(t *testing.T) {
	if got, want := tunnel.ReasonSessionFailed, tunnel.Reason("session_failed"); got != want {
		t.Fatalf("ReasonSessionFailed = %q; want %q", got, want)
	}
	assertClassification(t, tunnel.ErrReadAborted, "", false)
}

// TestPumpTunnelReportsGenericFailure verifies a genuine mid-session failure is
// surfaced as session_failed (not grant_rejected).
func TestPumpTunnelReportsGenericFailure(t *testing.T) {
	assertClassification(t, errors.New("mesh lost"), tunnel.ReasonSessionFailed, true)
}

// TestPumpTunnelReportsCleanClose verifies a clean EOF ends with reason closed.
func TestPumpTunnelReportsCleanClose(t *testing.T) {
	assertClassification(t, io.EOF, tunnel.ReasonClosed, true)
}

// TestPumpTunnelReportsClassifiedTeardown verifies a SessionError surfaces its
// classified reason and detail.
func TestPumpTunnelReportsClassifiedTeardown(t *testing.T) {
	server := &Server{}
	stream := newRecordingStream()
	conn := &tunnelStubConn{
		peer:    []byte("key"),
		readErr: &tunnel.SessionError{Reason: tunnel.ReasonExecutionEnded, Detail: "workload completed"},
	}
	server.pumpTunnel(stream, conn)
	stream.wait(t)
	msgs := stream.messages()
	last := msgs[len(msgs)-1].GetClose()
	if last == nil {
		t.Fatalf("last message is not a close: %v", msgs[len(msgs)-1].GetPayload())
	}
	if tunnel.Reason(last.GetReason()) != tunnel.ReasonExecutionEnded {
		t.Fatalf("reason = %q; want execution_ended", last.GetReason())
	}
	if last.GetDetail() != "workload completed" {
		t.Fatalf("detail = %q; want workload completed", last.GetDetail())
	}
}
