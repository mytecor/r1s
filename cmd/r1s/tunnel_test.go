package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mytecor/r1s/internal/localapi"
	"github.com/mytecor/r1s/internal/tunnel"
)

// syncBuffer is a bytes.Buffer guarded by a mutex so the CLI tunnel goroutine
// (which writes stdout) and the test (which reads it) can run concurrently.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func (s *syncBuffer) Bytes() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.b.Bytes()...)
}

// TestCLITunnelRelaysBytesProves the service-backed `r1s tunnel` bridge
// end-to-end: the CLI relays stdin bytes to the allocator target, replies
// reach stdout byte-clean, stdin EOF half-closes the write direction, and a
// clean teardown returns no error. It drives real localapi over a real socket
// with an in-memory overlay behind the serve backend.
func TestCLITunnelRelaysBytes(t *testing.T) {
	broker := tunnel.NewMemoryBroker()
	allocatorKey := []byte("alloc-key-0000000000000000000000000")
	clientKey := []byte("client-key-0000000000000000000000000")
	allocatorListener := broker.Listen([]byte("ygg-addr"), nil)
	defer allocatorListener.Close()

	backend := &cliWorkflowBackend{
		tunnelConn: func(ctx context.Context, executionID, targetSlot string) (tunnel.Conn, string, error) {
			conn, err := broker.Dial(ctx, tunnel.Endpoint{Address: []byte("ygg-addr"), PubKey: allocatorKey}, clientKey)
			return conn, "grant-1", err
		},
	}
	_, socket := serveBackend(t, backend)

	api, err := localapi.Dial(socket)
	if err != nil {
		t.Fatalf("localapi dial: %v", err)
	}
	defer api.Close()

	stdin, writeStdin, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer stdin.Close()

	var stdout syncBuffer
	cli := &localCLI{ctx: context.Background(), stdout: &stdout, stdin: stdin, socketPath: socket, client: api}

	// Launch the CLI bridge first: backend.Tunnel dials the mesh and delivers
	// the allocator edge to the listener.
	result := make(chan error, 1)
	go func() { result <- cli.tunnel([]string{"exec-1"}, io.Discard) }()

	// The allocator edge that the serve backend dialed.
	allocatorConn, err := allocatorListener.Accept()
	if err != nil {
		t.Fatalf("accept allocator edge: %v", err)
	}
	defer allocatorConn.Close()

	// Client -> serve -> allocator: write stdin, read on the allocator side.
	if _, err := writeStdin.Write([]byte("hello-from-stdin")); err != nil {
		t.Fatalf("write stdin: %v", err)
	}
	got := make([]byte, 64)
	n, err := allocatorConn.Read(got)
	if err != nil || string(got[:n]) != "hello-from-stdin" {
		t.Fatalf("allocator read = %q, %v; want hello-from-stdin", got[:n], err)
	}

	// Allocator -> serve -> CLI stdout.
	if _, err := allocatorConn.Write([]byte("reply-to-stdout")); err != nil {
		t.Fatalf("allocator write: %v", err)
	}
	if err := waitFor(2*time.Second, func() bool { return strings.Contains(stdout.String(), "reply-to-stdout") }); err != nil {
		t.Fatalf("stdout did not receive the reply: %q", stdout.String())
	}

	// stdin EOF half-closes the write direction; the allocator sees EOF.
	if err := writeStdin.Close(); err != nil {
		t.Fatalf("close stdin: %v", err)
	}
	_, err = allocatorConn.Read(got)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("allocator read after stdin EOF = %v; want EOF", err)
	}

	// Clean teardown: the allocator ends the session normally.
	reasonCloser, ok := allocatorConn.(tunnel.ReasonCloser)
	if !ok {
		t.Fatal("allocator edge does not support reason close")
	}
	if err := reasonCloser.CloseWithReason(tunnel.ReasonClosed, ""); err != nil {
		t.Fatalf("close with reason: %v", err)
	}

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("tunnel returned error on clean close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tunnel did not exit after clean close")
	}

	// stdout carried only payload bytes.
	if !bytes.Equal(stdout.Bytes(), []byte("reply-to-stdout")) {
		t.Fatalf("stdout = %q; want only payload bytes", stdout.String())
	}
}

// TestCLITunnelSurfacesAbnormalReason verifies an abnormal teardown reason is
// surfaced as a non-nil error (the CLI exits non-zero) while stdout stays
// byte-clean.
func TestCLITunnelSurfacesAbnormalReason(t *testing.T) {
	broker := tunnel.NewMemoryBroker()
	allocatorKey := []byte("alloc-key-0000000000000000000000000")
	clientKey := []byte("client-key-0000000000000000000000000")
	allocatorListener := broker.Listen([]byte("ygg-addr"), nil)
	defer allocatorListener.Close()

	backend := &cliWorkflowBackend{
		tunnelConn: func(ctx context.Context, executionID, targetSlot string) (tunnel.Conn, string, error) {
			conn, err := broker.Dial(ctx, tunnel.Endpoint{Address: []byte("ygg-addr"), PubKey: allocatorKey}, clientKey)
			return conn, "grant-1", err
		},
	}
	_, socket := serveBackend(t, backend)

	api, err := localapi.Dial(socket)
	if err != nil {
		t.Fatalf("localapi dial: %v", err)
	}
	defer api.Close()

	stdin, _, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer stdin.Close()

	var stdout syncBuffer
	cli := &localCLI{ctx: context.Background(), stdout: &stdout, stdin: stdin, socketPath: socket, client: api}

	result := make(chan error, 1)
	go func() { result <- cli.tunnel([]string{"exec-1"}, io.Discard) }()

	allocatorConn, err := allocatorListener.Accept()
	if err != nil {
		t.Fatalf("accept allocator edge: %v", err)
	}
	defer allocatorConn.Close()

	reasonCloser, ok := allocatorConn.(tunnel.ReasonCloser)
	if !ok {
		t.Fatal("allocator edge does not support reason close")
	}
	if err := reasonCloser.CloseWithReason(tunnel.ReasonUnauthorized, "peer key mismatch"); err != nil {
		t.Fatalf("close with reason: %v", err)
	}

	select {
	case err := <-result:
		if err == nil {
			t.Fatal("tunnel returned nil error for abnormal teardown")
		}
		if !strings.Contains(err.Error(), "unauthorized") {
			t.Fatalf("error = %v; want unauthorized surfaced", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tunnel did not exit after abnormal close")
	}
	if n := len(stdout.Bytes()); n != 0 {
		t.Fatalf("stdout was not byte-clean on abnormal teardown: %q", stdout.String())
	}
}

// TestCLITunnelTargetSlot verifies `r1s tunnel <exec> --target <slot>` passes
// the named slot through the local API to the serve backend, and the named
// slot's stream reaches stdout byte-clean. Diagnostics never touch stdout.
func TestCLITunnelTargetSlot(t *testing.T) {
	broker := tunnel.NewMemoryBroker()
	allocatorKey := []byte("alloc-key-0000000000000000000000000")
	clientKey := []byte("client-key-0000000000000000000000000")
	allocatorListener := broker.Listen([]byte("ygg-addr"), nil)
	defer allocatorListener.Close()

	var gotSlot string
	backend := &cliWorkflowBackend{
		tunnelConn: func(ctx context.Context, executionID, targetSlot string) (tunnel.Conn, string, error) {
			gotSlot = targetSlot
			conn, err := broker.Dial(ctx, tunnel.Endpoint{Address: []byte("ygg-addr"), PubKey: allocatorKey}, clientKey)
			return conn, "grant-1", err
		},
	}
	_, socket := serveBackend(t, backend)

	api, err := localapi.Dial(socket)
	if err != nil {
		t.Fatalf("localapi dial: %v", err)
	}
	defer api.Close()

	stdin, writeStdin, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	defer stdin.Close()

	var stdout syncBuffer
	var stderr syncBuffer
	cli := &localCLI{ctx: context.Background(), stdout: &stdout, stdin: stdin, socketPath: socket, client: api}

	result := make(chan error, 1)
	go func() { result <- cli.tunnel([]string{"exec-1", "--target", "http"}, &stderr) }()

	// The serve backend dials the mesh asynchronously; whoever finishes first
	// (the CLI failing setup, or the allocator edge being dialed) tells us how
	// the test proceeds.
	allocReady := make(chan tunnel.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		conn, err := allocatorListener.Accept()
		if err == nil {
			allocReady <- conn
		}
		acceptErr <- err
	}()
	var allocatorConn tunnel.Conn
	select {
	case err := <-result:
		t.Fatalf("cli.tunnel exited early with error: %v (stderr=%q)", err, stderr.String())
	case err := <-acceptErr:
		if err != nil {
			t.Fatalf("accept allocator edge: %v", err)
		}
		allocatorConn = <-allocReady
	case <-time.After(3 * time.Second):
		t.Fatal("neither the CLI tunnel nor the allocator edge arrived")
	}
	defer allocatorConn.Close()

	if gotSlot != "http" {
		t.Fatalf("backend target slot = %q; want http", gotSlot)
	}

	// The named slot's data reaches stdout byte-clean.
	if _, err := allocatorConn.Write([]byte("http-stream")); err != nil {
		t.Fatalf("allocator write: %v", err)
	}
	if err := waitFor(2*time.Second, func() bool { return strings.Contains(stdout.String(), "http-stream") }); err != nil {
		t.Fatalf("stdout did not receive the named-stream payload: %q", stdout.String())
	}

	// A normal teardown ends the command with no error and byte-clean stdout.
	if err := writeStdin.Close(); err != nil {
		t.Fatalf("close stdin: %v", err)
	}
	if rc, ok := allocatorConn.(tunnel.ReasonCloser); ok {
		_ = rc.CloseWithReason(tunnel.ReasonClosed, "")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("tunnel returned error on clean close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tunnel did not exit after clean close")
	}
	if !bytes.Equal(stdout.Bytes(), []byte("http-stream")) {
		t.Fatalf("stdout = %q; want only the named-stream payload", stdout.String())
	}
}

// waitFor polls a predicate until it succeeds, the deadline elapses, or ctx.
func waitFor(timeout time.Duration, predicate func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if predicate() {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return errors.New("condition not met within deadline")
}
