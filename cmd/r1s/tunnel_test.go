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

const cliTestTarget = "127.0.0.1:9000"

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
		tunnelConn: func(ctx context.Context, executionID string, targets []tunnel.Target, targetSlot string) (tunnel.Conn, string, error) {
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
	go func() { result <- cli.tunnel([]string{"exec-1", "--target-slot", cliTestTarget}, io.Discard) }()

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
		tunnelConn: func(ctx context.Context, executionID string, targets []tunnel.Target, targetSlot string) (tunnel.Conn, string, error) {
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
	go func() { result <- cli.tunnel([]string{"exec-1", "--target-slot", cliTestTarget}, io.Discard) }()

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
	var gotTargets []tunnel.Target
	backend := &cliWorkflowBackend{
		tunnelConn: func(ctx context.Context, executionID string, targets []tunnel.Target, targetSlot string) (tunnel.Conn, string, error) {
			gotSlot = targetSlot
			gotTargets = targets
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
	// The client owns the destination list: --target-slot http@127.0.0.1:8080
	// supplies it, and --target http selects that slot.
	go func() {
		result <- cli.tunnel([]string{"exec-1", "--target-slot", "http@127.0.0.1:8080", "--target", "http"}, &stderr)
	}()

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
	// The client-supplied destination list is what reaches the backend: the
	// allocator no longer resolves targets, so the backend must hand the grant
	// the exact (host, port) the client passed via --target-slot.
	if len(gotTargets) != 1 || gotTargets[0].ID != "http" || gotTargets[0].Host != "127.0.0.1" || gotTargets[0].Port != 8080 {
		t.Fatalf("backend targets = %+v; want [{http 127.0.0.1 8080}]", gotTargets)
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

// TestParseTargetSlot covers the CLI destination-slot parser. A bare host:port
// names the unnamed default slot (the interactive pipe); name@host:port names
// a named slot the client references with --target. Malformed shapes (missing
// port, empty host, zero or oversized port, empty @name) are rejected before
// the address is sent to the allocator.
func TestParseTargetSlot(t *testing.T) {
	tests := []struct {
		raw     string
		want    tunnel.Target
		wantErr bool
	}{
		{raw: "127.0.0.1:9000", want: tunnel.Target{Host: "127.0.0.1", Port: 9000}},
		{raw: "ssh@127.0.0.1:2222", want: tunnel.Target{ID: "ssh", Host: "127.0.0.1", Port: 2222}},
		{raw: "[::1]:8080", want: tunnel.Target{Host: "::1", Port: 8080}},
		{raw: "  host.example:443 ", want: tunnel.Target{Host: "host.example", Port: 443}},
		{raw: "", wantErr: true},
		{raw: "127.0.0.1", wantErr: true},
		{raw: "127.0.0.1:0", wantErr: true},
		{raw: "127.0.0.1:65536", wantErr: true},
		{raw: ":8080", wantErr: true},
		{raw: "@127.0.0.1:80", wantErr: true},
	}
	for _, tt := range tests {
		got, err := parseTargetSlot(tt.raw)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parseTargetSlot(%q) succeeded (%+v), want error", tt.raw, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseTargetSlot(%q) error: %v", tt.raw, err)
			continue
		}
		if got != tt.want {
			t.Errorf("parseTargetSlot(%q) = %+v; want %+v", tt.raw, got, tt.want)
		}
	}
}

// TestCLITunnelRejectsUnknownTarget verifies --target names one of the
// client-supplied destinations: an unknown slot name is rejected fail-fast on
// the client, before any network round-trip to the allocator edge.
func TestCLITunnelRejectsUnknownTarget(t *testing.T) {
	cli := &localCLI{ctx: context.Background()}
	err := cli.tunnel([]string{
		"exec-1", "--target-slot", "http@127.0.0.1:8080", "--target", "nope",
	}, io.Discard)
	if err == nil {
		t.Fatal("tunnel with unknown --target succeeded, want error")
	}
	if !strings.Contains(err.Error(), "not in the --target-slot list") {
		t.Fatalf("error = %q; want unknown-target diagnostic", err)
	}
}
