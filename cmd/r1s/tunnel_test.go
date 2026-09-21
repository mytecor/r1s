package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mytecor/r1s/internal/localapi"
	"github.com/mytecor/r1s/internal/tunnel"
)

// syncBuffer is a bytes.Buffer guarded by a mutex so the CLI tunnel goroutine
// (which writes diagnostics to stderr) and the test (which reads it) can run
// concurrently.
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

// freeLocalPort returns an unused TCP port on 127.0.0.1 for a tunnel host
// mapping. The listener is closed before the tunnel binds it; a small race for
// a dedicated test harness is acceptable.
func freeLocalPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return port
}

// dialLocalPort retries dialing 127.0.0.1:port until the CLI's listener is
// bound. The tunnel goroutine binds listeners asynchronously; a naive first
// dial races it (and is more flaky under -race).
func dialLocalPort(t *testing.T, port int) net.Conn {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial host port %s: %v (listener never bound)", addr, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCLITunnelPortForwards verifies the service-backed r1s tunnel bridge
// with a Docker-style --port host:container mapping: the client binds a local
// listener on 127.0.0.1:host, an inbound connection opens a tunnel stream to
// container port <container>, and bytes relay bidirectionally. Diagnostics
// never touch stdout; payload flows over the bound local socket.
func TestCLITunnelPortForwards(t *testing.T) {
	broker := tunnel.NewMemoryBroker()
	allocatorKey := []byte("alloc-key-0000000000000000000000000")
	clientKey := []byte("client-key-0000000000000000000000000")
	allocatorListener := broker.Listen([]byte("ygg-addr"), nil)
	defer allocatorListener.Close()

	var gotPort uint16
	var gotTargets []tunnel.Target
	backend := &cliWorkflowBackend{
		tunnelConn: func(ctx context.Context, executionID string, targets []tunnel.Target, targetPort uint16) (tunnel.Conn, string, error) {
			gotPort = targetPort
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

	hostPort := freeLocalPort(t)
	ctx, cancel := context.WithCancel(context.Background())
	var stderr syncBuffer
	cli := &localCLI{ctx: ctx, stdout: io.Discard, socketPath: socket, client: api}

	result := make(chan error, 1)
	go func() { result <- cli.tunnel([]string{"exec-1", "--port", fmt.Sprintf("%d:9000", hostPort)}, &stderr) }()

	// Connecting to the bound host port triggers the relay, which opens the
	// tunnel stream and dials the allocator edge. Retry until the CLI's
	// asynchronous bind has completed.
	localConn := dialLocalPort(t, hostPort)
	defer localConn.Close()

	// The serve backend dialed the memory mesh; claim the allocator edge.
	allocatorConn, err := allocatorListener.Accept()
	if err != nil {
		cancel()
		t.Fatalf("accept allocator edge: %v", err)
	}
	defer allocatorConn.Close()

	// The container port reaches the backend unchanged; the host port stays
	// local (bound by the CLI, never sent to the allocator).
	if gotPort != 9000 {
		cancel()
		t.Fatalf("backend target port = %d; want 9000", gotPort)
	}
	if len(gotTargets) != 1 || gotTargets[0].Port != 9000 {
		cancel()
		t.Fatalf("backend targets = %+v; want [{port 9000}]", gotTargets)
	}

	// Local socket -> serve -> allocator edge.
	if _, err := localConn.Write([]byte("hello-over-tcp")); err != nil {
		t.Fatalf("write local: %v", err)
	}
	got := make([]byte, 64)
	n, err := allocatorConn.Read(got)
	if err != nil || string(got[:n]) != "hello-over-tcp" {
		t.Fatalf("allocator read = %q, %v; want hello-over-tcp", got[:n], err)
	}

	// Allocator edge -> serve -> local socket.
	if _, err := allocatorConn.Write([]byte("reply-over-tcp")); err != nil {
		t.Fatalf("allocator write: %v", err)
	}
	_ = localConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	m, err := localConn.Read(got)
	if err != nil || string(got[:m]) != "reply-over-tcp" {
		t.Fatalf("local read = %q, %v; want reply-over-tcp", got[:m], err)
	}

	// The command runs until the context is cancelled; it never touches stdout.
	// Confirm diagnostics stayed on stderr only.
	if len(stderr.Bytes()) == 0 {
		t.Fatal("expected a forwarding diagnostic on stderr")
	}
	cancel()
	localConn.Close()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("tunnel returned error on cancel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("tunnel did not exit after cancel")
	}
}

// TestCLITunnelPortPropagatesMulti verifies repeatable --port mappings build
// the full container-port list the grant authorizes, and that an inbound
// connection on a second mapping carries its own container port.
func TestCLITunnelPortPropagatesMulti(t *testing.T) {
	broker := tunnel.NewMemoryBroker()
	allocatorKey := []byte("alloc-key-0000000000000000000000000")
	clientKey := []byte("client-key-0000000000000000000000000")
	allocatorListener := broker.Listen([]byte("ygg-addr"), nil)
	defer allocatorListener.Close()

	var gotPorts []uint16
	backend := &cliWorkflowBackend{
		tunnelConn: func(ctx context.Context, executionID string, targets []tunnel.Target, targetPort uint16) (tunnel.Conn, string, error) {
			gotPorts = append(gotPorts, targetPort)
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

	hostA := freeLocalPort(t)
	hostB := freeLocalPort(t)
	ctx, cancel := context.WithCancel(context.Background())
	var stderr syncBuffer
	cli := &localCLI{ctx: ctx, stdout: io.Discard, socketPath: socket, client: api}

	result := make(chan error, 1)
	go func() {
		result <- cli.tunnel([]string{
			"exec-1",
			"--port", fmt.Sprintf("%d:8080", hostA),
			"--port", fmt.Sprintf("%d:9000", hostB),
		}, &stderr)
	}()

	// Open a connection on the second mapping and claim the allocator edge its
	// stream dials.
	connB := dialLocalPort(t, hostB)
	defer connB.Close()
	allocatorConn, err := allocatorListener.Accept()
	if err != nil {
		cancel()
		t.Fatalf("accept allocator edge: %v", err)
	}
	defer allocatorConn.Close()

	// This connection's stream targets container port 9000 (the second mapping).
	if len(gotPorts) < 1 || gotPorts[0] != 9000 {
		cancel()
		t.Fatalf("backend target ports = %v; want first stream on 9000", gotPorts)
	}
	cancel()
	select {
	case <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("tunnel did not exit after cancel")
	}
}

// TestParsePortMapping covers the CLI --port parser. host is the local bind
// port and container is the container-side destination; both must be positive
// uint16. Malformed shapes (missing colon, empty side, zero or oversized port,
// extra colons) are rejected before any listener is bound.
func TestParsePortMapping(t *testing.T) {
	tests := []struct {
		raw           string
		wantHost      uint16
		wantContainer uint16
		wantErr       bool
	}{
		{raw: "8080:80", wantHost: 8080, wantContainer: 80},
		{raw: " 18099:9000 ", wantHost: 18099, wantContainer: 9000},
		{raw: "22:22", wantHost: 22, wantContainer: 22},
		{raw: "", wantErr: true},
		{raw: "8080", wantErr: true},
		{raw: "8080:", wantErr: true},
		{raw: ":80", wantErr: true},
		{raw: "0:80", wantErr: true},
		{raw: "8080:0", wantErr: true},
		{raw: "65536:80", wantErr: true},
		{raw: "8080:65536", wantErr: true},
		{raw: "8080:80:90", wantErr: true},
	}
	for _, tt := range tests {
		got, err := parsePortMapping(tt.raw)
		if tt.wantErr {
			if err == nil {
				t.Errorf("parsePortMapping(%q) succeeded (%+v), want error", tt.raw, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parsePortMapping(%q) error: %v", tt.raw, err)
			continue
		}
		if got.host != tt.wantHost || got.container != tt.wantContainer {
			t.Errorf("parsePortMapping(%q) = %+v; want host=%d container=%d", tt.raw, got, tt.wantHost, tt.wantContainer)
		}
	}
}

// TestCLITunnelRequiresPort verifies r1s tunnel rejects a command with no
// --port (the interactive default pipe is removed), and rejects an unknown flag.
func TestCLITunnelRequiresPort(t *testing.T) {
	cli := &localCLI{ctx: context.Background()}
	if err := cli.tunnel([]string{"exec-1"}, io.Discard); err == nil {
		t.Fatal("tunnel with no --port succeeded, want error")
	} else if !strings.Contains(err.Error(), "at least one --port") {
		t.Fatalf("no --port error = %q; want missing-port diagnostic", err)
	}
	if err := cli.tunnel([]string{"exec-1", "--port", "8080:80", "--bogus"}, io.Discard); err == nil {
		t.Fatal("tunnel with an unknown flag succeeded, want error")
	} else if !strings.Contains(err.Error(), "unknown flag") {
		t.Fatalf("unknown flag error = %q", err)
	}
}
