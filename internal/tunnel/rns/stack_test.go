package rns

import (
	"bytes"
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

// testClusterKey is a fixed 256-bit cluster secret for tests.
func testClusterKey() []byte { return bytes.Repeat([]byte{0x51}, 32) }

// freeTCPPort returns an available TCP port on loopback.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

// newTestListener creates a started allocator tunnel edge on loopback.
func newTestListener(t *testing.T, identityPrefix string) *Listener {
	t.Helper()
	root := t.TempDir()
	listener, err := NewListener(ListenerConfig{
		IdentitySource: filepath.Join(root, identityPrefix+".identity"),
		ClusterKey:     testClusterKey(),
		BindAddress:    "127.0.0.1",
		ListenPort:     freeTCPPort(t),
		ConfigPath:     filepath.Join(root, identityPrefix+"-reticulum"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := listener.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return listener
}

// newTestDialer creates a client tunnel edge and starts it.
func newTestDialer(t *testing.T, identityPrefix string) *Dialer {
	t.Helper()
	root := t.TempDir()
	dialer, err := NewDialer(DialerConfig{
		IdentitySource: filepath.Join(root, identityPrefix+".identity"),
		ClusterKey:     testClusterKey(),
		ConfigPath:     filepath.Join(root, identityPrefix+"-reticulum"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dialer.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := dialer.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return dialer
}

// connect establishes one tunnel Link between the dialer and the listener and
// returns both identified Conns.
func connect(t *testing.T, dialer *Dialer, listener *Listener) (allocator Conn, client Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	allocatorDone := make(chan Conn, 1)
	allocatorErr := make(chan error, 1)
	go func() {
		conn, err := listener.Accept(ctx)
		if err != nil {
			allocatorErr <- err
			return
		}
		allocatorDone <- conn
	}()
	clientConn, err := dialer.Dial(ctx, listener.Endpoint())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case allocatorConn := <-allocatorDone:
		return allocatorConn, clientConn
	case err := <-allocatorErr:
		_ = clientConn.Close()
		t.Fatalf("Accept: %v", err)
		return nil, nil
	}
}

func TestListenerDialerIdentify(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")

	allocatorConn, clientConn := connect(t, dialer, listener)
	defer allocatorConn.Close()
	defer clientConn.Close()

	// Each side must see the other's persistent identity hash via Link.Identify.
	allocatorRemote := allocatorConn.RemoteIdentity()
	clientRemote := clientConn.RemoteIdentity()
	if len(allocatorRemote) == 0 || len(clientRemote) == 0 {
		t.Fatalf("identification missing: allocator sees %x, client sees %x", allocatorRemote, clientRemote)
	}
	if !bytes.Equal(allocatorRemote, dialer.identity.Hash()) {
		t.Fatalf("allocator remote = %x, want client identity %x", allocatorRemote, dialer.identity.Hash())
	}
	if !bytes.Equal(clientRemote, listener.identity.Hash()) {
		t.Fatalf("client remote = %x, want allocator identity %x", clientRemote, listener.identity.Hash())
	}
}

func TestTunnelTransportHasSingleBackboneInterface(t *testing.T) {
	listener := newTestListener(t, "allocator")
	// The private tunnel stack carries exactly its one Backbone/TCP interface.
	if got := listener.stack.interfaceCount(); got != 1 {
		t.Fatalf("tunnel listener stack interface count = %d, want 1", got)
	}
	// EnableTransport must be false: the tunnel transport never participates in
	// public RNS routing or discovery and never borrows a control-plane interface.
	if enabled := listener.stack.transport.GetConfig().EnableTransport; enabled {
		t.Fatal("tunnel transport EnableTransport = true, want false")
	}
	// Assert the attached interface is the Backbone server (TCP listener), not
	// an interface borrowed from the control RNS.
	if names := listener.stack.interfaceNames(); names != "tunnel-backbone" {
		t.Fatalf("tunnel stack interface = %q, want tunnel-backbone", names)
	}
}

func TestLargePayloadRoundTrip(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocatorConn, clientConn := connect(t, dialer, listener)
	defer allocatorConn.Close()
	defer clientConn.Close()

	// A payload comfortably larger than the RNS MTU must round-trip through the
	// stock Channel/Buffer path without custom framing.
	payload := bytes.Repeat([]byte("payload-bytes-0123456789"), 8*1024) // ~ 256 KiB
	readDone := make(chan error, 1)
	go func() {
		got := make([]byte, 0, len(payload))
		buf := make([]byte, 32768)
		for int64(len(got)) < int64(len(payload)) {
			n, err := allocatorConn.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err != nil {
				readDone <- err
				return
			}
		}
		if !bytes.Equal(got, payload) {
			readDone <- io.ErrUnexpectedEOF
			return
		}
		readDone <- nil
	}()
	if n, err := clientConn.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	if err := <-readDone; err != nil {
		t.Fatalf("round-trip: %v", err)
	}
}

func TestHalfCloseDeliversEOF(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocatorConn, clientConn := connect(t, dialer, listener)
	defer allocatorConn.Close()

	// Writer side: client CloseWrite half-closes, so allocator read sees EOF
	// after the buffered byte is drained.
	const greeting = "hello-half-close"
	writeDone := make(chan error, 1)
	go func() {
		_, err := clientConn.Write([]byte(greeting))
		if err == nil {
			err = clientConn.CloseWrite()
		}
		writeDone <- err
	}()

	buf := make([]byte, len(greeting)+1)
	read := 0
	for {
		n, err := allocatorConn.Read(buf[read:])
		read += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v (after %d bytes)", err, read)
		}
		if read >= len(buf) {
			t.Fatal("did not reach EOF")
		}
	}
	if read != len(greeting) {
		t.Fatalf("read %d bytes, want %d", read, len(greeting))
	}
	if got := string(buf[:read]); got != greeting {
		t.Fatalf("payload = %q, want %q", got, greeting)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("Write/CloseWrite: %v", err)
	}
}
