package yggdrasil

import (
	"bytes"
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/mytecor/r1s/internal/tunnel"
)

// TestLiveMeshRoundTrip is the F19-01 live-mesh acceptance leg: two embedded
// Yggdrasil nodes peer over a local TCP link, the client dials the allocator's
// overlay address with a pinned key, the routing preamble and an accept frame
// cross the mesh, and a payload larger than one packet round-trips over the
// default stream. The mesh (not RNS) carries every tunnel byte.
func TestLiveMeshRoundTrip(t *testing.T) {
	clientNode, allocatorNode := newLiveMeshPair(t)

	allocatorListener, err := NewListener(allocatorNode)
	if err != nil {
		t.Fatalf("allocator listener: %v", err)
	}
	defer func() { _ = allocatorListener.Close() }()
	advertisement := tunnel.Endpoint{
		Address: allocatorNode.AddressBytes(),
		PubKey:  append([]byte(nil), allocatorNode.PublicKey()...),
	}

	acceptDone := make(chan error, 1)
	var allocatorPair *pairConn
	go func() {
		incoming, err := allocatorListener.Accept()
		if err != nil {
			acceptDone <- err
			return
		}
		if incoming.Preamble() != (tunnel.Preamble{ExecutionID: "live-exec"}) {
			acceptDone <- errors.New("preamble mismatch")
			return
		}
		if !bytes.Equal(incoming.PeerKey(), clientNode.PublicKey()) {
			acceptDone <- errors.New("peer key mismatch")
			return
		}
		if err := incoming.Promote(liveSession(clientNode.PublicKey())); err != nil {
			acceptDone <- err
			return
		}
		allocatorPair = incoming.pair
		acceptDone <- nil
	}()

	dialer, err := NewDialer(clientNode)
	if err != nil {
		t.Fatalf("dialer: %v", err)
	}
	conn, err := dialer.Dial(context.Background(), advertisement)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	preambleWriter, ok := conn.(tunnel.PreambleWriter)
	if !ok {
		t.Fatal("the yggdrasil conn must implement PreambleWriter")
	}
	if err := preambleWriter.WritePreamble(tunnel.Preamble{ExecutionID: "live-exec"}); err != nil {
		t.Fatalf("write preamble: %v", err)
	}
	if err := <-acceptDone; err != nil {
		t.Fatalf("accept side: %v", err)
	}

	// The client opens a stream for container port 9000 and the allocator side
	// surfaces it.
	streamConn, err := conn.(tunnel.StreamOpener).OpenStream(9000)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	stream, err := allocatorPair.AcceptStream()
	if err != nil {
		t.Fatalf("accept stream: %v", err)
	}
	if stream.Target.Port != 9000 {
		t.Fatalf("stream target port = %d; want 9000", stream.Target.Port)
	}

	// Payload round-trip, larger than one packet: the mesh segments and
	// reassembles through the stream adapter.
	payload := bytes.Repeat([]byte("r1s-live-mesh-payload|"), 8192) // > 16 KB frames and multiple flow-control windows
	writeDone := make(chan error, 1)
	go func() {
		if _, err := streamConn.Write(payload); err != nil {
			writeDone <- err
			return
		}
		writeDone <- streamConn.CloseWrite()
	}()

	var got []byte
	buf := make([]byte, 4096)
	for len(got) < len(payload) {
		n, err := stream.Conn.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			t.Fatalf("allocator read after %d bytes: %v", len(got), err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("client write side: %v", err)
	}
}

// TestLiveMeshTwoStreams is the F19-01 multi-stream live leg: one authenticated
// pair carries two concurrent streams over the live mesh, both round-tripping
// >MTU payloads.
func TestLiveMeshTwoStreams(t *testing.T) {
	clientNode, allocatorNode := newLiveMeshPair(t)

	allocatorListener, err := NewListener(allocatorNode)
	if err != nil {
		t.Fatalf("allocator listener: %v", err)
	}
	defer func() { _ = allocatorListener.Close() }()
	advertisement := tunnel.Endpoint{
		Address: allocatorNode.AddressBytes(),
		PubKey:  append([]byte(nil), allocatorNode.PublicKey()...),
	}

	acceptDone := make(chan error, 1)
	var allocatorPair *pairConn
	go func() {
		incoming, err := allocatorListener.Accept()
		if err != nil {
			acceptDone <- err
			return
		}
		if err := incoming.Promote(liveSession(clientNode.PublicKey())); err != nil {
			acceptDone <- err
			return
		}
		allocatorPair = incoming.pair
		acceptDone <- nil
	}()

	dialer, err := NewDialer(clientNode)
	if err != nil {
		t.Fatalf("dialer: %v", err)
	}
	conn, err := dialer.Dial(context.Background(), advertisement)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if err := conn.(tunnel.PreambleWriter).WritePreamble(tunnel.Preamble{ExecutionID: "live-exec"}); err != nil {
		t.Fatalf("write preamble: %v", err)
	}
	if err := <-acceptDone; err != nil {
		t.Fatalf("accept side: %v", err)
	}

	// Two concurrent streams, one per container port.
	httpConn, err := conn.(tunnel.StreamOpener).OpenStream(8080)
	if err != nil {
		t.Fatalf("open 8080 stream: %v", err)
	}
	appConn, err := conn.(tunnel.StreamOpener).OpenStream(9000)
	if err != nil {
		t.Fatalf("open 9000 stream: %v", err)
	}

	streams := make([]*IncomingStream, 2)
	collectDone := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			s, err := allocatorPair.AcceptStream()
			if err != nil {
				collectDone <- err
				return
			}
			streams[i] = s
		}
		collectDone <- nil
	}()
	if err := <-collectDone; err != nil {
		t.Fatalf("collect streams: %v", err)
	}

	var allocHTTP, allocApp tunnel.Conn
	for _, s := range streams {
		switch s.Target.Port {
		case 8080:
			allocHTTP = s.Conn
		case 9000:
			allocApp = s.Conn
		default:
			t.Fatalf("unexpected stream target port: %d", s.Target.Port)
		}
	}
	if allocHTTP == nil || allocApp == nil {
		t.Fatal("expected one 8080 stream and one 9000 stream")
	}

	payloadA := bytes.Repeat([]byte("live-a-|"), 300)
	payloadB := bytes.Repeat([]byte("live-b-|"), 350)
	writeErr := make(chan error, 2)
	go func() { _, err := httpConn.Write(payloadA); writeErr <- err }()
	go func() { _, err := appConn.Write(payloadB); writeErr <- err }()

	gotA := readAllLive(t, allocHTTP, len(payloadA))
	gotB := readAllLive(t, allocApp, len(payloadB))
	if !bytes.Equal(gotA, payloadA) {
		t.Fatalf("http payload mismatch: got %d bytes", len(gotA))
	}
	if !bytes.Equal(gotB, payloadB) {
		t.Fatalf("app payload mismatch: got %d bytes", len(gotB))
	}
	for i := 0; i < 2; i++ {
		if err := <-writeErr; err != nil {
			t.Fatalf("live concurrent write: %v", err)
		}
	}
}

// liveSession builds the allocator-validated session for a live-mesh test.
func liveSession(clientPubKey []byte) *tunnel.Session {
	return &tunnel.Session{
		ExecutionID: "live-exec",
		PeerKey:     append([]byte(nil), clientPubKey...),
		Targets:     []tunnel.Target{{Port: 9000}, {Port: 8080}},
	}
}

func readAllLive(t *testing.T, conn tunnel.Conn, want int) []byte {
	t.Helper()
	var got []byte
	buf := make([]byte, 2048)
	for len(got) < want {
		n, err := conn.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			t.Fatalf("read after %d/%d bytes: %v", len(got), want, err)
		}
	}
	return got
}

// newLiveMeshPair starts two embedded nodes peered over a local TCP link.
func newLiveMeshPair(t *testing.T) (*Node, *Node) {
	t.Helper()
	clientSeed := []byte("live-mesh-client-seed-0000000000000000000000")
	allocatorSeed := []byte("live-mesh-allocator-seed-000000000000000000")

	clientNode, err := NewNode(clientSeed, ClientNodeKeyContext, NodeOptions{})
	if err != nil {
		t.Fatalf("client node: %v", err)
	}
	t.Cleanup(func() { _ = clientNode.Close() })
	allocatorNode, err := NewNode(allocatorSeed, AllocatorNodeKeyContext, NodeOptions{})
	if err != nil {
		t.Fatalf("allocator node: %v", err)
	}
	t.Cleanup(func() { _ = allocatorNode.Close() })

	linkListener, err := allocatorNode.Core().Listen(&url.URL{Scheme: "tcp", Host: "localhost:0"}, "")
	if err != nil {
		t.Fatalf("allocator link listen: %v", err)
	}
	t.Cleanup(linkListener.Cancel)
	if err := clientNode.Core().CallPeer(&url.URL{Scheme: "tcp", Host: linkListener.Addr().String()}, ""); err != nil {
		t.Fatalf("client call peer: %v", err)
	}
	waitForMesh(t, clientNode, allocatorNode)
	return clientNode, allocatorNode
}

// waitForMesh blocks until both nodes have each other in their tree, or the
// test times out.
func waitForMesh(t *testing.T, client, allocator *Node) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(client.Core().GetTree()) > 1 && len(allocator.Core().GetTree()) > 1 {
			time.Sleep(500 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("mesh did not converge within 15s")
}
