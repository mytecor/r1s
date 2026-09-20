package yggdrasil

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/mytecor/r1s/internal/tunnel"
)

// TestLiveMeshRoundTrip is the F14-02 live-mesh acceptance leg: two embedded
// Yggdrasil nodes peer over a local TCP link, the client dials the allocator's
// overlay address with a pinned key, the routing preamble and an accept frame
// cross the mesh, and a payload larger than one packet round-trips. The mesh
// (not RNS) carries every tunnel byte; no RNS process participates.
func TestLiveMeshRoundTrip(t *testing.T) {
	clientSeed := []byte("live-mesh-client-seed-0000000000000000000000")
	allocatorSeed := []byte("live-mesh-allocator-seed-000000000000000000")

	clientNode, err := NewNode(clientSeed, ClientNodeKeyContext, NodeOptions{})
	if err != nil {
		t.Fatalf("client node: %v", err)
	}
	defer func() { _ = clientNode.Close() }()
	allocatorNode, err := NewNode(allocatorSeed, AllocatorNodeKeyContext, NodeOptions{})
	if err != nil {
		t.Fatalf("allocator node: %v", err)
	}
	defer func() { _ = allocatorNode.Close() }()

	// Peer the two nodes over a local TCP link: the allocator listens, the
	// client calls it once. This stands in for the overlay bootstrap; in a
	// real deployment the peer URIs come from edge configuration.
	linkListener, err := allocatorNode.Core().Listen(&url.URL{Scheme: "tcp", Host: "localhost:0"}, "")
	if err != nil {
		t.Fatalf("allocator link listen: %v", err)
	}
	defer linkListener.Cancel()
	if err := clientNode.Core().CallPeer(&url.URL{Scheme: "tcp", Host: linkListener.Addr().String()}, ""); err != nil {
		t.Fatalf("client call peer: %v", err)
	}
	waitForMesh(t, clientNode, allocatorNode)

	// The allocator edge listens on its overlay address; the client dials it
	// with the pinned key from the (would-be) grant advertisement.
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
	allocatorConn := make(chan tunnel.Conn, 1)
	go func() {
		incoming, err := allocatorListener.Accept()
		if err != nil {
			acceptDone <- err
			return
		}
		if incoming.Preamble() != (tunnel.Preamble{ExecutionID: "live-exec", GrantID: "live-grant"}) {
			acceptDone <- errors.New("preamble mismatch")
			return
		}
		if !bytes.Equal(incoming.PeerKey(), clientNode.PublicKey()) {
			acceptDone <- errors.New("peer key mismatch")
			return
		}
		if err := incoming.Promote(); err != nil {
			acceptDone <- err
			return
		}
		allocatorConn <- incoming.Conn()
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
	if err := preambleWriter.WritePreamble(tunnel.Preamble{ExecutionID: "live-exec", GrantID: "live-grant"}); err != nil {
		t.Fatalf("write preamble: %v", err)
	}
	if err := <-acceptDone; err != nil {
		t.Fatalf("accept side: %v", err)
	}

	// Payload round-trip, larger than one packet: the mesh segments and
	// reassembles through the stream adapter.
	payload := bytes.Repeat([]byte("r1s-live-mesh-payload|"), 400) // ~8.8 KB > packet MTU
	writeDone := make(chan error, 1)
	go func() {
		if _, err := conn.Write(payload); err != nil {
			writeDone <- err
			return
		}
		writeDone <- conn.CloseWrite()
	}()

	stream := <-allocatorConn
	var got []byte
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("allocator read after %d bytes: %v", len(got), err)
		}
		if len(got) >= len(payload) {
			break
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("client write side: %v", err)
	}
}

// waitForMesh blocks until both nodes have each other in their tree, or the
// test times out. The tree reflects the mesh's routing knowledge; a session
// dial before the tree converges would wait at the mesh layer instead.
func waitForMesh(t *testing.T, client, allocator *Node) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(client.Core().GetTree()) > 1 && len(allocator.Core().GetTree()) > 1 {
			// Paths settle shortly after the tree converges.
			time.Sleep(500 * time.Millisecond)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("mesh did not converge within 15s")
}
