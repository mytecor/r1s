package yggdrasil

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	iwt "github.com/Arceliar/ironwood/types"

	"github.com/mytecor/r1s/internal/tunnel"
)

// fakePacketIO is a deterministic in-memory packet bus standing in for the
// mesh: one node reads what the other writes, addressed by key. It honors the
// packetIO contract so the mux, the stream adapter, and the handshake are
// exercised end to end without the mesh.
type fakePacketIO struct {
	mu         sync.Mutex
	peer       *fakePacketIO
	remote     []byte // the source key stamped on packets this node reads
	inbox      chan []byte
	dropWrites bool // fail writes (mesh gone)
}

// newFakePair builds two connected ends of one packet bus: writes on one are
// delivered to the other's inbox; packets arriving at one carry the peer's
// key as their source address.
func newFakePair(aKey, bKey string) (*fakePacketIO, *fakePacketIO) {
	a := &fakePacketIO{inbox: make(chan []byte, 256)}
	b := &fakePacketIO{inbox: make(chan []byte, 256)}
	a.peer, b.peer = b, a
	a.remote, b.remote = []byte(bKey), []byte(aKey)
	return a, b
}

// ReadFrom implements packetIO.
func (f *fakePacketIO) ReadFrom(p []byte) (int, net.Addr, error) {
	packet, ok := <-f.inbox
	if !ok {
		return 0, nil, io.EOF
	}
	addr := make(iwt.Addr, len(f.remote))
	copy(addr, f.remote)
	n := copy(p, packet)
	return n, addr, nil
}

func (f *fakePacketIO) WriteTo(p []byte, addr net.Addr) (int, error) {
	f.mu.Lock()
	dropping := f.dropWrites
	f.mu.Unlock()
	if dropping {
		return 0, errors.New("mesh unreachable")
	}
	if os.Getenv("R1S_TRACE") != "" {
		println("WriteTo", f.remote, "type", p[0], "len", len(p))
	}
	select {
	case f.peer.inbox <- append([]byte(nil), p...):
		return len(p), nil
	default:
		return 0, errors.New("fake bus overflow")
	}
}

// MTU implements packetIO: a small MTU proves segmentation and reassembly.
func (f *fakePacketIO) MTU() uint64 { return 1400 }

// Close implements packetIO.
func (f *fakePacketIO) Close() error { return nil }

var (
	clientKey    = bytes.Repeat([]byte("C"), 32)
	allocatorKey = bytes.Repeat([]byte("A"), 32)
)

// TestStreamRoundTripThroughMux runs the whole edge path over the fake packet
// bus: two muxes, the dial + preamble + accept handshake, then a payload
// round-trip larger than one packet (segmentation and reassembly).
func TestStreamRoundTripThroughMux(t *testing.T) {
	clientBus, allocatorBus := newFakePair(string(clientKey), string(allocatorKey))
	clientMux := newMux(clientBus)
	go clientMux.readLoop()
	allocatorMux := newMux(allocatorBus)
	go allocatorMux.readLoop()

	client, err := clientMux.register(allocatorKey)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	// A payload Write before the routing preamble must fail on the client.
	if _, err := client.Write([]byte("payload")); err == nil {
		t.Fatal("payload write accepted before the routing preamble")
	}

	acceptDone := make(chan error, 1)
	// The accept goroutine mirrors the r1sd edge loop: take the pending
	// session, read the routing preamble, promote, and confirm with the
	// accept frame.
	go func() {
		incoming, err := allocatorMux.accept()
		if err != nil {
			acceptDone <- err
			return
		}
		preamble, err := incoming.conn.readPreamble()
		if err != nil {
			acceptDone <- err
			return
		}
		if preamble != (tunnel.Preamble{ExecutionID: "exec-1", GrantID: "grant-1"}) {
			acceptDone <- errors.New("preamble mismatch")
			return
		}
		if err := allocatorMux.promote(incoming); err != nil {
			acceptDone <- err
			return
		}
		acceptDone <- incoming.conn.writeAccept()
	}()

	preambleErr := make(chan error, 1)
	go func() {
		preambleErr <- client.WritePreamble(tunnel.Preamble{ExecutionID: "exec-1", GrantID: "grant-1"})
	}()
	for i := 0; i < 2; i++ {
		select {
		case err := <-acceptDone:
			if err != nil {
				t.Fatalf("accept side failed: %v", err)
			}
		case err := <-preambleErr:
			if err != nil {
				t.Fatalf("WritePreamble: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("handshake stalled")
		}
	}

	// Payload round-trip, larger than one packet to force segmentation.
	payload := bytes.Repeat([]byte("r1s-tunnel-payload|"), 200) // ~3.8 KB > MTU 1400
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(payload)
		if err == nil {
			err = client.CloseWrite()
		}
		writeDone <- err
	}()

	var got []byte
	buf := make([]byte, 1024)
	allocatorMux.mu.Lock()
	allocatorConn := allocatorMux.sessions[string(clientKey)]
	allocatorMux.mu.Unlock()
	if allocatorConn == nil {
		t.Fatal("no promoted session on the allocator side")
	}
	for len(got) < len(payload) {
		n, err := allocatorConn.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			t.Fatalf("read after %d bytes: %v", len(got), err)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload round-trip mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("client write side: %v", err)
	}
	// The client's write-EOF surfaces as io.EOF after the payload is drained.
	if n, err := allocatorConn.Read(buf); err != io.EOF {
		t.Fatalf("read after EOF: n=%d err=%v; want EOF", n, err)
	}
}

// TestStreamConcurrentWriteAndCloseWithReason exercises the payload Write path
// racing a classified teardown on the same session, under -race. The teardown
// (end -> writeFrame) must assemble its close frame into its own buffer, never
// the shared c.writeScratch owned by the payload Write, so the two must not
// race on the assembly buffer.
func TestStreamConcurrentWriteAndCloseWithReason(t *testing.T) {
	clientBus, _ := newFakePair(string(clientKey), string(allocatorKey))
	clientMux := newMux(clientBus)
	go clientMux.readLoop()
	client, err := clientMux.register(allocatorKey)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	// Allow payload writes (the client edge normally sets this in
	// WritePreamble; here we set it directly to skip the handshake).
	client.mu.Lock()
	client.preambleOK = true
	client.mu.Unlock()

	stop := make(chan struct{})
	writeErr := make(chan error, 1)
	payload := bytes.Repeat([]byte("x"), 4096)
	go func() {
		for {
			select {
			case <-stop:
				writeErr <- nil
				return
			default:
			}
			if _, err := client.Write(payload); err != nil {
				writeErr <- err
				return
			}
		}
	}()

	// Race a classified teardown against the write stream.
	time.Sleep(20 * time.Millisecond)
	_ = client.CloseWithReason(tunnel.ReasonSessionFailed, "teardown during a payload write")
	close(stop)

	select {
	case <-writeErr:
		// A write either completed before the teardown or failed after it;
		// either is fine. The point is that -race reports no data race on the
		// shared assembly buffer.
	case <-time.After(5 * time.Second):
		t.Fatal("write hung after teardown")
	}
}
