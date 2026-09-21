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
// packetIO contract so the mux, the pair adapter, and the handshake are
// exercised end to end without the mesh.
type fakePacketIO struct {
	mu         sync.Mutex
	peer       *fakePacketIO
	remote     []byte // the source key stamped on packets this node reads
	inbox      chan []byte
	dropWrites bool // fail writes (mesh gone)
	mtu        uint64
}

// newFakePair builds two connected ends of one packet bus: writes on one are
// delivered to the other's inbox; packets arriving at one carry the peer's
// key as their source address.
func newFakePair(aKey, bKey string) (*fakePacketIO, *fakePacketIO) {
	a := &fakePacketIO{inbox: make(chan []byte, 1024)}
	b := &fakePacketIO{inbox: make(chan []byte, 1024)}
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
	if len(p) > int(f.MTU()) {
		return 0, errors.New("packet exceeds MTU")
	}
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
func (f *fakePacketIO) MTU() uint64 {
	if f.mtu != 0 {
		return f.mtu
	}
	return 1400
}

// Close implements packetIO.
func (f *fakePacketIO) Close() error { return nil }

var (
	clientKey    = bytes.Repeat([]byte("C"), 32)
	allocatorKey = bytes.Repeat([]byte("A"), 32)
)

// testSession builds a Session with a container port list matching what an
// allocator would authorize. There are no named slots: each target is just the
// port to splice to.
func testSession() *tunnel.Session {
	return &tunnel.Session{
		ExecutionID: "exec-1",
		PeerKey:     append([]byte(nil), clientKey...),
		Targets:     []tunnel.Target{{Port: 9000}, {Port: 8080}},
	}
}

// establishPair runs the client and allocator sides of a pair up to the point
// where streams can be opened: the client registers, writes the preamble, the
// allocator accepts and authorizes, and both sides are ready.
func establishPair(t *testing.T) (*pairConn, *pairConn) {
	return establishPairMTU(t, 1400)
}

func establishPairMTU(t *testing.T, mtu uint64) (*pairConn, *pairConn) {
	t.Helper()
	clientBus, allocatorBus := newFakePair(string(clientKey), string(allocatorKey))
	clientBus.mtu, allocatorBus.mtu = mtu, mtu
	clientMux := newMux(clientBus)
	go clientMux.readLoop()
	allocatorMux := newMux(allocatorBus)
	go allocatorMux.readLoop()

	client, err := clientMux.register(allocatorKey)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	acceptDone := make(chan error, 1)
	var allocatorPair *pairConn
	go func() {
		pending, err := allocatorMux.accept()
		if err != nil {
			acceptDone <- err
			return
		}
		preamble, err := pending.pair.readPreamble()
		if err != nil {
			acceptDone <- err
			return
		}
		if preamble != (tunnel.Preamble{ExecutionID: "exec-1", GrantID: "grant-1"}) {
			acceptDone <- errors.New("preamble mismatch")
			return
		}
		if err := allocatorMux.promote(pending); err != nil {
			acceptDone <- err
			return
		}
		allocatorPair = pending.pair
		acceptDone <- pending.pair.Authorize(testSession())
	}()

	if err := client.WritePreamble(tunnel.Preamble{ExecutionID: "exec-1", GrantID: "grant-1"}); err != nil {
		t.Fatalf("WritePreamble: %v", err)
	}
	if err := <-acceptDone; err != nil {
		t.Fatalf("accept side: %v", err)
	}
	t.Cleanup(func() { client.Close(); allocatorPair.Close() })
	return client, allocatorPair
}

// TestPairHandshakeAndStream verifies the pair preamble handshake and that a
// client-side OpenStream reaches the allocator authorized for its container
// port, round-tripping a payload larger than one packet.
func TestPairHandshakeAndStream(t *testing.T) {
	client, allocator := establishPair(t)

	// The client opens a stream for container port 9000.
	streamConn, err := client.OpenStream(9000)
	if err != nil {
		t.Fatalf("OpenStream(9000): %v", err)
	}
	// The allocator side sees one authorized stream for that port.
	stream, err := allocator.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}
	if stream.Target.Port != 9000 {
		t.Fatalf("stream target = %+v; want port 9000", stream.Target)
	}
	payload := bytes.Repeat([]byte("r1s-tunnel-payload|"), 200) // ~3.8 KB > MTU 1400
	writeDone := make(chan error, 1)
	go func() {
		_, err := streamConn.Write(payload)
		if err == nil {
			err = streamConn.CloseWrite()
		}
		writeDone <- err
	}()
	var got []byte
	buf := make([]byte, 1024)
	for len(got) < len(payload) {
		n, err := stream.Conn.Read(buf)
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
	if n, err := stream.Conn.Read(buf); err != io.EOF {
		t.Fatalf("read after EOF: n=%d err=%v; want EOF", n, err)
	}
}

// TestMultistreamConcurrentTransport is the F19 acceptance leg: one pair
// carries two concurrent, independent streams to a single execution, and both
// round-trip >MTU payloads without one blocking the other. Closing one stream
// does not affect the other.
func TestMultistreamConcurrentTransport(t *testing.T) {
	client, allocator := establishPair(t)

	// Open two concurrent streams for container ports 9000 and 8080.
	httpConn, err := client.OpenStream(8080)
	if err != nil {
		t.Fatalf("OpenStream(8080): %v", err)
	}
	appConn, err := client.OpenStream(9000)
	if err != nil {
		t.Fatalf("OpenStream(9000): %v", err)
	}

	// The allocator side accepts both streams.
	pipes := make(chan *IncomingStream, 2)
	acceptDone := make(chan error, 1)
	go func() {
		for i := 0; i < 2; i++ {
			s, err := allocator.AcceptStream()
			if err != nil {
				acceptDone <- err
				return
			}
			pipes <- s
		}
		acceptDone <- nil
	}()
	if err := <-acceptDone; err != nil {
		t.Fatalf("accept streams: %v", err)
	}

	first := <-pipes
	second := <-pipes
	var allocHTTP, allocApp tunnel.Conn
	for _, s := range []*IncomingStream{first, second} {
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

	// Both directions carry >MTU payloads concurrently: 8080 -> its port,
	// 9000 -> its port.
	payloadA := bytes.Repeat([]byte("stream-A-|"), 300) // ~2.9 KB
	payloadB := bytes.Repeat([]byte("stream-B-|"), 400) // ~3.9 KB

	writeErr := make(chan error, 2)
	go func() { _, err := httpConn.Write(payloadA); writeErr <- err }()
	go func() { _, err := appConn.Write(payloadB); writeErr <- err }()

	gotA := readAll(t, allocHTTP, len(payloadA))
	gotB := readAll(t, allocApp, len(payloadB))
	if !bytes.Equal(gotA, payloadA) {
		t.Fatalf("stream-A payload mismatch: got %d bytes", len(gotA))
	}
	if !bytes.Equal(gotB, payloadB) {
		t.Fatalf("stream-B payload mismatch: got %d bytes", len(gotB))
	}
	for i := 0; i < 2; i++ {
		if err := <-writeErr; err != nil {
			t.Fatalf("concurrent write: %v", err)
		}
	}

	// Closing one stream (full close) leaves the other usable.
	_ = httpConn.Close()
	if _, err := appConn.Write([]byte("still-alive")); err != nil {
		t.Fatalf("write after sibling close: %v", err)
	}
	if _, err := allocApp.Read(make([]byte, 64)); err != nil {
		t.Fatalf("sibling read after close: %v", err)
	}
}

// TestStreamTargetUnauthorized verifies a stream referencing a container port
// the allocator did not pre-authorize is rejected before any payload is
// consumed.
func TestStreamTargetUnauthorized(t *testing.T) {
	client, allocator := establishPair(t)

	// The session authorizes 9000 and 8080 only; 3306 must be rejected.
	badConn, err := client.OpenStream(3306)
	if err != nil {
		t.Fatalf("OpenStream(3306): %v", err)
	}
	// The allocator side must not surface the stream; it sends a close with
	// ReasonUnauthorized instead. The client's Read observes the session error.
	done := make(chan error, 1)
	go func() {
		_, rerr := badConn.Read(make([]byte, 64))
		done <- rerr
	}()
	select {
	case err := <-done:
		var sessionErr *tunnel.SessionError
		if !errors.As(err, &sessionErr) || sessionErr.Reason != tunnel.ReasonUnauthorized {
			t.Fatalf("read = %v; want ReasonUnauthorized", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unauthorized stream was not rejected")
	}

	// The allocator side surfaces only the authorized 9000 stream; the
	// unauthorized 3306 stream must never arrive for splice — it gets a
	// classified close straight back instead.
	authorizedConn, err := client.OpenStream(9000)
	if err != nil {
		t.Fatalf("OpenStream(9000): %v", err)
	}
	okStream, err := allocator.AcceptStream()
	if err != nil {
		t.Fatalf("accept authorized stream: %v", err)
	}
	if okStream.Target.Port != 9000 {
		t.Fatalf("authorized stream target port = %d; want 9000", okStream.Target.Port)
	}
	// The allocator side must never surface the unauthorized stream, and the
	// pair must still carry authorized streams.
	select {
	case is := <-allocator.incoming:
		t.Fatalf("unauthorized stream surfaced for splice: %+v", is)
	case <-time.After(300 * time.Millisecond):
	}
	if _, err := client.OpenStream(8080); err != nil {
		t.Fatalf("open authorized stream after rejection: %v", err)
	}
	_ = authorizedConn
	_ = okStream
}

// readAll reads exactly want bytes from conn, failing on premature EOF.
func readAll(t *testing.T, conn tunnel.Conn, want int) []byte {
	t.Helper()
	var got []byte
	buf := make([]byte, 1024)
	for len(got) < want {
		n, err := conn.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil {
			t.Fatalf("read after %d/%d bytes: %v", len(got), want, err)
		}
	}
	return got
}

// TestStreamConcurrentWriteAndCloseWithReason exercises the payload Write path
// racing a classified teardown on the same stream, under -race.
func TestStreamConcurrentWriteAndCloseWithReason(t *testing.T) {
	client, allocator := establishPair(t)
	clientConn, err := client.OpenStream(9000)
	if err != nil {
		t.Fatalf("OpenStream(9000): %v", err)
	}
	s, err := allocator.AcceptStream()
	if err != nil {
		t.Fatalf("AcceptStream: %v", err)
	}

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
			if _, err := clientConn.Write(payload); err != nil {
				writeErr <- err
				return
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	if rc, ok := s.Conn.(tunnel.ReasonCloser); ok {
		_ = rc.CloseWithReason(tunnel.ReasonSessionFailed, "teardown during a payload write")
	}
	close(stop)
	select {
	case <-writeErr:
	case <-time.After(5 * time.Second):
		t.Fatal("write hung after teardown")
	}
}
