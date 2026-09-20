package yggdrasil

import (
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mytecor/r1s/internal/tunnel"
)

// TestListenerAcceptSurvivesMalformedPreamble is the regression guard for the
// F14-02 accept-loop means of failure: a peer sending a malformed first frame
// used to terminate the r1sd accept loop for every peer. Here two inbound
// sessions from two different peer keys are queued — one garbage, one valid —
// and a single Listener.Accept must drain the garbage (drop it as a malformed
// preamble) and continue to return the valid session, instead of surfacing the
// garbage error and terminating the loop.
func TestListenerAcceptSurvivesMalformedPreamble(t *testing.T) {
	// A bare mux is enough: Listener.Accept only touches edge.mux.accept and
	// the pending conns' readPreamble; no packet bus or Node is involved.
	// The conns need a packetIO only for its MTU (read at construction) and a
	// safe WriteTo (the garbage session's teardown writes a close frame), so a
	// stub supplies those without a real bus.
	m := newMux(nil)
	l := &Listener{edge: &edge{node: nil, mux: m}}

	garbageKey := []byte("garbage-peer-key-0000000000000000000")
	garbageConn := newStreamConn(newTestPacketIO(), garbageKey, garbageKey, m)
	garbageConn.mu.Lock()
	garbageConn.raw = []byte{'g', 0x7f, 0xff, 0x00, 0x01} // not a valid frame
	garbageConn.mu.Unlock()
	garbage := &pendingSession{
		conn:     garbageConn,
		remote:   string(garbageKey),
		deadline: time.Now().Add(pendingSessionTimeout),
	}

	validKey := []byte("valid-peer-key-00000000000000000000")
	validConn := newStreamConn(newTestPacketIO(), validKey, validKey, m)
	validPreamble := encodePreamble(tunnel.Preamble{ExecutionID: "exec-1", GrantID: "grant-1"})
	validConn.mu.Lock()
	validConn.raw = appendFrame(nil, frameTypePreamble, validPreamble)
	validConn.mu.Unlock()
	valid := &pendingSession{
		conn:     validConn,
		remote:   string(validKey),
		deadline: time.Now().Add(pendingSessionTimeout),
	}

	m.pending[garbage.remote] = garbage
	m.pending[valid.remote] = valid
	m.inbound <- garbage
	m.inbound <- valid

	// The single Accept call must skip the garbage and return the valid peer.
	done := make(chan error, 1)
	go func() {
		sess, err := l.Accept()
		if err != nil {
			done <- err
			return
		}
		if sess.Preamble() != (tunnel.Preamble{ExecutionID: "exec-1", GrantID: "grant-1"}) {
			done <- fmt.Errorf("preamble mismatch: %+v", sess.Preamble())
			return
		}
		if string(sess.PeerKey()) != string(validKey) {
			done <- fmt.Errorf("peer key mismatch")
			return
		}
		done <- sess.Promote()
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("accept loop died or stalled on a malformed preamble")
	}
	// The garbage pending must have been removed; the valid one promoted.
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.pending[garbage.remote]; ok {
		t.Fatal("garbage pending session was not dropped")
	}
	if _, ok := m.pending[valid.remote]; ok {
		t.Fatal("valid pending session was not promoted")
	}
	if _, ok := m.sessions[valid.remote]; !ok {
		t.Fatal("valid session was not promoted into the payload map")
	}
}

// testPacketIO is a minimal packetIO stub for handshake-level tests that never
// actually move packets: it reports an MTU and swallows writes.
type testPacketIO struct{}

func newTestPacketIO() *testPacketIO { return &testPacketIO{} }

func (t *testPacketIO) ReadFrom(p []byte) (int, net.Addr, error)  { return 0, nil, io.EOF }
func (t *testPacketIO) WriteTo(p []byte, a net.Addr) (int, error) { return len(p), nil }
func (t *testPacketIO) MTU() uint64                               { return maxPacketPayload }
func (t *testPacketIO) Close() error                              { return nil }
