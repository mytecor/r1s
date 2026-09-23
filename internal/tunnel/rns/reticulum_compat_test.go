package rns

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/backbone"
	"github.com/Quad4-Software/Reticulum-Go/pkg/buffer"
	"github.com/Quad4-Software/Reticulum-Go/pkg/channel"
)

func TestReticulumCompatIsIsolatedAndInstalled(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	hub := backbone.Get()
	if hub == nil {
		t.Fatal("Reticulum-Go Backbone hub was not initialised")
	}
	if got := hub.Backend(); got != backbone.BackendGo {
		t.Fatalf("Backbone backend = %q, want temporary compatibility backend %q", got, backbone.BackendGo)
	}

	checks := []struct {
		name      string
		transport *stack
		conn      Conn
	}{
		{name: "allocator", transport: listener.stack, conn: allocConn},
		{name: "client", transport: dialer.stack, conn: clientConn},
	}
	for _, check := range checks {
		registered := check.transport.transport.FindLink(check.conn.(*conn).link.GetLinkID())
		wrapped, ok := registered.(*serializedInboundLink)
		if !ok {
			t.Fatalf("%s registered Link = %T, want *serializedInboundLink", check.name, registered)
		}
		if wrapped.link != check.conn.(*conn).link {
			t.Fatalf("%s compatibility proxy wraps a different Link instance", check.name)
		}
	}
}

// TestSustainedKeepaliveBeaconInstalled is the removal contract for the
// liveness beacon (BACKLOG entry 10). It asserts both tunnel edges wire the
// periodic keepalive on every Conn, so deleting the beacon from
// newReticulumCompatStream without deleting this test fails loudly. It also
// pins the invariants the beacon depends on: the keepalive message type is a
// user-range type distinct from the stream's system StreamDataMessageType, so
// a beacon frame can never be mistaken for stream bytes.
func TestSustainedKeepaliveBeaconInstalled(t *testing.T) {
	if compatKeepaliveType >= channel.SystemMessageTypeMin {
		t.Fatalf("compatKeepaliveType 0x%04x collides with the system-reserved range (>= 0x%04x)", compatKeepaliveType, channel.SystemMessageTypeMin)
	}
	if compatKeepaliveType == buffer.StreamDataMessageType {
		t.Fatalf("compatKeepaliveType 0x%04x collides with StreamDataMessageType", compatKeepaliveType)
	}

	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	checks := []struct {
		name string
		conn Conn
	}{
		{name: "allocator", conn: allocConn},
		{name: "client", conn: clientConn},
	}
	for _, check := range checks {
		s := check.conn.(*conn).stream
		if !s.keepaliveInstalled() {
			t.Fatalf("%s tunnel stream has no liveness beacon installed", check.name)
		}
	}
}

// TestSustainedKeepaliveDoesNotCorruptStream exercises the beacon's wire
// behaviour without waiting for a tick: it injects keepalive frames directly
// through each side's Channel while stream data is in flight, and requires
// the payload to round-trip byte-exact. The keepalive and stream envelopes
// share the Channel's single per-link sequence space, so this is precisely the
// interleaving the beacon produces for the lifetime of a link; a corrupt dump
// (keepalive bytes leaking into the stream, reordering, or lost data) fails
// here. Fast and race-safe, so it runs under `go test -race` without the
// R1S_TEST_SUSTAINED_TUNNEL gate.
func TestSustainedKeepaliveDoesNotCorruptStream(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	clientCh := clientConn.(*conn).link.GetChannel()
	allocCh := allocConn.(*conn).link.GetChannel()

	payload := bytes.Repeat([]byte("keepalive-interleave-0123456789"), 8192) // ~256 KiB
	wdone := make(chan error, 1)
	go func() {
		// Interleave a beacon frame before each 4 KiB chunk; the real beacon
		// sends one every tunnelKeepaliveInterval, but this hammers the same
		// shared-sequence path far harder.
		for off := 0; off < len(payload); {
			if err := clientCh.Send(&channel.GenericMessage{Type: compatKeepaliveType, Data: []byte{0}}); err != nil {
				wdone <- fmt.Errorf("send keepalive at off=%d: %w", off, err)
			}
			end := off + 4096
			if end > len(payload) {
				end = len(payload)
			}
			n, err := clientConn.Write(payload[off:end])
			if err != nil {
				wdone <- fmt.Errorf("write at off=%d: %w", off, err)
				return
			}
			if n != end-off {
				wdone <- fmt.Errorf("short write at off=%d: n=%d want %d", off, n, end-off)
				return
			}
			off = end
		}
		// Also inject a beacon from the allocator side mid-stream to prove the
		// peer direction's beacon frames do not disturb the client stream.
		_ = allocCh.Send(&channel.GenericMessage{Type: compatKeepaliveType, Data: []byte{0}})
		wdone <- nil
	}()

	got := make([]byte, 0, len(payload))
	rdone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32768)
		deadline := time.Now().Add(20 * time.Second)
		for len(got) < len(payload) {
			if time.Now().After(deadline) {
				rdone <- fmt.Errorf("read timeout after %d bytes", len(got))
				return
			}
			n, err := allocConn.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err != nil {
				rdone <- err
				return
			}
		}
		rdone <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	select {
	case err := <-wdone:
		if err != nil {
			t.Fatalf("writer: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("writer timed out: %v", ctx.Err())
	}
	select {
	case err := <-rdone:
		if err != nil {
			t.Fatalf("reader: %v (got %d bytes)", err, len(got))
		}
	case <-ctx.Done():
		t.Fatalf("reader timed out: %v", ctx.Err())
	}
	analyzeMismatch(t, "keepalive-interleave", got, payload)
}
