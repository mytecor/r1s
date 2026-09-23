package rns

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/link"
)

// TestSustainedTransferOutlivesStaleTime is the acceptance gate for the F21-05
// blocker: a single-stream transfer that outlives the v1.2.0 keepalive
// staleTime window (10s on low-RTT Links) must complete byte-for-byte without
// "link not ready". It stays env-gated (R1S_TEST_SUSTAINED_TUNNEL=1) because
// it needs a transfer long enough to outlive staleTime, and it is meant to be
// run explicitly to gate the fix, never inside `make check`.
//
// Timing (2026-09-23, macOS loopback, Reticulum-Go v1.2.0 + compat adapter).
// The uncompressed-writer workaround raises loopback throughput to ~4 MiB/s,
// so the original 10 MiB payload no longer crosses staleTime. The 64 MiB gate
// takes ~13s unraced and therefore still proves the Link remains ACTIVE beyond
// the 10s watchdog boundary. It remains env-gated because its packet volume is
// unsuitable for the default suite, especially under the race detector.
//
// Context. On this private tunnel transport the Links run over a raw loopback
// Backbone/TCP interface with an RTT far below KeepaliveMaxRTT (1.75s).
// Reticulum-Go v1.2.0 computes keepalive = clamp(rtt * Keepalive/KeepaliveMaxRTT,
// KeepaliveMinSec=5s, Keepalive=360s) and staleTime = keepalive * 2, so on a
// low-RTT link keepalive floors at 5s and staleTime is exactly 10s.
//
// During a sustained one-direction transfer the writer side receives no inbound
// data packets: its Channel ACK/proof traffic is validated by the transport
// (handleProofPacket) without ever calling Link.HandleInbound, so the watchdog's
// lastInbound ages untouched and the writer's Link is CASed ACTIVE -> STALE at
// exactly staleTime (10s). WaitReady then returns ErrLinkNotReady and the
// transfer dies mid-stream. The allocator side stays ACTIVE because it receives
// the continuous data.
//
// The compat adapter fixes this by having every edge send a tiny keepalive
// Channel frame to the peer every tunnelKeepaliveInterval (see
// newReticulumCompatStream): the peer's Link.HandleInbound receives it as
// ordinary inbound data and refreshes its lastInboundNs, exactly the signal the
// watchdog needs. The BackendGo evWrite workaround (BACKLOG entry 11) does NOT
// cover this: it addresses a write-interest stall, not the keepalive/staleness
// timeout.
//
// Historical reproduction (2026-09-23, before the uncompressed-writer
// workaround): without the beacon the 10 MiB transfer failed with "link not
// ready" at ~10.0s and ~6.4 MiB written. The current 64 MiB payload preserves
// the same >10s liveness contract at the faster data rate. Because the defect
// is that the writer's Link goes STALE, this test also samples the client
// (writer) Link status throughout and asserts it never leaves ACTIVE, so a
// transfer that merely survives through another path cannot pass it.
func TestSustainedTransferOutlivesStaleTime(t *testing.T) {
	if os.Getenv("R1S_TEST_SUSTAINED_TUNNEL") != "1" {
		t.Skip("set R1S_TEST_SUSTAINED_TUNNEL=1 to gate the sustained >10s tunnel transfer")
	}
	listener := newTestListener(t, "allocator")
	defer listener.Close()
	dialer := newTestDialer(t, "client")
	defer dialer.Close()
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	// Sized so the transfer necessarily outlives staleTime (10s) at the
	// measured ~4 MiB/s uncompressed single-stream rate on loopback.
	const size = 64 << 20
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 31)
	}

	wdone := make(chan error, 1)
	go func() {
		wdone <- writeAllTest(clientConn, payload)
	}()

	// Monitor the client (writer) Link status for the whole transfer. The
	// defect this test gates is precisely the client Link being CASed
	// ACTIVE -> STALE by the v1.2.0 keepalive watchdog at staleTime (10s)
	// during a one-way transfer; a successful byte transfer alone would not
	// distinguish "link never staled" from "link staled and then some other
	// path revived it". Sampling the status pins the invariant directly.
	clientLink := clientConn.(*conn).link
	var wentStale atomic.Bool
	stop := make(chan struct{})
	pollDone := make(chan struct{})
	go func() {
		defer close(pollDone)
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if clientLink.GetStatus() != link.StatusActive {
					wentStale.Store(true)
				}
			}
		}
	}()

	got := make([]byte, 0, size)
	rdone := make(chan error, 1)
	go func() {
		buf := make([]byte, 64*1024)
		for len(got) < size {
			n, err := allocConn.Read(buf)
			got = append(got, buf[:n]...)
			if err != nil {
				rdone <- err
				return
			}
		}
		rdone <- nil
	}()

	start := time.Now()
	// Deadlock-safety net for the unraced gate (see timing comment above); a
	// regression that truly deadlocks — rather than merely being slow under
	// `-race`, which this gate is never run under — trips within minutes.
	timeout := 5 * time.Minute
	select {
	case err := <-wdone:
		if err != nil {
			t.Fatalf("client write (%s) failed: %v", time.Since(start).Round(time.Millisecond), err)
		}
	case <-time.After(timeout):
		t.Fatalf("write timed out after %s", time.Duration(timeout))
	}
	select {
	case err := <-rdone:
		if err != nil {
			t.Fatalf("allocator read: %v", err)
		}
	case <-time.After(timeout):
		t.Fatalf("read timed out after %s", time.Duration(timeout))
	}
	close(stop)
	<-pollDone
	if wentStale.Load() {
		t.Fatal("client tunnel Link left ACTIVE during the sustained transfer (keepalive/staleness watchdog fired); the compat liveness beacon is not keeping the writer alive")
	}
	if !bytes.Equal(got, payload) {
		mm, first := 0, -1
		for i := range payload {
			if i >= len(got) || got[i] != payload[i] {
				mm++
				if first < 0 {
					first = i
				}
			}
		}
		t.Fatalf("payload mismatch: mm=%d first=%d (got %d of %d)", mm, first, len(got), size)
	}
	t.Logf("sustained %d MiB transfer completed in %s", size>>20, time.Since(start).Round(time.Millisecond))
}

var errShortWriteTest = errors.New("short write")

func writeAllTest(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if n > 0 {
			p = p[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return errShortWriteTest
		}
	}
	return nil
}
