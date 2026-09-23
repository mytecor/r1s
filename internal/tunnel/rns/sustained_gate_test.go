package rns

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"
	"time"
)

// TestSustainedTransferOutlivesStaleTime documents a defect found by the F21-05
// benchmark and pins its boundary so the fix (upstream Reticulum-Go or the
// compat adapter) unskips it automatically.
//
// Context. On this private tunnel transport the Links run over a raw loopback
// Backbone/TCP interface with an RTT far below KeepaliveMaxRTT (1.75s).
// Reticulum-Go v1.2.0 computes keepalive = clamp(rtt * Keepalive/KeepaliveMaxRTT,
// KeepaliveMinSec=5s, Keepalive=360s) and staleTime = keepalive * 2, so on a
// low-RTT link keepalive floors at 5s and staleTime is exactly 10s.
//
// During a sustained one-direction transfer the writer side receives no
// inbound data; its lastInbound ages only as fast as the peer's Channel
// ACK/proof traffic refreshes it. On this loopback path that signal does not
// refresh quickly enough, and the writer's watchdog CASes its Link ACTIVE ->
// STALE at exactly staleTime (10s). WaitReady then returns ErrLinkNotReady and
// the transfer dies mid-stream. The allocator side stays ACTIVE because it
// receives the continuous data.
//
// Measured (2026-09-23, macOS loopback, Reticulum-Go v1.2.0 + compat adapter):
// a 10 MiB single-stream transfer fails with "link not ready" at ~10.0s with
// ~6.4 MiB written; the client Link flips STALE at that instant while the
// allocator Link remains ACTIVE. Transfers that complete inside staleTime
// (1 MiB ~2.3s, 4 MiB ~6.7s, 10x 1 MiB concurrent ~9.4s) pass.
//
// The BackendGo evWrite workaround (BACKLOG entry 11) does NOT cover this: it
// addresses a write-interest stall, not the keepalive/staleness timeout.
//
// This test is skip-by-default (R1S_TEST_SUSTAINED_TUNNEL=1) so it never runs
// inside `make check`, which does not pass -short. It spends ~11s to reproduce
// the defect; run it explicitly to gate the fix once one is landed.
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
	// measured ~0.6 MiB/s single-stream rate on loopback.
	const size = 10 << 20
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 31)
	}

	wdone := make(chan error, 1)
	go func() {
		wdone <- writeAllTest(clientConn, payload)
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
	timeout := 90 * time.Second
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
