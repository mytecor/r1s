package rns

// Regression tests for the tunnel RNS data plane (F21-01) transport fixes.
//
// These cover two Reticulum-Go v1.2.0 defects contained by the removable
// adapter in reticulum_compat.go:
//
//  1. Channel inbound reordering: the stock transport runs one packet-worker
//     goroutine per GOMAXPROCS; concurrent HandleInbound calls could drain
//     envelopes out of sequence order and append out-of-order bytes to the
//     shared RawChannelReader buffer. serializedInboundLink serializes the
//     complete per-Link ingress call. TestReorderMap and TestReorderLarge stress
//     this path with marker-carrying chunks.
//
//  2. Backbone hub TX-write race: the stock hub could clobber its own evWrite
//     interest when QueueSend raced the stream's write callback, stranding
//     bytes in the transmit buffer and stalling the link at ~10s. The adapter
//     selects Reticulum-Go's public synchronous BackendGo, which bypasses the
//     poller interest path. TestConnLargeCompressible stresses this with a full
//     1 MiB conn.Write path; TestConnChunkedFull and TestChunkMarkerCorruption
//     cover the same path through conn.Write chunking.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/buffer"
)

func TestReorderMap(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	clientCh := clientConn.(*conn).link.GetChannel()
	rawW := buffer.NewRawChannelWriter(streamID, clientCh)

	const chunk = 423
	const nChunks = 60
	payload := make([]byte, 0, nChunks*chunk)
	for k := 0; k < nChunks; k++ {
		word := []byte(fmt.Sprintf("C%03d-abcdefghijklmn", k))
		for len(payload) < (k+1)*chunk {
			payload = append(payload, word...)
			if len(payload) > (k+1)*chunk {
				payload = payload[:(k+1)*chunk]
			}
		}
	}

	// write all chunks without waiting
	wdone := make(chan error, 1)
	go func() {
		off := 0
		for off < len(payload) {
			end := off + chunk
			if end > len(payload) {
				end = len(payload)
			}
			n, err := rawW.Write(payload[off:end])
			if err != nil {
				wdone <- fmt.Errorf("write off=%d: %w", off, err)
				return
			}
			off += n
		}
		wdone <- nil
	}()

	got := make([]byte, 0, len(payload))
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32768)
		for len(got) < len(payload) {
			n, err := allocConn.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err != nil {
				readDone <- err
				return
			}
		}
		readDone <- nil
	}()
	select {
	case err := <-wdone:
		if err != nil {
			t.Fatalf("write batch: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("write timed out")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read: %v (got %d)", err, len(got))
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("read timed out (got %d)", len(got))
	}

	// map each got 423-chunk to its expected content by marker
	wantChunkOf := func(pos int) (int, bool) {
		// scan window for C%03
		for i := pos; i+4 <= len(got) && i < pos+30; i++ {
			if got[i] == 'C' && got[i+1] >= '0' && got[i+1] <= '9' {
				v := int(got[i+1]-'0')*100 + int(got[i+2]-'0')*10 + int(got[i+3]-'0')
				if v >= 0 && v < nChunks {
					return v, true
				}
			}
		}
		return -1, false
	}
	swaps := 0
	reports := []string{}
	for k := 0; k < nChunks; k++ {
		pos := k * chunk
		id, ok := wantChunkOf(pos)
		if !ok {
			reports = append(reports, fmt.Sprintf("chunk %d: no marker @%d", k, pos))
			continue
		}
		if id != k {
			swaps++
			if len(reports) < 20 {
				reports = append(reports, fmt.Sprintf("chunk %d expected got chunk %d", k, id))
			}
		}
	}
	t.Logf("nChunks=%d swaps=%d", nChunks, swaps)
	for _, r := range reports {
		t.Logf("  %s", r)
	}
	if swaps > 0 {
		t.Fail()
	}
}

// TestReorderLarge repeats TestReorderMap with 500 chunks, the same volume
// that produced 253 corrupted bytes before the packet-handler serialization fix.
func TestReorderLarge(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	clientCh := clientConn.(*conn).link.GetChannel()
	rawW := buffer.NewRawChannelWriter(streamID, clientCh)

	const chunk = 423
	const nChunks = 500
	payload := make([]byte, 0, nChunks*chunk)
	for k := 0; k < nChunks; k++ {
		word := []byte(fmt.Sprintf("C%03d-abcdefghijklmn", k))
		for len(payload) < (k+1)*chunk {
			payload = append(payload, word...)
			if len(payload) > (k+1)*chunk {
				payload = payload[:(k+1)*chunk]
			}
		}
	}

	wdone := make(chan error, 1)
	go func() {
		off := 0
		for off < len(payload) {
			end := off + chunk
			if end > len(payload) {
				end = len(payload)
			}
			n, err := rawW.Write(payload[off:end])
			if err != nil {
				wdone <- fmt.Errorf("write off=%d: %w", off, err)
				return
			}
			off += n
		}
		wdone <- nil
	}()
	got := make([]byte, 0, len(payload))
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32768)
		for len(got) < len(payload) {
			n, err := allocConn.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err != nil {
				readDone <- err
				return
			}
		}
		readDone <- nil
	}()
	select {
	case err := <-wdone:
		if err != nil {
			t.Fatalf("write batch: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("write timed out")
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read: %v (got %d)", err, len(got))
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("read timed out (got %d)", len(got))
	}

	mm := 0
	first := -1
	for i := 0; i < len(payload); i++ {
		if got[i] != payload[i] {
			mm++
			if first < 0 {
				first = i
			}
		}
	}
	if mm == 0 {
		t.Logf("OK: %d chunks (%d bytes) exact", nChunks, len(payload))
		return
	}
	t.Logf("FAIL: %d chunks mm=%d first=%d (chunk %d)", nChunks, mm, first, first/chunk)
	t.Fail()
}

// TestConnLargeCompressible sends 1 MiB of compressible data through the full
// conn.Write path, stressing window pressure with the serialized packet worker.
func TestConnLargeCompressible(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	const n = 4 * 256 * 1024 // 1 MiB
	payload := bytes.Repeat([]byte("compressible-payload-0123456789abcdef"), n/31)

	got := make([]byte, 0, len(payload))
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32768)
		for len(got) < len(payload) {
			n2, err := allocConn.Read(buf)
			if n2 > 0 {
				got = append(got, buf[:n2]...)
			}
			if err != nil {
				readDone <- err
				return
			}
		}
		readDone <- nil
	}()
	if w, err := clientConn.Write(payload); err != nil {
		t.Fatalf("Write: n=%d err=%v", w, err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read: %v (got %d bytes)", err, len(got))
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("read timed out (got %d bytes)", len(got))
	}
	if !bytes.Equal(got, payload) {
		mm := 0
		first := -1
		for i := 0; i < len(payload); i++ {
			if got[i] != payload[i] {
				mm++
				if first < 0 {
					first = i
				}
			}
		}
		t.Fatalf("mismatch: mm=%d first=%d", mm, first)
	}
	t.Logf("OK: %d MiB compressible exact", len(payload)/(1024*1024))
}

func analyzeMismatch(t *testing.T, label string, got, want []byte) {
	t.Helper()
	if bytes.Equal(got, want) {
		t.Logf("%s: OK (%d bytes)", label, len(want))
		return
	}
	mm := 0
	first := -1
	last := -1
	for i := 0; i < len(want) && i < len(got); i++ {
		if got[i] != want[i] {
			mm++
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	t.Logf("%s: mismatches=%d first=%d last=%d (want len %d, got len %d)",
		label, mm, first, last, len(want), len(got))
	// Show a window around the first mismatch.
	win := 32
	if first >= 0 {
		lo := first - win/2
		if lo < 0 {
			lo = 0
		}
		hi := first + win
		if hi > len(want) {
			hi = len(want)
		}
		t.Logf("  around first(%d):", first)
		t.Logf("  want %q", want[lo:hi])
		if hi <= len(got) {
			t.Logf("  got  %q", got[lo:hi])
		}
	}
}

// TestRawChunkedIsolation writes the payload in 423-byte chunks through a
// fresh RawChannelWriter on the client's channel, and reads through the
// allocator conn's OWN buffered reader. With one reader per channel this
// exercises exactly the stock RawChannelWriter->Channel->RawChannelReader
// compression path with the same chunking conn.Write would do, minus conn.Write.
func TestRawChunkedIsolation(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	clientCh := clientConn.(*conn).link.GetChannel()
	// A second writer on the same channel is fine: the conn's own writer is
	// never exercised in this test, so sequence numbers do not interleave.
	rawW := buffer.NewRawChannelWriter(streamID, clientCh)

	payload := bytes.Repeat([]byte("payload-bytes-0123456789"), 8*1024) // ~256 KiB
	wdone := make(chan error, 1)
	go func() {
		off := 0
		for off < len(payload) {
			end := off + 423
			if end > len(payload) {
				end = len(payload)
			}
			n, err := rawW.Write(payload[off:end])
			if err != nil {
				wdone <- fmt.Errorf("rawW.Write at off=%d: %w", off, err)
				return
			}
			if n != end-off {
				wdone <- fmt.Errorf("short write at off=%d: n=%d want %d", off, n, end-off)
				return
			}
			off += n
		}
		wdone <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := make([]byte, 0, len(payload))
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32768)
		deadline := time.Now().Add(20 * time.Second)
		for len(got) < len(payload) {
			if time.Now().After(deadline) {
				readDone <- fmt.Errorf("read timeout after %d bytes", len(got))
				return
			}
			n, err := allocConn.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err != nil {
				readDone <- err
				return
			}
		}
		readDone <- nil
	}()

	select {
	case err := <-wdone:
		if err != nil {
			t.Fatalf("write: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("write timed out: %v", ctx.Err())
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read: %v (got %d bytes)", err, len(got))
		}
	case <-ctx.Done():
		t.Fatalf("read timed out: %v", ctx.Err())
	}
	analyzeMismatch(t, "raw-chunked", got, payload)
}

// TestConnChunkedFull reproduces the real conn.Write path over the full payload.
func TestConnChunkedFull(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	payload := bytes.Repeat([]byte("payload-bytes-0123456789"), 8*1024)
	got := make([]byte, 0, len(payload))
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32768)
		deadline := time.Now().Add(20 * time.Second)
		for len(got) < len(payload) {
			if time.Now().After(deadline) {
				readDone <- fmt.Errorf("read timeout after %d bytes", len(got))
				return
			}
			n, err := allocConn.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err != nil {
				readDone <- err
				return
			}
		}
		readDone <- nil
	}()

	if n, err := clientConn.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read: %v (got %d bytes)", err, len(got))
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("read timed out (got %d bytes)", len(got))
	}
	analyzeMismatch(t, "conn-full", got, payload)
}

// TestChunkMarkerCorruption writes chunk k with an all-distinct body so any
// reordering, duplication, or loss of a chunk boundary is visible: chunk k
// carries byte pattern where every 24-byte word is "chunk-####-123..." with
// #### = k. Reading must reproduce the exact write order.
func TestChunkMarkerCorruption(t *testing.T) {
	listener := newTestListener(t, "allocator")
	dialer := newTestDialer(t, "client")
	allocConn, clientConn := connect(t, dialer, listener)
	defer allocConn.Close()
	defer clientConn.Close()

	const chunk = 423
	payload := make([]byte, 0, 20*chunk)
	for k := 0; k < 20; k++ {
		word := []byte(fmt.Sprintf("CHUNK-%03d-0123456789abc", k)) // 24 bytes
		for len(payload) < (k+1)*chunk {
			payload = append(payload, word...)
			if len(payload) > (k+1)*chunk {
				payload = payload[:(k+1)*chunk]
			}
		}
	}

	got := make([]byte, 0, len(payload))
	readDone := make(chan error, 1)
	go func() {
		buf := make([]byte, 32768)
		for len(got) < len(payload) {
			n, err := allocConn.Read(buf)
			if n > 0 {
				got = append(got, buf[:n]...)
			}
			if err != nil {
				readDone <- err
				return
			}
		}
		readDone <- nil
	}()
	if n, err := clientConn.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write: n=%d err=%v", n, err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatalf("read: %v (got %d bytes)", err, len(got))
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("read timed out (got %d bytes)", len(got))
	}
	if bytes.Equal(got, payload) {
		t.Logf("OK: exact round-trip (%d bytes)", len(payload))
		return
	}
	// Locate first divergence.
	first := -1
	for i := 0; i < len(payload) && i < len(got); i++ {
		if got[i] != payload[i] {
			first = i
			break
		}
	}
	t.Logf("FAIL: len got=%d want=%d first divergence=%d (chunk %d)", len(got), len(payload), first, first/chunk)
	if first >= 0 {
		lo := first - 20
		if lo < 0 {
			lo = 0
		}
		t.Logf("  want %q", payload[lo:first+60])
		t.Logf("  got  %q", got[lo:first+60])
	}
	t.Fail()
}
