package tunnel

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// TestMemoryPipeHalfClose verifies per-direction half-close: after the client
// close-writes, the allocator reads a clean EOF while the allocator can still
// reply; the client cannot write anymore afterward.
func TestMemoryPipeHalfClose(t *testing.T) {
	clientKey := []byte("client-edge-key-0000000000000000")
	allocKey := []byte("allocator-edge-key-00000000000000")
	client, got := newConnectedPipe(clientKey, allocKey)

	defer client.Close()
	defer got.Close()

	// Round trip: client -> allocator, allocator -> client.
	sent := "hello from client"
	if _, err := client.Write([]byte(sent)); err != nil {
		t.Fatalf("client write: %v", err)
	}
	buf := make([]byte, 64)
	n, err := got.Read(buf)
	if err != nil || string(buf[:n]) != sent {
		t.Fatalf("allocator read = %q, %v; want %q", buf[:n], err, sent)
	}

	// Client half-closes its write: allocator should see EOF after drain.
	if err := client.CloseWrite(); err != nil {
		t.Fatalf("client CloseWrite: %v", err)
	}
	_, err = got.Read(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("allocator expected EOF after client CloseWrite, got err=%v", err)
	}

	// The reverse direction still works: allocator can write back.
	if _, err := got.Write([]byte("reply")); err != nil {
		t.Fatalf("allocator write after client close-write: %v", err)
	}
	n, err = client.Read(buf)
	if err != nil || string(buf[:n]) != "reply" {
		t.Fatalf("client read = %q, %v; want reply", buf[:n], err)
	}

	// Client can no longer write after close-write.
	if _, err := client.Write([]byte("x")); err == nil {
		t.Fatal("client write after CloseWrite succeeded")
	}
}

// TestMemoryDialPinsPeerKey verifies the in-memory mesh enforces peer-key
// pinning at dial time: a client whose key does not match the listener's pin
// cannot connect.
func TestMemoryDialPinsPeerKey(t *testing.T) {
	broker := NewMemoryBroker()
	allocKey := []byte("alloc-key-000000000000000000")
	_ = broker.Listen([]byte("ygg-addr"), []byte("allowed-client"))

	if _, err := broker.Dial(context.Background(), Endpoint{Address: []byte("ygg-addr"), PubKey: allocKey}, []byte("allowed-client")); err != nil {
		t.Fatalf("dial with matching key failed: %v", err)
	}
	if _, err := broker.Dial(context.Background(), Endpoint{Address: []byte("ygg-addr"), PubKey: allocKey}, []byte("other-client")); err == nil {
		t.Fatal("dial with mismatched peer key succeeded")
	} else if !errors.Is(err, ErrPeerKeyMismatch) {
		t.Fatalf("dial mismatch error = %v; want ErrPeerKeyMismatch", err)
	}
}

// TestMemoryDialUnknownEndpoint verifies a dial to an unlisted address fails
// clearly (mesh unreachable).
func TestMemoryDialUnknownEndpoint(t *testing.T) {
	broker := NewMemoryBroker()
	if _, err := broker.Dial(context.Background(), Endpoint{Address: []byte("nope")}, []byte("k")); err == nil {
		t.Fatal("dial to unknown endpoint succeeded")
	} else if !errors.Is(err, ErrMeshUnreachable) {
		t.Fatalf("error = %v; want ErrMeshUnreachable", err)
	}
}

// TestMemoryCloseWithReason verifies the allocator side can end a session with
// a classified reason that the client's relay observes as a *SessionError, and
// that any buffered output is delivered BEFORE the reason surfaces (the
// ReasonCloser drain contract).
func TestMemoryCloseWithReason(t *testing.T) {
	clientKey := []byte("client-edge-key-0000000000000000")
	allocKey := []byte("allocator-edge-key-00000000000000")
	client, allocator := newConnectedPipe(clientKey, allocKey)
	defer client.Close()

	if _, err := allocator.Write([]byte("partial")); err != nil {
		t.Fatalf("allocator write: %v", err)
	}
	if err := allocator.CloseWithReason(ReasonExecutionEnded, "workload completed"); err != nil {
		t.Fatalf("CloseWithReason: %v", err)
	}
	// Buffered bytes are drained before the reason is observable.
	buf := make([]byte, 64)
	n, err := client.Read(buf)
	if err != nil || string(buf[:n]) != "partial" {
		t.Fatalf("client Read before reason = %q, %v; want buffered output then reason", buf[:n], err)
	}
	_, err = client.Read(buf)
	var sessionErr *SessionError
	if !errors.As(err, &sessionErr) {
		t.Fatalf("client Read after CloseWithReason = %v; want *SessionError", err)
	}
	if sessionErr.Reason != ReasonExecutionEnded {
		t.Fatalf("reason = %q; want execution_ended", sessionErr.Reason)
	}
}

// TestMemoryListenerCloseReleasesAccept verifies the listener releases a
// blocked Accept on Close.
func TestMemoryListenerCloseReleasesAccept(t *testing.T) {
	broker := NewMemoryBroker()
	listener := broker.Listen([]byte("a"), nil)
	done := make(chan error, 1)
	go func() {
		_, err := listener.Accept()
		done <- err
	}()
	_ = listener.Close()
	select {
	case err := <-done:
		if !errors.Is(err, ErrListenerClosed) {
			t.Fatalf("Accept after Close = %v; want ErrListenerClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Accept did not release on Close")
	}
}

// TestMemoryPipeDoesNotPreamble verifies the in-memory fake deliberately does
// NOT implement PreambleWriter: the test relay must stay byte-clean. The real
// framing/yggdrasil edge implements it; the fake keeps the CLI<->serve relay
// deterministic and payload-only.
func TestMemoryPipeDoesNotPreamble(t *testing.T) {
	clientKey := []byte("client-edge-key-0000000000000000")
	allocKey := []byte("allocator-edge-key-00000000000000")
	client, _ := newConnectedPipe(clientKey, allocKey)
	defer client.Close()
	if _, ok := interface{}(client).(PreambleWriter); ok {
		t.Fatal("in-memory pipe unexpectedly implements PreambleWriter; relay must stay byte-clean")
	}
}
