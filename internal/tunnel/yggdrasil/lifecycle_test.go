package yggdrasil

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/mytecor/r1s/internal/tunnel"
)

func TestFullFramesRoundTrip(t *testing.T) {
	for _, mtu := range []uint64{1400, maxPacketPayload, 65535} {
		t.Run(fmt.Sprint(mtu), func(t *testing.T) {
			client, allocator := establishPairMTU(t, mtu)
			sender, err := client.OpenStream(9000)
			if err != nil {
				t.Fatal(err)
			}
			receiver, err := allocator.AcceptStream()
			if err != nil {
				t.Fatal(err)
			}
			// Exercise several complete receive windows as well as frame boundaries.
			payload := bytes.Repeat([]byte("x"), 4*streamWindowSize+7)
			done := make(chan error, 1)
			go func() {
				got := make([]byte, len(payload))
				_, err := io.ReadFull(receiver.Conn, got)
				if err == nil && !bytes.Equal(payload, got) {
					err = errors.New("payload differs")
				}
				done <- err
			}()
			written := make(chan error, 1)
			go func() { _, err := sender.Write(payload); written <- err }()
			for _, ch := range []chan error{written, done} {
				select {
				case err := <-ch:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("transfer stalled")
				}
			}
		})
	}
}

func TestPairRejectionPreservesReason(t *testing.T) {
	bus, _ := newFakePair("a", "b")
	pair := newPair(bus, []byte("b"), []byte("b"), nil)
	pair.ingest(appendFrame(nil, frameTypeClose, streamIDNone, closePayload(tunnel.ReasonGrantExpired, "expired")))
	err := pair.awaitAccept(time.Second)
	var sessionErr *tunnel.SessionError
	if !errors.As(err, &sessionErr) || sessionErr.Reason != tunnel.ReasonGrantExpired {
		t.Fatalf("rejection: %v", err)
	}
}

func TestPairCloseUnblocksStreams(t *testing.T) {
	client, allocator := establishPair(t)
	sender, err := client.OpenStream(9000)
	if err != nil {
		t.Fatal(err)
	}
	_, err = allocator.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	read := make(chan error, 1)
	write := make(chan error, 1)
	go func() { _, err := sender.Read(make([]byte, 1)); read <- err }()
	go func() { _, err := sender.Write(make([]byte, 2*streamWindowSize)); write <- err }()
	allocator.CloseWithReason(tunnel.ReasonExecutionEnded, "finished")
	select {
	case err := <-read:
		var se *tunnel.SessionError
		if !errors.As(err, &se) || se.Reason != tunnel.ReasonExecutionEnded {
			t.Fatalf("read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader still blocked")
	}
	select {
	case err := <-write:
		if err == nil {
			t.Fatal("write succeeded after close")
		}
	case <-time.After(time.Second):
		t.Fatal("writer still blocked")
	}
	if _, err := client.OpenStream(8080); err == nil {
		t.Fatal("closed pair accepted a new stream")
	}
}

func TestRegistryInvalidationClosesPair(t *testing.T) {
	client, allocator := establishPair(t)
	registry, _ := tunnel.NewRegistry(tunnel.RegistryConfig{NewID: func() string { return "g" }})
	now := time.Now()
	grant, err := registry.Mint("e", clientKey, []tunnel.Target{{Port: 9000}}, tunnel.Endpoint{}, time.Minute, now)
	if err != nil {
		t.Fatal(err)
	}
	session, err := registry.Accept("e", grant.ID, clientKey, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.Authorize(session); err != nil {
		t.Fatal(err)
	}
	sender, err := client.OpenStream(9000)
	if err != nil {
		t.Fatal(err)
	}
	_, err = allocator.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	registry.Invalidate("e")
	if _, ok := session.ResolveTarget(9000); ok {
		t.Fatal("revoked session still resolves a target")
	}
	select {
	case <-client.stopped:
	case <-time.After(time.Second):
		t.Fatal("revocation never reached client")
	}
	if _, err := sender.Read(make([]byte, 1)); err == nil {
		t.Fatal("revoked stream readable")
	}
	if _, err := client.OpenStream(9000); err == nil {
		t.Fatal("revoked pair opened a stream")
	}
}
