package main

import (
	"errors"
	"net"
	"testing"
	"time"
)

type failedReadTunnel struct{ net.Conn }

func (failedReadTunnel) Read([]byte) (int, error) { return 0, errors.New("session revoked") }
func (failedReadTunnel) PeerKey() []byte          { return []byte("peer") }
func (c failedReadTunnel) CloseRead() error       { return c.Close() }
func (c failedReadTunnel) CloseWrite() error      { return c.Close() }

func TestRelayFailureClosesBlockedTarget(t *testing.T) {
	client, peer := net.Pipe()
	target, server := net.Pipe()
	defer client.Close()
	defer peer.Close()
	defer target.Close()
	defer server.Close()
	done := make(chan struct{})
	go func() { relay(failedReadTunnel{client}, target); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("relay kept waiting on target after tunnel failure")
	}
	if _, err := server.Write([]byte("late data")); err == nil {
		t.Fatal("target connection still open")
	}
}
