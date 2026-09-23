package rns

import (
	"testing"

	"github.com/Quad4-Software/Reticulum-Go/pkg/backbone"
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
