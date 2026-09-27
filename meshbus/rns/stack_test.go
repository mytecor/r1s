package rns

import (
	"errors"
	"net"
	"testing"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/sharedinstance"
	rnstransport "github.com/Quad4-Software/Reticulum-Go/pkg/transport"
)

func TestProductionStackRequiresSharedInstanceClientMode(t *testing.T) {
	value, err := newStack(nil)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	value.connect = func(*rnstransport.Transport) (*sharedinstance.Instance, error) {
		called = true
		return &sharedinstance.Instance{Mode: sharedinstance.ModeClient}, nil
	}
	if err := value.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = value.Close() })
	if !called {
		t.Fatal("production stack did not require the shared-instance connector")
	}
	if value.shared == nil || value.shared.Mode != sharedinstance.ModeClient {
		t.Fatalf("shared-instance mode = %v, want ModeClient", value.shared)
	}
	if len(value.interfaces) != 0 || len(value.started) != 0 {
		t.Fatalf("production stack constructed standalone interfaces: configured=%d started=%d", len(value.interfaces), len(value.started))
	}
}

func TestRequiredSharedInstanceFailsClosedWithoutListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().(*net.TCPAddr)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	transport := rnstransport.NewTransport(common.NewReticulumConfig())
	if _, err := connectSharedInstanceAt(transport, address.Port, "", false); !errors.Is(err, ErrSharedInstanceUnavailable) {
		t.Fatalf("connectSharedInstanceAt() error = %v, want %v", err, ErrSharedInstanceUnavailable)
	}
	probe, err := net.Listen("tcp", address.String())
	if err != nil {
		t.Fatalf("client-only attach left a listener behind: %v", err)
	}
	_ = probe.Close()
}

func TestNormalizeUDPInterfaceConfig(t *testing.T) {
	original := &common.InterfaceConfig{
		Type: "UDPInterface", Address: "127.0.0.1", Port: 4242,
		TargetHost: "127.0.0.1", TargetPort: 4243,
	}
	normalized := normalizeInterfaceConfig(original)
	if normalized.Address != "127.0.0.1:4242" || normalized.TargetHost != "127.0.0.1:4243" {
		t.Fatalf("normalized interface = %+v", normalized)
	}
	if original.Address != "127.0.0.1" || original.TargetHost != "127.0.0.1" {
		t.Fatalf("input configuration was mutated: %+v", original)
	}
}

func TestNormalizeUDPInterfacePrefersTargetAddress(t *testing.T) {
	original := &common.InterfaceConfig{
		Type: "UDPInterface", Address: "[::1]:4242",
		TargetHost: "ignored", TargetAddress: "::1", TargetPort: 4243,
	}
	normalized := normalizeInterfaceConfig(original)
	if normalized.Address != "[::1]:4242" || normalized.TargetHost != "[::1]:4243" {
		t.Fatalf("normalized interface = %+v", normalized)
	}
}
