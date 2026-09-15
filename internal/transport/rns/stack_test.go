package rns

import (
	"testing"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
)

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
