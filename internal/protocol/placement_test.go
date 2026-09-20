package protocol_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

func TestValidateCapabilities(t *testing.T) {
	valid := &r1sv1.NodeCapabilities{
		Os: "linux", Arch: "amd64", Runtime: "runc",
		ResourceProfiles: []string{"default", "gpu"},
		Devices:          []string{"nvidia/tesla"},
		Labels:           map[string]string{"tier": "edge"},
		CachedImages:     []string{"example.test/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
	if err := protocol.ValidateCapabilities(valid); err != nil {
		t.Fatalf("valid capabilities rejected: %v", err)
	}
	if err := protocol.ValidateCapabilities(nil); err != nil {
		t.Fatalf("nil capabilities rejected: %v", err)
	}

	tests := map[string]func(*r1sv1.NodeCapabilities){
		"os uppercase":    func(n *r1sv1.NodeCapabilities) { n.Os = "Linux" },
		"arch invalid":    func(n *r1sv1.NodeCapabilities) { n.Arch = "amd64 " },
		"too many labels": func(n *r1sv1.NodeCapabilities) { n.Labels = manyLabels(protocol.MaxCapabilityLabels + 1) },
		"label key too long": func(n *r1sv1.NodeCapabilities) {
			n.Labels = map[string]string{strings.Repeat("k", protocol.MaxCapabilityKeyLen+1): "v"}
		},
		"label value invalid": func(n *r1sv1.NodeCapabilities) { n.Labels = map[string]string{"k": "UPPER"} },
		"too many devices": func(n *r1sv1.NodeCapabilities) {
			n.Devices = manyStrings(protocol.MaxCapabilityDevices+1, "dev")
		},
		"device invalid": func(n *r1sv1.NodeCapabilities) { n.Devices = []string{"bad device"} },
		"too many profiles": func(n *r1sv1.NodeCapabilities) {
			n.ResourceProfiles = manyStrings(protocol.MaxCapabilityProfiles+1, "prof")
		},
		"too many cached images": func(n *r1sv1.NodeCapabilities) {
			n.CachedImages = manyStrings(protocol.MaxCapabilityCache+1, "example.test/img:tag")
		},
		"cached image too long": func(n *r1sv1.NodeCapabilities) {
			n.CachedImages = []string{strings.Repeat("x", protocol.MaxCapabilityCacheImage+1)}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			node := &r1sv1.NodeCapabilities{
				Os: "linux", Arch: "amd64", Runtime: "runc",
				ResourceProfiles: []string{"default"},
				Devices:          []string{"nvidia/tesla"},
				Labels:           map[string]string{"tier": "edge"},
				CachedImages:     []string{"example.test/app:tag"},
			}
			mutate(node)
			if err := protocol.ValidateCapabilities(node); err == nil {
				t.Fatalf("ValidateCapabilities() = nil, want error")
			}
		})
	}
}

func TestValidateConstraintsBoundsAtClientBoundary(t *testing.T) {
	if err := protocol.ValidateConstraints(nil); err != nil {
		t.Fatalf("nil constraints rejected: %v", err)
	}
	if err := protocol.ValidateConstraints(&r1sv1.PlacementConstraints{
		Os: "linux", Arch: "arm64", Runtime: "runc",
		Labels: map[string]string{"tier": "edge"}, Devices: []string{"nvidia/tesla"},
	}); err != nil {
		t.Fatalf("valid constraints rejected: %v", err)
	}
	if err := protocol.ValidateConstraints(&r1sv1.PlacementConstraints{
		Labels: map[string]string{strings.Repeat("k", protocol.MaxCapabilityKeyLen+1): "v"},
	}); err == nil {
		t.Fatal("oversized label key accepted")
	}
	if err := protocol.ValidateConstraints(&r1sv1.PlacementConstraints{
		Devices: manyStrings(protocol.MaxCapabilityDevices+1, "dev"),
	}); err == nil {
		t.Fatal("too many device constraints accepted")
	}
	if err := protocol.ValidateConstraints(&r1sv1.PlacementConstraints{Os: "Linux"}); err == nil {
		t.Fatal("non-lowercase os constraint accepted")
	}
}

func TestPlacementMatches(t *testing.T) {
	node := &r1sv1.NodeCapabilities{
		Os: "linux", Arch: "amd64", Runtime: "runc",
		Devices: []string{"nvidia/tesla", "gpu0"},
		Labels:  map[string]string{"tier": "edge", "zone:west": "1"},
	}
	if !protocol.PlacementMatches(nil, node) {
		t.Fatal("nil constraints must match any node")
	}
	if !protocol.PlacementMatches(&r1sv1.PlacementConstraints{}, node) {
		t.Fatal("empty constraints must match")
	}
	if !protocol.PlacementMatches(&r1sv1.PlacementConstraints{Os: "linux", Arch: "amd64"}, node) {
		t.Fatal("matching os/arch rejected")
	}
	if !protocol.PlacementMatches(&r1sv1.PlacementConstraints{
		Labels: map[string]string{"tier": "edge"}, Devices: []string{"gpu0"},
	}, node) {
		t.Fatal("matching labels/devices rejected")
	}
	if protocol.PlacementMatches(&r1sv1.PlacementConstraints{Os: "windows"}, node) {
		t.Fatal("os mismatch not detected")
	}
	if protocol.PlacementMatches(&r1sv1.PlacementConstraints{Arch: "arm64"}, node) {
		t.Fatal("arch mismatch not detected")
	}
	if protocol.PlacementMatches(&r1sv1.PlacementConstraints{Runtime: "wasm"}, node) {
		t.Fatal("runtime mismatch not detected")
	}
	if protocol.PlacementMatches(&r1sv1.PlacementConstraints{Labels: map[string]string{"tier": "core"}}, node) {
		t.Fatal("label mismatch not detected")
	}
	if protocol.PlacementMatches(&r1sv1.PlacementConstraints{Devices: []string{"nvidia/a100"}}, node) {
		t.Fatal("device missing not detected")
	}
	if protocol.PlacementMatches(&r1sv1.PlacementConstraints{}, nil) {
		t.Fatal("non-empty request must not match an unknown node")
	}
	if !protocol.PlacementMatches(nil, nil) {
		t.Fatal("nil constraints must match an unknown node")
	}
}

func TestPlacementCompatibleIsAdvisory(t *testing.T) {
	// A nil node is unknown, not incompatible: the allocator is still tried.
	if !protocol.PlacementCompatible(&r1sv1.PlacementConstraints{Os: "linux"}, nil) {
		t.Fatal("unknown node must remain compatible")
	}
	// Positive evidence of mismatch skips the send.
	if protocol.PlacementCompatible(&r1sv1.PlacementConstraints{Os: "linux"}, &r1sv1.NodeCapabilities{Os: "windows"}) {
		t.Fatal("advertised mismatch must be incompatible")
	}
	// Empty constraints always match.
	if !protocol.PlacementCompatible(&r1sv1.PlacementConstraints{}, &r1sv1.NodeCapabilities{Os: "windows"}) {
		t.Fatal("empty constraints must match any node")
	}
	if !protocol.PlacementCompatible(nil, &r1sv1.NodeCapabilities{Os: "windows"}) {
		t.Fatal("nil constraints must match any node")
	}
}

func TestPlacementConflictDetails(t *testing.T) {
	detail := protocol.PlacementConflictDetails(&r1sv1.PlacementConstraints{
		Os: "linux", Arch: "arm64", Runtime: "runc",
		Labels: map[string]string{"tier": "core"}, Devices: []string{"nvidia/a100"},
	}, &r1sv1.NodeCapabilities{Os: "windows", Runtime: "wasm", Labels: map[string]string{"tier": "edge"}})
	for _, want := range []string{"os=linux", "arch=arm64", "runtime=runc", "label=tier=core", "device=nvidia/a100"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("conflict detail %q missing %q", detail, want)
		}
	}
	// A conflict detail must never be empty for a genuine mismatch.
	if detail == "" {
		t.Fatal("conflict detail must not be empty")
	}
}

func TestCapabilitiesEqual(t *testing.T) {
	a := &r1sv1.NodeCapabilities{Os: "linux", Arch: "amd64", Labels: map[string]string{"tier": "edge"}}
	b := &r1sv1.NodeCapabilities{Os: "linux", Arch: "amd64", Labels: map[string]string{"tier": "edge"}}
	if !protocol.CapabilitiesEqual(a, b) {
		t.Fatal("equal capabilities reported unequal")
	}
	if protocol.CapabilitiesEqual(a, &r1sv1.NodeCapabilities{Os: "windows"}) {
		t.Fatal("different os reported equal")
	}
	if protocol.CapabilitiesEqual(a, &r1sv1.NodeCapabilities{Os: "linux", Arch: "amd64", Labels: map[string]string{"tier": "core"}}) {
		t.Fatal("different label reported equal")
	}
	if !protocol.CapabilitiesEqual(nil, nil) {
		t.Fatal("nil capabilities must be equal")
	}
	if protocol.CapabilitiesEqual(nil, a) {
		t.Fatal("nil and non-nil must differ")
	}
	if !errors.Is(protocol.ErrInvalidEnvelope, protocol.ErrInvalidEnvelope) {
		t.Fatalf("error sentinel sanity")
	}
}

func manyLabels(count int) map[string]string {
	labels := make(map[string]string, count)
	for index := 0; index < count; index++ {
		labels[fmt.Sprintf("k%d", index)] = "v"
	}
	return labels
}

func manyStrings(count int, prefix string) []string {
	values := make([]string, count)
	for index := range values {
		values[index] = prefix + string(rune('a'+index%26))
	}
	return values
}
