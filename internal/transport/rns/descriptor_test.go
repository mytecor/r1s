package rns

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

func TestDescriptorRoundTrip(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 32)
	descriptor, err := newDescriptor(clusterID, map[string]uint32{"gpu": 2, "default": 1}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := descriptor.marshal()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseDescriptor(data)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Protocol != protocolVersion || parsed.ClusterID != descriptor.ClusterID || parsed.Capacity["gpu"] != 2 || parsed.Capacity["default"] != 1 {
		t.Fatalf("parsed descriptor = %+v", parsed)
	}
}

// F16-01: the descriptor carries the bounded placement summary (os/arch/runtime)
// so a client can filter before sending, and parseDescriptor preserves it.
func TestDescriptorCarriesNodeSummary(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 32)
	node := &r1sv1.NodeCapabilities{
		Os: "linux", Arch: "amd64", Runtime: "runc",
		// The full advertisement (devices, labels, profiles) must NOT leak into
		// the announce summary: only os/arch/runtime fit the app-data budget.
		Devices: []string{"nvidia/tesla"}, Labels: map[string]string{"tier": "edge"},
		ResourceProfiles: []string{"default"},
	}
	descriptor, err := newDescriptor(clusterID, map[string]uint32{"default": 1}, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.OS != "linux" || descriptor.Arch != "amd64" || descriptor.Runtime != "runc" {
		t.Fatalf("descriptor summary = %+v", descriptor)
	}
	data, err := descriptor.marshal()
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseDescriptor(data)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.OS != "linux" || parsed.Arch != "amd64" || parsed.Runtime != "runc" {
		t.Fatalf("parsed summary = %+v", parsed)
	}
}

// F16-01: the descriptor (os/arch/runtime summary + capacity) must always fit
// the announce app-data budget (maxDescriptorBytes). marshal enforces the cap,
// so an oversized hand-built descriptor is rejected rather than advertised.
func TestDescriptorFitsAnnounceBudget(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 32)
	realistic := &r1sv1.NodeCapabilities{Os: "linux", Arch: "amd64", Runtime: "runc"}
	descriptor, err := newDescriptor(clusterID, map[string]uint32{"default": 1, "gpu": 2}, realistic, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := descriptor.marshal()
	if err != nil {
		t.Fatalf("realistic descriptor must marshal: %v", err)
	}
	if len(data) > maxDescriptorBytes {
		t.Fatalf("descriptor = %d bytes, exceeds %d", len(data), maxDescriptorBytes)
	}
	// Four capacity classes still fit even with the summary populated.
	wide := &r1sv1.NodeCapabilities{Os: "linux", Arch: "arm64", Runtime: "runc"}
	descriptor, err = newDescriptor(clusterID, map[string]uint32{"a": 1, "b": 1, "c": 1, "d": 1}, wide, nil)
	if err != nil {
		t.Fatal(err)
	}
	if data, err = descriptor.marshal(); err != nil {
		t.Fatalf("four-class descriptor must still fit: %v", err)
	}
	if len(data) > maxDescriptorBytes {
		t.Fatalf("descriptor = %d bytes, exceeds %d", len(data), maxDescriptorBytes)
	}
}

func TestDescriptorRejectsInvalidAndOversizedData(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`{"protocol":"other","cluster_id":"4242424242424242424242424242424242424242424242424242424242424242","capacity":{"default":1}}`),
		[]byte(`{"protocol":"r1s.v1","cluster_id":"bad","capacity":{"default":1}}`),
		[]byte(`{"protocol":"r1s.v1","cluster_id":"4242424242424242424242424242424242424242424242424242424242424242","capacity":{"default":0}}`),
		[]byte(strings.Repeat("x", maxDescriptorBytes+1)),
	} {
		if _, err := parseDescriptor(data); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatalf("parseDescriptor(%q) error = %v", data, err)
		}
	}
}

// F21-02: the descriptor advertises the minimum to create a private tunnel RNS
// transport (allocator Ygg IPv6 host:port + tunnel RNS destination hash), and
// never a Ygg public key.
func TestDescriptorCarriesTunnelAdvertisement(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 32)
	adv := &TunnelAdvertisement{Host: "201:1db8::1", Port: 4242, DestinationHash: "00112233445566778899aabbccddeeff"}
	descriptor, err := newDescriptor(clusterID, map[string]uint32{"default": 1}, nil, adv)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.TunnelHost != "201:1db8::1" || descriptor.TunnelPort != 4242 || descriptor.TunnelDestination != "00112233445566778899aabbccddeeff" {
		t.Fatalf("tunnel advertisement = %+v", descriptor)
	}
	data, err := descriptor.marshal()
	if err != nil {
		t.Fatalf("descriptor with tunnel advertisement must marshal: %v", err)
	}
	parsed, err := parseDescriptor(data)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.TunnelHost != adv.Host || parsed.TunnelPort != adv.Port || parsed.TunnelDestination != adv.DestinationHash {
		t.Fatalf("parsed tunnel advertisement = %+v", parsed)
	}
}

// F21-02: a descriptor without a configured tunnel edge advertises none; a
// tunnel port without a host is rejected.
func TestDescriptorTunnelAbsentOrMalformed(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 32)
	descriptor, err := newDescriptor(clusterID, map[string]uint32{"default": 1}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if descriptor.TunnelPort != 0 || descriptor.TunnelHost != "" || descriptor.TunnelDestination != "" {
		t.Fatalf("no-tunnel descriptor = %+v", descriptor)
	}
	if _, err := newDescriptor(clusterID, map[string]uint32{"default": 1}, nil, &TunnelAdvertisement{Port: 4242}); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatalf("tunnel port without host error = %v, want ErrInvalidDescriptor", err)
	}
}
