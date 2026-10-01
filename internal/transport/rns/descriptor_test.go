package rns

import (
	"bytes"
	"context"
	"errors"
	"testing"

	meshbus "github.com/mytecor/meshbus/core"
	meshrns "github.com/mytecor/meshbus/transport/rns"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

func TestDescriptorMetadataRoundTrip(t *testing.T) {
	descriptor, err := newDescriptor(
		map[string]uint32{"gpu/large": 2, "default": 1},
		&r1sv1.NodeCapabilities{Os: "linux", Arch: "amd64", Runtime: "runc"},
		&TunnelAdvertisement{Host: "201:1db8::1", Port: 4242, DestinationHash: "00112233445566778899aabbccddeeff"},
	)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseDescriptorMetadata(descriptor.metadata())
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Capacity["gpu/large"] != 2 || parsed.Capacity["default"] != 1 ||
		parsed.OS != "linux" || parsed.Arch != "amd64" || parsed.Runtime != "runc" ||
		parsed.TunnelHost != descriptor.TunnelHost || parsed.TunnelPort != descriptor.TunnelPort ||
		parsed.TunnelDestination != descriptor.TunnelDestination {
		t.Fatalf("parsed descriptor = %+v", parsed)
	}
}

func TestDescriptorMetadataRejectsInvalidValues(t *testing.T) {
	for _, metadata := range []map[string]string{
		{},
		{metadataCapacity: "default=0"},
		{metadataCapacity: "default=bad"},
		{metadataCapacity: "default=1", metadataTunnelHost: "::1"},
		{metadataCapacity: "default=1", metadataTunnelPort: "70000", metadataTunnelHost: "::1"},
	} {
		if _, err := parseDescriptorMetadata(metadata); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatalf("parseDescriptorMetadata(%v) error = %v", metadata, err)
		}
	}
	if _, err := newDescriptor(map[string]uint32{"": 1}, nil, nil); !errors.Is(err, ErrInvalidDescriptor) {
		t.Fatalf("empty class error = %v", err)
	}
}

func TestDescriptorMetadataOmitsDisabledTunnel(t *testing.T) {
	descriptor, err := newDescriptor(map[string]uint32{"default": 1}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata := descriptor.metadata()
	if metadata[metadataTunnelHost] != "" || metadata[metadataTunnelPort] != "" || metadata[metadataTunnelDestination] != "" {
		t.Fatalf("disabled tunnel metadata = %v", metadata)
	}
}

func TestDescriptorMetadataFitsMeshbusPresence(t *testing.T) {
	descriptor, err := newDescriptor(
		map[string]uint32{"a": 1, "b": 1, "c": 1, "d": 1},
		&r1sv1.NodeCapabilities{Os: "linux", Arch: "arm64", Runtime: "runc"},
		&TunnelAdvertisement{Host: "2001:0db8:0000:0000:0000:0000:0000:0001", Port: 4242, DestinationHash: "00112233445566778899aabbccddeeff"},
	)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := meshrns.New(meshrns.Config{
		EphemeralIdentity: true,
		RealmKey:          bytes.Repeat([]byte{0x42}, 32),
		PresenceMetadata:  descriptor.metadata(),
		Handler:           func(context.Context, meshbus.ReceivedMessage) error { return nil },
	})
	if err != nil {
		t.Fatalf("realistic r1s metadata does not fit meshbus presence: %v", err)
	}
	_ = endpoint.Close()
}
