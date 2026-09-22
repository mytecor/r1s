// Package rns implements authenticated r1s envelope delivery over Reticulum-Go.
package rns

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

const (
	protocolVersion    = "r1s.v1"
	maxDescriptorBytes = 256
)

var ErrInvalidDescriptor = errors.New("invalid RNS service descriptor")

// Descriptor is the small allocator capability record carried in announce app_data.
// It must stay within maxDescriptorBytes (the announce app-data budget); the
// full NodeCapabilities rides allocator offers instead, so the descriptor keeps
// only the coarse placement summary that fits: os, arch, and runtime. When the
// allocator runs a tunnel edge (F21-02), it also advertises the minimum needed
// to create a private tunnel RNS transport: the Backbone/TCP listener as host
// (Ygg IPv6) + port, and the tunnel RNS destination hash. No Ygg public key is
// ever advertised.
type Descriptor struct {
	Protocol  string            `json:"protocol"`
	ClusterID string            `json:"cluster_id"`
	Capacity  map[string]uint32 `json:"capacity"`
	OS        string            `json:"os,omitempty"`
	Arch      string            `json:"arch,omitempty"`
	Runtime   string            `json:"runtime,omitempty"`
	// TunnelHost is the allocator's tunnel Backbone/TCP listener host (its Ygg
	// IPv6 address), TunnelPort the tunnel listener port, and TunnelDestination
	// the hex-encoded tunnel RNS destination hash. All three are absent when no
	// tunnel edge is enabled.
	TunnelHost        string `json:"tunnel_host,omitempty"`
	TunnelPort        int    `json:"tunnel_port,omitempty"`
	TunnelDestination string `json:"tunnel_destination,omitempty"`
}

// TunnelAdvertisement wraps the allocator tunnel edge fields for newDescriptor.
type TunnelAdvertisement struct {
	// Host is the allocator's Ygg IPv6 address for the Backbone/TCP listener.
	Host string
	// Port is the tunnel Backbone/TCP listener port. Positive when enabled.
	Port int
	// DestinationHash is the hex-encoded tunnel RNS destination hash.
	DestinationHash string
}

func newDescriptor(clusterID []byte, capacity map[string]uint32, node *r1sv1.NodeCapabilities, tunnel *TunnelAdvertisement) (Descriptor, error) {
	if len(clusterID) != 32 {
		return Descriptor{}, fmt.Errorf("%w: cluster ID must be 32 bytes", ErrInvalidDescriptor)
	}
	descriptor := Descriptor{Protocol: protocolVersion, ClusterID: hex.EncodeToString(clusterID), Capacity: make(map[string]uint32, len(capacity))}
	for class, slots := range capacity {
		if strings.TrimSpace(class) == "" || slots == 0 {
			return Descriptor{}, fmt.Errorf("%w: capacity entries must have a class and non-zero slots", ErrInvalidDescriptor)
		}
		descriptor.Capacity[class] = slots
	}
	if len(descriptor.Capacity) == 0 {
		return Descriptor{}, fmt.Errorf("%w: capacity is required", ErrInvalidDescriptor)
	}
	// The announce summary is a bounded subset of the node advertisement. Only
	// normalized values are copied; anything else is rejected by the descriptor
	// size check below, so a malformed node cannot be advertised.
	if node != nil {
		descriptor.OS = node.GetOs()
		descriptor.Arch = node.GetArch()
		descriptor.Runtime = node.GetRuntime()
	}
	if tunnel != nil && tunnel.Port > 0 {
		if strings.TrimSpace(tunnel.Host) == "" {
			return Descriptor{}, fmt.Errorf("%w: tunnel host is required when a tunnel port is advertised", ErrInvalidDescriptor)
		}
		descriptor.TunnelHost = tunnel.Host
		descriptor.TunnelPort = tunnel.Port
		descriptor.TunnelDestination = tunnel.DestinationHash
	}
	return descriptor, nil
}

func (d Descriptor) marshal() ([]byte, error) {
	data, err := json.Marshal(d)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidDescriptor, err)
	}
	if len(data) > maxDescriptorBytes {
		return nil, fmt.Errorf("%w: encoded size %d exceeds %d bytes", ErrInvalidDescriptor, len(data), maxDescriptorBytes)
	}
	return data, nil
}

func parseDescriptor(data []byte) (Descriptor, error) {
	if len(data) == 0 || len(data) > maxDescriptorBytes {
		return Descriptor{}, fmt.Errorf("%w: encoded size must be between 1 and %d bytes", ErrInvalidDescriptor, maxDescriptorBytes)
	}
	var descriptor Descriptor
	if err := json.Unmarshal(data, &descriptor); err != nil {
		return Descriptor{}, fmt.Errorf("%w: %v", ErrInvalidDescriptor, err)
	}
	if descriptor.Protocol != protocolVersion {
		return Descriptor{}, fmt.Errorf("%w: unsupported protocol %q", ErrInvalidDescriptor, descriptor.Protocol)
	}
	clusterID, err := hex.DecodeString(descriptor.ClusterID)
	if err != nil {
		return Descriptor{}, fmt.Errorf("%w: cluster ID must be hexadecimal", ErrInvalidDescriptor)
	}
	result, err := newDescriptor(clusterID, descriptor.Capacity, nil, nil)
	if err != nil {
		return Descriptor{}, err
	}
	result.OS, result.Arch, result.Runtime = descriptor.OS, descriptor.Arch, descriptor.Runtime
	result.TunnelHost, result.TunnelPort, result.TunnelDestination = descriptor.TunnelHost, descriptor.TunnelPort, descriptor.TunnelDestination
	return result, nil
}
