// Package rns adapts r1s discovery metadata and envelopes to meshbus.
package rns

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

const (
	metadataCapacity          = "c"
	metadataOS                = "o"
	metadataArch              = "a"
	metadataRuntime           = "r"
	metadataTunnelHost        = "h"
	metadataTunnelPort        = "p"
	metadataTunnelDestination = "d"
)

var ErrInvalidDescriptor = errors.New("invalid r1s presence metadata")

// Descriptor is the r1s allocator projection decoded from bounded meshbus.v1
// presence metadata. Realm identity and protocol version belong to meshbus and
// are deliberately absent from this application-level value.
type Descriptor struct {
	Capacity map[string]uint32
	OS       string
	Arch     string
	Runtime  string

	TunnelHost        string
	TunnelPort        int
	TunnelDestination string
}

// TunnelAdvertisement is the allocator tunnel edge projected into presence
// metadata when enabled.
type TunnelAdvertisement struct {
	Host            string
	Port            int
	DestinationHash string
}

func newDescriptor(capacity map[string]uint32, node *r1sv1.NodeCapabilities, tunnel *TunnelAdvertisement) (Descriptor, error) {
	descriptor := Descriptor{Capacity: make(map[string]uint32, len(capacity))}
	for class, slots := range capacity {
		if strings.TrimSpace(class) == "" || slots == 0 {
			return Descriptor{}, fmt.Errorf("%w: capacity entries must have a class and non-zero slots", ErrInvalidDescriptor)
		}
		descriptor.Capacity[class] = slots
	}
	if len(descriptor.Capacity) == 0 {
		return Descriptor{}, fmt.Errorf("%w: capacity is required", ErrInvalidDescriptor)
	}
	if node != nil {
		descriptor.OS = node.GetOs()
		descriptor.Arch = node.GetArch()
		descriptor.Runtime = node.GetRuntime()
	}
	if tunnel != nil && tunnel.Port != 0 {
		if tunnel.Port < 1 || tunnel.Port > 65535 || strings.TrimSpace(tunnel.Host) == "" {
			return Descriptor{}, fmt.Errorf("%w: tunnel host and valid port are required", ErrInvalidDescriptor)
		}
		descriptor.TunnelHost = tunnel.Host
		if parsed := net.ParseIP(tunnel.Host); parsed != nil {
			descriptor.TunnelHost = parsed.String()
		}
		descriptor.TunnelPort = tunnel.Port
		descriptor.TunnelDestination = tunnel.DestinationHash
	}
	return descriptor, nil
}

func (d Descriptor) metadata() map[string]string {
	capacity := make(url.Values, len(d.Capacity))
	for class, slots := range d.Capacity {
		capacity.Set(class, strconv.FormatUint(uint64(slots), 10))
	}
	metadata := map[string]string{metadataCapacity: capacity.Encode()}
	for key, value := range map[string]string{
		metadataOS: d.OS, metadataArch: d.Arch, metadataRuntime: d.Runtime,
		metadataTunnelHost: d.TunnelHost, metadataTunnelDestination: d.TunnelDestination,
	} {
		if value != "" {
			metadata[key] = value
		}
	}
	if d.TunnelPort > 0 {
		metadata[metadataTunnelPort] = strconv.Itoa(d.TunnelPort)
	}
	return metadata
}

func parseDescriptorMetadata(metadata map[string]string) (Descriptor, error) {
	encodedCapacity := metadata[metadataCapacity]
	values, err := url.ParseQuery(encodedCapacity)
	if err != nil || encodedCapacity == "" {
		return Descriptor{}, fmt.Errorf("%w: capacity metadata is required", ErrInvalidDescriptor)
	}
	capacity := make(map[string]uint32, len(values))
	for class, entries := range values {
		if len(entries) != 1 {
			return Descriptor{}, fmt.Errorf("%w: duplicate capacity class %q", ErrInvalidDescriptor, class)
		}
		slots, parseErr := strconv.ParseUint(entries[0], 10, 32)
		if parseErr != nil || slots == 0 {
			return Descriptor{}, fmt.Errorf("%w: invalid capacity for class %q", ErrInvalidDescriptor, class)
		}
		capacity[class] = uint32(slots)
	}

	var tunnel *TunnelAdvertisement
	if portText := metadata[metadataTunnelPort]; portText != "" {
		port, parseErr := strconv.Atoi(portText)
		if parseErr != nil {
			return Descriptor{}, fmt.Errorf("%w: invalid tunnel port", ErrInvalidDescriptor)
		}
		tunnel = &TunnelAdvertisement{
			Host: metadata[metadataTunnelHost], Port: port, DestinationHash: metadata[metadataTunnelDestination],
		}
	} else if metadata[metadataTunnelHost] != "" || metadata[metadataTunnelDestination] != "" {
		return Descriptor{}, fmt.Errorf("%w: incomplete tunnel metadata", ErrInvalidDescriptor)
	}

	return newDescriptor(capacity, &r1sv1.NodeCapabilities{
		Os: metadata[metadataOS], Arch: metadata[metadataArch], Runtime: metadata[metadataRuntime],
	}, tunnel)
}
