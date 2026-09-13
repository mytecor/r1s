// Package rns implements authenticated r1s envelope delivery over Reticulum-Go.
package rns

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	protocolVersion    = "r1s.v1"
	maxDescriptorBytes = 256
)

var ErrInvalidDescriptor = errors.New("invalid RNS service descriptor")

// Descriptor is the small allocator capability record carried in announce app_data.
type Descriptor struct {
	Protocol string            `json:"protocol"`
	Capacity map[string]uint32 `json:"capacity"`
}

func newDescriptor(capacity map[string]uint32) (Descriptor, error) {
	descriptor := Descriptor{Protocol: protocolVersion, Capacity: make(map[string]uint32, len(capacity))}
	for class, slots := range capacity {
		if strings.TrimSpace(class) == "" || slots == 0 {
			return Descriptor{}, fmt.Errorf("%w: capacity entries must have a class and non-zero slots", ErrInvalidDescriptor)
		}
		descriptor.Capacity[class] = slots
	}
	if len(descriptor.Capacity) == 0 {
		return Descriptor{}, fmt.Errorf("%w: capacity is required", ErrInvalidDescriptor)
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
	return newDescriptor(descriptor.Capacity)
}
