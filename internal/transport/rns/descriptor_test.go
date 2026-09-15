package rns

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestDescriptorRoundTrip(t *testing.T) {
	clusterID := bytes.Repeat([]byte{0x42}, 32)
	descriptor, err := newDescriptor(clusterID, map[string]uint32{"gpu": 2, "default": 1})
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
