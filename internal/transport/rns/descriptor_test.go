package rns

import (
	"errors"
	"strings"
	"testing"
)

func TestDescriptorRoundTrip(t *testing.T) {
	descriptor, err := newDescriptor(map[string]uint32{"gpu": 2, "default": 1})
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
	if parsed.Protocol != protocolVersion || parsed.Capacity["gpu"] != 2 || parsed.Capacity["default"] != 1 {
		t.Fatalf("parsed descriptor = %+v", parsed)
	}
}

func TestDescriptorRejectsInvalidAndOversizedData(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`{"protocol":"other","capacity":{"default":1}}`),
		[]byte(`{"protocol":"r1s.v1","capacity":{"default":0}}`),
		[]byte(strings.Repeat("x", maxDescriptorBytes+1)),
	} {
		if _, err := parseDescriptor(data); !errors.Is(err, ErrInvalidDescriptor) {
			t.Fatalf("parseDescriptor(%q) error = %v", data, err)
		}
	}
}
