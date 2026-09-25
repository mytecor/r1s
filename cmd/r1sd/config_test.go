package main

import (
	"io"
	"strings"
	"testing"
)

func TestParseHexBytes(t *testing.T) {
	if value, err := parseHexBytes("tunnel-endpoint", ""); err != nil || value != nil {
		t.Fatalf("empty = %x, %v", value, err)
	}
	if value, err := parseHexBytes("tunnel-endpoint", "deadbeef"); err != nil || string(value) != "\xde\xad\xbe\xef" {
		t.Fatalf("decoded = %x, %v", value, err)
	}
	if _, err := parseHexBytes("tunnel-endpoint", "zz"); err == nil || !strings.Contains(err.Error(), "--tunnel-endpoint must be hex") {
		t.Fatalf("invalid hex error = %v", err)
	}
}

// TestTunnelStaticEndpointConflictsWithEdge verifies the daemon rejects a
// manually supplied static tunnel advertisement together with --tunnel-enabled:
// the embedded edge derives its own advertisement, and silently preferring it
// over a configured static value would hide a misconfiguration.
func TestTunnelStaticEndpointConflictsWithEdge(t *testing.T) {
	_, err := parseCommandLine([]string{
		"--identity", "unused",
		"--tunnel-enabled",
		"--tunnel-endpoint", "deadbeef", "--tunnel-endpoint-pubkey", "cafe",
		"deadbeef",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cannot be set together with --tunnel-enabled") {
		t.Fatalf("error = %v, want conflict with --tunnel-enabled diagnostic", err)
	}
}
