package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
)

func TestParseTunnelTargets(t *testing.T) {
	targets, err := ParseTunnelTargets("default=ssh@127.0.0.1:2222;http@127.0.0.1:8080,gpu=10.0.0.1:9001")
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets = %+v, want 2 classes", targets)
	}
	defaultSlots := targets["default"]
	if len(defaultSlots) != 2 {
		t.Fatalf("default slots = %+v, want 2", defaultSlots)
	}
	if defaultSlots[0].ID != "ssh" || defaultSlots[0].Host != "127.0.0.1" || defaultSlots[0].Port != 2222 {
		t.Fatalf("default[0] = %+v", defaultSlots[0])
	}
	if defaultSlots[1].ID != "http" || defaultSlots[1].Host != "127.0.0.1" || defaultSlots[1].Port != 8080 {
		t.Fatalf("default[1] = %+v", defaultSlots[1])
	}
	if gpu := targets["gpu"]; len(gpu) != 1 || gpu[0].ID != "" || gpu[0].Host != "10.0.0.1" || gpu[0].Port != 9001 {
		t.Fatalf("gpu target = %+v", gpu)
	}

	empty, err := ParseTunnelTargets("")
	if err != nil || len(empty) != 0 {
		t.Fatalf("ParseTunnelTargets(\"\") = %+v, %v", empty, err)
	}
	for _, value := range []string{"default", "default=127.0.0.1", "default=127.0.0.1:0", "default=host:99999", "default=127.0.0.1:80,default=127.0.0.1:81", "default=@127.0.0.1:80", "default="} {
		if _, err := ParseTunnelTargets(value); err == nil {
			t.Errorf("ParseTunnelTargets(%q) succeeded", value)
		}
	}
}

func TestParseTunnelTarget(t *testing.T) {
	target, err := ParseTunnelTarget("[::1]:8080")
	if err != nil {
		t.Fatal(err)
	}
	if target.Host != "::1" || target.Port != 8080 || target.ID != "" {
		t.Fatalf("target = %+v", target)
	}
	named, err := ParseTunnelTarget("ssh@127.0.0.1:2222")
	if err != nil {
		t.Fatal(err)
	}
	if named.ID != "ssh" || named.Host != "127.0.0.1" || named.Port != 2222 {
		t.Fatalf("named target = %+v", named)
	}
	for _, value := range []string{"", "127.0.0.1", "127.0.0.1:0", ":8080", "host:-1", "@127.0.0.1:80"} {
		if _, err := ParseTunnelTarget(value); err == nil {
			t.Errorf("ParseTunnelTarget(%q) succeeded", value)
		}
	}
}

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

// TestTunnelFlagValidation verifies the daemon rejects an invalid tunnel target
// through the command line.
func TestTunnelFlagValidation(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--rns-config", "unused", "--identity", "unused",
		"--tunnel-enabled", "--tunnel-target", "default=127.0.0.1",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "invalid tunnel target") {
		t.Fatalf("error = %v, want invalid tunnel target diagnostic", err)
	}
}

// TestTunnelStaticEndpointConflictsWithEdge verifies the daemon rejects a
// manually supplied static tunnel advertisement together with --tunnel-enabled:
// the embedded edge derives its own advertisement, and silently preferring it
// over a configured static value would hide a misconfiguration.
func TestTunnelStaticEndpointConflictsWithEdge(t *testing.T) {
	_, err := parseCommandLine([]string{
		"--rns-config", "unused", "--identity", "unused",
		"--tunnel-enabled",
		"--tunnel-endpoint", "deadbeef", "--tunnel-endpoint-pubkey", "cafe",
	}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "cannot be set together with --tunnel-enabled") {
		t.Fatalf("error = %v, want conflict with --tunnel-enabled diagnostic", err)
	}
}
