package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseCapacity(t *testing.T) {
	capacity, err := parseCapacity("default=2, gpu = 1")
	if err != nil {
		t.Fatal(err)
	}
	if capacity["default"] != 2 || capacity["gpu"] != 1 {
		t.Fatalf("capacity = %v", capacity)
	}
	for _, value := range []string{"", "default", "default=0", "default=x", "default=1,default=2"} {
		if _, err := parseCapacity(value); err == nil {
			t.Errorf("parseCapacity(%q) succeeded", value)
		}
	}
}

func TestSweepIntervalMustBePositive(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--identity", "unused", "--sweep-interval", "0s",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "--sweep-interval must be positive") {
		t.Fatalf("error = %v, want sweep-interval diagnostic", err)
	}
}

func TestVersionFlagPrintsVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--version"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(stdout.String()); got != version {
		t.Fatalf("r1sd --version = %q, want %q", got, version)
	}
	if stderr.Len() != 0 {
		t.Fatalf("r1sd --version wrote to stderr: %q", stderr.String())
	}
}

func TestClusterInitDoesNotRequireDaemonFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	path := filepath.Join(t.TempDir(), "cluster")
	if err := run(context.Background(), []string{"--cluster", path, "cluster", "init"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Join token: r1s1:") {
		t.Fatalf("output = %q", stdout.String())
	}
}

func TestInlineClusterTokenErrorIsRedacted(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--cluster", "r1s1:not-base64",
		"--identity", "unused",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "inline join token") {
		t.Fatalf("error = %v, want inline token diagnostic", err)
	}
	if strings.Contains(err.Error(), "not-base64") {
		t.Fatalf("error exposed inline token: %v", err)
	}
}

func TestRNSConfigFlagIsRemoved(t *testing.T) {
	_, err := parseCommandLine([]string{"--rns-config", "unused"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("parseCommandLine() error = %v, want removed flag diagnostic", err)
	}
}
