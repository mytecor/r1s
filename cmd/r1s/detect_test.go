package main

import (
	"net"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultSocketPathIsUnderConfigDir(t *testing.T) {
	// The default socket path is deterministic and named client.sock, the same
	// discovery candidate serve uses and the CLI auto-detects.
	path, err := defaultSocketPath()
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(path) != "client.sock" {
		t.Fatalf("unexpected default socket name: %q", path)
	}
}

func TestLocalAPISocketAlive(t *testing.T) {
	// With no server listening the path is inert.
	dead := filepath.Join(t.TempDir(), "missing.sock")
	if localAPISocketAlive(dead) {
		t.Fatalf("expected inert path %q to report not-live", dead)
	}
	// A listening Unix socket is live.
	live := filepath.Join(t.TempDir(), "live.sock")
	conn, err := net.Listen("unix", live)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(); _ = os.Remove(live) }()
	if !localAPISocketAlive(live) {
		t.Fatalf("expected listening path %q to report live", live)
	}
}
