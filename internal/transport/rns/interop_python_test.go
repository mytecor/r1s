package rns

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"quad4/reticulum-go/pkg/common"
)

// pythonReferenceExec resolves an interpreter that can import the upstream
// Python RNS reference module. Prefer PYTHON_INTEROP, then common pipx `rns`
// venvs, mirroring the Reticulum-Go interop harness.
func pythonReferenceExec(tb testing.TB) string {
	tb.Helper()
	if p := os.Getenv("PYTHON_INTEROP"); p != "" {
		if st, err := os.Stat(p); err == nil && !st.IsDir() {
			return p
		}
	}
	candidates := []string{
		filepath.Join(".venv", "bin", "python"),
		filepath.Join(".venv", "bin", "python3"),
	}
	if home, err := os.UserHomeDir(); err == nil {
		for _, pipxVenv := range []string{
			filepath.Join(home, ".local", "pipx", "venvs", "rns", "bin", "python"),
			filepath.Join(home, ".local", "share", "pipx", "venvs", "rns", "bin", "python"),
		} {
			candidates = append(candidates, pipxVenv)
		}
	}
	for _, cand := range candidates {
		if st, err := os.Stat(cand); err == nil && !st.IsDir() {
			return cand
		}
	}
	return "python3"
}

// writeReferenceConfig writes an RNS config for the reference peer on a UDP
// loopback pair. The field names mirror the upstream Python config format.
func writeReferenceConfig(tb testing.TB, dir string, listenPort, forwardPort int) string {
	tb.Helper()
	path := filepath.Join(dir, "config")
	config := fmt.Sprintf(`[reticulum]
enable_transport = No
share_instance = No

[logging]
loglevel = 2

[interfaces]
  [[interop_udp]]
    type = UDPInterface
    enabled = Yes
    listen_ip = 127.0.0.1
    listen_port = %d
    forward_ip = 127.0.0.1
    forward_port = %d
`, listenPort, forwardPort)
	if err := os.WriteFile(path, []byte(config), 0o600); err != nil {
		tb.Fatal(err)
	}
	return path
}

// startReferencePeer launches the Python RNS reference peer and blocks until it
// has announced a reachable destination. It returns the peer's destination hash
// and a cleanup that stops the process.
func startReferencePeer(tb testing.TB, listenPort, forwardPort int) (string, func()) {
	tb.Helper()
	if os.Getenv("RUN_LIVE_INTEROP") != "1" {
		tb.Skip("set RUN_LIVE_INTEROP=1 to run live Python-reference interop")
	}
	exe := pythonReferenceExec(tb)
	script := filepath.Join("testdata", "python_reference_peer.py")
	cfgDir := tb.TempDir()
	writeReferenceConfig(tb, cfgDir, listenPort, forwardPort)
	cmd := exec.Command(
		exe, script,
		"--configdir", cfgDir,
		"--capacity", `{"default": 2}`,
		"--no-ratchet", "--announce",
	)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		tb.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		tb.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		tb.Fatalf("start reference peer: %v", err)
	}
	cleanup := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	// Stream stderr to the test log so failures are debuggable.
	go func() {
		sc := bufio.NewScanner(stderr)
		for sc.Scan() {
			tb.Logf("[python] %s", sc.Text())
		}
	}()

	sc := bufio.NewScanner(stdout)
	deadline := time.After(30 * time.Second)
	readLine := func() (string, error) {
		ch := make(chan string, 1)
		go func() {
			if sc.Scan() {
				ch <- sc.Text()
			}
		}()
		select {
		case line := <-ch:
			return line, nil
		case <-deadline:
			return "", fmt.Errorf("timed out reading reference peer output")
		}
	}
	ready, err := readLine()
	if err != nil {
		cleanup()
		tb.Fatalf("reference peer did not become ready: %v", err)
	}
	if ready != "READY" {
		cleanup()
		tb.Fatalf("reference peer first line = %q, want READY", ready)
	}
	hashLine, err := readLine()
	if err != nil {
		cleanup()
		tb.Fatalf("reference peer did not print destination hash: %v", err)
	}
	hash := strings.TrimSpace(hashLine)
	return hash, cleanup
}

// TestPythonReferenceDiscovery proves the r1s Go Endpoint discovers an r1s
// service descriptor announced by the upstream Python RNS reference node. Both
// processes run as independent RNS endpoints over a UDP loopback pair.
func TestPythonReferenceDiscovery(t *testing.T) {
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	pyHash, cleanup := startReferencePeer(t, portA, portB)
	defer cleanup()

	config := &common.ReticulumConfig{Interfaces: map[string]*common.InterfaceConfig{
		"interop_udp": {
			Type: "UDPInterface", Enabled: true,
			Address: "127.0.0.1", Port: portB,
			TargetHost: "127.0.0.1", TargetPort: portA,
		},
	}}
	endpoint, err := New(Config{
		Reticulum:    config,
		IdentityPath: filepath.Join(t.TempDir(), "r1sd.identity"),
		Capacity:     map[string]uint32{"default": 3},
		NetworkWait:  15 * time.Second,
	}, func(context.Context, *r1sv1.Envelope) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := endpoint.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	select {
	case service := <-endpoint.Discoveries():
		if service.Destination != pyHash {
			t.Fatalf("discovered destination = %s, want %s", service.Destination, pyHash)
		}
		if service.Descriptor.Protocol != "r1s.v1" {
			t.Fatalf("descriptor protocol = %q, want r1s.v1", service.Descriptor.Protocol)
		}
		if got := service.Descriptor.Capacity["default"]; got != 2 {
			t.Fatalf("descriptor capacity[default] = %d, want 2", got)
		}
		t.Logf("discovered Python reference allocator: dest=%s identity=%s hops=%d",
			service.Destination, service.Identity, service.Hops)
	case <-time.After(20 * time.Second):
		t.Fatal("no Python reference discovery within 20s")
	}
}

// TestPythonReferenceLinkEnvelope documents the open channel-envelope gap. The
// Go Endpoint can initiate a link toward the reference peer, but reliable
// envelope delivery over a Channel to the upstream Python implementation is not
// yet demonstrated and is tracked in roadmap/BACKLOG.md. This test is skipped
// until that work lands so interop expectations stay explicit and honest.
func TestPythonReferenceLinkEnvelope(t *testing.T) {
	t.Skip("Channel envelope delivery to the Python reference is an open decision in roadmap/BACKLOG.md; reliable retransmission acceptance requires the canonical Reticulum-Go Channel pipeline (F2-01 remaining).")
}
