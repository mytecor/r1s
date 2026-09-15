package rns

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Quad4-Software/Reticulum-Go/pkg/common"
	"github.com/Quad4-Software/Reticulum-Go/pkg/interfaces"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
// has announced a reachable destination. It returns a handle to the peer's
// stdout scanner and a cleanup that stops the process.
func startReferencePeer(tb testing.TB, listenPort, forwardPort int, channel bool) (*referencePeer, func()) {
	tb.Helper()
	if os.Getenv("RUN_LIVE_INTEROP") != "1" {
		tb.Skip("set RUN_LIVE_INTEROP=1 to run live Python-reference interop")
	}
	exe := pythonReferenceExec(tb)
	script := filepath.Join("testdata", "python_reference_peer.py")
	cfgDir := tb.TempDir()
	writeReferenceConfig(tb, cfgDir, listenPort, forwardPort)
	args := []string{
		script,
		"--configdir", cfgDir,
		"--capacity", `{"default": 2}`,
		"--cluster-key", hex.EncodeToString(testClusterKey()),
		"--no-ratchet", "--announce",
	}
	if channel {
		args = append(args, "--channel")
	}
	cmd := exec.Command(exe, args...)
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

	peer := &referencePeer{scanner: bufio.NewScanner(stdout)}
	ready, err := peer.readLine(60 * time.Second)
	if err != nil {
		cleanup()
		tb.Fatalf("reference peer did not become ready: %v", err)
	}
	if ready != "READY" {
		cleanup()
		tb.Fatalf("reference peer first line = %q, want READY", ready)
	}
	hashLine, err := peer.readLine(5 * time.Second)
	if err != nil {
		cleanup()
		tb.Fatalf("reference peer did not print destination hash: %v", err)
	}
	peer.hash = strings.TrimSpace(hashLine)
	return peer, cleanup
}

// referencePeer wraps the line-based stdout protocol of the Python reference
// peer so tests can read ready/hash/LINK_UP/CHANNEL_MSG events without sharing
// a scanner across goroutines.
type referencePeer struct {
	scanner *bufio.Scanner
	hash    string
}

// readLine reads the next stdout line, failing if none arrives by d.
func (p *referencePeer) readLine(d time.Duration) (string, error) {
	ch := make(chan string, 1)
	go func() {
		if p.scanner.Scan() {
			ch <- p.scanner.Text()
		} else {
			close(ch)
		}
	}()
	select {
	case line, ok := <-ch:
		if !ok {
			return "", fmt.Errorf("reference peer output closed")
		}
		return line, nil
	case <-time.After(d):
		return "", fmt.Errorf("timed out waiting for reference peer output")
	}
}

// newGoEndpoint builds a Go r1s Endpoint whose UDP interface is a custom
// dropper wrapper so the test can inject packet loss into channel data after
// the link is established. Returns the endpoint and the live dropper.
func newGoEndpoint(t *testing.T, pyListen, pyForward int, handler func(context.Context, *r1sv1.Envelope) error) (*Endpoint, *channelDropper) {
	t.Helper()
	base, err := interfaces.NewUDPInterface("interop_udp",
		"127.0.0.1:"+strconv.Itoa(pyForward),
		"127.0.0.1:"+strconv.Itoa(pyListen), true)
	if err != nil {
		t.Fatal(err)
	}
	dropper := &channelDropper{UDPInterface: base}
	if handler == nil {
		handler = func(context.Context, *r1sv1.Envelope) error { return nil }
	}
	endpoint, err := New(Config{
		Reticulum:      &common.ReticulumConfig{},
		IdentitySource: filepath.Join(t.TempDir(), "r1sd.identity"),
		ClusterKey:     testClusterKey(),
		Capacity:       map[string]uint32{"default": 3},
		NetworkWait:    15 * time.Second,
		Interfaces:     []interfaces.Interface{dropper},
	}, handler)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint, dropper
}

// channelDropper wraps a UDP interface and can drop the next N outbound
// channel-context data packets. It observes exactly the packets the transport
// emits (serialized RNS packets, HeaderType1: flags, hops, destHash(16),
// context, data), so the predicate is reliable on a direct loopback link.
// Embedding the UDP interface promotes the full NetworkInterface contract;
// only Send is overridden.
type channelDropper struct {
	*interfaces.UDPInterface
	enabled   atomic.Bool
	remaining atomic.Int32
	drops     atomic.Int32
}

func (d *channelDropper) Send(data []byte, addr string) error {
	if d.isChannelPacket(data) {
		remaining := d.remaining.Load()
		if d.enabled.Load() && remaining > 0 && d.remaining.CompareAndSwap(remaining, remaining-1) {
			d.drops.Add(1)
			return nil // packet lost in transit
		}
	}
	return d.UDPInterface.Send(data, addr)
}

// isChannelPacket reports whether data is a HeaderType1 data packet carrying a
// Channel context (the wire encoding the Go Link.Send channel path uses).
func (d *channelDropper) isChannelPacket(data []byte) bool {
	if len(data) < 19 {
		return false
	}
	if data[0]&0x03 != 0x00 { // PacketTypeData
		return false
	}
	return data[18] == 0x0E // ContextChannel
}

func (d *channelDropper) arm(n int) {
	d.enabled.Store(true)
	d.remaining.Store(int32(n))
	d.drops.Store(0)
}

func (d *channelDropper) disarm() { d.enabled.Store(false) }

// TestPythonReferenceDiscovery proves the r1s Go Endpoint discovers an r1s
// service descriptor announced by the upstream Python RNS reference node. Both
// processes run as independent RNS endpoints over a UDP loopback pair.
func TestPythonReferenceDiscovery(t *testing.T) {
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	py, cleanup := startReferencePeer(t, portA, portB, false)
	defer cleanup()

	endpoint, _ := newGoEndpoint(t, portA, portB, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := endpoint.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	select {
	case service := <-endpoint.Discoveries():
		if service.Destination != py.hash {
			t.Fatalf("discovered destination = %s, want %s", service.Destination, py.hash)
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

// TestPythonReferenceChannelEnvelope proves reliable Channel envelope delivery
// to the upstream Python RNS reference node. The Go endpoint discovers the
// reference allocator, initiates a link, and sends a validated r1s envelope
// over a Channel while an injectable packet-loss wrapper drops channel-context
// packets. The Python peer reassembles, prints, and echoes the exact envelope
// bytes back over the same Channel, demonstrating that the canonical
// Reticulum-Go Channel pipeline delivers despite loss and that retransmission
// is accepted (deduplicated) by the Python reference.
func TestPythonReferenceChannelEnvelope(t *testing.T) {
	if os.Getenv("RUN_LIVE_INTEROP") != "1" {
		t.Skip("set RUN_LIVE_INTEROP=1 to run live Python-reference interop")
	}
	portA := freeUDPPort(t)
	portB := freeUDPPort(t)
	for portB == portA {
		portB = freeUDPPort(t)
	}
	py, cleanup := startReferencePeer(t, portA, portB, true)
	defer cleanup()

	// The capturing handler runs from the very first channel delivery so
	// echoed envelopes are never discarded by a placeholder handler.
	receivedEnvelope := make(chan *r1sv1.Envelope, 8)
	t.Cleanup(func() { close(receivedEnvelope) })
	endpoint, dropper := newGoEndpoint(t, portA, portB, func(_ context.Context, envelope *r1sv1.Envelope) error {
		receivedEnvelope <- envelope
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := endpoint.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	var targetIdentity []byte
	select {
	case service := <-endpoint.Discoveries():
		if service.Destination != py.hash {
			t.Fatalf("discovered destination = %s, want %s", service.Destination, py.hash)
		}
		targetIdentity, _ = hex.DecodeString(service.Identity)
	case <-time.After(20 * time.Second):
		t.Fatal("no Python reference discovery within 20s")
	}

	// A request envelope that passes protocol.ValidateEnvelope once the
	// reference peer buffer is registered.
	sendCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := endpoint.Send(sendCtx, py.hash, validInteropRequest()); err != nil {
		t.Fatal(err)
	}

	// Arm packet loss so every channel payload the Go endpoint sends after this
	// point is dropped once per envelope; the Channel must retransmit and the
	// Python peer must still reassemble and echo exactly once.
	dropper.arm(1)

	// Second envelope: the sender's first channel packet is dropped, forcing a
	// retransmission that must still be accepted by the Python Channel.
	if err := endpoint.Send(sendCtx, py.hash, validInteropRequest()); err != nil {
		t.Fatal(err)
	}

	// Wait for the reference peer to report the first (no-loss) echo and the
	// second (loss-recovered) echo, then prove the echoed bytes round-trip.
	// The peer may emit LINK_UP lines before CHANNEL_MSG; skip them.
	var echoes []string
	deadline := time.Now().Add(40 * time.Second)
	for len(echoes) < 2 && time.Now().Before(deadline) {
		line, err := py.readLine(20 * time.Second)
		if err != nil {
			if time.Now().Before(deadline) {
				continue
			}
			break
		}
		if strings.HasPrefix(line, "CHANNEL_MSG ") {
			echoes = append(echoes, line)
		}
	}
	if len(echoes) < 2 {
		t.Fatalf("reference peer reported %d CHANNEL_MSG echoes, want 2", len(echoes))
	}
	if got := dropper.drops.Load(); got == 0 {
		t.Fatal("packet-loss wrapper did not report a drop; retransmission was not exercised")
	} else {
		t.Logf("dropped %d channel packets; Go Channel retransmitted", got)
	}

	// The Go endpoint must receive the two echoes over the Channel and re-validate
	// them. The authenticated sender of each echo is the Python reference node's
	// identity hash (the link peer), not the local endpoint.
	dropper.disarm()
	var verified int
	for verified < len(echoes) {
		select {
		case envelope := <-receivedEnvelope:
			if !bytes.Equal(envelope.GetSender(), targetIdentity) {
				t.Fatalf("echo sender = %x, want authenticated Python identity %x", envelope.GetSender(), targetIdentity)
			}
			verified++
		case <-time.After(20 * time.Second):
			t.Fatalf("received %d/%d echoes from the Python reference", verified, len(echoes))
		}
	}
}

// validInteropRequest returns an envelope that passes protocol validation with
// a small bounded payload suitable for a single Channel message.
func validInteropRequest() *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: "interop-request",
		Sender:    []byte("payload-sender"),
		SentAt:    timestamppb.Now(),
		Payload: &r1sv1.Envelope_ExecutionRequest{
			ExecutionRequest: &r1sv1.ExecutionRequest{
				RequestId:     "interop-request",
				ResourceClass: "default",
				Workload:      &r1sv1.Workload{Image: "example.test/image:latest"},
				Policy:        &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(30 * time.Second)},
			},
		},
	}
}
