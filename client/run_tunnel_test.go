package client

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/mytecor/r1s/internal/tunnel"
)

func TestNewRunTunnelValidatesAndDeduplicatesTargets(t *testing.T) {
	h := newRunTunnelHarness(t, []TunnelTarget{{Port: 9000}, {Port: 8080}, {Port: 9000}})
	if len(h.runTunnel.targets) != 2 {
		t.Fatalf("targets = %+v; want two deduplicated ports", h.runTunnel.targets)
	}
	if h.factoryCalls != 0 {
		t.Fatal("creating RunTunnel eagerly created the transport edge")
	}
	for _, targets := range [][]TunnelTarget{nil, {}, {{Port: 0}}} {
		if _, err := newRunTunnel(&Client{}, targets, h.factory, h.open); err == nil {
			t.Fatalf("NewRunTunnel accepted invalid targets %+v", targets)
		}
	}
}

func TestClientOwnsOneLiveRunTunnel(t *testing.T) {
	c := &Client{identity: []byte("client-identity")}
	first, err := c.NewRunTunnel([]TunnelTarget{{Port: 9000}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.NewRunTunnel([]TunnelTarget{{Port: 8080}}); !errors.Is(err, ErrRunTunnelActive) {
		t.Fatalf("second NewRunTunnel error = %v; want ErrRunTunnelActive", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := c.NewRunTunnel([]TunnelTarget{{Port: 8080}})
	if err != nil {
		t.Fatalf("NewRunTunnel after Close: %v", err)
	}
	_ = second.Close()
}

func TestRunTunnelDialRequiresActiveAllowedTarget(t *testing.T) {
	h := newRunTunnelHarness(t, []TunnelTarget{{Port: 9000}})
	if _, err := h.runTunnel.Dial(t.Context(), 9000); !errors.Is(err, ErrNoActiveExecution) {
		t.Fatalf("Dial before SetActive error = %v; want ErrNoActiveExecution", err)
	}
	h.runTunnel.SetActive("execution-a")
	if _, err := h.runTunnel.Dial(t.Context(), 8080); !errors.Is(err, ErrTunnelTargetNotAllowed) {
		t.Fatalf("Dial unknown port error = %v; want ErrTunnelTargetNotAllowed", err)
	}
	if h.factoryCalls != 0 || h.openCalls != 0 {
		t.Fatal("rejected Dial initialized tunnel resources")
	}
}

func TestRunTunnelLazilyReusesPair(t *testing.T) {
	h := newRunTunnelHarness(t, []TunnelTarget{{Port: 9000}, {Port: 8080}})
	h.runTunnel.SetActive("execution-a")
	first, err := h.runTunnel.Dial(t.Context(), 9000)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.runTunnel.Dial(t.Context(), 8080)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("two Dial calls returned the same logical stream")
	}
	if h.factoryCalls != 1 || h.openCalls != 1 || h.dialer.dials != 1 {
		t.Fatalf("factory/open/pair dials = %d/%d/%d; want 1/1/1", h.factoryCalls, h.openCalls, h.dialer.dials)
	}
	if got := h.dialer.pairs[0].preamble; got != "execution-a" {
		t.Fatalf("routing preamble execution = %q", got)
	}
}

func TestRunTunnelConcurrentDialCreatesOnePair(t *testing.T) {
	h := newRunTunnelHarness(t, []TunnelTarget{{Port: 9000}})
	h.runTunnel.SetActive("execution-a")
	const count = 24
	errs := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := h.runTunnel.Dial(context.Background(), 9000)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if h.factoryCalls != 1 || h.openCalls != 1 || h.dialer.dials != 1 {
		t.Fatalf("factory/open/pair dials = %d/%d/%d; want 1/1/1", h.factoryCalls, h.openCalls, h.dialer.dials)
	}
	if got := len(h.dialer.pairs[0].streams); got != count {
		t.Fatalf("opened streams = %d; want %d", got, count)
	}
}

func TestRunTunnelSetActiveClosesOldPairAndStreams(t *testing.T) {
	h := newRunTunnelHarness(t, []TunnelTarget{{Port: 9000}})
	h.runTunnel.SetActive("execution-a")
	oldStreamRaw, err := h.runTunnel.Dial(t.Context(), 9000)
	if err != nil {
		t.Fatal(err)
	}
	oldPair := h.dialer.pairs[0]
	oldStream := oldStreamRaw.(*fakeRunTunnelStream)

	h.runTunnel.SetActive("execution-b")
	if !oldPair.isClosed() || !oldStream.isClosed() {
		t.Fatal("SetActive did not terminate the old pair and its existing stream")
	}
	if h.openCalls != 1 {
		t.Fatal("SetActive eagerly established the replacement pair")
	}
	newStream, err := h.runTunnel.Dial(t.Context(), 9000)
	if err != nil {
		t.Fatal(err)
	}
	if newStream == oldStreamRaw || h.openExecutions[1] != "execution-b" || h.dialer.dials != 2 {
		t.Fatalf("replacement stream did not use execution-b: executions=%v dials=%d", h.openExecutions, h.dialer.dials)
	}
}

func TestRunTunnelCloseIsIdempotentAndForbidsDial(t *testing.T) {
	h := newRunTunnelHarness(t, []TunnelTarget{{Port: 9000}})
	h.runTunnel.SetActive("execution-a")
	if _, err := h.runTunnel.Dial(t.Context(), 9000); err != nil {
		t.Fatal(err)
	}
	if err := h.runTunnel.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.runTunnel.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if !h.dialer.pairs[0].isClosed() || h.dialer.closeCalls != 1 {
		t.Fatal("Close did not release pair and edge exactly once")
	}
	if _, err := h.runTunnel.Dial(t.Context(), 9000); !errors.Is(err, ErrRunTunnelClosed) {
		t.Fatalf("Dial after Close error = %v; want ErrRunTunnelClosed", err)
	}
}

type runTunnelHarness struct {
	runTunnel      *RunTunnel
	dialer         *fakeRunTunnelDialer
	factoryCalls   int
	openCalls      int
	openExecutions []string
}

func newRunTunnelHarness(t *testing.T, targets []TunnelTarget) *runTunnelHarness {
	t.Helper()
	h := &runTunnelHarness{dialer: &fakeRunTunnelDialer{}}
	runTunnel, err := newRunTunnel(&Client{identity: []byte("client-identity")}, targets, h.factory, h.open)
	if err != nil {
		t.Fatal(err)
	}
	h.runTunnel = runTunnel
	t.Cleanup(func() { _ = runTunnel.Close() })
	return h
}

func (h *runTunnelHarness) factory([]byte) (tunnel.Dialer, []byte, error) {
	h.factoryCalls++
	return h.dialer, []byte("client-peer-key"), nil
}

func (h *runTunnelHarness) open(_ context.Context, executionID string, peerKey []byte, targets []TunnelTarget) (TunnelEndpoint, error) {
	h.openCalls++
	h.openExecutions = append(h.openExecutions, executionID)
	if string(peerKey) != "client-peer-key" || len(targets) == 0 {
		return TunnelEndpoint{}, errors.New("bad owner open")
	}
	return TunnelEndpoint{Address: []byte("allocator-address"), PubKey: []byte("allocator-key")}, nil
}

type fakeRunTunnelDialer struct {
	dials      int
	closeCalls int
	pairs      []*fakeRunTunnelPair
}

func (d *fakeRunTunnelDialer) Dial(context.Context, tunnel.Endpoint) (tunnel.Conn, error) {
	d.dials++
	pair := &fakeRunTunnelPair{}
	d.pairs = append(d.pairs, pair)
	return pair, nil
}

func (d *fakeRunTunnelDialer) Close() error {
	d.closeCalls++
	return nil
}

type fakeRunTunnelPair struct {
	mu       sync.Mutex
	closed   bool
	preamble string
	streams  []*fakeRunTunnelStream
}

func (p *fakeRunTunnelPair) Read([]byte) (int, error)    { return 0, io.EOF }
func (p *fakeRunTunnelPair) Write(b []byte) (int, error) { return len(b), nil }
func (p *fakeRunTunnelPair) PeerKey() []byte             { return []byte("allocator-key") }
func (p *fakeRunTunnelPair) CloseRead() error            { return nil }
func (p *fakeRunTunnelPair) CloseWrite() error           { return nil }
func (p *fakeRunTunnelPair) WritePreamble(value tunnel.Preamble) error {
	p.mu.Lock()
	p.preamble = value.ExecutionID
	p.mu.Unlock()
	return nil
}
func (p *fakeRunTunnelPair) OpenStream(uint16) (tunnel.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, errors.New("pair closed")
	}
	stream := &fakeRunTunnelStream{}
	p.streams = append(p.streams, stream)
	return stream, nil
}
func (p *fakeRunTunnelPair) Close() error {
	p.mu.Lock()
	p.closed = true
	streams := append([]*fakeRunTunnelStream(nil), p.streams...)
	p.mu.Unlock()
	for _, stream := range streams {
		_ = stream.Close()
	}
	return nil
}
func (p *fakeRunTunnelPair) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

type fakeRunTunnelStream struct {
	mu     sync.Mutex
	closed bool
}

func (s *fakeRunTunnelStream) Read([]byte) (int, error)    { return 0, io.EOF }
func (s *fakeRunTunnelStream) Write(b []byte) (int, error) { return len(b), nil }
func (s *fakeRunTunnelStream) PeerKey() []byte             { return []byte("allocator-key") }
func (s *fakeRunTunnelStream) CloseRead() error            { return nil }
func (s *fakeRunTunnelStream) CloseWrite() error           { return nil }
func (s *fakeRunTunnelStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
func (s *fakeRunTunnelStream) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
