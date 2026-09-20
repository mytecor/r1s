package localapi

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/localserver"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/types/known/durationpb"
)

// stubBackend serves a minimal local API so the client contract is exercised
// against a real Unix socket.
type stubBackend struct {
	requestID   string
	executionID string
	allocator   []byte
	requests    int
	state       *r1sv1.ExecutionState
	logsData    []byte

	maintainRerequest bool
}

func (s *stubBackend) RunRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration, constraints *r1sv1.PlacementConstraints) (string, string, []byte, error) {
	return s.requestID, s.executionID, s.allocator, nil
}

func (s *stubBackend) Inspect(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	return s.state, nil
}

func (s *stubBackend) Cancel(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	return s.state, nil
}

func (s *stubBackend) Logs(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
	return &r1sv1.ExecutionLogsResponse{ExecutionId: executionID, Stream: stream, Offset: offset, Data: s.logsData, NextOffset: offset + uint64(len(s.logsData)), Eof: true}, nil
}

func (s *stubBackend) Requests(ctx context.Context) []client.RequestSnapshot {
	s.requests++
	return nil
}

func (s *stubBackend) State(ctx context.Context, executionID string) (*r1sv1.ExecutionState, bool, error) {
	return s.state, s.state != nil, nil
}

func (s *stubBackend) WatchSeq(ctx context.Context) uint64 { return 0 }

func (s *stubBackend) WatchAfter(ctx context.Context, after uint64) ([]client.WatchEvent, bool) {
	return nil, true
}

func (s *stubBackend) SubscribeWatch(ctx context.Context, observer func(client.WatchEvent)) (cancel func()) {
	return func() {}
}

// Tunnel is not exercised by the basic client contract tests.
func (s *stubBackend) Tunnel(ctx context.Context, executionID, targetSlot string) (tunnel.Conn, string, error) {
	return nil, "", errors.New("tunnel: not exercised")
}

func TestLocalAPIClientTalksToServer(t *testing.T) {
	socket := shortSocket(t)
	backend := &stubBackend{requestID: "request-1", executionID: "execution-1", allocator: []byte("allocator-a"), state: &r1sv1.ExecutionState{ExecutionId: "execution-1", Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED}, logsData: []byte("log-line")}
	ctx, cancel := context.WithCancel(context.Background())
	go localserver.New(backend).ListenAndServe(ctx, socket, 0o600)
	t.Cleanup(cancel)
	waitReady(t, socket)

	client, err := Dial(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if err := client.Ping(context.Background()); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	requestID, executionID, allocator, err := client.Request(context.Background(), &r1sv1.Workload{Image: "example.test/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}, &r1sv1.ExecutionPolicy{ResultRetention: durationpb.New(time.Hour)}, "default", 2*time.Second, nil, 0, nil)
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if requestID != "request-1" || executionID != "execution-1" || string(allocator) != "allocator-a" {
		t.Fatalf("Request = %s %s %x", requestID, executionID, allocator)
	}

	state, err := client.Inspect(context.Background(), "execution-1", 2*time.Second)
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if state.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED {
		t.Fatalf("Inspect phase = %v", state.GetPhase())
	}

	if _, err := client.Result(context.Background(), "execution-1", 2*time.Second); err != nil {
		t.Fatalf("Result: %v", err)
	}

	logs, err := client.Logs(context.Background(), "execution-1", "stdout", 0, 16, 2*time.Second)
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	if string(logs.GetData()) != "log-line" {
		t.Fatalf("Logs data = %q", logs.GetData())
	}

	if _, err := client.List(context.Background()); err != nil {
		t.Fatalf("List: %v", err)
	}
}

func TestLocalAPIClientUnavailable(t *testing.T) {
	client, err := Dial(shortSocket(t))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Ping(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Ping = %v, want ErrUnavailable", err)
	}
}

func shortSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "r1s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "u.sock")
}

func waitReady(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("socket did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
