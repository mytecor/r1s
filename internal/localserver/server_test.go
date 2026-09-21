package localserver

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
)

// fakeBackend drives the local API against an in-memory engine without any RNS
// or state-store machinery, so the socket contract is exercised in isolation.
type fakeBackend struct {
	mu         sync.Mutex
	requests   []client.RequestSnapshot
	executions map[string]*r1sv1.ExecutionState
	watchSeq   uint64
	journal    []client.WatchEvent
	observers  []func(client.WatchEvent)

	runRequest func(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration, constraints *r1sv1.PlacementConstraints) (string, string, []byte, error)
	inspect    func(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error)
	cancel     func(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error)
	logs       func(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error)
	tunnelConn func(ctx context.Context, executionID string, targets []tunnel.Target, targetPort uint16) (tunnel.Conn, string, error)
}

func (f *fakeBackend) RunRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration, constraints *r1sv1.PlacementConstraints) (string, string, []byte, error) {
	return f.runRequest(ctx, workload, policy, resourceClass, offerWait, allocators, keepAlive, constraints)
}

func (f *fakeBackend) Inspect(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	return f.inspect(ctx, executionID, wait)
}

func (f *fakeBackend) Cancel(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	return f.cancel(ctx, executionID, reason, wait)
}

func (f *fakeBackend) Logs(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
	return f.logs(ctx, executionID, stream, offset, maxBytes, wait)
}

func (f *fakeBackend) Tunnel(ctx context.Context, executionID string, targets []tunnel.Target, targetPort uint16) (tunnel.Conn, string, error) {
	if f.tunnelConn == nil {
		return nil, "", io.ErrClosedPipe
	}
	return f.tunnelConn(ctx, executionID, targets, targetPort)
}

func (f *fakeBackend) Requests(ctx context.Context) []client.RequestSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func (f *fakeBackend) State(ctx context.Context, executionID string) (*r1sv1.ExecutionState, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	state, ok := f.executions[executionID]
	return state, ok, nil
}

func (f *fakeBackend) WatchSeq(ctx context.Context) uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watchSeq
}

func (f *fakeBackend) WatchAfter(ctx context.Context, after uint64) ([]client.WatchEvent, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var result []client.WatchEvent
	for _, event := range f.journal {
		if event.Sequence > after {
			result = append(result, event)
		}
	}
	return result, true
}

func (f *fakeBackend) SubscribeWatch(ctx context.Context, observer func(client.WatchEvent)) (cancel func()) {
	f.mu.Lock()
	f.observers = append(f.observers, observer)
	f.mu.Unlock()
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		for index, candidate := range f.observers {
			if &candidate == &observer {
				f.observers = append(f.observers[:index], f.observers[index+1:]...)
				break
			}
		}
	}
}

func (f *fakeBackend) emit(event client.WatchEvent) {
	f.mu.Lock()
	f.watchSeq = event.Sequence
	f.journal = append(f.journal, event)
	observers := make([]func(client.WatchEvent), len(f.observers))
	copy(observers, f.observers)
	f.mu.Unlock()
	for _, observer := range observers {
		observer(event)
	}
}

// TestLocalAPIContractThroughSocket exercises every unary operation through a
// real Unix socket.
func TestLocalAPIContractThroughSocket(t *testing.T) {
	backend := &fakeBackend{
		executions: make(map[string]*r1sv1.ExecutionState),
		runRequest: func(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string, keepAlive time.Duration, constraints *r1sv1.PlacementConstraints) (string, string, []byte, error) {
			return "request-1", "execution-1", []byte("allocator-a"), nil
		},
		inspect: func(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
			return state("execution-1", r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED), nil
		},
		cancel: func(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error) {
			return state(executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED), nil
		},
		logs: func(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
			return &r1sv1.ExecutionLogsResponse{ExecutionId: executionID, Stream: stream, Offset: offset, Data: []byte("hello"), NextOffset: offset + 5, Eof: true}, nil
		},
	}
	backend.requests = []client.RequestSnapshot{{Request: &r1sv1.ExecutionRequest{RequestId: "request-1", ResourceClass: "default"}, ExecutionID: "execution-1", OfferCount: 2}}

	clientConn := r1sv1.NewLocalClientClient(dialConn(t, backend))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Request
	reqResp, err := clientConn.Request(ctx, &r1sv1.LocalRequest{
		Workload:  &r1sv1.Workload{Image: "example.test/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		Policy:    &r1sv1.ExecutionPolicy{ResultRetention: durationpb.New(time.Hour)},
		OfferWait: durationpb.New(3 * time.Second),
	})
	if err != nil {
		t.Fatalf("Request: %v", err)
	}
	if reqResp.GetRequestId() != "request-1" || reqResp.GetExecutionId() != "execution-1" || string(reqResp.GetAllocator()) != "allocator-a" {
		t.Fatalf("Request response = %v", reqResp)
	}

	// List
	listResp, err := clientConn.List(ctx, &r1sv1.LocalListRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(listResp.GetRequests()) != 1 || listResp.GetRequests()[0].GetRequestId() != "request-1" {
		t.Fatalf("List response = %v", listResp)
	}

	// Inspect
	inspectResp, err := clientConn.Inspect(ctx, &r1sv1.LocalInspectRequest{ExecutionId: "execution-1", Wait: durationpb.New(2 * time.Second)})
	if err != nil {
		t.Fatalf("Inspect: %v", err)
	}
	if inspectResp.GetState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED {
		t.Fatalf("Inspect phase = %v", inspectResp.GetState().GetPhase())
	}

	// Result (terminal OK)
	if _, err := clientConn.Result(ctx, &r1sv1.LocalResultRequest{ExecutionId: "execution-1", Wait: durationpb.New(2 * time.Second)}); err != nil {
		t.Fatalf("Result: %v", err)
	}

	// Cancel
	cancelResp, err := clientConn.Cancel(ctx, &r1sv1.LocalCancelRequest{ExecutionId: "execution-1", Reason: "user"})
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if cancelResp.GetState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED {
		t.Fatalf("Cancel phase = %v", cancelResp.GetState().GetPhase())
	}

	// Logs
	logsResp, err := clientConn.Logs(ctx, &r1sv1.LocalLogsRequest{ExecutionId: "execution-1", Stream: "stdout", MaxBytes: 16})
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	if string(logsResp.GetData()) != "hello" || !logsResp.GetEof() {
		t.Fatalf("Logs response = %v", logsResp)
	}
}

func TestLocalAPIResultRefusesNonTerminal(t *testing.T) {
	backend := &fakeBackend{
		inspect: func(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
			return state(executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING), nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := r1sv1.NewLocalClientClient(dialConn(t, backend)).Result(ctx, &r1sv1.LocalResultRequest{ExecutionId: "e"}); err == nil {
		t.Fatal("Result succeeded for a running execution")
	}
}

func TestLocalAPIWatchStreamsOrderedRevisions(t *testing.T) {
	backend := &fakeBackend{executions: make(map[string]*r1sv1.ExecutionState)}
	clientConn := r1sv1.NewLocalClientClient(dialConn(t, backend))

	// Emit two durable transitions before the watcher subscribes.
	backend.emit(client.WatchEvent{Sequence: 1, ExecutionID: "e1", State: state("e1", r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING)})
	backend.emit(client.WatchEvent{Sequence: 2, ExecutionID: "e1", State: state("e1", r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING)})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := clientConn.Watch(ctx, &r1sv1.LocalWatchRequest{After: 1})
	if err != nil {
		t.Fatal(err)
	}
	// Replay of the one transition after position 1.
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.GetSequence() != 2 || first.GetExecutionId() != "e1" || first.GetState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("first watch event = %v", first)
	}

	// A live transition after subscription.
	backend.emit(client.WatchEvent{Sequence: 3, ExecutionID: "e1", State: state("e1", r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED)})
	second, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if second.GetSequence() != 3 || second.GetState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED {
		t.Fatalf("live watch event = %v", second)
	}
}

func TestLocalAPIWatchResumesFromDurablePosition(t *testing.T) {
	backend := &fakeBackend{executions: make(map[string]*r1sv1.ExecutionState)}
	for seq := uint64(1); seq <= 5; seq++ {
		backend.emit(client.WatchEvent{Sequence: seq, ExecutionID: "e1", State: state("e1", r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING)})
	}
	clientConn := r1sv1.NewLocalClientClient(dialConn(t, backend))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := clientConn.Watch(ctx, &r1sv1.LocalWatchRequest{After: 3})
	if err != nil {
		t.Fatal(err)
	}
	var sequences []uint64
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		sequences = append(sequences, event.GetSequence())
		if len(sequences) == 2 {
			break
		}
	}
	if len(sequences) != 2 || sequences[0] != 4 || sequences[1] != 5 {
		t.Fatalf("resumed sequences = %v, want [4 5]", sequences)
	}
}

func dialConn(t *testing.T, backend *fakeBackend) *grpc.ClientConn {
	t.Helper()
	// macOS Unix-socket paths are limited to ~104 bytes, so use a short path.
	socket := shortSocket(t)
	ctx, cancel := context.WithCancel(context.Background())
	serveErr := make(chan error, 1)
	go func() { serveErr <- New(backend).ListenAndServe(ctx, socket, 0o600) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		probe, err := net.Dial("unix", socket)
		if err == nil {
			probe.Close()
			break
		}
		select {
		case err := <-serveErr:
			t.Fatalf("local API server failed: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("socket did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return net.Dial("unix", socket)
	}))
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(); cancel() })
	return conn
}

func state(executionID string, phase r1sv1.ExecutionPhase) *r1sv1.ExecutionState {
	return &r1sv1.ExecutionState{ExecutionId: executionID, Phase: phase}
}

// shortSocket builds a short Unix-socket path (macOS caps paths at ~104 bytes).
func shortSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "r1s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "u.sock")
}
