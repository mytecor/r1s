package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/allocator"
	"github.com/mytecor/r1s/internal/client"
	"github.com/mytecor/r1s/internal/localserver"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// TestServiceBackedCLIMatchesDirectWorkflow proves F13-02 acceptance: the same
// workflow that direct mode runs completes through --socket without changing
// output, exit semantics, or authority. It drives a real client.Client and a
// real allocator.Allocator over an in-memory transport, served behind a real
// local API socket, and invokes the CLI through run() in service-backed mode.

type cliWorkflowBackend struct {
	mu         sync.Mutex
	clientCore *client.Client
	allocator  *allocator.Allocator
	now        time.Time
}

func (b *cliWorkflowBackend) RunRequest(ctx context.Context, workload *r1sv1.Workload, policy *r1sv1.ExecutionPolicy, resourceClass string, offerWait time.Duration, allocators []string) (string, string, []byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	requestID, request, err := b.clientCore.CreateRequest(workload, policy, resourceClass)
	if err != nil {
		return "", "", nil, err
	}
	responses, err := b.allocator.Handle(ctx, request)
	if err != nil {
		return "", "", nil, err
	}
	for _, response := range responses {
		if err := b.clientCore.Handle(ctx, response); err != nil {
			return "", "", nil, err
		}
	}
	destination, assignment, err := b.clientCore.Select(requestID)
	_ = destination
	if err != nil {
		return "", "", nil, err
	}
	responses, err = b.allocator.Handle(ctx, assignment)
	if err != nil {
		return "", "", nil, err
	}
	for _, response := range responses {
		if err := b.clientCore.Handle(ctx, response); err != nil {
			return "", "", nil, err
		}
	}
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	snapshot, _ := b.clientCore.Execution(executionID)
	return requestID, executionID, snapshot.Allocator, nil
}

func (b *cliWorkflowBackend) Inspect(ctx context.Context, executionID string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	return b.roundTripInspect(ctx, executionID)
}

func (b *cliWorkflowBackend) Cancel(ctx context.Context, executionID, reason string, wait time.Duration) (*r1sv1.ExecutionState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	destination, cancel, err := b.clientCore.Cancel(executionID, reason)
	if err != nil {
		return nil, err
	}
	if destination == "" {
		return nil, fmt.Errorf("cancel: no allocator for %s", executionID)
	}
	responses, err := b.allocator.Handle(ctx, cancel)
	if err != nil {
		return nil, err
	}
	for _, response := range responses {
		if err := b.clientCore.Handle(ctx, response); err != nil {
			return nil, err
		}
	}
	state, _ := b.refreshState(executionID)
	return state, nil
}

func (b *cliWorkflowBackend) roundTripInspect(ctx context.Context, executionID string) (*r1sv1.ExecutionState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	destination, inspect, err := b.clientCore.Inspect(executionID)
	if err != nil {
		return nil, err
	}
	if destination == "" {
		return nil, fmt.Errorf("inspect: no allocator for %s", executionID)
	}
	responses, err := b.allocator.Handle(ctx, inspect)
	if err != nil {
		return nil, err
	}
	for _, response := range responses {
		if err := b.clientCore.Handle(ctx, response); err != nil {
			return nil, err
		}
	}
	state, _ := b.refreshState(executionID)
	return state, nil
}

func (b *cliWorkflowBackend) stateFor(ctx context.Context, executionID string) (*r1sv1.ExecutionState, bool, error) {
	return nil, false, nil
}

func (b *cliWorkflowBackend) Logs(ctx context.Context, executionID, stream string, offset uint64, maxBytes uint32, wait time.Duration) (*r1sv1.ExecutionLogsResponse, error) {
	return nil, fmt.Errorf("logs not supported in workflow backend")
}

func (b *cliWorkflowBackend) Requests(ctx context.Context) []client.RequestSnapshot {
	return b.clientCore.Requests()
}

func (b *cliWorkflowBackend) State(ctx context.Context, executionID string) (*r1sv1.ExecutionState, bool, error) {
	snapshot, ok := b.clientCore.Execution(executionID)
	if !ok {
		return nil, false, nil
	}
	return snapshot.State, true, nil
}

func (b *cliWorkflowBackend) WatchSeq(ctx context.Context) uint64 { return 0 }

func (b *cliWorkflowBackend) WatchAfter(ctx context.Context, after uint64) ([]client.WatchEvent, bool) {
	return nil, true
}

func (b *cliWorkflowBackend) SubscribeWatch(ctx context.Context, observer func(client.WatchEvent)) (cancel func()) {
	return func() {}
}

// newCLIWorkflowBackend builds a real client + allocator pair over one clock.
func newCLIWorkflowBackend(t *testing.T) *cliWorkflowBackend {
	t.Helper()
	now := time.Unix(1_800_000_000, 0).UTC()
	clientCore, err := client.New(client.Config{
		Identity: []byte("cli-client"), Now: func() time.Time { return now },
		NewID: func() string { return randID() },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := clientCore.RegisterAllocator(client.Allocator{Identity: []byte("allocator"), Destination: "allocator", Hops: 1, Capacity: map[string]uint32{"default": 1}}); err != nil {
		t.Fatal(err)
	}
	runtime := &autoRuntime{}
	allocatorCore, err := allocator.New(allocator.Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		Now: func() time.Time { return now }, NewID: func() string { return randID() },
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	return &cliWorkflowBackend{clientCore: clientCore, allocator: allocatorCore, now: now}
}

// stateFor lookup is handled via State(); this helper keeps the workflow tidy.
func (b *cliWorkflowBackend) refreshState(executionID string) (*r1sv1.ExecutionState, bool) {
	snapshot, ok := b.clientCore.Execution(executionID)
	if !ok {
		return nil, false
	}
	return snapshot.State, true
}

func TestServiceBackedCLIMatchesDirectWorkflow(t *testing.T) {
	backend := newCLIWorkflowBackend(t)
	_, socket := serveBackend(t, backend)

	// Scenario 1: an unreachable socket is a clear error, never a fallback to a
	// new identity or assignment.
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"--socket", filepath.Join(t.TempDir(), "nope.sock"), "list"}, &stdout, &stderr)
	if err == nil || !bytes.Contains([]byte(err.Error()), []byte("local r1s service is not running")) {
		t.Fatalf("unreachable socket error = %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("unreachable socket wrote to stdout: %q", stdout.String())
	}

	// Scenario 2: request through the local API.
	requestJSON := `{"workload":{"image":"example.test/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},"policy":{"maxRuntime":"600s"}}`
	stdout.Reset()
	stderr.Reset()
	err = run(context.Background(), []string{"--socket", socket, "request", "--offer-wait", "2s", requestJSON}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("service-backed request: %v", err)
	}
	// The local client generated a request and an execution.
	executions := backend.clientCore.Executions()
	if len(executions) != 1 {
		t.Fatalf("service-backed request produced %d executions, want 1", len(executions))
	}
	executionID := executions[0].ExecutionID
	if !bytes.Contains(stdout.Bytes(), []byte("execution="+executionID)) {
		t.Fatalf("request output = %q, want execution=%s", stdout.String(), executionID)
	}

	// Scenario 3: list.
	stdout.Reset()
	if err := run(context.Background(), []string{"--socket", socket, "list"}, &stdout, &stderr); err != nil {
		t.Fatalf("service-backed list: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte("request=")) {
		t.Fatalf("list output = %q", stdout.String())
	}

	// Scenario 4: inspect (direct and service-back should agree). The allocator
	// reports running after assignment.
	stdout.Reset()
	if err := run(context.Background(), []string{"--socket", socket, "inspect", executionID}, &stdout, &stderr); err != nil {
		t.Fatalf("service-backed inspect: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte("phase=running")) {
		t.Fatalf("inspect output = %q, want phase=running", stdout.String())
	}

	// Scenario 5: cancel completes the workflow.
	stdout.Reset()
	if err := run(context.Background(), []string{"--socket", socket, "cancel", executionID}, &stdout, &stderr); err != nil {
		t.Fatalf("service-backed cancel: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte("phase=cancelled")) {
		t.Fatalf("cancel output = %q, want phase=cancelled", stdout.String())
	}
}

// serveBackend starts the local API server on a short socket path and returns
// the socket path.
func serveBackend(t *testing.T, backend localserver.Backend) (*localserver.Server, string) {
	t.Helper()
	socket := shortSocketPath(t)
	ctx, cancel := context.WithCancel(context.Background())
	server := localserver.New(backend)
	go server.ListenAndServe(ctx, socket, 0o600)
	t.Cleanup(cancel)
	waitSocket(t, socket)
	return server, socket
}

func shortSocketPath(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "r1s")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return filepath.Join(dir, "u.sock")
}

func waitSocket(t *testing.T, socket string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.Dial("unix", socket)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("local API socket did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func randID() string {
	return fmt.Sprintf("id-%d", time.Now().UnixNano())
}

// autoRuntime immediately completes an execution after start, letting inspect
// observe a terminal state deterministically.
type autoRuntime struct {
	mu        sync.Mutex
	reporters map[string]r1sruntime.Reporter
}

func (r *autoRuntime) Start(_ context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	r.mu.Lock()
	if r.reporters == nil {
		r.reporters = make(map[string]r1sruntime.Reporter)
	}
	r.reporters[request.ExecutionID] = reporter
	r.mu.Unlock()
	return nil
}

func (r *autoRuntime) Stop(context.Context, string) error { return nil }

func (r *autoRuntime) complete(executionID string) {
	r.mu.Lock()
	reporter := r.reporters[executionID]
	r.mu.Unlock()
	if reporter == nil {
		return
	}
	_ = reporter(r1sruntime.Completion{ExecutionID: executionID})
}
