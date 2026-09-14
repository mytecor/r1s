package containerd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestStartIsIdempotentAndReportsExit(t *testing.T) {
	implementation := newFakeBackend()
	runtime := newWithBackend(Config{}, implementation)
	request := testRequest("execution", time.Minute)
	reported := make(chan r1sruntime.Completion, 1)
	reporter := func(completion r1sruntime.Completion) error {
		reported <- completion
		return nil
	}

	const callers = 12
	var wait sync.WaitGroup
	errorsSeen := make(chan error, callers)
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			errorsSeen <- runtime.Start(context.Background(), request, reporter)
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Errorf("Start() error = %v", err)
		}
	}
	if implementation.startCount() != 1 {
		t.Fatalf("backend starts = %d, want 1", implementation.startCount())
	}

	implementation.process.exit(exitResult{code: 7})
	select {
	case completion := <-reported:
		if completion.Err != nil || completion.ExitCode == nil || *completion.ExitCode != 7 {
			t.Fatalf("completion = %+v", completion)
		}
	case <-time.After(time.Second):
		t.Fatal("completion was not reported")
	}
	if implementation.process.cleanupCount() != 1 {
		t.Fatalf("cleanups = %d, want 1", implementation.process.cleanupCount())
	}
	if err := runtime.Start(context.Background(), request, reporter); err != nil {
		t.Fatalf("completed duplicate Start() error = %v", err)
	}
	if implementation.startCount() != 1 {
		t.Fatalf("completed duplicate restarted backend: %d", implementation.startCount())
	}
}

func TestStartRejectsConflictingExecution(t *testing.T) {
	implementation := newFakeBackend()
	runtime := newWithBackend(Config{}, implementation)
	request := testRequest("execution", time.Minute)
	if err := runtime.Start(context.Background(), request, func(r1sruntime.Completion) error { return nil }); err != nil {
		t.Fatal(err)
	}
	conflict := testRequest("execution", 2*time.Minute)
	if err := runtime.Start(context.Background(), conflict, func(r1sruntime.Completion) error { return nil }); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("conflicting Start() error = %v, want ErrExecutionConflict", err)
	}
}

func TestStopKillsAndCleansWithoutReportingCompletion(t *testing.T) {
	implementation := newFakeBackend()
	runtime := newWithBackend(Config{}, implementation)
	reported := make(chan r1sruntime.Completion, 1)
	if err := runtime.Start(context.Background(), testRequest("execution", time.Minute), func(completion r1sruntime.Completion) error {
		reported <- completion
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := runtime.Stop(ctx, "execution"); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Stop(ctx, "execution"); err != nil {
		t.Fatalf("duplicate Stop() error = %v", err)
	}
	if implementation.process.killCount() != 1 || implementation.process.cleanupCount() != 1 {
		t.Fatalf("kills=%d cleanups=%d, want 1 each", implementation.process.killCount(), implementation.process.cleanupCount())
	}
	select {
	case completion := <-reported:
		t.Fatalf("cancellation reported ordinary completion: %+v", completion)
	default:
	}
}

func TestMaximumRuntimeIsEnforcedLocally(t *testing.T) {
	implementation := newFakeBackend()
	runtime := newWithBackend(Config{CleanupTimeout: time.Second}, implementation)
	reported := make(chan r1sruntime.Completion, 1)
	if err := runtime.Start(context.Background(), testRequest("execution", 10*time.Millisecond), func(completion r1sruntime.Completion) error {
		reported <- completion
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case completion := <-reported:
		if !errors.Is(completion.Err, ErrDeadlineExceeded) {
			t.Fatalf("completion error = %v, want ErrDeadlineExceeded", completion.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("maximum runtime was not enforced")
	}
	if implementation.process.killCount() != 1 || implementation.process.cleanupCount() != 1 {
		t.Fatalf("kills=%d cleanups=%d, want 1 each", implementation.process.killCount(), implementation.process.cleanupCount())
	}
}

func TestCancelledStopDoesNotSuppressLaterTerminalReport(t *testing.T) {
	implementation := newFakeBackend()
	implementation.process.killDoesNotExit = true
	runtime := newWithBackend(Config{}, implementation)
	reported := make(chan r1sruntime.Completion, 1)
	if err := runtime.Start(context.Background(), testRequest("execution", time.Minute), func(completion r1sruntime.Completion) error {
		reported <- completion
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := runtime.Stop(ctx, "execution"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Stop() error = %v, want context deadline", err)
	}
	implementation.process.exit(exitResult{code: 137})
	select {
	case completion := <-reported:
		if !errors.Is(completion.Err, context.DeadlineExceeded) {
			t.Fatalf("completion error = %v, want abandoned stop error", completion.Err)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal completion remained suppressed after Stop returned an error")
	}
}

func TestPinnedDigestAndContainerID(t *testing.T) {
	valid := "registry.example/r1s/fixture@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if digest, err := pinnedDigest(valid); err != nil || digest != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" {
		t.Fatalf("pinnedDigest() = %q, %v", digest, err)
	}
	for _, image := range []string{"registry.example/r1s/fixture:latest", "not a reference", "registry.example/r1s/fixture@sha256:short"} {
		if _, err := pinnedDigest(image); err == nil {
			t.Errorf("pinnedDigest(%q) succeeded", image)
		}
	}
	first := containerID("execution")
	if first != containerID("execution") || first == containerID("other") || len(first) != 68 {
		t.Fatalf("unexpected container IDs: %q and %q", first, containerID("other"))
	}
}

func testRequest(executionID string, maximum time.Duration) r1sruntime.StartRequest {
	return r1sruntime.StartRequest{
		ExecutionID: executionID,
		Owner:       []byte("owner"),
		Workload: &r1sv1.Workload{
			Image:       "registry.example/r1s/fixture@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Command:     []string{"/bin/sh", "-c"},
			Args:        []string{"exit 7"},
			Environment: map[string]string{"B": "2", "A": "1"},
		},
		Policy: &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(maximum)},
	}
}

type fakeBackend struct {
	mu       sync.Mutex
	starts   int
	stops    int
	closed   int
	process  *fakeProcess
	startErr error
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{process: newFakeProcess()}
}

func (b *fakeBackend) Start(context.Context, r1sruntime.StartRequest, string) (process, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.starts++
	return b.process, b.startErr
}

func (b *fakeBackend) Stop(context.Context, string) error {
	b.mu.Lock()
	b.stops++
	b.mu.Unlock()
	return nil
}

func (b *fakeBackend) Close() error {
	b.mu.Lock()
	b.closed++
	b.mu.Unlock()
	return nil
}

func (b *fakeBackend) startCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.starts
}

type fakeProcess struct {
	mu              sync.Mutex
	wait            chan exitResult
	kills           int
	cleanups        int
	exitOnce        sync.Once
	killDoesNotExit bool
}

func newFakeProcess() *fakeProcess {
	return &fakeProcess{wait: make(chan exitResult, 1)}
}

func (p *fakeProcess) Wait() <-chan exitResult { return p.wait }

func (p *fakeProcess) Kill(context.Context) error {
	p.mu.Lock()
	p.kills++
	p.mu.Unlock()
	if !p.killDoesNotExit {
		p.exit(exitResult{code: 137})
	}
	return nil
}

func (p *fakeProcess) Cleanup(context.Context) error {
	p.mu.Lock()
	p.cleanups++
	p.mu.Unlock()
	return nil
}

func (p *fakeProcess) exit(result exitResult) {
	p.exitOnce.Do(func() {
		p.wait <- result
		close(p.wait)
	})
}

func (p *fakeProcess) killCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.kills
}

func (p *fakeProcess) cleanupCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cleanups
}
