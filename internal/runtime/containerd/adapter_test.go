package containerd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
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
	runtime.executions.mu.Lock()
	done := runtime.executions.entries[request.ExecutionID].done
	runtime.executions.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not finish")
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
	conflict := testRequest("execution")
	conflict.Workload.Args = []string{"exit 9"}
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

func TestRecoverPropagatesMissingRuntimeObject(t *testing.T) {
	implementation := newFakeBackend()
	implementation.recoverErr = ErrExecutionMissing
	runtime := newWithBackend(Config{}, implementation)
	if err := runtime.Recover(context.Background(), testRequest("execution", time.Minute), func(r1sruntime.Completion) error { return nil }); !errors.Is(err, ErrExecutionMissing) {
		t.Fatalf("Recover() error = %v, want ErrExecutionMissing", err)
	}
	if implementation.startCount() != 0 {
		t.Fatal("Recover() started a missing workload")
	}
}

func TestReporterFailureRetainsStoppedTaskForRecovery(t *testing.T) {
	implementation := newFakeBackend()
	runtime := newWithBackend(Config{}, implementation)
	reportErr := errors.New("state store unavailable")
	if err := runtime.Start(context.Background(), testRequest("execution", time.Minute), func(r1sruntime.Completion) error {
		return reportErr
	}); err != nil {
		t.Fatal(err)
	}
	implementation.process.exit(exitResult{code: 9})
	runtime.executions.mu.Lock()
	done := runtime.executions.entries["execution"].done
	runtime.executions.mu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runtime did not finish reporting")
	}
	if implementation.process.cleanupCount() != 0 {
		t.Fatal("runtime metadata was removed before terminal state became durable")
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

func TestRunMetadataIsFencedIntoFingerprintAndEnvironment(t *testing.T) {
	request := testRequest("execution")
	request.Workload.Environment[runEnv] = "forged"
	request.Workload.Environment[attemptEnv] = "999"
	values := environment(request.Workload, request.RunID, request.Attempt)
	wantRun := runEnv + "=" + request.RunID
	wantAttempt := attemptEnv + "=1"
	if !containsString(values, wantRun) || !containsString(values, wantAttempt) || containsString(values, runEnv+"=forged") || containsString(values, attemptEnv+"=999") {
		t.Fatalf("environment() = %v, want authoritative %q and %q", values, wantRun, wantAttempt)
	}
	first, err := fingerprint(request)
	if err != nil {
		t.Fatal(err)
	}
	request.Attempt++
	second, err := fingerprint(request)
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("attempt mutation did not change runtime fingerprint")
	}
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
