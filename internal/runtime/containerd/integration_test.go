package containerd

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestContainerdFixtureLifecycle(t *testing.T) {
	if os.Getenv("RUN_CONTAINERD_INTEGRATION") != "1" {
		t.Skip("set RUN_CONTAINERD_INTEGRATION=1 to run against a live containerd daemon")
	}
	image := os.Getenv("R1S_CONTAINERD_TEST_IMAGE")
	if image == "" {
		t.Fatal("R1S_CONTAINERD_TEST_IMAGE must name a digest-pinned fixture image")
	}
	address := os.Getenv("CONTAINERD_ADDRESS")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	runtime, err := New(ctx, Config{Address: address, Namespace: "r1s-integration"})
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()

	executionID := fmt.Sprintf("fixture-exit-%d", time.Now().UnixNano())
	reported := make(chan r1sruntime.Completion, 1)
	request := r1sruntime.StartRequest{
		ExecutionID: executionID,
		Owner:       []byte("integration-owner"),
		Workload: &r1sv1.Workload{
			Image:   image,
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{"exit 7"},
		},
		Policy: &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute)},
	}
	if err := runtime.Start(ctx, request, func(completion r1sruntime.Completion) error {
		reported <- completion
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(ctx, request, func(r1sruntime.Completion) error { return nil }); err != nil {
		t.Fatalf("duplicate Start() error = %v", err)
	}
	select {
	case completion := <-reported:
		if completion.Err != nil || completion.ExitCode == nil || *completion.ExitCode != 7 {
			t.Fatalf("completion = %+v", completion)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	cancelID := fmt.Sprintf("fixture-cancel-%d", time.Now().UnixNano())
	cancelRequest := request
	cancelRequest.ExecutionID = cancelID
	cancelRequest.Workload = &r1sv1.Workload{
		Image:   image,
		Command: []string{"/bin/sh", "-c"},
		Args:    []string{"while true; do sleep 1; done"},
	}
	if err := runtime.Start(ctx, cancelRequest, func(r1sruntime.Completion) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Stop(ctx, cancelID); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Stop(ctx, cancelID); err != nil {
		t.Fatalf("duplicate Stop() error = %v", err)
	}
}

func TestContainerdFixtureRecovery(t *testing.T) {
	if os.Getenv("RUN_CONTAINERD_INTEGRATION") != "1" {
		t.Skip("set RUN_CONTAINERD_INTEGRATION=1 to run against a live containerd daemon")
	}
	image := os.Getenv("R1S_CONTAINERD_TEST_IMAGE")
	if image == "" {
		t.Fatal("R1S_CONTAINERD_TEST_IMAGE must name a digest-pinned fixture image")
	}
	config := withDefaults(Config{Address: os.Getenv("CONTAINERD_ADDRESS"), Namespace: "r1s-integration-recovery"})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	startDetached := func(request r1sruntime.StartRequest) {
		t.Helper()
		implementation, err := newClientBackend(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		fingerprint, err := validateAndFingerprint(request, func(r1sruntime.Completion) error { return nil }, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := implementation.Start(ctx, request, fingerprint); err != nil {
			t.Fatal(err)
		}
		if err := implementation.Close(); err != nil {
			t.Fatal(err)
		}
	}

	completed := r1sruntime.StartRequest{
		ExecutionID: fmt.Sprintf("fixture-recover-complete-%d", time.Now().UnixNano()),
		Owner:       []byte("integration-owner"),
		Workload: &r1sv1.Workload{
			Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"sleep 1; exit 19"},
		},
		Policy:    &r1sv1.ExecutionPolicy{MaxRuntime: durationpb.New(time.Minute)},
		StartedAt: time.Now(),
	}
	startDetached(completed)
	time.Sleep(1500 * time.Millisecond)

	recovered, err := New(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	reported := make(chan r1sruntime.Completion, 1)
	if err := recovered.Recover(ctx, completed, func(completion r1sruntime.Completion) error {
		reported <- completion
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case completion := <-reported:
		if completion.Err != nil || completion.ExitCode == nil || *completion.ExitCode != 19 {
			t.Fatalf("recovered completion = %+v", completion)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	running := completed
	running.ExecutionID = fmt.Sprintf("fixture-recover-running-%d", time.Now().UnixNano())
	running.StartedAt = time.Now()
	running.Workload = &r1sv1.Workload{
		Image: image, Command: []string{"/bin/sh", "-c"}, Args: []string{"while true; do sleep 1; done"},
	}
	startDetached(running)
	if err := recovered.Recover(ctx, running, func(r1sruntime.Completion) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := recovered.Stop(ctx, running.ExecutionID); err != nil {
		t.Fatal(err)
	}
}
