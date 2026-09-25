package containerd

import (
	"context"
	"errors"
	"fmt"

	containerdclient "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// Recover reattaches only to durable objects that prove the expected execution
// identity and specification. It never provisions or starts missing work.
func (b *clientBackend) Recover(ctx context.Context, request r1sruntime.StartRequest, fingerprint string) (process, error) {
	ctx = b.context(ctx)
	container, err := b.client.LoadContainer(ctx, containerID(request.ExecutionID))
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %q", ErrExecutionMissing, request.ExecutionID)
		}
		return nil, fmt.Errorf("load recovery container %q: %w", containerID(request.ExecutionID), err)
	}
	if err := verifyLabels(ctx, container, request.ExecutionID, fingerprint); err != nil {
		return nil, err
	}
	if err := verifyRunLabels(ctx, container, request); err != nil {
		return nil, err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			cleanupErr := container.Delete(ctx, containerdclient.WithSnapshotCleanup)
			return nil, errors.Join(fmt.Errorf("%w: task for %q", ErrExecutionMissing, request.ExecutionID), ignoreNotFound(cleanupErr))
		}
		return nil, fmt.Errorf("load recovery task %q: %w", container.ID(), err)
	}
	status, err := task.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect recovery task %q: %w", container.ID(), err)
	}
	if status.Status == containerdclient.Created {
		_, taskErr := task.Delete(ctx, containerdclient.WithProcessKill)
		containerErr := container.Delete(ctx, containerdclient.WithSnapshotCleanup)
		return nil, errors.Join(fmt.Errorf("%w: task for %q was never started", ErrExecutionMissing, request.ExecutionID), ignoreNotFound(taskErr), ignoreNotFound(containerErr))
	}
	return b.process(container, task)
}

// Stop is the backend-level fallback for executions absent from the in-memory
// registry, such as work left behind by a previous allocator process.
func (b *clientBackend) Stop(ctx context.Context, executionID string) error {
	ctx = b.context(ctx)
	container, err := b.client.LoadContainer(ctx, containerID(executionID))
	if err != nil {
		if errdefs.IsNotFound(err) {
			return nil
		}
		return err
	}
	if err := verifyLabels(ctx, container, executionID, ""); err != nil {
		return err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		if errdefs.IsNotFound(err) {
			return ignoreNotFound(container.Delete(ctx, containerdclient.WithSnapshotCleanup))
		}
		return err
	}
	started, err := b.process(container, task)
	if err != nil {
		return err
	}
	if err := started.Kill(ctx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-started.Wait():
		return started.Cleanup(ctx)
	}
}
