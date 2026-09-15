package containerd

import (
	"context"
	"fmt"

	containerdclient "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/errdefs"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// Start provisions the image, container, and task needed for a new execution.
// Existing matching objects are resumed to make retries idempotent.
func (b *clientBackend) Start(ctx context.Context, request r1sruntime.StartRequest, fingerprint string) (process, error) {
	ctx = b.context(ctx)
	containerID := containerID(request.ExecutionID)
	container, err := b.client.LoadContainer(ctx, containerID)
	if err == nil {
		return b.resume(ctx, container, request.ExecutionID, fingerprint)
	}
	if !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("load container %q: %w", containerID, err)
	}

	requestedDigest, err := pinnedDigest(request.Workload.GetImage())
	if err != nil {
		return nil, err
	}
	pullOptions := []containerdclient.RemoteOpt{containerdclient.WithPullUnpack}
	if b.snapshotter != "" {
		pullOptions = append(pullOptions, containerdclient.WithPullSnapshotter(b.snapshotter))
	}
	image, err := b.client.Pull(ctx, request.Workload.GetImage(), pullOptions...)
	if err != nil {
		return nil, fmt.Errorf("pull image %q: %w", request.Workload.GetImage(), err)
	}
	if actual := image.Target().Digest.String(); actual != requestedDigest {
		return nil, fmt.Errorf("pulled image digest %q does not match requested digest %q", actual, requestedDigest)
	}

	specOptions := []oci.SpecOpts{oci.WithImageConfig(image)}
	if r := request.Resources; r != (r1sruntime.Resources{}) {
		specOptions = append(specOptions, oci.WithMemoryLimit(uint64(r.MemoryBytes)), oci.WithCPUCFS(r.CPUMilli*100, 100000), oci.WithPidsLimit(r.Pids))
	}
	if len(request.Workload.GetCommand()) > 0 {
		arguments := append([]string{}, request.Workload.GetCommand()...)
		arguments = append(arguments, request.Workload.GetArgs()...)
		specOptions = append(specOptions, oci.WithProcessArgs(arguments...))
	} else if len(request.Workload.GetArgs()) > 0 {
		specOptions[0] = oci.WithImageConfigArgs(image, request.Workload.GetArgs())
	}
	if values := environment(request.Workload); len(values) > 0 {
		specOptions = append(specOptions, oci.WithEnv(values))
	}
	if directory := request.Workload.GetWorkingDirectory(); directory != "" {
		specOptions = append(specOptions, oci.WithProcessCwd(directory))
	}

	containerOptions := []containerdclient.NewContainerOpts{}
	if b.snapshotter != "" {
		containerOptions = append(containerOptions, containerdclient.WithSnapshotter(b.snapshotter))
	}
	containerOptions = append(containerOptions,
		containerdclient.WithImage(image),
		containerdclient.WithNewSnapshot(containerID+"-rootfs", image),
		containerdclient.WithNewSpec(specOptions...),
		containerdclient.WithContainerLabels(map[string]string{
			executionLabel: request.ExecutionID,
			specLabel:      fingerprint,
		}),
	)
	container, err = b.client.NewContainer(ctx, containerID, containerOptions...)
	if err != nil {
		if errdefs.IsAlreadyExists(err) {
			container, err = b.client.LoadContainer(ctx, containerID)
			if err == nil {
				return b.resume(ctx, container, request.ExecutionID, fingerprint)
			}
		}
		return nil, fmt.Errorf("create container %q: %w", containerID, err)
	}
	created := true
	defer func() {
		if created {
			_ = container.Delete(b.context(context.Background()), containerdclient.WithSnapshotCleanup)
		}
	}()

	taskIO, err := b.taskIO(request.ExecutionID)
	if err != nil {
		return nil, err
	}
	task, err := container.NewTask(ctx, taskIO)
	if err != nil {
		return nil, fmt.Errorf("create task %q: %w", containerID, err)
	}
	started, err := b.process(container, task)
	if err != nil {
		_, _ = task.Delete(b.context(context.Background()), containerdclient.WithProcessKill)
		return nil, err
	}
	if err := task.Start(ctx); err != nil {
		_, _ = task.Delete(b.context(context.Background()), containerdclient.WithProcessKill)
		return nil, fmt.Errorf("start task %q: %w", containerID, err)
	}
	created = false
	return started, nil
}

func (b *clientBackend) resume(ctx context.Context, container containerdclient.Container, executionID, fingerprint string) (process, error) {
	if err := verifyLabels(ctx, container, executionID, fingerprint); err != nil {
		return nil, err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("load task %q: %w", container.ID(), err)
		}
		taskIO, ioErr := b.taskIO(executionID)
		if ioErr != nil {
			return nil, ioErr
		}
		task, err = container.NewTask(ctx, taskIO)
		if err != nil {
			return nil, fmt.Errorf("recreate task %q: %w", container.ID(), err)
		}
		started, waitErr := b.process(container, task)
		if waitErr != nil {
			return nil, waitErr
		}
		if err := task.Start(ctx); err != nil {
			_, _ = task.Delete(b.context(context.Background()), containerdclient.WithProcessKill)
			return nil, fmt.Errorf("start recreated task %q: %w", container.ID(), err)
		}
		return started, nil
	}
	started, err := b.process(container, task)
	if err != nil {
		return nil, err
	}
	status, err := task.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect task %q: %w", container.ID(), err)
	}
	if status.Status == containerdclient.Created {
		if err := task.Start(ctx); err != nil {
			return nil, fmt.Errorf("start existing task %q: %w", container.ID(), err)
		}
	}
	return started, nil
}
