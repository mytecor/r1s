package containerd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"syscall"

	containerdclient "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/containerd/v2/pkg/oci"
	"github.com/containerd/containerd/v2/pkg/reference"
	"github.com/containerd/errdefs"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

const (
	executionLabel = "io.r1s.execution-id"
	specLabel      = "io.r1s.spec-digest"
)

type clientBackend struct {
	client      *containerdclient.Client
	namespace   string
	snapshotter string
}

type clientProcess struct {
	backend   *clientBackend
	container containerdclient.Container
	task      containerdclient.Task
	wait      <-chan exitResult
}

func newClientBackend(ctx context.Context, config Config) (*clientBackend, error) {
	client, err := containerdclient.New(config.Address, containerdclient.WithDefaultNamespace(config.Namespace))
	if err != nil {
		return nil, fmt.Errorf("connect to containerd: %w", err)
	}
	implementation := &clientBackend{client: client, namespace: config.Namespace, snapshotter: config.Snapshotter}
	if _, err := client.Version(implementation.context(ctx)); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("query containerd version: %w", err)
	}
	return implementation, nil
}

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

	task, err := container.NewTask(ctx, cio.NullIO)
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

func (b *clientBackend) resume(ctx context.Context, container containerdclient.Container, executionID, fingerprint string) (process, error) {
	if err := verifyLabels(ctx, container, executionID, fingerprint); err != nil {
		return nil, err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		if !errdefs.IsNotFound(err) {
			return nil, fmt.Errorf("load task %q: %w", container.ID(), err)
		}
		task, err = container.NewTask(ctx, cio.NullIO)
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

func (b *clientBackend) process(container containerdclient.Container, task containerdclient.Task) (*clientProcess, error) {
	statuses, err := task.Wait(b.context(context.Background()))
	if err != nil {
		return nil, fmt.Errorf("wait for task %q: %w", task.ID(), err)
	}
	wait := make(chan exitResult, 1)
	go func() {
		status, ok := <-statuses
		if !ok {
			wait <- exitResult{err: errors.New("containerd task wait channel closed without status")}
			close(wait)
			return
		}
		code, _, resultErr := status.Result()
		wait <- exitResult{code: code, err: resultErr}
		close(wait)
	}()
	return &clientProcess{backend: b, container: container, task: task, wait: wait}, nil
}

func (p *clientProcess) Wait() <-chan exitResult { return p.wait }

func (p *clientProcess) Kill(ctx context.Context) error {
	ctx = p.backend.context(ctx)
	status, err := p.task.Status(ctx)
	if err != nil {
		return ignoreNotFound(err)
	}
	if status.Status == containerdclient.Stopped {
		return nil
	}
	return ignoreNotFound(p.task.Kill(ctx, syscall.SIGKILL, containerdclient.WithKillAll))
}

func (p *clientProcess) Cleanup(ctx context.Context) error {
	ctx = p.backend.context(ctx)
	_, taskErr := p.task.Delete(ctx)
	containerErr := p.container.Delete(ctx, containerdclient.WithSnapshotCleanup)
	return errors.Join(ignoreNotFound(taskErr), ignoreNotFound(containerErr))
}

func (b *clientBackend) Close() error { return b.client.Close() }

func (b *clientBackend) context(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, b.namespace)
}

func verifyLabels(ctx context.Context, container containerdclient.Container, executionID, fingerprint string) error {
	labels, err := container.Labels(ctx)
	if err != nil {
		return fmt.Errorf("read container %q labels: %w", container.ID(), err)
	}
	if labels[executionLabel] != executionID || (fingerprint != "" && labels[specLabel] != fingerprint) {
		return fmt.Errorf("%w: %q", ErrExecutionConflict, executionID)
	}
	return nil
}

func pinnedDigest(image string) (string, error) {
	specification, err := reference.Parse(image)
	if err != nil {
		return "", fmt.Errorf("parse image reference %q: %w", image, err)
	}
	digest := specification.Digest()
	if digest == "" {
		return "", fmt.Errorf("image reference %q must be pinned by digest", image)
	}
	if err := digest.Validate(); err != nil {
		return "", fmt.Errorf("image reference %q has invalid digest: %w", image, err)
	}
	return digest.String(), nil
}

func containerID(executionID string) string {
	digest := sha256.Sum256([]byte(executionID))
	return "r1s-" + hex.EncodeToString(digest[:])
}

func ignoreNotFound(err error) error {
	if errdefs.IsNotFound(err) {
		return nil
	}
	return err
}
