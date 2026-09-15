package containerd

import (
	"context"
	"errors"
	"fmt"
	"syscall"

	containerdclient "github.com/containerd/containerd/v2/client"
)

// clientProcess adapts one concrete containerd task to the process contract
// consumed by the execution manager.
type clientProcess struct {
	backend   *clientBackend
	container containerdclient.Container
	task      containerdclient.Task
	wait      <-chan exitResult
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
