package containerd

import (
	"context"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/types/known/durationpb"
)

func testRequest(executionID string, _ ...time.Duration) r1sruntime.StartRequest {
	return r1sruntime.StartRequest{
		ExecutionID: executionID,
		RunID:       "0123456789abcdef0123456789abcdef",
		Attempt:     1,
		Client:      []byte("client"),
		Workload: &r1sv1.Workload{
			Image:       "registry.example/r1s/fixture@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Command:     []string{"/bin/sh", "-c"},
			Args:        []string{"exit 7"},
			Environment: map[string]string{"B": "2", "A": "1"},
		},
		Policy: &r1sv1.ExecutionPolicy{ResultRetention: durationpb.New(time.Hour)},
	}
}

type fakeBackend struct {
	mu         sync.Mutex
	starts     int
	recovers   int
	stops      int
	closed     int
	process    *fakeProcess
	startErr   error
	recoverErr error
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

func (b *fakeBackend) Recover(context.Context, r1sruntime.StartRequest, string) (process, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.recovers++
	return b.process, b.recoverErr
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
