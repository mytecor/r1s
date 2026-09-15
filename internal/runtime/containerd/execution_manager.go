package containerd

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

type execution struct {
	fingerprint string
	ready       chan struct{}
	done        chan struct{}
	process     process
	startErr    error
	doneErr     error
	stopping    bool
	finishing   bool
	stopFailure error
}

// executionManager serializes access to the local execution registry. It is
// the idempotency and coordination layer between the runtime API and backend.
type executionManager struct {
	mu sync.Mutex

	backend        backend
	cleanupTimeout time.Duration
	now            func() time.Time
	closed         bool
	entries        map[string]*execution
}

func newExecutionManager(implementation backend, cleanupTimeout time.Duration, now func() time.Time) *executionManager {
	return &executionManager{
		backend:        implementation,
		cleanupTimeout: cleanupTimeout,
		now:            now,
		entries:        make(map[string]*execution),
	}
}

func (m *executionManager) start(ctx context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	spec, err := prepareExecution(request, reporter, m.now(), false)
	if err != nil {
		return err
	}

	current, owner, err := m.claim(request.ExecutionID, spec.fingerprint)
	if err != nil {
		return err
	}
	if !owner {
		return waitUntilReady(ctx, current)
	}

	startContext, cancel := spec.startContext(ctx)
	started, startErr := m.backend.Start(startContext, spec.request, spec.fingerprint)
	cancel()
	m.finishStart(request.ExecutionID, current, started, startErr)
	if startErr != nil {
		return startErr
	}

	go m.monitor(spec.started(m.now()), current, reporter)
	return nil
}

func (m *executionManager) recover(ctx context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	spec, err := prepareExecution(request, reporter, m.now(), true)
	if err != nil {
		return err
	}

	current, owner, err := m.claim(request.ExecutionID, spec.fingerprint)
	if err != nil {
		return err
	}
	if !owner {
		return waitUntilReady(ctx, current)
	}

	started, recoverErr := m.backend.Recover(ctx, spec.request, spec.fingerprint)
	m.finishStart(request.ExecutionID, current, started, recoverErr)
	if recoverErr != nil {
		return recoverErr
	}

	go m.monitor(spec.started(m.now()), current, reporter)
	return nil
}

func (m *executionManager) claim(executionID, fingerprint string) (*execution, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, false, ErrClosed
	}
	if current := m.entries[executionID]; current != nil {
		if current.fingerprint != fingerprint {
			return nil, false, fmt.Errorf("%w: %q", ErrExecutionConflict, executionID)
		}
		return current, false, nil
	}
	current := &execution{fingerprint: fingerprint, ready: make(chan struct{}), done: make(chan struct{})}
	m.entries[executionID] = current
	return current, true, nil
}

func (m *executionManager) finishStart(executionID string, current *execution, started process, startErr error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	current.process = started
	current.startErr = startErr
	close(current.ready)
	if startErr != nil {
		delete(m.entries, executionID)
	}
}

func waitUntilReady(ctx context.Context, current *execution) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-current.ready:
		return current.startErr
	}
}

func (m *executionManager) stop(ctx context.Context, executionID string) error {
	if strings.TrimSpace(executionID) == "" {
		return fmt.Errorf("%w: execution ID is required", ErrInvalidRequest)
	}

	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return ErrClosed
		}
		current := m.entries[executionID]
		if current == nil {
			m.mu.Unlock()
			return m.backend.Stop(ctx, executionID)
		}
		if !channelClosed(current.ready) {
			ready := current.ready
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ready:
				continue
			}
		}
		if channelClosed(current.done) {
			m.mu.Unlock()
			return nil
		}
		if current.finishing {
			done := current.done
			m.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-done:
				return nil
			}
		}
		current.stopping = true
		current.stopFailure = nil
		started := current.process
		m.mu.Unlock()

		if err := started.Kill(ctx); err != nil {
			m.mu.Lock()
			if !channelClosed(current.done) {
				current.stopping = false
			}
			m.mu.Unlock()
			return err
		}
		select {
		case <-ctx.Done():
			m.mu.Lock()
			if !channelClosed(current.done) {
				current.stopping = false
				current.stopFailure = ctx.Err()
			}
			m.mu.Unlock()
			return ctx.Err()
		case <-current.done:
			return current.doneErr
		}
	}
}

func (m *executionManager) forget(ctx context.Context, executionID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if current := m.entries[executionID]; current != nil {
		if !channelClosed(current.done) {
			return fmt.Errorf("runtime cleanup still active")
		}
		if current.doneErr != nil {
			return current.doneErr
		}
		delete(m.entries, executionID)
	}
	return nil
}

func (m *executionManager) close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.mu.Unlock()
	return m.backend.Close()
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}
