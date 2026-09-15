// Package containerd implements the runtime boundary with the containerd Go client.
package containerd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mytecor/r1s/internal/logstore"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

const (
	// DefaultAddress is the standard containerd Unix socket.
	DefaultAddress = "/run/containerd/containerd.sock"
	// DefaultNamespace isolates r1s metadata from other containerd consumers.
	DefaultNamespace = "r1s"

	defaultCleanupTimeout = 30 * time.Second
)

var (
	ErrInvalidConfig     = errors.New("invalid containerd runtime configuration")
	ErrInvalidRequest    = errors.New("invalid containerd start request")
	ErrExecutionConflict = r1sruntime.ErrExecutionConflict
	ErrExecutionMissing  = r1sruntime.ErrExecutionMissing
	ErrDeadlineExceeded  = errors.New("execution policy deadline exceeded")
	ErrClosed            = errors.New("containerd runtime is closed")
)

// Config selects the containerd daemon and isolated metadata namespace.
type Config struct {
	Logs           *logstore.Store
	LogBinary      string
	Address        string
	Namespace      string
	Snapshotter    string
	CleanupTimeout time.Duration
	Now            func() time.Time
}

type exitResult struct {
	code uint32
	err  error
}

type process interface {
	Wait() <-chan exitResult
	Kill(context.Context) error
	Cleanup(context.Context) error
}

type backend interface {
	Start(context.Context, r1sruntime.StartRequest, string) (process, error)
	Recover(context.Context, r1sruntime.StartRequest, string) (process, error)
	Stop(context.Context, string) error
	Close() error
}

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

// Adapter starts and stops containerd tasks without tying their lifetime to
// the caller's context or to an RNS connection.
type Adapter struct {
	mu sync.Mutex

	backend        backend
	cleanupTimeout time.Duration
	now            func() time.Time
	closed         bool
	executions     map[string]*execution
}

// New connects to containerd and verifies the configured namespace can be used.
func New(ctx context.Context, config Config) (*Adapter, error) {
	config = withDefaults(config)
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	implementation, err := newClientBackend(ctx, config)
	if err != nil {
		return nil, err
	}
	return newWithBackend(config, implementation), nil
}

func newWithBackend(config Config, implementation backend) *Adapter {
	config = withDefaults(config)
	return &Adapter{
		backend:        implementation,
		cleanupTimeout: config.CleanupTimeout,
		now:            config.Now,
		executions:     make(map[string]*execution),
	}
}

// Start creates and starts one task. Repeating the same execution ID and
// specification returns success without creating another container or task.
func (a *Adapter) Start(ctx context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	spec, err := prepareExecution(request, reporter, a.now(), false)
	if err != nil {
		return err
	}

	for {
		a.mu.Lock()
		if a.closed {
			a.mu.Unlock()
			return ErrClosed
		}
		if current, ok := a.executions[request.ExecutionID]; ok {
			if current.fingerprint != spec.fingerprint {
				a.mu.Unlock()
				return fmt.Errorf("%w: %q", ErrExecutionConflict, request.ExecutionID)
			}
			ready := current.ready
			a.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ready:
				return current.startErr
			}
		}

		current := &execution{
			fingerprint: spec.fingerprint,
			ready:       make(chan struct{}),
			done:        make(chan struct{}),
		}
		a.executions[request.ExecutionID] = current
		a.mu.Unlock()

		startContext, cancel := spec.startContext(ctx)
		started, startErr := a.backend.Start(startContext, spec.request, spec.fingerprint)
		cancel()

		a.mu.Lock()
		current.process = started
		current.startErr = startErr
		close(current.ready)
		if startErr != nil {
			delete(a.executions, request.ExecutionID)
			a.mu.Unlock()
			return startErr
		}
		a.mu.Unlock()

		go a.monitor(spec.started(a.now()), current, reporter)
		return nil
	}
}

// Recover reattaches to a matching task without creating or starting one.
// Missing and partially-created runtime objects are reported to the allocator
// so it can resolve its durable state without duplicating work.
func (a *Adapter) Recover(ctx context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	spec, err := prepareExecution(request, reporter, a.now(), true)
	if err != nil {
		return err
	}

	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return ErrClosed
	}
	if current, exists := a.executions[request.ExecutionID]; exists {
		if current.fingerprint != spec.fingerprint {
			a.mu.Unlock()
			return fmt.Errorf("%w: %q", ErrExecutionConflict, request.ExecutionID)
		}
		ready := current.ready
		a.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ready:
			return current.startErr
		}
	}
	current := &execution{fingerprint: spec.fingerprint, ready: make(chan struct{}), done: make(chan struct{})}
	a.executions[request.ExecutionID] = current
	a.mu.Unlock()

	started, recoverErr := a.backend.Recover(ctx, spec.request, spec.fingerprint)
	a.mu.Lock()
	current.process = started
	current.startErr = recoverErr
	close(current.ready)
	if recoverErr != nil {
		delete(a.executions, request.ExecutionID)
		a.mu.Unlock()
		return recoverErr
	}
	a.mu.Unlock()

	go a.monitor(spec.started(a.now()), current, reporter)
	return nil
}

// Stop terminates and cleans an execution. Missing and previously stopped
// executions are successful no-ops, so retries are safe.
func (a *Adapter) Stop(ctx context.Context, executionID string) error {
	if strings.TrimSpace(executionID) == "" {
		return fmt.Errorf("%w: execution ID is required", ErrInvalidRequest)
	}

	for {
		a.mu.Lock()
		if a.closed {
			a.mu.Unlock()
			return ErrClosed
		}
		current, ok := a.executions[executionID]
		if !ok {
			a.mu.Unlock()
			return a.backend.Stop(ctx, executionID)
		}
		ready := current.ready
		if !channelClosed(ready) {
			a.mu.Unlock()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ready:
				continue
			}
		}
		if channelClosed(current.done) {
			a.mu.Unlock()
			return nil
		}
		if current.finishing {
			done := current.done
			a.mu.Unlock()
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
		a.mu.Unlock()

		if err := started.Kill(ctx); err != nil {
			a.mu.Lock()
			if !channelClosed(current.done) {
				current.stopping = false
			}
			a.mu.Unlock()
			return err
		}
		select {
		case <-ctx.Done():
			a.mu.Lock()
			if !channelClosed(current.done) {
				current.stopping = false
				current.stopFailure = ctx.Err()
			}
			a.mu.Unlock()
			return ctx.Err()
		case <-current.done:
			return current.doneErr
		}
	}
}

// Close releases the containerd client connection. It deliberately does not
// stop tasks: execution lifetime is independent of the r1sd process lifetime.
func (a *Adapter) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	a.mu.Unlock()
	return a.backend.Close()
}

func (a *Adapter) Forget(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if current := a.executions[id]; current != nil {
		if !channelClosed(current.done) {
			return fmt.Errorf("runtime cleanup still active")
		}
		if current.doneErr != nil {
			return current.doneErr
		}
		delete(a.executions, id)
	}
	return nil
}

func (a *Adapter) monitor(spec executionSpec, current *execution, reporter r1sruntime.Reporter) {
	var timer <-chan time.Time
	var deadlineTimer *time.Timer
	if deadline, ok := spec.deadline(); ok {
		deadlineTimer = time.NewTimer(deadline.Sub(a.now()))
		timer = deadlineTimer.C
		defer deadlineTimer.Stop()
	}

	result := exitResult{}
	deadlineExceeded := false
	select {
	case result = <-current.process.Wait():
	case <-timer:
		deadlineExceeded = true
		a.mu.Lock()
		current.stopping = true
		a.mu.Unlock()
		killContext, cancel := context.WithTimeout(context.Background(), a.cleanupTimeout)
		killErr := current.process.Kill(killContext)
		cancel()
		if killErr != nil {
			result.err = errors.Join(ErrDeadlineExceeded, killErr)
		} else {
			result = <-current.process.Wait()
		}
	}

	var completionErr error
	detail := ""
	if deadlineExceeded {
		completionErr = errors.Join(ErrDeadlineExceeded, result.err)
		detail = ErrDeadlineExceeded.Error()
	} else {
		completionErr = result.err
		if result.err == nil {
			detail = fmt.Sprintf("container exited with code %d", result.code)
		}
	}
	exitCode := int32(result.code)
	completion := r1sruntime.Completion{
		ExecutionID: spec.request.ExecutionID,
		Err:         completionErr,
		Detail:      detail,
		ExitCode:    &exitCode,
	}

	a.mu.Lock()
	stopping := current.stopping && !deadlineExceeded
	stopFailure := current.stopFailure
	current.finishing = true
	a.mu.Unlock()
	completion.Err = errors.Join(completion.Err, stopFailure)
	var reportErr error
	if !stopping {
		reportErr = reporter(completion)
	}
	var cleanupErr error
	if stopping || reportErr == nil {
		cleanupContext, cancel := context.WithTimeout(context.Background(), a.cleanupTimeout)
		cleanupErr = current.process.Cleanup(cleanupContext)
		cancel()
	}
	a.mu.Lock()
	current.doneErr = errors.Join(cleanupErr, reportErr)
	close(current.done)
	a.mu.Unlock()
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}
