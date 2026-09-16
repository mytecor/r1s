// Package containerd implements the runtime boundary with the containerd Go client.
package containerd

import (
	"context"
	"errors"
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

// Adapter exposes the runtime contract while executionManager owns lifecycle
// coordination independently of transport connections and caller contexts.
type Adapter struct {
	executions *executionManager
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
	return &Adapter{executions: newExecutionManager(implementation, config.CleanupTimeout, config.Now)}
}

// Start creates and starts one task. Repeating the same execution ID and
// specification returns success without creating another container or task.
func (a *Adapter) Start(ctx context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	return a.executions.start(ctx, request, reporter)
}

// Recover reattaches to a matching task without creating or starting one.
// Missing runtime objects are reported so durable state can be reconciled.
func (a *Adapter) Recover(ctx context.Context, request r1sruntime.StartRequest, reporter r1sruntime.Reporter) error {
	return a.executions.recover(ctx, request, reporter)
}

// Stop terminates and cleans an execution idempotently.
func (a *Adapter) Stop(ctx context.Context, executionID string) error {
	return a.executions.stop(ctx, executionID)
}

// Forget drops terminal in-memory bookkeeping after durable result expiry.
func (a *Adapter) Forget(ctx context.Context, executionID string) error {
	return a.executions.forget(ctx, executionID)
}

// Close releases the containerd client connection. It deliberately does not
// stop tasks: execution lifetime is independent of the r1sd process lifetime.
func (a *Adapter) Close() error {
	return a.executions.close()
}
