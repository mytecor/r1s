// Package containerd implements the runtime boundary with the containerd Go client.
package containerd

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sruntime "github.com/mytecor/r1s/internal/runtime"
	"google.golang.org/protobuf/proto"
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
	ErrExecutionConflict = errors.New("execution ID conflicts with an existing containerd workload")
	ErrDeadlineExceeded  = errors.New("execution policy deadline exceeded")
	ErrClosed            = errors.New("containerd runtime is closed")
)

// Config selects the containerd daemon and isolated metadata namespace.
type Config struct {
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
	fingerprint, err := validateAndFingerprint(request, reporter, a.now())
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
			if current.fingerprint != fingerprint {
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
			fingerprint: fingerprint,
			ready:       make(chan struct{}),
			done:        make(chan struct{}),
		}
		a.executions[request.ExecutionID] = current
		a.mu.Unlock()

		startContext := ctx
		cancel := func() {}
		if deadline := request.Policy.GetDeadline(); deadline != nil {
			startContext, cancel = context.WithDeadline(ctx, deadline.AsTime())
		}
		started, startErr := a.backend.Start(startContext, request, fingerprint)
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

		go a.monitor(request, current, reporter, a.now())
		return nil
	}
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

func (a *Adapter) monitor(request r1sruntime.StartRequest, current *execution, reporter r1sruntime.Reporter, startedAt time.Time) {
	var timer <-chan time.Time
	var deadlineTimer *time.Timer
	if deadline, ok := executionDeadline(request.Policy, startedAt); ok {
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

	cleanupContext, cancel := context.WithTimeout(context.Background(), a.cleanupTimeout)
	cleanupErr := current.process.Cleanup(cleanupContext)
	cancel()

	var completionErr error
	detail := ""
	if deadlineExceeded {
		completionErr = errors.Join(ErrDeadlineExceeded, result.err, cleanupErr)
		detail = ErrDeadlineExceeded.Error()
	} else {
		completionErr = errors.Join(result.err, cleanupErr)
		if result.err == nil {
			detail = fmt.Sprintf("container exited with code %d", result.code)
		}
	}
	exitCode := int32(result.code)
	completion := r1sruntime.Completion{
		ExecutionID: request.ExecutionID,
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
	a.mu.Lock()
	current.doneErr = errors.Join(cleanupErr, reportErr)
	close(current.done)
	a.mu.Unlock()
}

func validateAndFingerprint(request r1sruntime.StartRequest, reporter r1sruntime.Reporter, now time.Time) (string, error) {
	if strings.TrimSpace(request.ExecutionID) == "" {
		return "", fmt.Errorf("%w: execution ID is required", ErrInvalidRequest)
	}
	if reporter == nil {
		return "", fmt.Errorf("%w: completion reporter is required", ErrInvalidRequest)
	}
	if request.Workload == nil || request.Policy == nil {
		return "", fmt.Errorf("%w: workload and policy are required", ErrInvalidRequest)
	}
	if _, err := pinnedDigest(request.Workload.GetImage()); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidRequest, err)
	}
	if deadline := request.Policy.GetDeadline(); deadline != nil {
		if err := deadline.CheckValid(); err != nil {
			return "", fmt.Errorf("%w: invalid deadline: %v", ErrInvalidRequest, err)
		}
		if !deadline.AsTime().After(now) {
			return "", ErrDeadlineExceeded
		}
	}
	if maximum := request.Policy.GetMaxRuntime(); maximum != nil {
		if err := maximum.CheckValid(); err != nil || maximum.AsDuration() <= 0 {
			return "", fmt.Errorf("%w: max runtime must be a positive duration", ErrInvalidRequest)
		}
	}
	if request.Policy.GetDeadline() == nil && request.Policy.GetMaxRuntime() == nil {
		return "", fmt.Errorf("%w: deadline or max runtime is required", ErrInvalidRequest)
	}

	marshal := proto.MarshalOptions{Deterministic: true}
	workload, err := marshal.Marshal(request.Workload)
	if err != nil {
		return "", fmt.Errorf("%w: marshal workload: %v", ErrInvalidRequest, err)
	}
	policy, err := marshal.Marshal(request.Policy)
	if err != nil {
		return "", fmt.Errorf("%w: marshal policy: %v", ErrInvalidRequest, err)
	}
	hash := sha256.New()
	writeHashPart(hash, request.Owner)
	writeHashPart(hash, workload)
	writeHashPart(hash, policy)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

type hashWriter interface {
	Write([]byte) (int, error)
}

func writeHashPart(writer hashWriter, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func executionDeadline(policy *r1sv1.ExecutionPolicy, startedAt time.Time) (time.Time, bool) {
	var deadline time.Time
	if policy.GetDeadline() != nil {
		deadline = policy.GetDeadline().AsTime()
	}
	if policy.GetMaxRuntime() != nil {
		maximum := startedAt.Add(policy.GetMaxRuntime().AsDuration())
		if deadline.IsZero() || maximum.Before(deadline) {
			deadline = maximum
		}
	}
	return deadline, !deadline.IsZero()
}

func environment(workload *r1sv1.Workload) []string {
	keys := make([]string, 0, len(workload.GetEnvironment()))
	for key := range workload.GetEnvironment() {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		values = append(values, key+"="+workload.GetEnvironment()[key])
	}
	return values
}

func withDefaults(config Config) Config {
	if config.Address == "" {
		config.Address = DefaultAddress
	}
	if config.Namespace == "" {
		config.Namespace = DefaultNamespace
	}
	if config.CleanupTimeout == 0 {
		config.CleanupTimeout = defaultCleanupTimeout
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return config
}

func validateConfig(config Config) error {
	if strings.TrimSpace(config.Address) == "" || strings.TrimSpace(config.Namespace) == "" {
		return fmt.Errorf("%w: address and namespace are required", ErrInvalidConfig)
	}
	if config.Namespace == "version" {
		return fmt.Errorf("%w: namespace %q is reserved", ErrInvalidConfig, config.Namespace)
	}
	if config.CleanupTimeout <= 0 {
		return fmt.Errorf("%w: cleanup timeout must be positive", ErrInvalidConfig)
	}
	return nil
}

func channelClosed(channel <-chan struct{}) bool {
	select {
	case <-channel:
		return true
	default:
		return false
	}
}
