// Package runtime defines the OCI-runtime boundary used by the allocator core.
package runtime

import (
	"context"
	"errors"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

var (
	// ErrExecutionMissing means durable allocator state has no matching runtime object.
	ErrExecutionMissing = errors.New("runtime execution is missing")
	// ErrExecutionConflict means a runtime object exists but its authority or specification differs.
	ErrExecutionConflict = errors.New("runtime execution conflicts with durable state")
)

// StartRequest is an immutable execution specification passed to a runtime.
type StartRequest struct {
	ExecutionID string
	Client      []byte
	Workload    *r1sv1.Workload
	Policy      *r1sv1.ExecutionPolicy
	// StartedAt is the durable beginning of local policy timing. Runtimes use
	// the current time when it is zero for compatibility with direct callers.
	StartedAt time.Time
}

// Completion reports the terminal runtime outcome for an execution.
type Completion struct {
	ExecutionID string
	Err         error
	Detail      string
	ExitCode    *int32
}

// Reporter applies a terminal runtime outcome to the allocator.
type Reporter func(Completion) error

// Runtime starts and stops executions idempotently by execution ID.
//
// The context bounds the Start or Stop operation only. A successful Start must
// not tie the execution lifetime to context cancellation or a network link.
type Runtime interface {
	Start(context.Context, StartRequest, Reporter) error
	Stop(context.Context, string) error
}

// Recoverer reattaches monitoring to an existing execution without creating
// or restarting a missing workload.
type Recoverer interface {
	Recover(context.Context, StartRequest, Reporter) error
}
