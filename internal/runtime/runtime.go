// Package runtime defines the OCI-runtime boundary used by the allocator core.
package runtime

import (
	"context"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// StartRequest is an immutable execution specification passed to a runtime.
type StartRequest struct {
	ExecutionID string
	Owner       []byte
	Workload    *r1sv1.Workload
	Policy      *r1sv1.ExecutionPolicy
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
