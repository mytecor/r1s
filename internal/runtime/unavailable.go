package runtime

import (
	"context"
	"errors"
)

// ErrUnavailable reports that an OCI adapter has not been configured yet.
var ErrUnavailable = errors.New("OCI runtime is unavailable")

// Unavailable is the F2 bridge runtime used by r1sd until F3 wires containerd.
// It never claims to have started or stopped an execution.
type Unavailable struct{}

func (Unavailable) Start(context.Context, StartRequest, Reporter) error { return ErrUnavailable }
func (Unavailable) Stop(context.Context, string) error                  { return ErrUnavailable }
