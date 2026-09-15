package containerd

import (
	"context"
	"errors"
	"fmt"
	"time"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// monitor owns process completion policy: deadline enforcement, durable result
// reporting, and cleanup ordering. Registry coordination stays in the manager.
func (m *executionManager) monitor(spec executionSpec, current *execution, reporter r1sruntime.Reporter) {
	var timer <-chan time.Time
	var deadlineTimer *time.Timer
	if deadline, ok := spec.deadline(); ok {
		deadlineTimer = time.NewTimer(deadline.Sub(m.now()))
		timer = deadlineTimer.C
		defer deadlineTimer.Stop()
	}

	result := exitResult{}
	deadlineExceeded := false
	select {
	case result = <-current.process.Wait():
	case <-timer:
		deadlineExceeded = true
		m.mu.Lock()
		current.stopping = true
		m.mu.Unlock()
		killContext, cancel := context.WithTimeout(context.Background(), m.cleanupTimeout)
		killErr := current.process.Kill(killContext)
		cancel()
		if killErr != nil {
			result.err = errors.Join(ErrDeadlineExceeded, killErr)
		} else {
			result = <-current.process.Wait()
		}
	}

	completion := processCompletion(spec, result, deadlineExceeded)
	m.mu.Lock()
	stopping := current.stopping && !deadlineExceeded
	stopFailure := current.stopFailure
	current.finishing = true
	m.mu.Unlock()

	completion.Err = errors.Join(completion.Err, stopFailure)
	var reportErr error
	if !stopping {
		reportErr = reporter(completion)
	}
	var cleanupErr error
	if stopping || reportErr == nil {
		cleanupContext, cancel := context.WithTimeout(context.Background(), m.cleanupTimeout)
		cleanupErr = current.process.Cleanup(cleanupContext)
		cancel()
	}
	m.mu.Lock()
	current.doneErr = errors.Join(cleanupErr, reportErr)
	close(current.done)
	m.mu.Unlock()
}

func processCompletion(spec executionSpec, result exitResult, deadlineExceeded bool) r1sruntime.Completion {
	completionErr := result.err
	detail := ""
	if deadlineExceeded {
		completionErr = errors.Join(ErrDeadlineExceeded, result.err)
		detail = ErrDeadlineExceeded.Error()
	} else if result.err == nil {
		detail = fmt.Sprintf("container exited with code %d", result.code)
	}
	exitCode := int32(result.code)
	return r1sruntime.Completion{
		ExecutionID: spec.request.ExecutionID,
		Err:         completionErr,
		Detail:      detail,
		ExitCode:    &exitCode,
	}
}
