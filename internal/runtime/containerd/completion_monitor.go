package containerd

import (
	"context"
	"errors"
	"fmt"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

// monitor owns process completion reporting and cleanup ordering. Execution
// lifetime is bounded by the allocator's lease sweep, not by a request-time
// deadline, so the monitor only waits for the process to exit. Registry
// coordination stays in the manager.
func (m *executionManager) monitor(spec executionSpec, current *execution, reporter r1sruntime.Reporter) {
	result := <-current.process.Wait()

	completion := processCompletion(spec, result)
	m.mu.Lock()
	stopping := current.stopping
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

func processCompletion(spec executionSpec, result exitResult) r1sruntime.Completion {
	completionErr := result.err
	detail := ""
	if result.err == nil {
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
