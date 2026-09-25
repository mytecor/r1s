package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

const (
	runInspectInterval = 5 * time.Second
	runInspectWait     = 5 * time.Second
)

// workloadExitError lets main preserve a workload's process status without
// presenting it as an r1s diagnostic. Status zero is represented by nil.
type workloadExitError struct{ status int }

func (e *workloadExitError) Error() string {
	return fmt.Sprintf("workload exited with status %d", e.status)
}
func (e *workloadExitError) ExitStatus() int { return e.status }

func (a *application) runExecution(arguments []string, stderr io.Writer) error {
	flags := newFlagSet("r1s run <cluster> [options] '<ExecutionRequest JSON>'", stderr)
	offerWait := flags.Duration("offer-wait", defaultOfferWait, "time to discover allocators and collect offers")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("run: exactly one workload JSON argument is required")
	}
	if *offerWait <= 0 {
		return errors.New("run: --offer-wait must be positive")
	}
	request, err := decodeRequestJSON(flags.Arg(0))
	if err != nil {
		return err
	}
	_, executionID, allocator, err := a.runRequest(
		a.ctx,
		request.GetWorkload(),
		request.GetPolicy(),
		request.GetResourceClass(),
		*offerWait,
		nil,
		defaultLeaseDuration,
		request.GetConstraints(),
	)
	if err != nil {
		return err
	}
	created, ok := a.client.RequestForExecution(executionID)
	if !ok {
		return fmt.Errorf("run: execution %s has no in-memory request", executionID)
	}
	fmt.Fprintf(a.stdout, "run=%s attempt=%d execution=%s allocator=%x status=assignment-sent\n", created.GetRunId(), created.GetAttempt(), executionID, allocator)

	activeID, state, err := a.holdRun(a.ctx, executionID, *offerWait)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			a.bestEffortRunCancel(activeID)
			if cause := context.Cause(a.ctx); cause != nil {
				return cause
			}
		}
		return err
	}
	snapshot, _ := a.client.Execution(activeID)
	printState(a.stdout, snapshot)
	return workloadStatus(state)
}

// holdRun owns one logical run in memory. Missing responses are retried and
// never imply loss; only authenticated NOT_FOUND/EXPIRED feedback or the
// allocator's lease-expiry terminal state advances to a fresh attempt.
func (a *application) holdRun(ctx context.Context, executionID string, offerWait time.Duration) (string, *r1sv1.ExecutionState, error) {
	activeID := executionID
	ticker := time.NewTicker(runInspectInterval)
	defer ticker.Stop()

	for {
		if snapshot, ok := a.client.Execution(activeID); ok && snapshot.State != nil && protocol.Terminal(snapshot.State.GetPhase()) && !a.client.LeaseIntentLost(activeID) {
			return activeID, snapshot.State, nil
		}
		if a.client.LeaseIntentLost(activeID) {
			previous := activeID
			replacement, err := a.reRequestLostLeaseWithWait(ctx, activeID, offerWait)
			if err != nil {
				return activeID, nil, err
			}
			activeID = replacement
			request, _ := a.client.RequestForExecution(activeID)
			fmt.Fprintf(a.stdout, "execution=%s status=lease-lost\n", previous)
			fmt.Fprintf(a.stdout, "run=%s attempt=%d execution=%s status=rerequested\n", request.GetRunId(), request.GetAttempt(), activeID)
			continue
		}
		if a.client.LeaseDue(activeID) {
			previous := activeID
			newID, _, rerequested, err := a.renewOrReRequestWithOfferWait(ctx, activeID, 0, defaultRenewWait, offerWait)
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(a.stdout, "execution=%s lease-renewal-error=%v\n", activeID, err)
			} else if err == nil && rerequested {
				activeID = newID
				request, _ := a.client.RequestForExecution(activeID)
				fmt.Fprintf(a.stdout, "execution=%s status=lease-lost\n", previous)
				fmt.Fprintf(a.stdout, "run=%s attempt=%d execution=%s status=rerequested\n", request.GetRunId(), request.GetAttempt(), activeID)
			}
		}

		select {
		case <-ctx.Done():
			return activeID, nil, ctx.Err()
		case <-ticker.C:
		}
		// Inspection is an explicit authenticated recovery read after a quiet
		// period or reconnect. A timeout is inconclusive and does not reschedule.
		if _, err := a.inspectState(ctx, activeID, runInspectWait); err != nil && errors.Is(err, context.Canceled) {
			return activeID, nil, err
		}
	}
}

func (a *application) bestEffortRunCancel(executionID string) {
	if executionID == "" {
		return
	}
	destination, envelope, err := a.client.Cancel(executionID, "run process interrupted")
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = a.sendWithContext(ctx, destination, envelope)
}

func workloadStatus(state *r1sv1.ExecutionState) error {
	if state == nil {
		return errors.New("run: allocator returned no terminal state")
	}
	if state.ExitCode != nil {
		code := state.GetExitCode()
		if code == 0 {
			return nil
		}
		if code > 0 && code <= 255 {
			return &workloadExitError{status: int(code)}
		}
	}
	if state.GetPhase() == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED {
		return nil
	}
	return &workloadExitError{status: 1}
}
