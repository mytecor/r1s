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
	detach := flags.Bool("d", false, "detach: hand the run to a background process and return")
	detachLong := flags.Bool("detach", false, "detach: hand the run to a background process and return")
	logFile := flags.String("log-file", "", "detached output file (defaults to the run state directory)")
	var publishes portListValue
	flags.Var(&publishes, "p", "publish host:container to the run for its lifetime (repeatable)")
	flags.Var(&publishes, "publish", "publish host:container to the run for its lifetime (repeatable)")
	// Internal child flags are set only when the detached parent re-executes
	// this binary as the lease-holding child. They are invisible to users.
	childFlag := flags.Bool("r1s-child", false, "internal: run as the detached lease-holding child")
	handshakeFD := flags.Int("r1s-fd", detachHandshakeFD, "internal: handshake pipe file descriptor")
	childLogFile := flags.String("r1s-log-file", "", "internal: resolved detached output file")
	childPublish := flags.String("r1s-publish", "", "internal: comma-separated host:container publish list")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("run: exactly one workload JSON argument is required")
	}
	if *offerWait <= 0 {
		return errors.New("run: --offer-wait must be positive")
	}
	if *detach && *childFlag {
		return errors.New("run: -d and --r1s-child are mutually exclusive")
	}
	detached := *detach || *detachLong
	if *childFlag {
		// The child is the detached lease-holder. It must not also detach.
		childPublishes, err := parsePublishFlag(*childPublish)
		if err != nil {
			return err
		}
		return a.runDetachedChild(flags.Arg(0), *offerWait, *handshakeFD, *childLogFile, childPublishes)
	}
	if detached {
		// The parent never owns the run: it spawns a child and waits for the
		// ownership handshake so it reports a live run only after the child has
		// safely taken over (and the state paths exist with owner-only perms).
		return a.launchDetached(a.clusterSelector, flags.Arg(0), *offerWait, *logFile, publishes.mappings, stderr)
	}
	return a.runForeground(flags.Arg(0), *offerWait, publishes.mappings, stderr)
}

// runRequestJSON decodes the single workload JSON argument, performs the
// initial request/assignment, and returns the created request, the assigned
// execution ID, and the chosen allocator identity.
func (a *application) runRequestJSON(workloadJSON string, offerWait time.Duration) (*r1sv1.ExecutionRequest, string, []byte, error) {
	request, err := decodeRequestJSON(workloadJSON)
	if err != nil {
		return nil, "", nil, err
	}
	_, executionID, allocator, err := a.runRequest(
		a.ctx,
		request.GetWorkload(),
		request.GetPolicy(),
		request.GetResourceClass(),
		offerWait,
		nil,
		defaultLeaseDuration,
		request.GetConstraints(),
	)
	if err != nil {
		return nil, "", nil, err
	}
	created, ok := a.client.RequestForExecution(executionID)
	if !ok {
		return nil, "", nil, fmt.Errorf("run: execution %s has no in-memory request", executionID)
	}
	return created, executionID, allocator, nil
}

// foreground run: assign, announce to the terminal, then hold the run while a
// concurrent tail copies allocator stdout/stderr to the matching terminal
// streams and (when --publish is given) the publisher relays published ports to
// the active execution attempt.
func (a *application) runForeground(workloadJSON string, offerWait time.Duration, publishes []portMapping, stderr io.Writer) error {
	created, executionID, allocator, err := a.runRequestJSON(workloadJSON, offerWait)
	if err != nil {
		return err
	}
	// Run-control lines go to stderr so foreground stdout carries only the
	// workload's stdout; workload stderr still maps to the terminal's stderr.
	fmt.Fprintf(stderr, "run=%s attempt=%d execution=%s allocator=%x status=assignment-sent\n", created.GetRunId(), created.GetAttempt(), executionID, allocator)

	var publisher *runPublisher
	if len(publishes) > 0 {
		if err := a.ensureRunTunnelEdge(); err != nil {
			return err
		}
		publisher, err = newRunPublisher(a.ctx, a, publishes)
		if err != nil {
			return err
		}
		publisher.SetActive(executionID)
		publisher.start()
		defer publisher.close()
		for _, m := range publishes {
			fmt.Fprintf(stderr, "publish: 127.0.0.1:%d -> container port %d (execution %s)\n", m.host, m.container, executionID)
		}
	}

	tail := newForegroundTail(a, a.stdout, stderr)
	tail.SetActive(executionID, created.GetRunId(), created.GetAttempt())
	stop := make(chan struct{})
	go tail.run(a.ctx, stop)

	activeID, state, err := a.holdRun(a.ctx, executionID, offerWait, func(id string, request *r1sv1.ExecutionRequest) {
		tail.SetActive(id, request.GetRunId(), request.GetAttempt())
		if publisher != nil {
			publisher.SetActive(id)
		}
	}, stderr)
	close(stop)
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
	printState(stderr, snapshot)
	return workloadStatus(state)
}

// holdRun owns one logical run in memory. Missing responses are retried and
// never imply loss; only authenticated NOT_FOUND/EXPIRED feedback or the
// allocator's lease-expiry terminal state advances to a fresh attempt. When onActive
// is non-nil it is called with each newly active execution so a concurrent log
// tail can track rescheduling; statusLines writes residency status.
func (a *application) holdRun(ctx context.Context, executionID string, offerWait time.Duration, onActive func(id string, request *r1sv1.ExecutionRequest), statusLines io.Writer) (string, *r1sv1.ExecutionState, error) {
	if statusLines == nil {
		statusLines = a.stdout
	}
	activeID := executionID
	if onActive != nil {
		if request, ok := a.client.RequestForExecution(activeID); ok {
			onActive(activeID, request)
		}
	}
	ticker := time.NewTicker(runInspectInterval)
	defer ticker.Stop()

	replace := func(previous, replacement string) {
		request, _ := a.client.RequestForExecution(replacement)
		fmt.Fprintf(statusLines, "execution=%s status=lease-lost\n", previous)
		fmt.Fprintf(statusLines, "run=%s attempt=%d execution=%s status=rerequested\n", request.GetRunId(), request.GetAttempt(), replacement)
		if onActive != nil {
			onActive(replacement, request)
		}
	}

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
			replace(previous, activeID)
			continue
		}
		if a.client.LeaseDue(activeID) {
			previous := activeID
			newID, _, rerequested, err := a.renewOrReRequestWithOfferWait(ctx, activeID, 0, defaultRenewWait, offerWait)
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintf(statusLines, "execution=%s lease-renewal-error=%v\n", activeID, err)
			} else if err == nil && rerequested {
				activeID = newID
				replace(previous, activeID)
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
