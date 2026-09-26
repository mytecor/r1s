package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	r1sclient "github.com/mytecor/r1s/client"
)

const (
	// defaultOfferWait paces re-request offer collection and is the default
	// --offer-wait for `r1s run`.
	defaultOfferWait = 10 * time.Second
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

// foreground run: assign, announce to the terminal, then hold the run while a
// concurrent tail copies allocator stdout/stderr to the matching terminal
// streams and (when --publish is given) the publisher relays published ports to
// the active execution attempt.
func (a *application) runForeground(workloadJSON string, offerWait time.Duration, publishes []portMapping, stderr io.Writer) error {
	request, err := decodeRequestJSON(workloadJSON)
	if err != nil {
		return err
	}

	var publisher *runPublisher
	if len(publishes) > 0 {
		if err := a.ensureRunTunnelEdge(); err != nil {
			return err
		}
		publisher, err = newRunPublisher(a.ctx, a, publishes)
		if err != nil {
			return err
		}
		publisher.start()
		defer publisher.close()
	}

	tail := newForegroundTail(a, a.stdout, stderr)
	stop := make(chan struct{})
	go tail.run(a.ctx, stop)
	result, err := a.controller.Run(a.ctx, request, r1sclient.RunOptions{
		OfferWait: offerWait,
		OnEvent: func(event r1sclient.Event) {
			switch event.Kind {
			case r1sclient.EventLeaseLost:
				fmt.Fprintf(stderr, "execution=%s status=lease-lost\n", event.Previous)
			case r1sclient.EventRenewalError:
				fmt.Fprintf(stderr, "execution=%s lease-renewal-error=%v\n", event.Attempt.ExecutionID, event.Err)
			case r1sclient.EventAttemptAssigned:
				status := "assignment-sent"
				if event.Previous != "" {
					status = "rerequested"
				}
				fmt.Fprintf(stderr, "run=%s attempt=%d execution=%s allocator=%x status=%s\n", event.Attempt.RunID, event.Attempt.Number, event.Attempt.ExecutionID, event.Attempt.Allocator, status)
				tail.SetActive(event.Attempt.ExecutionID, event.Attempt.RunID, event.Attempt.Number)
				if publisher != nil {
					publisher.SetActive(event.Attempt.ExecutionID)
					if event.Previous == "" {
						for _, mapping := range publishes {
							fmt.Fprintf(stderr, "publish: 127.0.0.1:%d -> container port %d (execution %s)\n", mapping.host, mapping.container, event.Attempt.ExecutionID)
						}
					}
				}
			}
		},
	})
	close(stop)
	if err != nil {
		if errors.Is(err, context.Canceled) && context.Cause(a.ctx) != nil {
			return context.Cause(a.ctx)
		}
		return err
	}
	printState(stderr, result.Attempt.ExecutionID, result.State)
	return workloadStatus(result.State)
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
