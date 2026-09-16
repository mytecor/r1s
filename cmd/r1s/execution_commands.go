package main

import (
	"errors"
	"fmt"
	"io"
	"time"
)

func (a *application) list(arguments []string, stderr io.Writer) error {
	flags := newFlagSet("r1s list", stderr)
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("list: unexpected arguments")
	}
	for _, request := range a.client.Requests() {
		fmt.Fprintf(a.stdout, "request=%s class=%s offers=%d execution=%s\n", request.Request.GetRequestId(), request.Request.GetResourceClass(), request.OfferCount, request.ExecutionID)
	}
	return nil
}

func (a *application) inspect(arguments []string, stderr io.Writer, resultOnly bool) error {
	name := "inspect"
	if resultOnly {
		name = "result"
	}
	flags := newFlagSet("r1s "+name, stderr)
	wait := flags.Duration("wait", 30*time.Second, "time to wait for allocator state")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("%s: execution ID is required", name)
	}
	if *wait <= 0 {
		return fmt.Errorf("%s: --wait must be positive", name)
	}
	executionID := flags.Arg(0)
	state, err := a.inspectState(a.ctx, executionID, *wait)
	if err != nil {
		return err
	}
	if resultOnly && !terminal(state.GetPhase()) {
		return fmt.Errorf("result is not terminal: phase=%s", phaseName(state.GetPhase()))
	}
	snapshot, _ := a.client.Execution(executionID)
	printState(a.stdout, snapshot)
	return nil
}

func (a *application) cancel(arguments []string, stderr io.Writer) error {
	flags := newFlagSet("r1s cancel", stderr)
	reason := flags.String("reason", "cancelled by client", "cancellation reason")
	wait := flags.Duration("wait", 30*time.Second, "time to wait for allocator state")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("cancel: execution ID is required")
	}
	if *wait <= 0 {
		return errors.New("cancel: --wait must be positive")
	}
	executionID := flags.Arg(0)
	if _, err := a.cancelState(a.ctx, executionID, *reason, *wait); err != nil {
		return err
	}
	snapshot, _ := a.client.Execution(executionID)
	printState(a.stdout, snapshot)
	return nil
}
