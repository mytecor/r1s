package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/mytecor/r1s/internal/protocol"
)

// defaultLeaseDuration matches the allocator's default initial lease so a
// keep-alive caller renews at a sane cadence without tuning anything.
const defaultLeaseDuration = protocol.DefaultLease

func (a *application) request(arguments []string, stderr io.Writer) error {
	flags := newFlagSet("r1s request [options] '<ExecutionRequest JSON>'", stderr)
	offerWait := flags.Duration("offer-wait", 10*time.Second, "time to discover allocators and collect offers")
	holdAlive := flags.Bool("keep-alive", false, "keep the execution leased and running until it terminates; re-requests the workload if the lease is ever lost")
	lease := flags.Duration("lease", defaultLeaseDuration, "lease duration for --keep-alive renewals")
	var allocators stringValues
	flags.Var(&allocators, "allocator", "allocator destination hash; repeat or omit for announce discovery")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("request: exactly one JSON argument is required")
	}
	if *offerWait <= 0 {
		return errors.New("request: --offer-wait must be positive")
	}
	if *lease <= 0 {
		return errors.New("request: --lease must be positive")
	}
	leaseSet := false
	flags.Visit(func(candidate *flag.Flag) {
		if candidate.Name == "lease" {
			leaseSet = true
		}
	})
	if leaseSet && !*holdAlive {
		return errors.New("request: --lease requires --keep-alive")
	}
	request, err := decodeRequestJSON(flags.Arg(0))
	if err != nil {
		return err
	}
	hold := time.Duration(0)
	if *holdAlive {
		hold = *lease
	}
	requestID, executionID, allocator, err := a.runRequest(a.ctx, request.GetWorkload(), request.GetPolicy(), request.GetResourceClass(), *offerWait, allocators, hold, request.GetConstraints())
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "request=%s execution=%s allocator=%x status=assignment-sent\n", requestID, executionID, allocator)
	if *holdAlive {
		return a.holdLease(a.ctx, executionID, *lease, a.stdout)
	}
	return nil
}
