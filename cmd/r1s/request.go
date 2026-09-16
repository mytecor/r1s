package main

import (
	"errors"
	"fmt"
	"io"
	"time"
)

func (a *application) request(arguments []string, stderr io.Writer) error {
	flags := newFlagSet("r1s request [options] '<ExecutionRequest JSON>'", stderr)
	offerWait := flags.Duration("offer-wait", 10*time.Second, "time to discover allocators and collect offers")
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
	request, err := decodeRequestJSON(flags.Arg(0))
	if err != nil {
		return err
	}
	requestID, executionID, allocator, err := a.runRequest(a.ctx, request.GetWorkload(), request.GetPolicy(), request.GetResourceClass(), *offerWait, allocators)
	if err != nil {
		return err
	}
	fmt.Fprintf(a.stdout, "request=%s execution=%s allocator=%x status=assignment-sent\n", requestID, executionID, allocator)
	return nil
}
