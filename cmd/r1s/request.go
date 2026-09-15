package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/mytecor/r1s/internal/client"
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
	requestID, envelope, err := a.client.CreateRequest(request.GetWorkload(), request.GetPolicy(), request.GetResourceClass())
	if err != nil {
		return err
	}
	sent := make(map[string]bool)
	var rejections []error
	for _, destination := range allocators {
		if err := a.send(destination, envelope); err != nil {
			return fmt.Errorf("send request to %s: %w", destination, err)
		}
		sent[strings.ToLower(destination)] = true
	}
	timer := time.NewTimer(*offerWait)
	defer timer.Stop()
collect:
	for {
		select {
		case <-a.ctx.Done():
			return a.ctx.Err()
		case <-timer.C:
			break collect
		case response := <-a.events:
			if response.GetCorrelationId() == envelope.GetMessageId() {
				if err := client.RemoteFailure(response); err != nil {
					rejections = append(rejections, err)
					fmt.Fprintln(stderr, err)
				}
			}
		case service := <-a.endpoint.Discoveries():
			if service.Descriptor.Capacity[request.GetResourceClass()] == 0 {
				continue
			}
			identity, decodeErr := hex.DecodeString(service.Identity)
			if decodeErr != nil {
				continue
			}
			if err := a.client.RegisterAllocator(client.Allocator{Identity: identity, Destination: service.Destination, Hops: service.Hops, Capacity: service.Descriptor.Capacity}); err != nil {
				return err
			}
			if !sent[service.Destination] {
				if err := a.send(service.Destination, envelope); err != nil {
					continue
				}
				sent[service.Destination] = true
			}
		}
	}
	destination, assignment, err := a.client.Select(requestID)
	if err != nil {
		return fmt.Errorf("request %s: %w", requestID, errors.Join(append(rejections, err)...))
	}
	if err := a.send(destination, assignment); err != nil {
		return fmt.Errorf("send assignment: %w", err)
	}
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	snapshot, _ := a.client.Execution(executionID)
	fmt.Fprintf(a.stdout, "request=%s execution=%s allocator=%x status=assignment-sent\n", requestID, executionID, snapshot.Allocator)
	return nil
}
