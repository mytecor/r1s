package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
)

// tunnelEndpointAdvertisement returns the endpoint advertisement handed to
// minted grants: the running edge's address and node key when the edge is on,
// the explicitly configured static value otherwise.
func (d *daemon) tunnelEndpointAdvertisement() tunnel.Endpoint {
	if d.tunnel != nil {
		return d.tunnel.listener.Endpoint()
	}
	return d.tunnelStaticEndpoint
}

// handleEnvelope applies one inbound control-plane envelope to the allocator
// core and relays any responses back to the sender.
func (d *daemon) handleEnvelope(_ context.Context, envelope *r1sv1.Envelope) error {
	responses, handleErr := d.core.Handle(context.Background(), envelope)
	if handleErr != nil {
		d.logger.Printf("reject message %q from %x: %v", envelope.GetMessageId(), envelope.GetSender(), handleErr)
	}
	var responseErr error
	for _, response := range responses {
		sendContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		sendErr := d.endpoint.Send(sendContext, hex.EncodeToString(envelope.GetSender()), response)
		cancel()
		if sendErr != nil {
			responseErr = errors.Join(responseErr, fmt.Errorf("send response: %w", sendErr))
		}
	}
	return errors.Join(handleErr, responseErr)
}
