package rns

import (
	"bytes"
	"context"
	"fmt"

	"github.com/mytecor/meshbus"
	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	coretransport "github.com/mytecor/r1s/internal/transport"
	"google.golang.org/protobuf/proto"
)

// envelopeHandler is the r1s protocol adapter above authenticated meshbus
// delivery. The transport sender always replaces any serialized sender.
func envelopeHandler(handler coretransport.Handler) meshbus.Handler {
	return func(ctx context.Context, message meshbus.ReceivedMessage) error {
		var envelope r1sv1.Envelope
		if err := proto.Unmarshal(message.Payload(), &envelope); err != nil {
			return nil
		}
		envelope.Sender = message.Sender().Bytes()
		if err := protocol.ValidateEnvelope(&envelope); err != nil {
			return nil
		}
		return handler(ctx, &envelope)
	}
}

// Send encodes an r1s control envelope over generic authenticated direct
// messaging. The local identity is written for wire compatibility; receivers
// still replace it with their session-authenticated sender.
func (e *Endpoint) Send(ctx context.Context, target string, envelope *r1sv1.Envelope) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if envelope == nil {
		return fmt.Errorf("%w: envelope is required", coretransport.ErrInvalidEndpoint)
	}
	cloned := proto.Clone(envelope).(*r1sv1.Envelope)
	cloned.Sender = bytes.Clone(e.identity)
	data, err := proto.Marshal(cloned)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	if err := e.transport.SendToDestination(ctx, target, data); err != nil {
		return translateSendError(err)
	}
	return nil
}
