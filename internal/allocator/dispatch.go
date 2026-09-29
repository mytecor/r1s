package allocator

import (
	"context"
	"errors"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

// Handle validates an authenticated envelope and applies one allocator command.
// Duplicate message IDs from the same sender are acknowledged without mutation.
//
// This is the protocol dispatch surface of the allocator: it checks freshness
// and envelope shape, keys every command into the replay cache so a redelivered
// message is answered from durable state instead of being re-applied, and
// routes to the per-command handlers in this package (offers.go, executions.go,
// lease.go, release.go, logs.go, tunnel.go). State transitions themselves live
// in those handler files; dispatch.go owns only the routing.
func (a *Allocator) Handle(ctx context.Context, envelope *r1sv1.Envelope) (responses []*r1sv1.Envelope, err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			a.observeRejectionMetric(err)
		}
		a.observeDispatchLatency(commandName(envelope), time.Since(start))
	}()
	if err = protocol.ValidateEnvelope(envelope); err != nil {
		return nil, err
	}
	if err = a.checkFreshness(envelope); err != nil {
		return a.errorResponse(envelope, err), err
	}
	if envelope.GetExecutionLogsRequest() != nil {
		return a.handleLogs(ctx, envelope)
	}
	switch envelope.GetPayload().(type) {
	case *r1sv1.Envelope_ExecutionRequest, *r1sv1.Envelope_ExecutionAssign, *r1sv1.Envelope_ExecutionCancel, *r1sv1.Envelope_ExecutionInspect, *r1sv1.Envelope_ExecutionOfferRelease, *r1sv1.Envelope_ExecutionLeaseRenew, *r1sv1.Envelope_ExecutionTunnelOpen:
	default:
		return nil, ErrUnsupportedMessage
	}

	replayKey := authorityKey(envelope.GetSender(), envelope.GetMessageId())
	entry, duplicate, replayErr := a.beginReplay(replayKey, envelope, a.now().UTC())
	if replayErr != nil {
		return a.errorResponse(envelope, replayErr), replayErr
	}
	if duplicate {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-entry.done:
			return cloneEnvelopes(entry.responses), entry.err
		}
	}

	switch payload := envelope.GetPayload().(type) {
	case *r1sv1.Envelope_ExecutionRequest:
		responses, err = a.handleRequest(envelope, payload.ExecutionRequest)
	case *r1sv1.Envelope_ExecutionAssign:
		responses, err = a.handleAssign(ctx, envelope, payload.ExecutionAssign)
	case *r1sv1.Envelope_ExecutionCancel:
		responses, err = a.handleCancel(ctx, envelope, payload.ExecutionCancel)
	case *r1sv1.Envelope_ExecutionInspect:
		responses, err = a.handleInspect(envelope, payload.ExecutionInspect)
	case *r1sv1.Envelope_ExecutionOfferRelease:
		responses, err = a.handleOfferRelease(envelope, payload.ExecutionOfferRelease)
	case *r1sv1.Envelope_ExecutionLeaseRenew:
		responses, err = a.handleLeaseRenew(envelope, payload.ExecutionLeaseRenew)
	case *r1sv1.Envelope_ExecutionTunnelOpen:
		responses, err = a.handleTunnelOpen(envelope, payload.ExecutionTunnelOpen)
	default:
		panic("payload type checked above")
	}
	if err != nil && len(responses) == 0 {
		responses = a.errorResponse(envelope, err)
	}
	if persistErr := a.finishReplay(entry, responses, err); persistErr != nil {
		err = errors.Join(err, persistErr)
	}
	return responses, err
}

// commandName returns the stable dispatch-latency label for an envelope's
// command. It is derived from the payload type rather than a variable updated
// as Handle progresses, so every exit path — including validation, freshness,
// and replay-duplicate ack — records the command it was actually handling
// instead of falling back to "unknown".
func commandName(envelope *r1sv1.Envelope) string {
	if envelope.GetExecutionLogsRequest() != nil {
		return "logs"
	}
	switch envelope.GetPayload().(type) {
	case *r1sv1.Envelope_ExecutionRequest:
		return "request"
	case *r1sv1.Envelope_ExecutionAssign:
		return "assign"
	case *r1sv1.Envelope_ExecutionCancel:
		return "cancel"
	case *r1sv1.Envelope_ExecutionInspect:
		return "inspect"
	case *r1sv1.Envelope_ExecutionOfferRelease:
		return "release"
	case *r1sv1.Envelope_ExecutionLeaseRenew:
		return "renew"
	case *r1sv1.Envelope_ExecutionTunnelOpen:
		return "tunnel_open"
	default:
		return "unknown"
	}
}
