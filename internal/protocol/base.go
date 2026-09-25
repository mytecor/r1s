package protocol

import (
	"errors"
	"fmt"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrInvalidEnvelope = errors.New("invalid envelope")

// ValidationError identifies a field that violates the wire contract.
type ValidationError struct {
	Field   string
	Problem string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s: %s", ErrInvalidEnvelope, e.Field, e.Problem)
}

func (e *ValidationError) Unwrap() error { return ErrInvalidEnvelope }

func invalid(field, problem string) error {
	return &ValidationError{Field: field, Problem: problem}
}

// ValidateEnvelope validates required metadata and the complete active payload.
func ValidateEnvelope(envelope *r1sv1.Envelope) error {
	if envelope == nil {
		return invalid("envelope", "is required")
	}
	if strings.TrimSpace(envelope.GetMessageId()) == "" {
		return invalid("message_id", "is required")
	}
	if len(envelope.GetSender()) == 0 {
		return invalid("sender", "is required")
	}
	if err := validTimestamp("sent_at", envelope.GetSentAt()); err != nil {
		return err
	}

	switch payload := envelope.GetPayload().(type) {
	case *r1sv1.Envelope_CommandError:
		return validateError(envelope, payload.CommandError)
	case *r1sv1.Envelope_ExecutionLogsRequest:
		return validateLogsRequest(payload.ExecutionLogsRequest)
	case *r1sv1.Envelope_ExecutionLogsResponse:
		return validateLogsResponse(envelope, payload.ExecutionLogsResponse)
	case *r1sv1.Envelope_ExecutionRequest:
		return validateRequest(envelope, payload.ExecutionRequest)
	case *r1sv1.Envelope_ExecutionOffer:
		return validateOffer(envelope, payload.ExecutionOffer)
	case *r1sv1.Envelope_ExecutionAssign:
		return validateAssign(payload.ExecutionAssign)
	case *r1sv1.Envelope_ExecutionCancel:
		return validateCancel(payload.ExecutionCancel)
	case *r1sv1.Envelope_ExecutionState:
		return validateState(payload.ExecutionState)
	case *r1sv1.Envelope_ExecutionInspect:
		return validateInspect(payload.ExecutionInspect)
	case *r1sv1.Envelope_ExecutionOfferRelease:
		return validateOfferRelease(payload.ExecutionOfferRelease)
	case *r1sv1.Envelope_ExecutionLeaseRenew:
		return validateLeaseRenew(payload.ExecutionLeaseRenew)
	case *r1sv1.Envelope_ExecutionLeaseRenewAck:
		return validateLeaseRenewAck(envelope, payload.ExecutionLeaseRenewAck)
	case *r1sv1.Envelope_ExecutionTunnelOpen:
		return validateTunnelOpen(payload.ExecutionTunnelOpen)
	case *r1sv1.Envelope_ExecutionTunnelOpenAck:
		return validateTunnelOpenAck(payload.ExecutionTunnelOpenAck)
	case *r1sv1.Envelope_ExecutionOfferReleaseAck:
		return validateOfferReleaseAck(envelope, payload.ExecutionOfferReleaseAck)
	case nil:
		return invalid("payload", "is required")
	default:
		return invalid("payload", "is not supported")
	}
}

func validTimestamp(field string, value *timestamppb.Timestamp) error {
	if value == nil {
		return invalid(field, "is required")
	}
	if err := value.CheckValid(); err != nil {
		return invalid(field, err.Error())
	}
	return nil
}
