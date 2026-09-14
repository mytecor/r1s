package allocator

import (
	"bytes"
	"errors"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Error text is deliberately independent of runtime diagnostics or workload output.
func (a *Allocator) errorResponse(command *r1sv1.Envelope, err error) []*r1sv1.Envelope {
	code, detail, retry := "INTERNAL", "allocator could not process command", false
	switch {
	case errors.Is(err, ErrAdmission):
		code, detail = "DENIED", "identity is not admitted by local policy"
	case errors.Is(err, ErrStore):
		code, detail, retry = "OUTCOME_UNKNOWN", "state commit failed; inspect the same allocator", true
	case errors.Is(err, ErrUnauthorized), errors.Is(err, ErrOfferNotFound), errors.Is(err, ErrExecutionNotFound):
		code, detail = "NOT_FOUND", "resource unavailable to this identity"
	case errors.Is(err, ErrCapacityExhausted), errors.Is(err, ErrReplayCapacity):
		code, detail, retry = "CAPACITY", "allocator capacity or command queue exhausted", true
	case errors.Is(err, ErrOfferExpired), errors.Is(err, ErrOfferReleased), errors.Is(err, ErrResultExpired), errors.Is(err, ErrCommandExpired):
		code, detail = "EXPIRED", "reservation or retained result expired"
	case errors.Is(err, ErrExecutionConflict), errors.Is(err, ErrReplayConflict), errors.Is(err, ErrOfferAlreadyAssigned):
		code, detail = "CONFLICT", "command conflicts with durable state"
	case errors.Is(err, protocol.ErrInvalidEnvelope), errors.Is(err, ErrUnsupportedMessage):
		code, detail = "INVALID_REQUEST", "invalid or unsupported command"
	case errors.Is(err, ErrRuntimeStart), errors.Is(err, ErrRuntimeStop):
		code, detail, retry = "UNAVAILABLE", "runtime operation failed; inspect the same allocator", true
	}
	return []*r1sv1.Envelope{{MessageId: randomID(), Sender: bytes.Clone(a.identity), CorrelationId: command.GetMessageId(), SentAt: timestamppb.New(a.now().UTC()), Payload: &r1sv1.Envelope_CommandError{CommandError: &r1sv1.CommandError{Code: code, Detail: detail, Retryable: retry}}}}
}
