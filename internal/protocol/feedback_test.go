package protocol_test

import (
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// CommandError validation: a known code, correlation, and bounded detail are
// required; unknown codes never reach a client as a fabricated rejection.
func TestCommandErrorValidation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	valid := feedbackEnvelope(now, &r1sv1.Envelope_CommandError{CommandError: &r1sv1.CommandError{Code: "CAPACITY", Detail: "capacity exhausted", Retryable: true}})
	valid.CorrelationId = "request-message"
	if err := protocol.ValidateEnvelope(valid); err != nil {
		t.Fatalf("valid command error rejected: %v", err)
	}

	tests := map[string]func(*r1sv1.Envelope){
		"missing correlation": func(e *r1sv1.Envelope) { e.CorrelationId = "" },
		"missing code": func(e *r1sv1.Envelope) {
			e.GetCommandError().Code = ""
		},
		"unknown code": func(e *r1sv1.Envelope) {
			e.GetCommandError().Code = "FORGED"
		},
		"oversized detail": func(e *r1sv1.Envelope) {
			e.GetCommandError().Detail = string(make([]byte, 257))
		},
		"nil command error": func(e *r1sv1.Envelope) {
			e.Payload = &r1sv1.Envelope_CommandError{}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			envelope := proto.Clone(valid).(*r1sv1.Envelope)
			mutate(envelope)
			if err := protocol.ValidateEnvelope(envelope); !errors.Is(err, protocol.ErrInvalidEnvelope) {
				t.Fatalf("ValidateEnvelope() = %v, want ErrInvalidEnvelope", err)
			}
		})
	}
}

// Logs request/response are explicit and bounded: 1..128 bytes, one stream, a
// contiguous range whose checksum must match. Responses that violate any of
// these cannot be accepted by the client.
func TestLogsRequestAndResponseValidation(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	data := []byte("hello container stderr")
	request := logsRequestEnvelope(now, "execution", "stderr", 0, uint32(len(data)))
	response := logsResponseEnvelope(now, "logs-message", data, 0, true)
	if err := protocol.ValidateEnvelope(request); err != nil {
		t.Fatalf("valid logs request rejected: %v", err)
	}
	if err := protocol.ValidateEnvelope(response); err != nil {
		t.Fatalf("valid logs response rejected: %v", err)
	}

	responseTests := map[string]func(*r1sv1.Envelope){
		"missing correlation": func(e *r1sv1.Envelope) { e.CorrelationId = "" },
		"wrong stream": func(e *r1sv1.Envelope) {
			e.GetExecutionLogsResponse().Stream = "combined"
		},
		"oversized data": func(e *r1sv1.Envelope) {
			e.GetExecutionLogsResponse().Data = make([]byte, protocol.MaxLogBytes+1)
		},
		"non-contiguous range": func(e *r1sv1.Envelope) {
			e.GetExecutionLogsResponse().NextOffset = e.GetExecutionLogsResponse().Offset + 1
		},
		"bad checksum": func(e *r1sv1.Envelope) {
			e.GetExecutionLogsResponse().Sha256 = make([]byte, 32)
		},
	}
	for name, mutate := range responseTests {
		t.Run("response/"+name, func(t *testing.T) {
			candidate := proto.Clone(response).(*r1sv1.Envelope)
			mutate(candidate)
			if err := protocol.ValidateEnvelope(candidate); !errors.Is(err, protocol.ErrInvalidEnvelope) {
				t.Fatalf("ValidateEnvelope() = %v, want ErrInvalidEnvelope", err)
			}
		})
	}

	requestTests := map[string]func(*r1sv1.Envelope){
		"missing execution": func(e *r1sv1.Envelope) {
			e.GetExecutionLogsRequest().ExecutionId = ""
		},
		"zero bytes": func(e *r1sv1.Envelope) {
			e.GetExecutionLogsRequest().MaxBytes = 0
		},
		"over max bytes": func(e *r1sv1.Envelope) {
			e.GetExecutionLogsRequest().MaxBytes = protocol.MaxLogBytes + 1
		},
		"unknown stream": func(e *r1sv1.Envelope) {
			e.GetExecutionLogsRequest().Stream = "stdin"
		},
	}
	for name, mutate := range requestTests {
		t.Run("request/"+name, func(t *testing.T) {
			candidate := proto.Clone(request).(*r1sv1.Envelope)
			mutate(candidate)
			if err := protocol.ValidateEnvelope(candidate); !errors.Is(err, protocol.ErrInvalidEnvelope) {
				t.Fatalf("ValidateEnvelope() = %v, want ErrInvalidEnvelope", err)
			}
		})
	}
}

// A valid logs response must be rejected when the range it claims does not
// match its actual bytes, even if the correlation and stream are correct.
func TestLogsResponseRangeMustMatchBytes(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	response := logsResponseEnvelope(now, "logs-message", []byte("abc"), 1, false)
	response.GetExecutionLogsResponse().Offset = 5
	if err := protocol.ValidateEnvelope(response); !errors.Is(err, protocol.ErrInvalidEnvelope) {
		t.Fatalf("range mismatch accepted: %v", err)
	}
}

func feedbackEnvelope(now time.Time, payload any) *r1sv1.Envelope {
	envelope := &r1sv1.Envelope{
		MessageId: "message",
		Sender:    []byte("allocator"),
		SentAt:    timestamppb.New(now),
	}
	switch payload := payload.(type) {
	case *r1sv1.Envelope_CommandError:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionLogsRequest:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionLogsResponse:
		envelope.Payload = payload
	default:
		panic("unsupported test payload")
	}
	return envelope
}

func logsRequestEnvelope(now time.Time, execution, stream string, offset uint64, limit uint32) *r1sv1.Envelope {
	return feedbackEnvelope(now, &r1sv1.Envelope_ExecutionLogsRequest{ExecutionLogsRequest: &r1sv1.ExecutionLogsRequest{
		ExecutionId: execution, Stream: stream, Offset: offset, MaxBytes: limit,
	}})
}

func logsResponseEnvelope(now time.Time, correlationID string, data []byte, offset uint64, eof bool) *r1sv1.Envelope {
	sum := sha256.Sum256(data)
	return &r1sv1.Envelope{
		MessageId: "logs-response", Sender: []byte("allocator"), CorrelationId: correlationID, SentAt: timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionLogsResponse{ExecutionLogsResponse: &r1sv1.ExecutionLogsResponse{
			ExecutionId: "execution", Stream: "stderr", Offset: offset, Data: data, NextOffset: offset + uint64(len(data)), Eof: eof, Sha256: sum[:],
		}},
	}
}
