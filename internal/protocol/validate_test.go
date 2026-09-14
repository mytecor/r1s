package protocol_test

import (
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestValidateEnvelope(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	valid := validRequestEnvelope(now)

	tests := map[string]func(*r1sv1.Envelope){
		"missing message ID": func(envelope *r1sv1.Envelope) { envelope.MessageId = "" },
		"missing sender":     func(envelope *r1sv1.Envelope) { envelope.Sender = nil },
		"missing sent time":  func(envelope *r1sv1.Envelope) { envelope.SentAt = nil },
		"missing payload":    func(envelope *r1sv1.Envelope) { envelope.Payload = nil },
		"missing request ID": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().RequestId = ""
		},
		"missing resource class": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().ResourceClass = ""
		},
		"missing workload": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().Workload = nil
		},
		"missing image": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().Workload.Image = ""
		},
		"empty environment key": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().Workload.Environment[""] = "value"
		},
		"missing policy": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().Policy = nil
		},
		"unbounded policy": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().Policy = &r1sv1.ExecutionPolicy{}
		},
		"non-positive max runtime": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().Policy.MaxRuntime = durationpb.New(0)
		},
		"deadline before message": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().Policy.Deadline = timestamppb.New(now)
		},
		"negative result retention": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionRequest().Policy.ResultRetention = durationpb.New(-time.Second)
		},
	}

	if err := protocol.ValidateEnvelope(valid); err != nil {
		t.Fatalf("valid envelope rejected: %v", err)
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			envelope := proto.Clone(valid).(*r1sv1.Envelope)
			mutate(envelope)
			if err := protocol.ValidateEnvelope(envelope); !errors.Is(err, protocol.ErrInvalidEnvelope) {
				t.Fatalf("ValidateEnvelope() error = %v, want ErrInvalidEnvelope", err)
			}
		})
	}
}

func TestValidateEveryPayload(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	tests := []*r1sv1.Envelope{
		validRequestEnvelope(now),
		envelope(now, &r1sv1.Envelope_ExecutionOffer{ExecutionOffer: &r1sv1.ExecutionOffer{
			OfferId: "offer", RequestId: "request", ResourceClass: "default", ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		}}),
		envelope(now, &r1sv1.Envelope_ExecutionAssign{ExecutionAssign: &r1sv1.ExecutionAssign{
			RequestId: "request", OfferId: "offer", ExecutionId: "execution",
		}}),
		envelope(now, &r1sv1.Envelope_ExecutionCancel{ExecutionCancel: &r1sv1.ExecutionCancel{ExecutionId: "execution"}}),
		envelope(now, &r1sv1.Envelope_ExecutionState{ExecutionState: &r1sv1.ExecutionState{
			ExecutionId: "execution", Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, OccurredAt: timestamppb.New(now),
		}}),
		envelope(now, &r1sv1.Envelope_ExecutionInspect{ExecutionInspect: &r1sv1.ExecutionInspect{ExecutionId: "execution"}}),
	}
	for _, candidate := range tests {
		if err := protocol.ValidateEnvelope(candidate); err != nil {
			t.Errorf("ValidateEnvelope(%T) error = %v", candidate.GetPayload(), err)
		}
	}
}

func validRequestEnvelope(now time.Time) *r1sv1.Envelope {
	return envelope(now, &r1sv1.Envelope_ExecutionRequest{ExecutionRequest: &r1sv1.ExecutionRequest{
		RequestId:     "request",
		ResourceClass: "default",
		Workload: &r1sv1.Workload{
			Image:       "example.test/image:latest",
			Environment: map[string]string{"KEY": "value"},
		},
		Policy: &r1sv1.ExecutionPolicy{
			Deadline:        timestamppb.New(now.Add(time.Hour)),
			MaxRuntime:      durationpb.New(time.Minute),
			ResultRetention: durationpb.New(time.Minute),
		},
	}})
}

func envelope(now time.Time, payload any) *r1sv1.Envelope {
	envelope := &r1sv1.Envelope{
		MessageId: "message",
		Sender:    []byte("client"),
		SentAt:    timestamppb.New(now),
	}
	switch payload := payload.(type) {
	case *r1sv1.Envelope_ExecutionRequest:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionOffer:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionAssign:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionCancel:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionState:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionInspect:
		envelope.Payload = payload
	default:
		panic("unsupported test payload")
	}
	return envelope
}
