package protocol_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"github.com/mytecor/r1s/internal/tunnel"
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
		envelope(now, &r1sv1.Envelope_ExecutionLeaseRenew{ExecutionLeaseRenew: &r1sv1.ExecutionLeaseRenew{
			ExecutionId: "execution", LeaseDuration: durationpb.New(time.Minute),
		}}),
		envelope(now, &r1sv1.Envelope_ExecutionTunnelGrant{ExecutionTunnelGrant: &r1sv1.ExecutionTunnelGrant{
			ExecutionId: "execution", YggPeerPubkey: []byte("edge-node-public-key"),
		}}),
		envelope(now, &r1sv1.Envelope_ExecutionTunnelGrantAck{ExecutionTunnelGrantAck: &r1sv1.ExecutionTunnelGrantAck{
			ExecutionId: "execution", GrantId: "grant", ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		}}),
	}
	for _, candidate := range tests {
		if err := protocol.ValidateEnvelope(candidate); err != nil {
			t.Errorf("ValidateEnvelope(%T) error = %v", candidate.GetPayload(), err)
		}
	}
}

func TestValidateLeaseRenew(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	valid := envelope(now, &r1sv1.Envelope_ExecutionLeaseRenew{ExecutionLeaseRenew: &r1sv1.ExecutionLeaseRenew{
		ExecutionId: "execution", LeaseDuration: durationpb.New(time.Minute),
	}})
	tests := map[string]func(*r1sv1.Envelope){
		"missing execution ID": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionLeaseRenew().ExecutionId = ""
		},
		"missing lease duration": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionLeaseRenew().LeaseDuration = nil
		},
		"non-positive lease duration": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionLeaseRenew().LeaseDuration = durationpb.New(0)
		},
	}
	if err := protocol.ValidateEnvelope(valid); err != nil {
		t.Fatalf("valid lease renewal rejected: %v", err)
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := proto.Clone(valid).(*r1sv1.Envelope)
			mutate(candidate)
			if err := protocol.ValidateEnvelope(candidate); !errors.Is(err, protocol.ErrInvalidEnvelope) {
				t.Fatalf("ValidateEnvelope() error = %v, want ErrInvalidEnvelope", err)
			}
		})
	}
}

func TestValidateTunnelGrant(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	valid := envelope(now, &r1sv1.Envelope_ExecutionTunnelGrant{ExecutionTunnelGrant: &r1sv1.ExecutionTunnelGrant{
		ExecutionId: "execution", YggPeerPubkey: []byte("edge-node-public-key"),
	}})
	tests := map[string]func(*r1sv1.Envelope){
		"missing execution ID": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionTunnelGrant().ExecutionId = ""
		},
		"missing peer public key": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionTunnelGrant().YggPeerPubkey = nil
		},
		"peer public key too large": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionTunnelGrant().YggPeerPubkey = bytes.Repeat([]byte{1}, tunnel.MaxPeerKeySize+1)
		},
	}
	if err := protocol.ValidateEnvelope(valid); err != nil {
		t.Fatalf("valid tunnel grant rejected: %v", err)
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := proto.Clone(valid).(*r1sv1.Envelope)
			mutate(candidate)
			if err := protocol.ValidateEnvelope(candidate); !errors.Is(err, protocol.ErrInvalidEnvelope) {
				t.Fatalf("ValidateEnvelope() error = %v, want ErrInvalidEnvelope", err)
			}
		})
	}
}

func TestValidateTunnelGrantAck(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	valid := envelope(now, &r1sv1.Envelope_ExecutionTunnelGrantAck{ExecutionTunnelGrantAck: &r1sv1.ExecutionTunnelGrantAck{
		ExecutionId: "execution", GrantId: "grant", ExpiresAt: timestamppb.New(now.Add(time.Minute)),
		AllocatorEndpoint: []byte("transport-neutral-address"), AllocatorEndpointPubkey: []byte("transport-neutral-key"),
	}})
	tests := map[string]func(*r1sv1.Envelope){
		"missing execution ID": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionTunnelGrantAck().ExecutionId = ""
		},
		"missing grant ID": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionTunnelGrantAck().GrantId = ""
		},
		"missing expiry": func(envelope *r1sv1.Envelope) {
			envelope.GetExecutionTunnelGrantAck().ExpiresAt = nil
		},
	}
	if err := protocol.ValidateEnvelope(valid); err != nil {
		t.Fatalf("valid tunnel grant ack rejected: %v", err)
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			candidate := proto.Clone(valid).(*r1sv1.Envelope)
			mutate(candidate)
			if err := protocol.ValidateEnvelope(candidate); !errors.Is(err, protocol.ErrInvalidEnvelope) {
				t.Fatalf("ValidateEnvelope() error = %v, want ErrInvalidEnvelope", err)
			}
		})
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
	case *r1sv1.Envelope_ExecutionLeaseRenew:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionTunnelGrant:
		envelope.Payload = payload
	case *r1sv1.Envelope_ExecutionTunnelGrantAck:
		envelope.Payload = payload
	default:
		panic("unsupported test payload")
	}
	return envelope
}
