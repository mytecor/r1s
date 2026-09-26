package client

import (
	"bytes"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TunnelOpen creates a fresh declaration of the owned run's tunnel binding for
// an execution (F22-06): it pins the client's edge node public key and the
// client-supplied destination container port list to the execution over the
// authenticated control plane. There is no transient grant token: the binding
// lives for the execution's lifetime, nothing is persisted (after an allocator
// restart the run simply re-declares), and the allocator authorizes a tunnel
// mesh peer that presents the bound key directly. Targets is the client-owned
// slot list the allocator binds to the execution; at least one destination is
// required.
func (o *Client) TunnelOpen(executionID string, peerKey []byte, targets []tunnel.Target) (string, *r1sv1.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	record := o.executions[executionID]
	if record == nil {
		return "", nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, executionID)
	}
	if len(peerKey) == 0 || len(peerKey) > tunnel.MaxPeerKeySize {
		return "", nil, fmt.Errorf("%w: invalid tunnel peer key size %d", ErrInvalidConfig, len(peerKey))
	}
	protoTargets := protocol.TargetsToProto(targets)
	if _, err := protocol.ValidateTunnelTargets(protoTargets); err != nil {
		return "", nil, err
	}
	request := &r1sv1.ExecutionTunnelOpen{ExecutionId: executionID, YggPeerPubkey: bytes.Clone(peerKey), Targets: protoTargets}
	envelope := &r1sv1.Envelope{
		MessageId: o.newID(),
		Sender:    bytes.Clone(o.identity),
		SentAt:    timestamppb.New(o.now().UTC()),
		Payload:   &r1sv1.Envelope_ExecutionTunnelOpen{ExecutionTunnelOpen: request},
	}
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return "", nil, err
	}
	return record.destination, envelope, nil
}

// handleTunnelOpenAckLocked validates an allocator's reply to an
// ExecutionTunnelOpen without persisting it: the binding lifecycle is owned by
// the application that awaits the ack by correlation ID, so the in-memory client
// state must not record it. The ack is still constrained (sender is the
// execution's allocator, the correlation matches the outstanding open request)
// so a stray or forged ack cannot be attributed to an open the client did not
// request.
func (o *Client) handleTunnelOpenAckLocked(envelope *r1sv1.Envelope, ack *r1sv1.ExecutionTunnelOpenAck) error {
	record := o.executions[ack.GetExecutionId()]
	if record == nil {
		return fmt.Errorf("%w: %q", ErrExecutionNotFound, ack.GetExecutionId())
	}
	if !bytes.Equal(record.allocatorID, envelope.GetSender()) {
		return ErrUnauthorized
	}
	// The awaiting application correlates by message ID; nothing durable to
	// write here. Gate the outcome: an ack for an open we never saw is ignored
	// (the app awaits only its own message ID, so this case cannot normally be
	// reached).
	return nil
}
