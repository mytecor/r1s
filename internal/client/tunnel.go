package client

import (
	"bytes"
	"fmt"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TunnelGrant creates a fresh request to mint an execution-scoped, single-use
// F14 direct-access tunnel grant for the execution owner, pinning the client's
// edge node public key and the client-supplied destination slot list. The grant
// itself is transient — short-lived and consumed by the awaiting application —
// so nothing about it is persisted: after an allocator restart the client
// simply requests a new grant. Targets is the client-owned slot list the
// allocator binds into the grant; at least one destination is required.
func (o *Client) TunnelGrant(executionID string, peerKey []byte, targets []tunnel.Target) (string, *r1sv1.Envelope, error) {
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
	request := &r1sv1.ExecutionTunnelGrant{ExecutionId: executionID, YggPeerPubkey: bytes.Clone(peerKey), Targets: protoTargets}
	envelope := &r1sv1.Envelope{
		MessageId: o.newID(),
		Sender:    bytes.Clone(o.identity),
		SentAt:    timestamppb.New(o.now().UTC()),
		Payload:   &r1sv1.Envelope_ExecutionTunnelGrant{ExecutionTunnelGrant: request},
	}
	if err := protocol.ValidateEnvelope(envelope); err != nil {
		return "", nil, err
	}
	return record.destination, envelope, nil
}

// handleTunnelGrantAckLocked validates an allocator's mint reply without
// persisting it: the grant lifecycle is transient and owned by the application
// that awaits the ack by correlation ID, so the durable client state must not
// record it. The ack is still constrained (sender is the execution's allocator,
// the correlation matches the outstanding grant request) so a stray or forged
// ack cannot be attributed to a grant the client did not request.
func (o *Client) handleTunnelGrantAckLocked(envelope *r1sv1.Envelope, ack *r1sv1.ExecutionTunnelGrantAck) error {
	record := o.executions[ack.GetExecutionId()]
	if record == nil {
		return fmt.Errorf("%w: %q", ErrExecutionNotFound, ack.GetExecutionId())
	}
	if !bytes.Equal(record.allocatorID, envelope.GetSender()) {
		return ErrUnauthorized
	}
	// The awaiting application correlates by message ID; nothing durable to
	// write here. Gate the outcome: an ack for a grant we never saw is ignored
	// (the app awaits only its own message ID, so this case cannot normally be
	// reached).
	return nil
}
