// Package protocol validates the versioned wire contract before domain state is mutated.
package protocol

import (
	"errors"
	"fmt"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/tunnel"
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
	case *r1sv1.Envelope_ExecutionTunnelGrant:
		return validateTunnelGrant(payload.ExecutionTunnelGrant)
	case *r1sv1.Envelope_ExecutionTunnelGrantAck:
		return validateTunnelGrantAck(payload.ExecutionTunnelGrantAck)
	case *r1sv1.Envelope_ExecutionOfferReleaseAck:
		return validateOfferReleaseAck(envelope, payload.ExecutionOfferReleaseAck)
	case nil:
		return invalid("payload", "is required")
	default:
		return invalid("payload", "is not supported")
	}
}

func validateRequest(envelope *r1sv1.Envelope, request *r1sv1.ExecutionRequest) error {
	if request == nil {
		return invalid("execution_request", "is required")
	}
	if strings.TrimSpace(request.GetRequestId()) == "" {
		return invalid("execution_request.request_id", "is required")
	}
	if strings.TrimSpace(request.GetResourceClass()) == "" {
		return invalid("execution_request.resource_class", "is required")
	}
	if err := ValidateConstraints(request.GetConstraints()); err != nil {
		return err
	}
	workload := request.GetWorkload()
	if workload == nil {
		return invalid("execution_request.workload", "is required")
	}
	if strings.TrimSpace(workload.GetImage()) == "" {
		return invalid("execution_request.workload.image", "is required")
	}
	for key := range workload.GetEnvironment() {
		if strings.TrimSpace(key) == "" {
			return invalid("execution_request.workload.environment", "contains an empty key")
		}
	}

	policy := request.GetPolicy()
	if policy == nil {
		return invalid("execution_request.policy", "is required")
	}
	if policy.GetResultRetention() != nil {
		if err := policy.GetResultRetention().CheckValid(); err != nil {
			return invalid("execution_request.policy.result_retention", err.Error())
		}
		if policy.GetResultRetention().AsDuration() < 0 {
			return invalid("execution_request.policy.result_retention", "must not be negative")
		}
	}
	return nil
}

func validateOffer(envelope *r1sv1.Envelope, offer *r1sv1.ExecutionOffer) error {
	if offer == nil {
		return invalid("execution_offer", "is required")
	}
	if strings.TrimSpace(offer.GetOfferId()) == "" {
		return invalid("execution_offer.offer_id", "is required")
	}
	if strings.TrimSpace(offer.GetRequestId()) == "" {
		return invalid("execution_offer.request_id", "is required")
	}
	if strings.TrimSpace(offer.GetResourceClass()) == "" {
		return invalid("execution_offer.resource_class", "is required")
	}
	if err := validTimestamp("execution_offer.expires_at", offer.GetExpiresAt()); err != nil {
		return err
	}
	if !offer.GetExpiresAt().AsTime().After(envelope.GetSentAt().AsTime()) {
		return invalid("execution_offer.expires_at", "must be after sent_at")
	}
	if err := ValidateCapabilities(offer.GetNode()); err != nil {
		return err
	}
	return nil
}

func validateAssign(assign *r1sv1.ExecutionAssign) error {
	if assign == nil {
		return invalid("execution_assign", "is required")
	}
	if strings.TrimSpace(assign.GetRequestId()) == "" {
		return invalid("execution_assign.request_id", "is required")
	}
	if strings.TrimSpace(assign.GetOfferId()) == "" {
		return invalid("execution_assign.offer_id", "is required")
	}
	if strings.TrimSpace(assign.GetExecutionId()) == "" {
		return invalid("execution_assign.execution_id", "is required")
	}
	return nil
}

func validateCancel(cancel *r1sv1.ExecutionCancel) error {
	if cancel == nil {
		return invalid("execution_cancel", "is required")
	}
	if strings.TrimSpace(cancel.GetExecutionId()) == "" {
		return invalid("execution_cancel.execution_id", "is required")
	}
	return nil
}

func validateInspect(inspect *r1sv1.ExecutionInspect) error {
	if inspect == nil {
		return invalid("execution_inspect", "is required")
	}
	if strings.TrimSpace(inspect.GetExecutionId()) == "" {
		return invalid("execution_inspect.execution_id", "is required")
	}
	return nil
}

func validateLeaseRenew(renew *r1sv1.ExecutionLeaseRenew) error {
	if renew == nil {
		return invalid("execution_lease_renew", "is required")
	}
	if strings.TrimSpace(renew.GetExecutionId()) == "" {
		return invalid("execution_lease_renew.execution_id", "is required")
	}
	if renew.GetLeaseDuration() == nil {
		return invalid("execution_lease_renew.lease_duration", "is required")
	}
	if err := renew.GetLeaseDuration().CheckValid(); err != nil {
		return invalid("execution_lease_renew.lease_duration", err.Error())
	}
	if renew.GetLeaseDuration().AsDuration() <= 0 {
		return invalid("execution_lease_renew.lease_duration", "must be positive")
	}
	return nil
}

func validateLeaseRenewAck(envelope *r1sv1.Envelope, ack *r1sv1.ExecutionLeaseRenewAck) error {
	if ack == nil {
		return invalid("execution_lease_renew_ack", "is required")
	}
	if strings.TrimSpace(ack.GetExecutionId()) == "" {
		return invalid("execution_lease_renew_ack.execution_id", "is required")
	}
	if err := validTimestamp("execution_lease_renew_ack.expires_at", ack.GetExpiresAt()); err != nil {
		return err
	}
	return nil
}

// validateTunnelGrant validates a client's request to mint a direct-access
// tunnel grant. The peer node public key is an opaque value; it is required
// because the allocator pins it into the grant (nothing secret is embedded,
// but the key must be present to authorize the tunnel edge at connect time).
func validateTunnelGrant(grant *r1sv1.ExecutionTunnelGrant) error {
	if grant == nil {
		return invalid("execution_tunnel_grant", "is required")
	}
	if strings.TrimSpace(grant.GetExecutionId()) == "" {
		return invalid("execution_tunnel_grant.execution_id", "is required")
	}
	if len(grant.GetYggPeerPubkey()) == 0 {
		return invalid("execution_tunnel_grant.ygg_peer_pubkey", "is required")
	}
	if len(grant.GetYggPeerPubkey()) > tunnel.MaxPeerKeySize {
		return invalid("execution_tunnel_grant.ygg_peer_pubkey", "is too large")
	}
	return nil
}

// validateTunnelGrantAck validates the allocator's mint reply. The endpoint
// advertisement is opaque transport-neutral bytes: it may be empty on an older
// peer that acknowledges a grant before its edge advertises an endpoint, so
// only the structural fields are enforced here. The ontology of "grant bound to
// the wrong execution" lives in the allocator, not in wire validation: the ack
// echoes the execution the allocator minted for.
func validateTunnelGrantAck(ack *r1sv1.ExecutionTunnelGrantAck) error {
	if ack == nil {
		return invalid("execution_tunnel_grant_ack", "is required")
	}
	if strings.TrimSpace(ack.GetExecutionId()) == "" {
		return invalid("execution_tunnel_grant_ack.execution_id", "is required")
	}
	if strings.TrimSpace(ack.GetGrantId()) == "" {
		return invalid("execution_tunnel_grant_ack.grant_id", "is required")
	}
	if err := validTimestamp("execution_tunnel_grant_ack.expires_at", ack.GetExpiresAt()); err != nil {
		return err
	}
	return nil
}

func validateState(state *r1sv1.ExecutionState) error {
	if state == nil {
		return invalid("execution_state", "is required")
	}
	if strings.TrimSpace(state.GetExecutionId()) == "" {
		return invalid("execution_state.execution_id", "is required")
	}
	switch state.GetPhase() {
	case r1sv1.ExecutionPhase_EXECUTION_PHASE_STARTING,
		r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING,
		r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLING,
		r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED,
		r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED,
		r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED:
	default:
		return invalid("execution_state.phase", "is required and must be known")
	}
	return validTimestamp("execution_state.occurred_at", state.GetOccurredAt())
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
