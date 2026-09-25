package protocol

import (
	"encoding/hex"
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

const runIDBytes = 16

func validateRequest(envelope *r1sv1.Envelope, request *r1sv1.ExecutionRequest) error {
	if request == nil {
		return invalid("execution_request", "is required")
	}
	if strings.TrimSpace(request.GetRequestId()) == "" {
		return invalid("execution_request.request_id", "is required")
	}
	if !validRunID(request.GetRunId()) {
		return invalid("execution_request.run_id", "must be 32 lowercase hexadecimal characters")
	}
	if request.GetAttempt() == 0 {
		return invalid("execution_request.attempt", "must be positive")
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

func validRunID(value string) bool {
	if len(value) != runIDBytes*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == runIDBytes
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
