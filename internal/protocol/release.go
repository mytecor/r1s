package protocol

import (
	"strings"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

func validateOfferRelease(release *r1sv1.ExecutionOfferRelease) error {
	if release == nil {
		return invalid("execution_offer_release", "is required")
	}
	if strings.TrimSpace(release.GetRequestId()) == "" || strings.TrimSpace(release.GetOfferId()) == "" {
		return invalid("execution_offer_release", "request_id and offer_id are required")
	}
	return nil
}

func validateOfferReleaseAck(envelope *r1sv1.Envelope, ack *r1sv1.ExecutionOfferReleaseAck) error {
	if ack == nil {
		return invalid("execution_offer_release_ack", "is required")
	}
	if strings.TrimSpace(envelope.GetCorrelationId()) == "" {
		return invalid("correlation_id", "is required for offer release acknowledgement")
	}
	if strings.TrimSpace(ack.GetRequestId()) == "" || strings.TrimSpace(ack.GetOfferId()) == "" {
		return invalid("execution_offer_release_ack", "request_id and offer_id are required")
	}
	if ack.GetOutcome() < r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_RELEASED || ack.GetOutcome() > r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_ASSIGNED {
		return invalid("execution_offer_release_ack.outcome", "is not supported")
	}
	return nil
}
