package protocol_test

import (
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	"google.golang.org/protobuf/proto"
)

func TestReleaseValidation(t *testing.T) {
	release := validRequestEnvelope(time.Now().UTC())
	release.Payload = &r1sv1.Envelope_ExecutionOfferRelease{ExecutionOfferRelease: &r1sv1.ExecutionOfferRelease{RequestId: "request", OfferId: "offer"}}
	ack := proto.Clone(release).(*r1sv1.Envelope)
	ack.CorrelationId = release.GetMessageId()
	ack.Payload = &r1sv1.Envelope_ExecutionOfferReleaseAck{ExecutionOfferReleaseAck: &r1sv1.ExecutionOfferReleaseAck{RequestId: "request", OfferId: "offer", Outcome: r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_RELEASED}}
	for _, valid := range []*r1sv1.Envelope{release, ack} {
		if err := protocol.ValidateEnvelope(valid); err != nil {
			t.Fatal(err)
		}
	}
	cases := []struct {
		base   *r1sv1.Envelope
		mutate func(*r1sv1.Envelope)
	}{
		{release, func(e *r1sv1.Envelope) { e.GetExecutionOfferRelease().RequestId = " " }},
		{release, func(e *r1sv1.Envelope) { e.GetExecutionOfferRelease().OfferId = "" }},
		{release, func(e *r1sv1.Envelope) { e.Payload = &r1sv1.Envelope_ExecutionOfferRelease{} }},
		{ack, func(e *r1sv1.Envelope) { e.CorrelationId = "" }},
		{ack, func(e *r1sv1.Envelope) { e.GetExecutionOfferReleaseAck().RequestId = "" }},
		{ack, func(e *r1sv1.Envelope) { e.GetExecutionOfferReleaseAck().OfferId = "" }},
		{ack, func(e *r1sv1.Envelope) { e.GetExecutionOfferReleaseAck().Outcome = 0 }},
		{ack, func(e *r1sv1.Envelope) { e.GetExecutionOfferReleaseAck().Outcome = 99 }},
		{ack, func(e *r1sv1.Envelope) { e.Payload = &r1sv1.Envelope_ExecutionOfferReleaseAck{} }},
	}
	for i, test := range cases {
		candidate := proto.Clone(test.base).(*r1sv1.Envelope)
		test.mutate(candidate)
		if err := protocol.ValidateEnvelope(candidate); err == nil {
			t.Errorf("invalid release case %d accepted", i)
		}
	}
}
