package client

import (
	"context"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type releaseStore struct {
	data []byte
	fail bool
}

func (s *releaseStore) Load(context.Context) ([]byte, error) {
	return append([]byte(nil), s.data...), nil
}
func (s *releaseStore) Save(_ context.Context, data []byte) error {
	if s.fail {
		return errors.New("injected failure")
	}
	s.data = append([]byte(nil), data...)
	return nil
}

func TestReleaseIntentIsAtomicDurableAndHandlesLateOffers(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	store := &releaseStore{}
	config := Config{Identity: []byte("client"), Store: store, Now: func() time.Time { return now }}
	core, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	for i, name := range []string{"winner", "loser", "late"} {
		registerAllocator(t, core, name, name, uint8(i))
	}
	id, request, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	mustObserveOffer(t, core, now, request, "winner", "offer-winner")
	mustObserveOffer(t, core, now, request, "loser", "offer-loser")
	store.fail = true
	if _, _, err := core.Select(id); !errors.Is(err, ErrStore) {
		t.Fatalf("select: %v", err)
	}
	if len(core.Executions()) != 0 || len(core.PendingReleases()) != 0 {
		t.Fatal("failed selection leaked assignment or releases")
	}
	store.fail = false
	_, assignment, err := core.Select(id)
	if err != nil {
		t.Fatal(err)
	}
	pending := core.PendingReleases()
	if len(pending) != 1 || pending[0].Destination != "loser" {
		t.Fatalf("pending=%v", pending)
	}
	first := pending[0].Envelope
	core, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	if pending = core.PendingReleases(); len(pending) != 1 || !proto.Equal(pending[0].Envelope, first) {
		t.Fatal("restart changed release")
	}
	_, replay, err := core.Select(id)
	if err != nil || !proto.Equal(replay, assignment) {
		t.Fatal("restart changed assignment")
	}
	late := offerEnvelope(now, request, "late", "offer-late")
	store.fail = true
	if err := core.Handle(context.Background(), late); !errors.Is(err, ErrStore) {
		t.Fatalf("late store failure=%v", err)
	}
	if len(core.PendingReleases()) != 1 {
		t.Fatal("failed late offer commit leaked release")
	}
	store.fail = false
	if err := core.Handle(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if err := core.Handle(context.Background(), late); err != nil {
		t.Fatal(err)
	}
	if len(core.PendingReleases()) != 2 {
		t.Fatal("late offer was lost or duplicated")
	}
	core, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(core.PendingReleases()) != 2 {
		t.Fatal("late release did not survive restart")
	}
	for _, release := range core.PendingReleases() {
		ack := &r1sv1.Envelope{MessageId: "ack", Sender: []byte(release.Destination), CorrelationId: release.Envelope.GetMessageId(), SentAt: timestamppb.New(now),
			Payload: &r1sv1.Envelope_ExecutionOfferReleaseAck{ExecutionOfferReleaseAck: &r1sv1.ExecutionOfferReleaseAck{RequestId: id, OfferId: release.Envelope.GetExecutionOfferRelease().GetOfferId(), Outcome: r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_RELEASED}},
		}
		forged := proto.Clone(ack).(*r1sv1.Envelope)
		forged.Sender = []byte("winner")
		if err := core.Handle(context.Background(), forged); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("forged ack=%v", err)
		}
		forged = proto.Clone(ack).(*r1sv1.Envelope)
		forged.CorrelationId = "wrong"
		if err := core.Handle(context.Background(), forged); !errors.Is(err, ErrConflict) {
			t.Fatalf("uncorrelated ack=%v", err)
		}
		store.fail = true
		if err := core.Handle(context.Background(), ack); !errors.Is(err, ErrStore) {
			t.Fatalf("ack store failure=%v", err)
		}
		store.fail = false
		if err := core.Handle(context.Background(), ack); err != nil {
			t.Fatal(err)
		}
		if err := core.Handle(context.Background(), ack); err != nil {
			t.Fatal(err)
		}
	}
	core, err = New(config)
	if err != nil {
		t.Fatal(err)
	}
	if len(core.PendingReleases()) != 0 {
		t.Fatal("acknowledged releases retried after restart")
	}
}

func TestLostReleaseExpiresWithoutChangingAssignment(t *testing.T) {
	now := time.Unix(1_800_000_000, 0).UTC()
	core, err := New(Config{Identity: []byte("client"), Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	registerAllocator(t, core, "winner", "winner", 0)
	registerAllocator(t, core, "loser", "loser", 1)
	id, request, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	mustObserveOffer(t, core, now, request, "winner", "offer-winner")
	mustObserveOffer(t, core, now, request, "loser", "offer-loser")
	_, assignment, err := core.Select(id)
	if err != nil {
		t.Fatal(err)
	}
	if len(core.PendingReleases()) != 1 {
		t.Fatal("missing release")
	}
	now = now.Add(time.Hour)
	if len(core.PendingReleases()) != 0 {
		t.Fatal("expired release still queued")
	}
	_, replay, err := core.Select(id)
	if err != nil || !proto.Equal(assignment, replay) {
		t.Fatal("lost release changed selection")
	}
}
