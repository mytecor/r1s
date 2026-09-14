package allocator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func releaseEnvelope(now time.Time, message, owner, offer string) *r1sv1.Envelope {
	return &r1sv1.Envelope{MessageId: message, Sender: []byte(owner), SentAt: timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionOfferRelease{ExecutionOfferRelease: &r1sv1.ExecutionOfferRelease{RequestId: "request", OfferId: offer}},
	}
}

func TestReleaseAuthorityReplayAndRestart(t *testing.T) {
	clock := newFakeClock()
	store := newMemoryStateStore()
	runtime := newFakeRuntime()
	core := newStoredTestAllocator(t, clock, runtime, store)
	offer := mustHandle(t, core, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	forged := releaseEnvelope(clock.Now(), "forged", "other", offer.GetOfferId())
	if _, err := core.Handle(context.Background(), forged); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized release: %v", err)
	}
	wrong := releaseEnvelope(clock.Now(), "wrong", "client", offer.GetOfferId())
	wrong.GetExecutionOfferRelease().RequestId = "different"
	if _, err := core.Handle(context.Background(), wrong); !errors.Is(err, ErrExecutionConflict) {
		t.Fatalf("wrong request: %v", err)
	}
	if core.Available("default") != 0 {
		t.Fatal("rejected release returned capacity")
	}
	release := releaseEnvelope(clock.Now(), "release", "client", offer.GetOfferId())
	first := mustHandle(t, core, release)
	if first.GetExecutionOfferReleaseAck().GetOutcome() != r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_RELEASED || core.Available("default") != 1 {
		t.Fatalf("release failed: %v", first)
	}
	for _, restored := range []*Allocator{core, newStoredTestAllocator(t, clock, runtime, store)} {
		if replay := mustHandle(t, restored, release); !proto.Equal(first, replay) {
			t.Fatalf("changed replay: %v", replay)
		}
		if _, err := restored.Handle(context.Background(), assignEnvelope(clock.Now(), "assign", "client", "request", offer.GetOfferId(), "execution")); !errors.Is(err, ErrOfferReleased) {
			t.Fatalf("assigned released offer: %v", err)
		}
		if restored.Available("default") != 1 || runtime.startCount() != 0 {
			t.Fatal("release replay changed capacity or started work")
		}
	}
}

func TestReleaseAfterExpiryOrAssignment(t *testing.T) {
	for _, assigned := range []bool{false, true} {
		t.Run(fmt.Sprintf("assigned=%v", assigned), func(t *testing.T) {
			clock := newFakeClock()
			runtime := newFakeRuntime()
			core := newTestAllocator(t, clock, runtime, 1)
			offer := mustHandle(t, core, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
			want := r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_EXPIRED
			if assigned {
				mustHandle(t, core, assignEnvelope(clock.Now(), "assign", "client", "request", offer.GetOfferId(), "execution"))
				want = r1sv1.OfferReleaseOutcome_OFFER_RELEASE_OUTCOME_ASSIGNED
			}
			clock.Advance(time.Minute)
			for i := range 2 {
				ack := mustHandle(t, core, releaseEnvelope(clock.Now(), fmt.Sprint("release-", i), "client", offer.GetOfferId()))
				if ack.GetExecutionOfferReleaseAck().GetOutcome() != want {
					t.Fatalf("ack=%v", ack)
				}
			}
			if assigned {
				if core.Available("default") != 0 || runtime.startCount() != 1 || runtime.stopCount() != 0 {
					t.Fatal("release changed assigned execution")
				}
			} else if core.Available("default") != 1 {
				t.Fatal("expired reservation was not freed")
			}
		})
	}
}

func TestConcurrentReleaseReturnsCapacityOnce(t *testing.T) {
	clock := newFakeClock()
	core := newTestAllocator(t, clock, newFakeRuntime(), 1)
	offer := mustHandle(t, core, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if _, err := core.Handle(context.Background(), releaseEnvelope(clock.Now(), fmt.Sprint("release-", i), "client", offer.GetOfferId())); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	mustHandle(t, core, requestEnvelope(clock.Now(), "next-message", "next-client", "next-request"))
	if core.Available("default") != 0 {
		t.Fatal("concurrent releases returned excess capacity")
	}
}

type releaseFailStore struct {
	*memoryStateStore
	saves  int
	failAt int
}

func (s *releaseFailStore) Save(ctx context.Context, data []byte) error {
	s.saves++
	if s.saves == s.failAt {
		return errors.New("injected store failure")
	}
	return s.memoryStateStore.Save(ctx, data)
}

func TestReleaseStoreFailurePreservesReservation(t *testing.T) {
	clock := newFakeClock()
	store := &releaseFailStore{memoryStateStore: newMemoryStateStore()}
	core := newStoredTestAllocator(t, clock, newFakeRuntime(), store)
	offer := mustHandle(t, core, requestEnvelope(clock.Now(), "request-message", "client", "request")).GetExecutionOffer()
	store.failAt = store.saves + 2 // beginReplay succeeds, release transition fails.
	if _, err := core.Handle(context.Background(), releaseEnvelope(clock.Now(), "release", "client", offer.GetOfferId())); !errors.Is(err, ErrStore) {
		t.Fatalf("release error=%v", err)
	}
	if core.Available("default") != 0 {
		t.Fatal("failed commit freed reservation")
	}
	core = newStoredTestAllocator(t, clock, newFakeRuntime(), store)
	if core.Available("default") != 0 {
		t.Fatal("restart lost reservation")
	}
	mustHandle(t, core, releaseEnvelope(clock.Now(), "retry-with-new-id", "client", offer.GetOfferId()))
	if core.Available("default") != 1 {
		t.Fatal("successful retry did not free reservation")
	}
}
