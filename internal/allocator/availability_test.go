package allocator

import (
	"context"
	"errors"
	"testing"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

func TestDrainingRejectsNewDemandButPreservesExistingOfferAndLease(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	a := newTestAllocator(t, clock, runtime, 2)
	offer := mustHandle(t, a, requestEnvelope(clock.Now(), "request-1", "client", "request-1")).GetExecutionOffer()
	a.SetDraining(true)
	if _, err := a.Handle(context.Background(), requestEnvelope(clock.Now(), "request-2", "client", "request-2")); !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("new demand while draining: %v", err)
	}
	response := mustHandle(t, a, assignEnvelope(clock.Now(), "assign", "client", "request-1", offer.OfferId, "execution"))
	if response.GetExecutionState().GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatal("draining revoked a reserved offer")
	}
	snapshot, ok := a.Execution("execution")
	if !ok || snapshot.State.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatal("execution lifetime changed")
	}
	available := a.AvailableCapacity()
	available["default"] = 999
	if a.Available("default") != 1 {
		t.Fatal("availability snapshot changed capacity authority")
	}
	a.SetDraining(false)
	mustHandle(t, a, requestEnvelope(clock.Now(), "request-3", "client", "request-3"))
	if a.Available("default") != 0 {
		t.Fatal("ready allocator failed to reserve capacity")
	}
}
