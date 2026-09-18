package main

import (
	"testing"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// The waiter registry replaces the shared consume-and-drop channel for inbound
// control-plane replies. The property these tests lock in is exactly the race
// the old design had: several concurrent awaiters must never steal or drop each
// other's envelopes. Each waiter receives only the envelope whose correlation ID
// it registered for, and dispatch is safe (non-blocking) for unmatched or
// already-cancelled waiters.

func TestWaiterRegistryRoutesByCorrelation(t *testing.T) {
	a := &application{}
	chA, cancelA := a.registerWaiter("corr-A")
	defer cancelA()
	chB, cancelB := a.registerWaiter("corr-B")
	defer cancelB()

	// A corr-B reply must reach only the corr-B waiter, not the concurrently
	// waiting corr-A one.
	a.dispatchEnvelope(&r1sv1.Envelope{CorrelationId: "corr-B", MessageId: "m-B"})

	select {
	case env := <-chB:
		if env.GetMessageId() != "m-B" {
			t.Fatalf("delivered wrong envelope to corr-B waiter: %s", env.GetMessageId())
		}
	default:
		t.Fatal("registered waiter for corr-B did not receive its envelope")
	}

	// The corr-A waiter must not have stolen the corr-B reply.
	select {
	case env := <-chA:
		t.Fatalf("corr-A waiter stole a corr-B envelope: %s", env.GetMessageId())
	default:
	}
}

func TestWaiterRegistryDeliversEachCorrelationOnce(t *testing.T) {
	a := &application{}
	ch1, cancel1 := a.registerWaiter("corr")
	defer cancel1()
	ch2, cancel2 := a.registerWaiter("corr")
	defer cancel2()

	a.dispatchEnvelope(&r1sv1.Envelope{CorrelationId: "corr", MessageId: "m"})

	got := 0
	for _, ch := range []chan *r1sv1.Envelope{ch1, ch2} {
		if env := <-ch; env != nil && env.GetMessageId() == "m" {
			got++
		}
	}
	if got != 2 {
		t.Fatalf("each waiter should receive the envelope; got %d deliveries", got)
	}
}

func TestWaiterRegistryCancelUnregisters(t *testing.T) {
	a := &application{}
	ch, cancel := a.registerWaiter("corr")
	cancel()

	// After cancellation the waiter is unregistered: dispatch must not deliver
	// to (or block on) it.
	a.dispatchEnvelope(&r1sv1.Envelope{CorrelationId: "corr", MessageId: "m"})
	select {
	case env := <-ch:
		if env != nil {
			t.Fatalf("cancelled waiter received an envelope: %s", env.GetMessageId())
		}
	default:
	}
}

func TestWaiterRegistryDispatchUnmatchedIsSafe(t *testing.T) {
	a := &application{}
	// No waiter registered for these: dispatch must not panic or block.
	a.dispatchEnvelope(&r1sv1.Envelope{CorrelationId: "corr", MessageId: "m"})
	a.dispatchEnvelope(&r1sv1.Envelope{MessageId: "no-corr"})
}
