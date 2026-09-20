package allocator

import (
	"context"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/proto"
)

// F16-02: an allocator with declared capabilities rejects an incompatible
// request before reserving capacity or returning an offer, and embeds its node
// in the offer it does return.
func TestAllocatorRejectsIncompatibleRequestWithoutReserving(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator, err := New(Config{
		Identity: []byte("allocator"),
		Capacity: map[string]uint32{"default": 1, "gpu": 1},
		OfferTTL: 30 * time.Second,
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Node: &r1sv1.NodeCapabilities{
			Os: "linux", Arch: "amd64", Runtime: "runc",
			ResourceProfiles: []string{"default"},
			Devices:          []string{"nvidia/tesla"},
			Labels:           map[string]string{"tier": "edge"},
		},
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}

	// Incompatible constraint: arch mismatch. Must be ErrIncompatible and must
	// not reserve the single slot.
	bad := requestEnvelope(clock.Now(), "message-bad", "client", "request-bad")
	bad.GetExecutionRequest().Constraints = &r1sv1.PlacementConstraints{Arch: "arm64"}
	if _, err := allocator.Handle(context.Background(), bad); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("Handle() error = %v, want ErrIncompatible", err)
	}
	if available := allocator.Available("default"); available != 1 {
		t.Fatalf("incompatible request reserved capacity: available=%d", available)
	}
	if runtime.startCount() != 0 {
		t.Fatal("incompatible request started work")
	}

	// Resource class not among the node's declared profiles: ErrAdmission.
	noProfile := requestEnvelope(clock.Now(), "message-noprofile", "client", "request-noprofile")
	noProfile.GetExecutionRequest().ResourceClass = "gpu"
	if _, err := allocator.Handle(context.Background(), noProfile); !errors.Is(err, ErrAdmission) {
		t.Fatalf("Handle() error = %v, want ErrAdmission", err)
	}

	// Compatible request (no constraints) offered last, once the slot is free.
	plain := requestEnvelope(clock.Now(), "message-plain", "client", "request-plain")
	response := mustHandle(t, allocator, plain)
	offer := response.GetExecutionOffer()
	if offer == nil {
		t.Fatal("compatible request returned no offer")
	}
	if offer.GetNode() == nil || offer.GetNode().GetOs() != "linux" {
		t.Fatalf("offer node = %+v, want embedded advertisement", offer.GetNode())
	}
}

// F16-02: assignment re-validates placement against current capabilities.
// A node whose advertisement changed after the offer was minted must reject the
// assignment explicitly rather than start an incompatible execution.
func TestAllocatorRevalidatesPlacementAtAssignment(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator, err := New(Config{
		Identity: []byte("allocator"),
		Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second,
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Node: &r1sv1.NodeCapabilities{
			Os: "linux", Arch: "amd64", Runtime: "runc",
			ResourceProfiles: []string{"default"},
		},
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}

	constrained := requestEnvelope(clock.Now(), "message-req", "client", "request-req")
	constrained.GetExecutionRequest().Constraints = &r1sv1.PlacementConstraints{Runtime: "runc"}
	response := mustHandle(t, allocator, constrained)
	offer := response.GetExecutionOffer()

	// Simulate the node advertising a changed runtime after the offer. The
	// assignment must now fail with ErrIncompatible.
	allocator.mu.Lock()
	allocator.node.Runtime = "wasm"
	allocator.mu.Unlock()

	if _, err := allocator.Handle(context.Background(), assignEnvelope(clock.Now(), "message-assign", "client", "request-req", offer.GetOfferId(), "execution")); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("Handle() error = %v, want ErrIncompatible", err)
	}
	if runtime.startCount() != 0 {
		t.Fatal("incompatible assignment started work")
	}
}

// F16-01/F16-02: an allocator that advertises no node (nil) refuses explicit
// placement constraints — per Config.Node, only empty constraints match an
// unknown node, so the client is never told an unadvertised node matches.
// Unconstrained requests still flow exactly as placement-free behavior did.
func TestAllocatorWithNilNodeRejectsConstraintsOffersOtherwise(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTestAllocator(t, clock, runtime, 2)
	plain := requestEnvelope(clock.Now(), "message-plain", "client", "request-plain")
	plainResponse := mustHandle(t, allocator, plain)
	if plainResponse.GetExecutionOffer() == nil {
		t.Fatal("nil-node allocator rejected an unconstrained request")
	}
	if plainResponse.GetExecutionOffer().GetNode() != nil {
		t.Fatalf("nil-node allocator offered node %+v, want nil", plainResponse.GetExecutionOffer().GetNode())
	}
	if allocator.Node() != nil {
		t.Fatalf("Node() = %+v, want nil", allocator.Node())
	}

	// A second, constrained request must be refused: no evidence to match on.
	request := requestEnvelope(clock.Now(), "message-req", "client", "request-req")
	request.GetExecutionRequest().Constraints = &r1sv1.PlacementConstraints{Os: "linux"}
	if _, err := allocator.Handle(context.Background(), request); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("Handle() error = %v, want ErrIncompatible", err)
	}
}

// F16-02: the offer embeds a cloned node that is stable and independent of the
// allocator's internal copy (a later advertisement update must not mutate a
// previously returned offer).
func TestAllocatorOfferNodeIsIsolatedClone(t *testing.T) {
	clock := newFakeClock()
	node := &r1sv1.NodeCapabilities{Os: "linux", Arch: "amd64", Runtime: "runc", ResourceProfiles: []string{"default"}}
	allocator, err := New(Config{
		Identity: []byte("allocator"),
		Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second,
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Node:     node,
	}, newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	request := requestEnvelope(clock.Now(), "message-req", "client", "request-req")
	response := mustHandle(t, allocator, request)
	offered := response.GetExecutionOffer().GetNode()

	// Mutating the caller's original node and the allocator's live state must
	// not change the already-issued offer.
	node.Os = "windows"
	allocator.mu.Lock()
	allocator.node.Arch = "arm64"
	allocator.mu.Unlock()
	if offered.GetOs() != "linux" || offered.GetArch() != "amd64" {
		t.Fatalf("offer node mutated: %+v", offered)
	}
	if !proto.Equal(offered, &r1sv1.NodeCapabilities{Os: "linux", Arch: "amd64", Runtime: "runc", ResourceProfiles: []string{"default"}}) {
		t.Fatalf("offer node = %+v", offered)
	}
}
