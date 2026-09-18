package client

import (
	"context"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// tunnelTestClient builds a client with one running execution owned by the
// allocator "allocator" at destination "destination", mirroring the workflow
// used across the client tests.
func tunnelTestClient(t *testing.T) *Client {
	t.Helper()
	now := time.Unix(1_800_000_000, 0).UTC()
	core, err := New(Config{Identity: []byte("client"), Now: func() time.Time { return now }, NewID: sequenceIDs("request", "request-message", "execution", "assign-message", "grant-message")})
	if err != nil {
		t.Fatal(err)
	}
	registerAllocator(t, core, "allocator", "destination", 1)
	requestID, requestEnvelope, err := core.CreateRequest(testWorkload(), testPolicy(), "default")
	if err != nil {
		t.Fatal(err)
	}
	mustObserveOffer(t, core, now, requestEnvelope, "allocator", "offer")
	_, assignment, err := core.Select(requestID)
	if err != nil {
		t.Fatal(err)
	}
	executionID := assignment.GetExecutionAssign().GetExecutionId()
	if err := core.Handle(context.Background(), stateEnvelope(now.Add(time.Minute), "allocator", executionID, r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING, 0)); err != nil {
		t.Fatal(err)
	}
	return core
}

// TestTunnelGrantMintsForOwner verifies the grant envelope targets the
// execution's allocator, pins the peer key, and validates the wire before any
// send.
func TestTunnelGrantMintsForOwner(t *testing.T) {
	core := tunnelTestClient(t)
	executionID := firstExecution(t, core)

	destination, envelope, err := core.TunnelGrant(executionID, []byte("client-node-pubkey-32-byte-material"))
	if err != nil {
		t.Fatalf("TunnelGrant: %v", err)
	}
	if destination != "destination" {
		t.Fatalf("destination = %q; want destination", destination)
	}
	grant := envelope.GetExecutionTunnelGrant()
	if grant == nil {
		t.Fatal("no tunnel grant payload in envelope")
	}
	if grant.GetExecutionId() != executionID {
		t.Fatalf("grant execution = %q; want %q", grant.GetExecutionId(), executionID)
	}
	if string(grant.GetYggPeerPubkey()) != "client-node-pubkey-32-byte-material" {
		t.Fatalf("pinned peer key = %q", grant.GetYggPeerPubkey())
	}
}

// TestTunnelGrantRejectsBadPeerKey verifies peer-key size validation.
func TestTunnelGrantRejectsBadPeerKey(t *testing.T) {
	core := tunnelTestClient(t)
	executionID := firstExecution(t, core)
	if _, _, err := core.TunnelGrant(executionID, nil); err == nil {
		t.Fatal("TunnelGrant with empty peer key succeeded")
	}
	big := make([]byte, 65)
	if _, _, err := core.TunnelGrant(executionID, big); err == nil {
		t.Fatal("TunnelGrant with oversized peer key succeeded")
	}
}

// TestTunnelGrantAckAuthority verifies the ack is accepted only from the
// execution's allocator; a forged ack is rejected.
func TestTunnelGrantAckAuthority(t *testing.T) {
	core := tunnelTestClient(t)
	executionID := firstExecution(t, core)

	ack := func(sender string) error {
		return core.Handle(context.Background(), &r1sv1.Envelope{
			MessageId: "ack-1", Sender: []byte(sender), SentAt: timestamppb.New(time.Now().UTC()),
			Payload: &r1sv1.Envelope_ExecutionTunnelGrantAck{ExecutionTunnelGrantAck: &r1sv1.ExecutionTunnelGrantAck{
				ExecutionId: executionID, GrantId: "grant-1", ExpiresAt: timestamppb.New(time.Now().Add(time.Minute).UTC()),
			}},
		})
	}
	if err := ack("allocator"); err != nil {
		t.Fatalf("ack from owning allocator: %v", err)
	}
	if err := ack("forged"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("forged ack error = %v; want ErrUnauthorized", err)
	}
}

func firstExecution(t *testing.T, core *Client) string {
	t.Helper()
	for _, snapshot := range core.Executions() {
		return snapshot.ExecutionID
	}
	t.Fatal("no executions recorded")
	return ""
}
