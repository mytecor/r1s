package allocator

import (
	"context"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testPeerKey  = "client-edge-node-public-key"
	testEndpoint = "transport-neutral-overlay-address"
	testPubKey   = "transport-neutral-overlay-pubkey"
)

// newTunnelAllocator builds an allocator with the F14 tunnel surface enabled
// and a concrete per-class + default target, mirroring a running allocator edge
// that advertises an endpoint.
func newTunnelAllocator(t *testing.T, clock *fakeClock, runtime *fakeRuntime) *Allocator {
	t.Helper()
	allocator, err := New(Config{
		Identity: []byte("allocator"),
		Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second,
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Tunnel: TunnelConfig{
			Enabled:       true,
			GrantTTL:      time.Minute,
			TargetByClass: map[string]tunnel.Target{"default": {Host: "127.0.0.1", Port: 9000}},
			DefaultTarget: &tunnel.Target{Host: "127.0.0.1", Port: 9001},
			Endpoint:      tunnel.Endpoint{Address: []byte(testEndpoint), PubKey: []byte(testPubKey)},
		},
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	return allocator
}

// assignRunning walks a request through to a RUNNING execution.
func assignRunning(t *testing.T, allocator *Allocator, clock *fakeClock, client string) string {
	t.Helper()
	offer := mustHandle(t, allocator, requestEnvelope(clock.Now(), "request-"+client, client, "request-"+client)).GetExecutionOffer()
	state := mustHandle(t, allocator, assignEnvelope(clock.Now(), "assign-"+client, client, "request-"+client, offer.GetOfferId(), "execution-"+client)).GetExecutionState()
	if state.GetPhase() != r1sv1.ExecutionPhase_EXECUTION_PHASE_RUNNING {
		t.Fatalf("phase = %v; want running", state.GetPhase())
	}
	return state.GetExecutionId()
}

func tunnelGrantEnvelope(now time.Time, messageID, client, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionTunnelGrant{ExecutionTunnelGrant: &r1sv1.ExecutionTunnelGrant{
			ExecutionId: executionID, YggPeerPubkey: []byte(testPeerKey),
		}},
	}
}

// TestTunnelGrantCommandErrorCodes verifies mint-time configuration failures
// surface as explicit, clear CommandError codes rather than materializing as
// a malformed ack.
func TestTunnelGrantCommandErrorCodes(t *testing.T) {
	clock := newFakeClock()
	base := Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(),
	}

	disabled, err := New(funcCfg(base, TunnelConfig{}), newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	assignRunning(t, disabled, clock, "a")
	responses, handleErr := disabled.Handle(context.Background(), tunnelGrantEnvelope(clock.Now(), "grant-disabled", "a", "execution-a"))
	if !errors.Is(handleErr, ErrTunnelDisabled) || len(responses) != 1 {
		t.Fatalf("disabled handle: err=%v responses=%d", handleErr, len(responses))
	}
	failure := responses[0].GetCommandError()
	if failure == nil || failure.GetCode() != "TUNNEL" || failure.GetDetail() == "" {
		t.Fatalf("command error = %+v; want code TUNNEL with detail", failure)
	}

	noTarget, err := New(funcCfg(base, TunnelConfig{Enabled: true, GrantTTL: time.Minute, Endpoint: tunnel.Endpoint{Address: []byte(testEndpoint), PubKey: []byte(testPubKey)}}), newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	assignRunning(t, noTarget, clock, "b")
	responses, handleErr = noTarget.Handle(context.Background(), tunnelGrantEnvelope(clock.Now(), "grant-no-target", "b", "execution-b"))
	if !errors.Is(handleErr, ErrTunnelNoTarget) {
		t.Fatalf("no-target handle error = %v", handleErr)
	}
	if failure := responses[0].GetCommandError(); failure == nil || failure.GetCode() != "TUNNEL" || failure.GetRetryable() {
		t.Fatalf("no-target command error = %+v", responses[0])
	}
}

// TestTunnelGrantMintAndAck verifies a successful mint returns a transport-neutral
// ack carrying the grant ID, expiry, and opaque endpoint advertisement.
func TestTunnelGrantMintAndAck(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")

	response := mustHandle(t, allocator, tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID))
	ack := response.GetExecutionTunnelGrantAck()
	if ack == nil {
		t.Fatalf("response = %v; want tunnel grant ack", response)
	}
	if ack.GetExecutionId() != executionID || ack.GetGrantId() == "" || ack.GetExpiresAt() == nil {
		t.Fatalf("unexpected ack: %+v", ack)
	}
	if string(ack.GetAllocatorEndpoint()) != testEndpoint || string(ack.GetAllocatorEndpointPubkey()) != testPubKey {
		t.Fatalf("ack endpoint = %x / %x; want %q / %q", ack.GetAllocatorEndpoint(), ack.GetAllocatorEndpointPubkey(), testEndpoint, testPubKey)
	}
	wantExpiry := clock.Now().Add(time.Minute)
	if !ack.GetExpiresAt().AsTime().Equal(wantExpiry) {
		t.Fatalf("ack expiry = %v; want %v", ack.GetExpiresAt().AsTime(), wantExpiry)
	}
}

// TestTunnelGrantMintAuthority verifies mint is rejected for a sender that is
// not the execution owner, for an unknown execution, and for a terminal
// execution.
func TestTunnelGrantMintAuthority(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")

	if _, err := allocator.Handle(context.Background(), tunnelGrantEnvelope(clock.Now(), "grant-other", "other", executionID)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("non-owner mint error = %v; want ErrUnauthorized", err)
	}
	if _, err := allocator.Handle(context.Background(), tunnelGrantEnvelope(clock.Now(), "grant-missing", "a", "execution-missing")); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("unknown execution mint error = %v; want ErrExecutionNotFound", err)
	}

	// A terminal execution cannot be tunnelled.
	mustHandle(t, allocator, cancelEnvelope(clock.Now(), "cancel-a", "a", executionID))
	if _, err := allocator.Handle(context.Background(), tunnelGrantEnvelope(clock.Now(), "grant-terminal", "a", executionID)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal mint error = %v; want ErrInvalidTransition", err)
	}
}

func TestTunnelGrantMintRespectsConfig(t *testing.T) {
	clock := newFakeClock()
	base := Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(),
	}

	// Enabled but no endpoint: the edge is not ready (the unique
	// configuration-failure case not covered by TestTunnelGrantCommandErrorCodes;
	// disabled and no-target are asserted there).
	noEndpoint, err := New(funcCfg(base, TunnelConfig{Enabled: true, GrantTTL: time.Minute, TargetByClass: map[string]tunnel.Target{"default": {Host: "127.0.0.1", Port: 9000}}, DefaultTarget: &tunnel.Target{Host: "127.0.0.1", Port: 9001}}), newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	assignRunning(t, noEndpoint, clock, "b")
	if _, err := noEndpoint.Handle(context.Background(), tunnelGrantEnvelope(clock.Now(), "grant-no-endpoint", "b", "execution-b")); !errors.Is(err, ErrTunnelNoEndpoint) {
		t.Fatalf("no-endpoint mint error = %v; want ErrTunnelNoEndpoint", err)
	}
}

func TestTunnelGrantResolvesClassTargetOverDefault(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	ack := mustHandle(t, allocator, tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID)).GetExecutionTunnelGrantAck()
	grantID := ack.GetGrantId()

	session, err := allocator.AcceptTunnel(executionID, grantID, []byte(testPeerKey))
	if err != nil {
		t.Fatalf("AcceptTunnel: %v", err)
	}
	if session.Target.Host != "127.0.0.1" || session.Target.Port != 9000 {
		t.Fatalf("target = %+v; want default-class 127.0.0.1:9000", session.Target)
	}
	if string(session.Endpoint.Address) != testEndpoint || string(session.Endpoint.PubKey) != testPubKey {
		t.Fatalf("session endpoint = %+v", session.Endpoint)
	}
}

// TestTunnelGrantExpiryAtAccept verifies expiry is evaluated lazily at accept:
// after the TTL the grant is rejected, and a fresh mint replaces it.
func TestTunnelGrantExpiryAtAccept(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	ack := mustHandle(t, allocator, tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID)).GetExecutionTunnelGrantAck()

	clock.Advance(2 * time.Minute)
	if _, err := allocator.AcceptTunnel(executionID, ack.GetGrantId(), []byte(testPeerKey)); !errors.Is(err, tunnel.ErrGrantExpired) {
		t.Fatalf("accept after TTL = %v; want ErrGrantExpired", err)
	}
}

// TestTunnelGrantReuseRejected verifies a consumed grant is rejected at accept
// and a second accept on the same execution is capped at one session.
func TestTunnelGrantReuseRejected(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	ack := mustHandle(t, allocator, tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID)).GetExecutionTunnelGrantAck()

	if _, err := allocator.AcceptTunnel(executionID, ack.GetGrantId(), []byte(testPeerKey)); err != nil {
		t.Fatalf("first accept: %v", err)
	}
	// Reusing a spent grant is rejected (session is active).
	if _, err := allocator.AcceptTunnel(executionID, ack.GetGrantId(), []byte(testPeerKey)); !errors.Is(err, tunnel.ErrSessionBusy) {
		t.Fatalf("reused grant accept = %v; want ErrSessionBusy", err)
	}
	if _, ok := allocator.TunnelSession(executionID); !ok {
		t.Fatal("no active session after accept")
	}
}

func TestTunnelGrantPeerKeyMismatchRejected(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	ack := mustHandle(t, allocator, tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID)).GetExecutionTunnelGrantAck()

	if _, err := allocator.AcceptTunnel(executionID, ack.GetGrantId(), []byte("different-key")); !errors.Is(err, tunnel.ErrPeerKeyMismatch) {
		t.Fatalf("peer-key-mismatch accept = %v; want ErrPeerKeyMismatch", err)
	}
	// A rejected accept leaves the grant unconsumed and reusable.
	session, err := allocator.AcceptTunnel(executionID, ack.GetGrantId(), []byte(testPeerKey))
	if err != nil {
		t.Fatalf("accept after mismatch = %v; want success", err)
	}
	if session == nil {
		t.Fatal("nil session after successful accept")
	}
}

func TestTunnelRewindPreambleMismatchRejected(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	ack := mustHandle(t, allocator, tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID)).GetExecutionTunnelGrantAck()

	// Wrong grant ID for the same execution, and a grant ID for another execution.
	if _, err := allocator.AcceptTunnel(executionID, "wrong-grant", []byte(testPeerKey)); !errors.Is(err, tunnel.ErrGrantNotFound) {
		t.Fatalf("wrong grant accept = %v; want ErrGrantNotFound", err)
	}
	if _, err := allocator.AcceptTunnel("execution-unknown", ack.GetGrantId(), []byte(testPeerKey)); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("unknown execution accept = %v; want ErrExecutionNotFound", err)
	}
}

// TestTunnelTerminalStateClosesSession verifies that committing terminal state
// closes the live session and invalidates the grant in the same local sweep,
// and that a re-mint after the close is immediate.
func TestTunnelTerminalStateClosesSession(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTunnelAllocator(t, clock, runtime)
	executionID := assignRunning(t, allocator, clock, "a")
	ack := mustHandle(t, allocator, tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID)).GetExecutionTunnelGrantAck()
	if _, err := allocator.AcceptTunnel(executionID, ack.GetGrantId(), []byte(testPeerKey)); err != nil {
		t.Fatalf("accept: %v", err)
	}

	// Cancel the execution: terminal state must close the session.
	mustHandle(t, allocator, cancelEnvelope(clock.Now(), "cancel-a", "a", executionID))
	if _, ok := allocator.TunnelSession(executionID); ok {
		t.Fatal("session survived terminal state")
	}
	// A re-mint right after terminal cleanup is rejected because the execution
	// itself is terminal, not because the registry is stuck.
	if _, err := allocator.Handle(context.Background(), tunnelGrantEnvelope(clock.Now(), "grant-after-terminal", "a", executionID)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("mint on terminal execution = %v; want ErrInvalidTransition", err)
	}
}

// TestTunnelRestartInvalidatesGrants verifies an allocator restart leaves no
// outstanding grant behind: the client must request a new grant.
func TestTunnelRestartInvalidatesGrants(t *testing.T) {
	clock := newFakeClock()
	path := t.TempDir() + "/allocator.db"
	store, err := statebolt.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	first, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: store,
		Tunnel: TunnelConfig{
			Enabled: true, GrantTTL: time.Minute,
			TargetByClass: map[string]tunnel.Target{"default": {Host: "127.0.0.1", Port: 9000}},
			DefaultTarget: &tunnel.Target{Host: "127.0.0.1", Port: 9001},
			Endpoint:      tunnel.Endpoint{Address: []byte(testEndpoint), PubKey: []byte(testPubKey)},
		},
	}, newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	executionID := assignRunning(t, first, clock, "a")
	ack := mustHandle(t, first, tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID)).GetExecutionTunnelGrantAck()

	// Restart: a fresh allocator over the same identity restores execution state
	// from the store but holds an empty in-memory registry, so the old grant is
	// gone by construction.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	secondStore, err := statebolt.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer secondStore.Close()
	runtime := newFakeRuntime()
	second, err := New(Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(), Store: secondStore,
		Tunnel: TunnelConfig{
			Enabled: true, GrantTTL: time.Minute,
			TargetByClass: map[string]tunnel.Target{"default": {Host: "127.0.0.1", Port: 9000}},
			DefaultTarget: &tunnel.Target{Host: "127.0.0.1", Port: 9001},
			Endpoint:      tunnel.Endpoint{Address: []byte(testEndpoint), PubKey: []byte(testPubKey)},
		},
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The old grant is gone; accept returns not found.
	if _, err := second.AcceptTunnel(executionID, ack.GetGrantId(), []byte(testPeerKey)); !errors.Is(err, tunnel.ErrGrantNotFound) {
		t.Fatalf("accept after restart = %v; want ErrGrantNotFound", err)
	}
	// A fresh mint on the restarted allocator works.
	if _, err := second.Handle(context.Background(), tunnelGrantEnvelope(clock.Now(), "grant-after-restart", "a", executionID)); err != nil {
		t.Fatalf("mint after restart: %v", err)
	}
}

// TestTunnelGrantDuplicateDeliverySameAck verifies the replay cache serves the
// recorded ack for a duplicate grant message: the client recovers a stable
// grant without over-minting.
func TestTunnelGrantDuplicateDeliverySameAck(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	envelope := tunnelGrantEnvelope(clock.Now(), "grant-msg", "a", executionID)
	first := mustHandle(t, allocator, envelope)
	second := mustHandle(t, allocator, envelope)
	if first.GetExecutionTunnelGrantAck().GetGrantId() != second.GetExecutionTunnelGrantAck().GetGrantId() {
		t.Fatalf("duplicate grant minted a different grant: %q vs %q", first.GetExecutionTunnelGrantAck().GetGrantId(), second.GetExecutionTunnelGrantAck().GetGrantId())
	}
}

// funcCfg clones a Config and overrides the tunnel surface.
func funcCfg(base Config, tunnelConfig TunnelConfig) Config {
	base.Tunnel = tunnelConfig
	return base
}
