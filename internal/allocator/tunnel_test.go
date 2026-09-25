package allocator

import (
	"context"
	"errors"
	"testing"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
	statebolt "github.com/mytecor/r1s/internal/store/bolt"
	"github.com/mytecor/r1s/internal/tunnel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testPeerKey  = "client-edge-node-public-key"
	testEndpoint = "transport-neutral-overlay-address"
	testPubKey   = "transport-neutral-overlay-pubkey"
)

// clientTestTargets is the client-supplied destination container port list used
// by the tunnel open tests. The allocator no longer owns target resolution; it
// binds whatever the client sends, validated only for shape. There are no named
// slots: each target is just the container port to export.
var clientTestTargets = []*r1sv1.TunnelTarget{
	{Port: 2222},
	{Port: 8080},
	{Port: 9000},
}

// newTunnelAllocator builds an allocator with the F14 tunnel surface enabled,
// mirroring a running allocator edge that advertises an endpoint. Targets are
// not configured here: the client supplies them in each open request.
func newTunnelAllocator(t *testing.T, clock *fakeClock, runtime *fakeRuntime) *Allocator {
	t.Helper()
	allocator, err := New(Config{
		Identity: []byte("allocator"),
		Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second,
		Now:      clock.Now,
		NewID:    sequenceIDs(),
		Tunnel: TunnelConfig{
			Enabled:  true,
			Endpoint: tunnel.Endpoint{Address: []byte(testEndpoint), PubKey: []byte(testPubKey)},
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

func tunnelOpenEnvelope(now time.Time, messageID, client, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionTunnelOpen{ExecutionTunnelOpen: &r1sv1.ExecutionTunnelOpen{
			ExecutionId: executionID, YggPeerPubkey: []byte(testPeerKey), Targets: clientTestTargets,
		}},
	}
}

// noTargetOpenEnvelope is an open request carrying no target slots, which the
// allocator must reject as an invalid request.
func noTargetOpenEnvelope(now time.Time, messageID, client, executionID string) *r1sv1.Envelope {
	return &r1sv1.Envelope{
		MessageId: messageID,
		Sender:    []byte(client),
		SentAt:    timestamppb.New(now),
		Payload: &r1sv1.Envelope_ExecutionTunnelOpen{ExecutionTunnelOpen: &r1sv1.ExecutionTunnelOpen{
			ExecutionId: executionID, YggPeerPubkey: []byte(testPeerKey),
		}},
	}
}

// TestTunnelOpenCommandErrorCodes verifies open-time configuration failures
// surface as explicit, clear CommandError codes rather than materializing as
// a malformed ack.
func TestTunnelOpenCommandErrorCodes(t *testing.T) {
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
	responses, handleErr := disabled.Handle(context.Background(), tunnelOpenEnvelope(clock.Now(), "open-disabled", "a", "execution-a"))
	if !errors.Is(handleErr, ErrTunnelDisabled) || len(responses) != 1 {
		t.Fatalf("disabled handle: err=%v responses=%d", handleErr, len(responses))
	}
	failure := responses[0].GetCommandError()
	if failure == nil || failure.GetCode() != "TUNNEL" || failure.GetDetail() == "" {
		t.Fatalf("command error = %+v; want code TUNNEL with detail", failure)
	}

	noTarget, err := New(funcCfg(base, TunnelConfig{Enabled: true, Endpoint: tunnel.Endpoint{Address: []byte(testEndpoint), PubKey: []byte(testPubKey)}}), newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	assignRunning(t, noTarget, clock, "b")
	// An open with no client-supplied targets is an invalid request, not a
	// config failure: the allocator binds the client list, so an empty list is
	// malformed regardless of allocator setup.
	responses, handleErr = noTarget.Handle(context.Background(), noTargetOpenEnvelope(clock.Now(), "open-no-target", "b", "execution-b"))
	if !errors.Is(handleErr, protocol.ErrInvalidEnvelope) {
		t.Fatalf("no-target handle error = %v", handleErr)
	}
	if len(responses) != 0 {
		t.Fatalf("no-target responses = %+v; want none (validation failed before bind)", responses)
	}
}

// TestTunnelOpenBindAndAck verifies a successful open returns a transport-neutral
// ack echoing the endpoint advertisement and the client-supplied port list.
func TestTunnelOpenBindAndAck(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")

	response := mustHandle(t, allocator, tunnelOpenEnvelope(clock.Now(), "open-msg", "a", executionID))
	ack := response.GetExecutionTunnelOpenAck()
	if ack == nil {
		t.Fatalf("response = %v; want tunnel open ack", response)
	}
	if ack.GetExecutionId() != executionID {
		t.Fatalf("ack execution = %q; want %q", ack.GetExecutionId(), executionID)
	}
	if string(ack.GetAllocatorEndpoint()) != testEndpoint || string(ack.GetAllocatorEndpointPubkey()) != testPubKey {
		t.Fatalf("ack endpoint = %x / %x; want %q / %q", ack.GetAllocatorEndpoint(), ack.GetAllocatorEndpointPubkey(), testEndpoint, testPubKey)
	}
	if got := ack.GetTargets(); len(got) != len(clientTestTargets) {
		t.Fatalf("ack echoed %d targets; want %d", len(got), len(clientTestTargets))
	}
}

// TestTunnelOpenAuthority verifies open is rejected for a sender that is not
// the execution owner, for an unknown execution, and for a terminal execution.
func TestTunnelOpenAuthority(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")

	if _, err := allocator.Handle(context.Background(), tunnelOpenEnvelope(clock.Now(), "open-other", "other", executionID)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("non-owner open error = %v; want ErrUnauthorized", err)
	}
	if _, err := allocator.Handle(context.Background(), tunnelOpenEnvelope(clock.Now(), "open-missing", "a", "execution-missing")); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("unknown execution open error = %v; want ErrExecutionNotFound", err)
	}

	// A terminal execution cannot be tunnelled.
	mustHandle(t, allocator, cancelEnvelope(clock.Now(), "cancel-a", "a", executionID))
	if _, err := allocator.Handle(context.Background(), tunnelOpenEnvelope(clock.Now(), "open-terminal", "a", executionID)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal open error = %v; want ErrInvalidTransition", err)
	}
}

func TestTunnelOpenRespectsConfig(t *testing.T) {
	clock := newFakeClock()
	base := Config{
		Identity: []byte("allocator"), Capacity: map[string]uint32{"default": 1},
		OfferTTL: 30 * time.Second, Now: clock.Now, NewID: sequenceIDs(),
	}

	// Enabled but no endpoint: the edge is not ready (the unique
	// configuration-failure case not covered by TestTunnelOpenCommandErrorCodes;
	// disabled and no-target are asserted there).
	noEndpoint, err := New(funcCfg(base, TunnelConfig{Enabled: true}), newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	assignRunning(t, noEndpoint, clock, "b")
	if _, err := noEndpoint.Handle(context.Background(), tunnelOpenEnvelope(clock.Now(), "open-no-endpoint", "b", "execution-b")); !errors.Is(err, ErrTunnelNoEndpoint) {
		t.Fatalf("no-endpoint open error = %v; want ErrTunnelNoEndpoint", err)
	}
}

func TestTunnelOpenResolvesClientSuppliedTargets(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	mustHandle(t, allocator, tunnelOpenEnvelope(clock.Now(), "open-msg", "a", executionID))

	session, err := allocator.OpenTunnelSession(executionID, []byte(testPeerKey))
	if err != nil {
		t.Fatalf("OpenTunnelSession: %v", err)
	}
	// Every client-supplied container port resolves; sessions carry no named
	// slots and no default target.
	if target, ok := session.ResolveTarget(2222); !ok || target.Port != 2222 {
		t.Fatalf("resolve 2222 = %+v, %v; want 2222", target, ok)
	}
	if target, ok := session.ResolveTarget(8080); !ok || target.Port != 8080 {
		t.Fatalf("resolve 8080 = %+v, %v; want 8080", target, ok)
	}
	if target, ok := session.ResolveTarget(9000); !ok || target.Port != 9000 {
		t.Fatalf("resolve 9000 = %+v, %v; want 9000", target, ok)
	}
	// An unauthorized port is rejected.
	if _, ok := session.ResolveTarget(3306); ok {
		t.Fatalf("resolve unauthorized 3306 = %v; want rejected", ok)
	}
	if string(session.Endpoint.Address) != testEndpoint || string(session.Endpoint.PubKey) != testPubKey {
		t.Fatalf("session endpoint = %+v", session.Endpoint)
	}
}

// TestTunnelOpenReopenRejected verifies the per-execution cap of one live
// session is enforced at open.
func TestTunnelOpenReopenRejected(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	mustHandle(t, allocator, tunnelOpenEnvelope(clock.Now(), "open-msg", "a", executionID))

	if _, err := allocator.OpenTunnelSession(executionID, []byte(testPeerKey)); err != nil {
		t.Fatalf("first open: %v", err)
	}
	// A second open against the same execution is rejected while the first
	// session is live.
	if _, err := allocator.OpenTunnelSession(executionID, []byte(testPeerKey)); !errors.Is(err, tunnel.ErrSessionBusy) {
		t.Fatalf("second open = %v; want ErrSessionBusy", err)
	}
	if _, ok := allocator.TunnelSession(executionID); !ok {
		t.Fatal("no active session after open")
	}
}

func TestTunnelOpenPeerKeyMismatchRejected(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	mustHandle(t, allocator, tunnelOpenEnvelope(clock.Now(), "open-msg", "a", executionID))

	if _, err := allocator.OpenTunnelSession(executionID, []byte("different-key")); !errors.Is(err, tunnel.ErrPeerKeyMismatch) {
		t.Fatalf("peer-key-mismatch open = %v; want ErrPeerKeyMismatch", err)
	}
	// A rejected open leaves the binding intact and reusable (it is not
	// single-use, unlike the retired grant token).
	session, err := allocator.OpenTunnelSession(executionID, []byte(testPeerKey))
	if err != nil {
		t.Fatalf("open after mismatch = %v; want success", err)
	}
	if session == nil {
		t.Fatal("nil session after successful open")
	}
}

func TestTunnelOpenNotBoundRejected(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")

	// A session can only be opened for an execution that first declared a
	// binding over the control plane.
	if _, err := allocator.OpenTunnelSession(executionID, []byte(testPeerKey)); !errors.Is(err, tunnel.ErrTunnelNotBound) {
		t.Fatalf("open without bind = %v; want ErrTunnelNotBound", err)
	}
	if _, err := allocator.OpenTunnelSession("execution-unknown", []byte(testPeerKey)); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("unknown execution open = %v; want ErrExecutionNotFound", err)
	}
}

// TestTunnelTerminalStateClosesSession verifies that committing terminal state
// closes the live session in the same local sweep, and that a re-open after the
// close is rejected because the execution itself is terminal.
func TestTunnelTerminalStateClosesSession(t *testing.T) {
	clock := newFakeClock()
	runtime := newFakeRuntime()
	allocator := newTunnelAllocator(t, clock, runtime)
	executionID := assignRunning(t, allocator, clock, "a")
	mustHandle(t, allocator, tunnelOpenEnvelope(clock.Now(), "open-msg", "a", executionID))
	session, err := allocator.OpenTunnelSession(executionID, []byte(testPeerKey))
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	// Cancel the execution: terminal state must close the session.
	mustHandle(t, allocator, cancelEnvelope(clock.Now(), "cancel-a", "a", executionID))
	if _, ok := allocator.TunnelSession(executionID); ok {
		t.Fatal("session survived terminal state")
	}
	select {
	case <-session.Done():
	default:
		t.Fatal("terminal transition did not signal edge revocation")
	}
	// A re-open right after terminal cleanup is rejected because the execution
	// itself is terminal, not because the registry is stuck.
	if _, err := allocator.Handle(context.Background(), tunnelOpenEnvelope(clock.Now(), "open-after-terminal", "a", executionID)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("open on terminal execution = %v; want ErrInvalidTransition", err)
	}
}

// TestTunnelRestartRequiresFreshOpen verifies an allocator restart leaves no
// outstanding binding behind: the client must declare a fresh authenticated
// open.
func TestTunnelRestartRequiresFreshOpen(t *testing.T) {
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
			Enabled:  true,
			Endpoint: tunnel.Endpoint{Address: []byte(testEndpoint), PubKey: []byte(testPubKey)},
		},
	}, newFakeRuntime())
	if err != nil {
		t.Fatal(err)
	}
	executionID := assignRunning(t, first, clock, "a")
	mustHandle(t, first, tunnelOpenEnvelope(clock.Now(), "open-msg", "a", executionID))

	// Restart: a fresh allocator over the same identity restores execution state
	// from the store but holds an empty in-memory registry, so the old binding
	// is gone by construction.
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
			Enabled:  true,
			Endpoint: tunnel.Endpoint{Address: []byte(testEndpoint), PubKey: []byte(testPubKey)},
		},
	}, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The old binding is gone; but the execution is not itself authoritative,
	// so the session open now fails as not bound.
	if _, err := second.OpenTunnelSession(executionID, []byte(testPeerKey)); !errors.Is(err, tunnel.ErrTunnelNotBound) {
		t.Fatalf("open after restart = %v; want ErrTunnelNotBound", err)
	}
	// A fresh authenticated open on the restarted allocator works.
	if _, err := second.Handle(context.Background(), tunnelOpenEnvelope(clock.Now(), "open-after-restart", "a", executionID)); err != nil {
		t.Fatalf("open after restart: %v", err)
	}
}

// TestTunnelOpenDuplicateDeliverySameAck verifies the replay cache serves the
// recorded ack for a duplicate open message: the client recovers a stable ack
// without re-binding.
func TestTunnelOpenDuplicateDeliverySameAck(t *testing.T) {
	clock := newFakeClock()
	allocator := newTunnelAllocator(t, clock, newFakeRuntime())
	executionID := assignRunning(t, allocator, clock, "a")
	envelope := tunnelOpenEnvelope(clock.Now(), "open-msg", "a", executionID)
	first := mustHandle(t, allocator, envelope)
	second := mustHandle(t, allocator, envelope)
	if string(first.GetExecutionTunnelOpenAck().GetAllocatorEndpoint()) != string(second.GetExecutionTunnelOpenAck().GetAllocatorEndpoint()) {
		t.Fatalf("duplicate open returned a different ack: %v vs %v", first, second)
	}
}

// funcCfg clones a Config and overrides the tunnel surface.
func funcCfg(base Config, tunnelConfig TunnelConfig) Config {
	base.Tunnel = tunnelConfig
	return base
}
