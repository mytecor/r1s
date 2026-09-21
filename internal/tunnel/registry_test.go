package tunnel

import (
	"errors"
	"testing"
	"time"
)

func testRegistry(t *testing.T) (*Registry, *time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	idCounter := 0
	registry, err := NewRegistry(RegistryConfig{
		NewID: func() string { idCounter++; return "grant-" + string(rune('a'+idCounter)) },
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	return registry, &now
}

func TestMintCreatesGrant(t *testing.T) {
	registry, now := testRegistry(t)
	_, err := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 9000}}, Endpoint{Address: []byte("addr"), PubKey: []byte("pub")}, 60*time.Second, *now)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
}

func TestMintRequiresIDs(t *testing.T) {
	registry, _ := testRegistry(t)
	if _, err := registry.Mint("", []byte("k"), []Target{{Port: 1}}, Endpoint{}, time.Minute, time.Now()); err == nil {
		t.Fatal("Mint with empty execution ID succeeded")
	}
	if _, err := registry.Mint("exec-1", []byte("k"), []Target{{Port: 1}}, Endpoint{}, 0, time.Now()); err == nil {
		t.Fatal("Mint with non-positive TTL succeeded")
	}
}

func TestAcceptOpensSessionAndConsumesGrant(t *testing.T) {
	registry, now := testRegistry(t)
	wantExpiry := now.Add(2 * time.Minute)
	grant, err := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 8080}}, Endpoint{Address: []byte("addr")}, 2*time.Minute, *now)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if !grant.ExpiresAt.Equal(wantExpiry) {
		t.Fatalf("expires_at = %v; want %v", grant.ExpiresAt, wantExpiry)
	}
	session, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if session.ExecutionID != "exec-1" || string(session.PeerKey) != "peer-key" || len(session.Targets) != 1 || session.Targets[0].Port != 8080 {
		t.Fatalf("unexpected session: %+v", session)
	}
}

func TestAcceptRejectsReuse(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("second Accept = %v; want ErrSessionBusy", err)
	}
}

func TestAcceptRejectsExpired(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	*now = now.Add(61 * time.Second)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrGrantExpired) {
		t.Fatalf("Accept after expiry = %v; want ErrGrantExpired", err)
	}
	// The grant stays unconsumed and reusable within its TTL; a fresh mint
	// re-arms it for the same execution.
	if _, err := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now); err != nil {
		t.Fatalf("re-mint after expiry: %v", err)
	}
}

func TestAcceptRejectsPeerKeyMismatch(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("pinned-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("other-key"), *now); !errors.Is(err, ErrPeerKeyMismatch) {
		t.Fatalf("Accept with wrong key = %v; want ErrPeerKeyMismatch", err)
	}
	// A rejected accept must leave the grant unconsumed and reusable.
	if _, err := registry.Accept("exec-1", grant.ID, []byte("pinned-key"), *now); err != nil {
		t.Fatalf("Accept after rejection = %v; want success", err)
	}
}

func TestAcceptRejectsWrongGrantAndUnknownExecution(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", "wrong-grant", []byte("peer-key"), *now); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("Accept with wrong grant = %v; want ErrGrantNotFound", err)
	}
	if _, err := registry.Accept("exec-unknown", grant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("Accept unknown execution = %v; want ErrGrantNotFound", err)
	}
}

func TestSessionBusyAtAccept(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	// A repeat mint replaces the outstanding grant; the session stays active
	// and a second accept for the same execution is rejected at accept time.
	newGrant, err := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	if err != nil {
		t.Fatalf("re-mint while session active: %v", err)
	}
	if _, err := registry.Accept("exec-1", newGrant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("second Accept while session active = %v; want ErrSessionBusy", err)
	}
}

func TestCloseSessionThenRemintIsImmediate(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	registry.CloseSession("exec-1")
	if _, ok := registry.Session("exec-1"); ok {
		t.Fatal("session still active after CloseSession")
	}
	// A re-mint right after a session close is immediate and acceptable.
	newGrant, err := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	if err != nil {
		t.Fatalf("re-mint after session close: %v", err)
	}
	if _, err := registry.Accept("exec-1", newGrant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("Accept after session close and re-mint: %v", err)
	}
}

func TestInvalidateRemovesGrantAndSession(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	registry.Invalidate("exec-1")
	if _, ok := registry.Session("exec-1"); ok {
		t.Fatal("session survived invalidation")
	}
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("Accept after invalidation = %v; want ErrGrantNotFound", err)
	}
}

func TestAcceptUnknownSessionNil(t *testing.T) {
	registry, _ := testRegistry(t)
	if _, ok := registry.Session("exec-missing"); ok {
		t.Fatal("Session returned a record for an unknown execution")
	}
}

func TestMintRequiresATargetPort(t *testing.T) {
	registry, now := testRegistry(t)
	if _, err := registry.Mint("exec-1", []byte("k"), nil, Endpoint{}, time.Minute, *now); err == nil {
		t.Fatal("Mint with no target ports succeeded")
	}
	if _, err := registry.Mint("exec-1", []byte("k"), []Target{}, Endpoint{}, time.Minute, *now); err == nil {
		t.Fatal("Mint with an empty target port list succeeded")
	}
}

// TestMultiPortGrantAndResolve verifies a grant can carry several client-owned
// container ports and that each opening stream can reference one by port, with
// an unauthorized port rejected.
func TestMultiPortGrantAndResolve(t *testing.T) {
	registry, now := testRegistry(t)
	ports := []Target{{Port: 2222}, {Port: 8080}, {Port: 9000}}
	grant, err := registry.Mint("exec-1", []byte("peer-key"), ports, Endpoint{Address: []byte("addr")}, time.Minute, *now)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	session, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now)
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if len(session.Targets) != 3 {
		t.Fatalf("session carries %d ports; want 3", len(session.Targets))
	}
	// Resolve by port.
	if target, ok := session.ResolveTarget(2222); !ok || target.Port != 2222 {
		t.Fatalf("resolve 2222 = %+v, %v", target, ok)
	}
	if target, ok := session.ResolveTarget(8080); !ok || target.Port != 8080 {
		t.Fatalf("resolve 8080 = %+v, %v", target, ok)
	}
	// A port that was not pre-authorized is not authorized.
	if _, ok := session.ResolveTarget(3306); ok {
		t.Fatalf("resolve unauthorized port 3306 = %v; want rejected", ok)
	}
}
