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
	_, err := registry.Mint("exec-1", []byte("peer-key"), Target{Host: "127.0.0.1", Port: 9000}, Endpoint{Address: []byte("addr"), PubKey: []byte("pub")}, 60*time.Second, *now)
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
}

func TestMintRequiresIDs(t *testing.T) {
	registry, _ := testRegistry(t)
	if _, err := registry.Mint("", []byte("k"), Target{}, Endpoint{}, time.Minute, time.Now()); err == nil {
		t.Fatal("Mint with empty execution ID succeeded")
	}
	if _, err := registry.Mint("exec-1", []byte("k"), Target{}, Endpoint{}, 0, time.Now()); err == nil {
		t.Fatal("Mint with non-positive TTL succeeded")
	}
}

func TestAcceptOpensSessionAndConsumesGrant(t *testing.T) {
	registry, now := testRegistry(t)
	wantExpiry := now.Add(2 * time.Minute)
	grant, err := registry.Mint("exec-1", []byte("peer-key"), Target{Host: "[::1]", Port: 8080}, Endpoint{Address: []byte("addr")}, 2*time.Minute, *now)
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
	if session.ExecutionID != "exec-1" || string(session.PeerKey) != "peer-key" || session.Target.Host != "[::1]" || session.Target.Port != 8080 {
		t.Fatalf("unexpected session: %+v", session)
	}
}

func TestAcceptRejectsReuse(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("second Accept = %v; want ErrSessionBusy", err)
	}
}

func TestAcceptRejectsExpired(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now)
	*now = now.Add(61 * time.Second)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrGrantExpired) {
		t.Fatalf("Accept after expiry = %v; want ErrGrantExpired", err)
	}
	// The grant stays unconsumed and reusable within its TTL; a fresh mint
	// re-arms it for the same execution.
	if _, err := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now); err != nil {
		t.Fatalf("re-mint after expiry: %v", err)
	}
}

func TestAcceptRejectsPeerKeyMismatch(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("pinned-key"), Target{}, Endpoint{}, time.Minute, *now)
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
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", "wrong-grant", []byte("peer-key"), *now); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("Accept with wrong grant = %v; want ErrGrantNotFound", err)
	}
	if _, err := registry.Accept("exec-unknown", grant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrGrantNotFound) {
		t.Fatalf("Accept unknown execution = %v; want ErrGrantNotFound", err)
	}
}

func TestSessionBusyAtAccept(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("first Accept: %v", err)
	}
	// A repeat mint replaces the outstanding grant; the session stays active
	// and a second accept for the same execution is rejected at accept time.
	newGrant, err := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now)
	if err != nil {
		t.Fatalf("re-mint while session active: %v", err)
	}
	if _, err := registry.Accept("exec-1", newGrant.ID, []byte("peer-key"), *now); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("second Accept while session active = %v; want ErrSessionBusy", err)
	}
}

func TestCloseSessionThenRemintIsImmediate(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now)
	if _, err := registry.Accept("exec-1", grant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	registry.CloseSession("exec-1")
	if _, ok := registry.Session("exec-1"); ok {
		t.Fatal("session still active after CloseSession")
	}
	// A re-mint right after a session close is immediate and acceptable.
	newGrant, err := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now)
	if err != nil {
		t.Fatalf("re-mint after session close: %v", err)
	}
	if _, err := registry.Accept("exec-1", newGrant.ID, []byte("peer-key"), *now); err != nil {
		t.Fatalf("Accept after session close and re-mint: %v", err)
	}
}

func TestInvalidateRemovesGrantAndSession(t *testing.T) {
	registry, now := testRegistry(t)
	grant, _ := registry.Mint("exec-1", []byte("peer-key"), Target{}, Endpoint{}, time.Minute, *now)
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
