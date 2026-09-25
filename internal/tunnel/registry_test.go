package tunnel

import (
	"errors"
	"testing"
)

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	return NewRegistry()
}

func TestBindCreatesRecord(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("exec-1", []byte("peer-key"), []Target{{Port: 9000}}, Endpoint{Address: []byte("addr"), PubKey: []byte("pub")}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
}

func TestBindRequiresFieldst(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("", []byte("k"), []Target{{Port: 1}}, Endpoint{}); err == nil {
		t.Fatal("Bind with empty execution ID succeeded")
	}
	if err := registry.Bind("exec-1", nil, []Target{{Port: 1}}, Endpoint{}); err == nil {
		t.Fatal("Bind with no peer key succeeded")
	}
}

func TestBindRequiresATargetPort(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("exec-1", []byte("k"), nil, Endpoint{}); err == nil {
		t.Fatal("Bind with no target ports succeeded")
	}
	if err := registry.Bind("exec-1", []byte("k"), []Target{}, Endpoint{}); err == nil {
		t.Fatal("Bind with an empty target port list succeeded")
	}
}

func TestBindDoesNotDisturbActiveSession(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("exec-1", []byte("peer-key"), []Target{{Port: 80}}, Endpoint{Address: []byte("addr")}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, err := registry.Open("exec-1", []byte("peer-key")); err != nil {
		t.Fatalf("Open: %v", err)
	}
	// A re-bind replaces the bound key/list without disturbing the session.
	if err := registry.Bind("exec-1", []byte("new-key"), []Target{{Port: 80}, {Port: 443}}, Endpoint{Address: []byte("addr")}); err != nil {
		t.Fatalf("re-bind: %v", err)
	}
	if _, ok := registry.Session("exec-1"); !ok {
		t.Fatal("re-bind closed the active session")
	}
}

func TestOpenOpensSession(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("exec-1", []byte("peer-key"), []Target{{Port: 8080}}, Endpoint{Address: []byte("addr"), PubKey: []byte("pub")}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	session, err := registry.Open("exec-1", []byte("peer-key"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if session.ExecutionID != "exec-1" || string(session.PeerKey) != "peer-key" || len(session.Targets) != 1 || session.Targets[0].Port != 8080 {
		t.Fatalf("unexpected session: %+v", session)
	}
	if string(session.Endpoint.Address) != "addr" || string(session.Endpoint.PubKey) != "pub" {
		t.Fatalf("session endpoint not carried: %+v", session.Endpoint)
	}
}

func TestOpenRejectsUnboundExecution(t *testing.T) {
	registry := testRegistry(t)
	if _, err := registry.Open("exec-missing", []byte("peer-key")); !errors.Is(err, ErrTunnelNotBound) {
		t.Fatalf("Open unknown execution = %v; want ErrTunnelNotBound", err)
	}
}

func TestOpenRejectsPeerKeyMismatch(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("exec-1", []byte("pinned-key"), []Target{{Port: 1}}, Endpoint{}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, err := registry.Open("exec-1", []byte("other-key")); !errors.Is(err, ErrPeerKeyMismatch) {
		t.Fatalf("Open with wrong key = %v; want ErrPeerKeyMismatch", err)
	}
	// A rejected open must leave the binding intact and reusable (it is not
	// single-use, unlike the retired grant token).
	if _, err := registry.Open("exec-1", []byte("pinned-key")); err != nil {
		t.Fatalf("Open after rejection = %v; want success", err)
	}
}

func TestOpenRejectsSecondSession(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, err := registry.Open("exec-1", []byte("peer-key")); err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if _, err := registry.Open("exec-1", []byte("peer-key")); !errors.Is(err, ErrSessionBusy) {
		t.Fatalf("second Open = %v; want ErrSessionBusy", err)
	}
}

func TestCloseSessionThenReopenIsImmediate(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, err := registry.Open("exec-1", []byte("peer-key")); err != nil {
		t.Fatalf("Open: %v", err)
	}
	registry.CloseSession("exec-1")
	if _, ok := registry.Session("exec-1"); ok {
		t.Fatal("session still active after CloseSession")
	}
	// A re-open right after a session close is immediate against the same
	// binding (no TTL, no single-use consumption).
	if _, err := registry.Open("exec-1", []byte("peer-key")); err != nil {
		t.Fatalf("Open after session close: %v", err)
	}
}

func TestInvalidateRemovesBindingAndSession(t *testing.T) {
	registry := testRegistry(t)
	if err := registry.Bind("exec-1", []byte("peer-key"), []Target{{Port: 1}}, Endpoint{}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	if _, err := registry.Open("exec-1", []byte("peer-key")); err != nil {
		t.Fatalf("Open: %v", err)
	}
	registry.Invalidate("exec-1")
	if _, ok := registry.Session("exec-1"); ok {
		t.Fatal("session survived invalidation")
	}
	if _, err := registry.Open("exec-1", []byte("peer-key")); !errors.Is(err, ErrTunnelNotBound) {
		t.Fatalf("Open after invalidation = %v; want ErrTunnelNotBound", err)
	}
}

func TestSessionUnknownNil(t *testing.T) {
	registry := testRegistry(t)
	if _, ok := registry.Session("exec-missing"); ok {
		t.Fatal("Session returned a record for an unknown execution")
	}
}

// TestMultiPortBindingAndResolve verifies a binding can carry several
// client-owned container ports and that each opening stream can reference one
// by port, with an unauthorized port rejected.
func TestMultiPortBindingAndResolve(t *testing.T) {
	registry := testRegistry(t)
	ports := []Target{{Port: 2222}, {Port: 8080}, {Port: 9000}}
	if err := registry.Bind("exec-1", []byte("peer-key"), ports, Endpoint{Address: []byte("addr")}); err != nil {
		t.Fatalf("Bind: %v", err)
	}
	session, err := registry.Open("exec-1", []byte("peer-key"))
	if err != nil {
		t.Fatalf("Open: %v", err)
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

func TestReleaseDoesNotCloseReplacementSession(t *testing.T) {
	registry := testRegistry(t)
	open := func() *Session {
		session, err := registry.Open("e", []byte("k"))
		if err != nil {
			t.Fatal(err)
		}
		return session
	}
	if err := registry.Bind("e", []byte("k"), []Target{{Port: 80}}, Endpoint{}); err != nil {
		t.Fatal(err)
	}
	old := open()
	registry.Release(old)
	select {
	case <-old.Done():
	default:
		t.Fatal("release did not close session")
	}
	replacement := open()
	registry.Release(old)
	if !registry.Active(replacement) {
		t.Fatal("late release removed replacement")
	}
	select {
	case <-replacement.Done():
		t.Fatal("replacement closed")
	default:
	}
}
