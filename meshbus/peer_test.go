package meshbus

import (
	"bytes"
	"errors"
	"testing"
	"time"
)

func testPeer(id byte, route string) Peer {
	peerID, err := NewPeerID([]byte{id, 0x01, 0x02, 0x03})
	if err != nil {
		panic(err)
	}
	return Peer{ID: peerID, Route: route, Hops: 1, LastSeen: time.Unix(0, 0)}
}

func TestPeerDirectoryRememberAndResolve(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{})
	peer := testPeer(0xAA, "route-aa")
	if err := directory.Remember(peer); err != nil {
		t.Fatal(err)
	}
	if got := directory.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
	route, ok := directory.Resolve(peer.ID)
	if !ok || route != "route-aa" {
		t.Fatalf("Resolve = %q, %v, want %q, true", route, ok, "route-aa")
	}
}

// F24-04: a duplicate discovery of the same authenticated identity must update
// the existing peer instead of creating a second entry.
func TestPeerDirectoryUpdateDoesNotDuplicate(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{})
	peer := testPeer(0xBB, "route-1")
	if err := directory.Remember(peer); err != nil {
		t.Fatal(err)
	}
	moved := testPeer(0xBB, "route-2")
	moved.Hops = 5
	if err := directory.Remember(moved); err != nil {
		t.Fatal(err)
	}
	if got := directory.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1 (duplicate must update, not add)", got)
	}
	route, ok := directory.Resolve(peer.ID)
	if !ok || route != "route-2" {
		t.Fatalf("Resolve after update = %q, %v, want %q", route, ok, "route-2")
	}
	stored, ok := directory.Get(peer.ID)
	if !ok || stored.Hops != 5 {
		t.Fatalf("updated hops = %d, want 5", stored.Hops)
	}
}

// F24-04: application metadata must never be used as an authenticated identity.
// A peer record without a valid identity is rejected regardless of its route.
func TestPeerDirectoryRequiresAuthenticatedIdentity(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{})
	// A zero PeerID is not authenticated and must be rejected even with a route.
	if err := directory.Remember(Peer{Route: "untrusted-route"}); !errors.Is(err, ErrInvalidPeer) {
		t.Fatalf("Remember without identity error = %v, want ErrInvalidPeer", err)
	}
	if got := directory.Len(); got != 0 {
		t.Fatalf("Len() = %d, want 0", got)
	}
}

// F24-04: bounded application metadata; a peer cannot grow memory unbounded.
func TestPeerDirectoryMetadataBounds(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{MaxMetadataBytes: 16})
	peer := testPeer(0xCC, "route")
	peer.Metadata = map[string]string{"os": "linux"} // 9 bytes
	if err := directory.Remember(peer); err != nil {
		t.Fatalf("small metadata must be accepted: %v", err)
	}
	overflow := testPeer(0xDD, "route")
	overflow.Metadata = map[string]string{"payload": "0123456789ABCDEF"} // 24 bytes
	if err := directory.Remember(overflow); !errors.Is(err, ErrMetadataLimit) {
		t.Fatalf("over-bound metadata error = %v, want ErrMetadataLimit", err)
	}
	if got := directory.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1 (overflow must not be stored)", got)
	}

	// A tiny metadata bound rejects even small metadata.
	tiny := NewPeerDirectory(DirectoryConfig{MaxMetadataBytes: 1})
	withMeta := testPeer(0xEE, "route")
	withMeta.Metadata = map[string]string{"k": "v"}
	if err := tiny.Remember(withMeta); !errors.Is(err, ErrMetadataLimit) {
		t.Fatalf("tiny-bound metadata error = %v, want ErrMetadataLimit", err)
	}
}

// F24-04: explicit peer count bounds; the directory rejects new identities once
// at capacity (updates to known identities remain allowed).
func TestPeerDirectoryPeerLimit(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{MaxPeers: 2})
	for id := byte(0); id < 2; id++ {
		if err := directory.Remember(testPeer(id, "r")); err != nil {
			t.Fatalf("Remember %d: %v", id, err)
		}
	}
	third := testPeer(0x02, "r")
	if err := directory.Remember(third); !errors.Is(err, ErrPeerLimit) {
		t.Fatalf("over-capacity error = %v, want ErrPeerLimit", err)
	}
	// Updating a known identity is allowed at capacity.
	known := testPeer(0x00, "updated")
	if err := directory.Remember(known); err != nil {
		t.Fatalf("update of known identity at capacity failed: %v", err)
	}
	if got := directory.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

// F24-04: stale peers can be expired deterministically based on LastSeen.
func TestPeerDirectoryExpireStale(t *testing.T) {
	base := time.Unix(1000, 0)
	now := base
	directory := NewPeerDirectory(DirectoryConfig{now: func() time.Time { return now }})

	if err := directory.Remember(testPeer(0x01, "r1")); err != nil {
		t.Fatal(err)
	}
	now = base.Add(5 * time.Second)
	if err := directory.Remember(testPeer(0x02, "r2")); err != nil {
		t.Fatal(err)
	}
	// Re-discover peer 1 well after peer 2's discovery; peer 2 stays old.
	now = base.Add(8 * time.Second)
	if err := directory.Remember(testPeer(0x01, "r1-refreshed")); err != nil {
		t.Fatal(err)
	}
	now = base.Add(10 * time.Second)
	removed := directory.ExpireStale(3 * time.Second) // cutoff = 7s
	if removed != 1 {
		t.Fatalf("ExpireStale removed %d, want 1", removed)
	}
	if _, ok := directory.Resolve(testPeer(0x02, "").ID); ok {
		t.Fatal("stale peer 2 must be expired")
	}
	if route, ok := directory.Resolve(testPeer(0x01, "").ID); !ok || route != "r1-refreshed" {
		t.Fatalf("refreshed peer 1 must survive: %q, %v", route, ok)
	}
}

// F24-04: peer snapshots are immutable and copy-safe — mutating the returned
// records or metadata must not affect the directory.
func TestPeerDirectorySnapshotCopySafety(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{MaxMetadataBytes: 64})
	peer := testPeer(0x01, "route")
	peer.Metadata = map[string]string{"os": "linux"}
	if err := directory.Remember(peer); err != nil {
		t.Fatal(err)
	}

	snapshot := directory.Peers()
	if len(snapshot) != 1 {
		t.Fatalf("Peers() len = %d, want 1", len(snapshot))
	}
	// Mutate the returned record and its metadata.
	snapshot[0].Route = "hijacked"
	snapshot[0].Metadata["os"] = "hijacked"
	snapshot[0].Hops = 99

	// The directory must be unaffected.
	route, ok := directory.Resolve(peer.ID)
	if !ok || route != "route" {
		t.Fatalf("directory route mutated to %q", route)
	}
	stored, _ := directory.Get(peer.ID)
	if stored.Metadata["os"] != "linux" || stored.Hops != 1 {
		t.Fatalf("directory metadata/hops mutated: %+v", stored)
	}

	// Mutating the caller's metadata before Remember must not leak in either.
	caller := testPeer(0x02, "r2")
	caller.Metadata = map[string]string{"k": "v"}
	if err := directory.Remember(caller); err != nil {
		t.Fatal(err)
	}
	caller.Metadata["k"] = "mutated-after"
	if next, _ := directory.Get(caller.ID); next.Metadata["k"] != "v" {
		t.Fatalf("caller metadata leaked into directory: %+v", next.Metadata)
	}
}

// F24-04: deterministic snapshots — Peers() order is stable across calls.
func TestPeerDirectoryDeterministicSnapshot(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{})
	for id := byte(0); id < 8; id++ {
		if err := directory.Remember(testPeer(id, "r")); err != nil {
			t.Fatal(err)
		}
	}
	first := directory.Peers()
	second := directory.Peers()
	if len(first) != len(second) {
		t.Fatalf("snapshot lengths differ: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if !bytes.Equal(first[i].ID.Bytes(), second[i].ID.Bytes()) {
			t.Fatalf("snapshot order not deterministic at index %d", i)
		}
	}
}

// F24-04: Routes() feeds the Bus PeerSource fan-out without r1s types.
func TestPeerDirectoryRoutesFeedBus(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{})
	for id, route := range map[byte]string{0x01: "dest-a", 0x02: "dest-b"} {
		if err := directory.Remember(testPeer(id, route)); err != nil {
			t.Fatal(err)
		}
	}
	source := PeerSourceFunc(directory.Routes)
	got := source.Peers()
	if len(got) != 2 {
		t.Fatalf("Routes() len = %d, want 2", len(got))
	}
}

// F24-04: a peer that ages past its record can be removed explicitly.
func TestPeerDirectoryRemove(t *testing.T) {
	directory := NewPeerDirectory(DirectoryConfig{})
	peer := testPeer(0x01, "route")
	if err := directory.Remember(peer); err != nil {
		t.Fatal(err)
	}
	directory.Remove(peer.ID)
	if got := directory.Len(); got != 0 {
		t.Fatalf("Len() after Remove = %d, want 0", got)
	}
	directory.Remove(peer.ID) // no-op on unknown peer
	if got := directory.Len(); got != 0 {
		t.Fatalf("Len() after double Remove = %d, want 0", got)
	}
}
