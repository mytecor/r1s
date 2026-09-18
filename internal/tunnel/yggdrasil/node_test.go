package yggdrasil

import (
	"bytes"
	"crypto/ed25519"
	"testing"
)

// TestNodeKeyFromSeedDeterministic verifies the derivation is deterministic:
// the same seed and context yield the same key, and different contexts never
// collide.
func TestNodeKeyFromSeedDeterministic(t *testing.T) {
	seed := []byte("persistent-identity-seed-0123456789abcdef")
	first, err := NodeKeyFromSeed(seed, ClientNodeKeyContext)
	if err != nil {
		t.Fatalf("NodeKeyFromSeed: %v", err)
	}
	second, err := NodeKeyFromSeed(seed, ClientNodeKeyContext)
	if err != nil {
		t.Fatalf("NodeKeyFromSeed reuse: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("derivation is not deterministic")
	}
	if len(first) != ed25519.PrivateKeySize {
		t.Fatalf("derived private key size = %d; want %d", len(first), ed25519.PrivateKeySize)
	}
}

// TestNodeKeyFromSeedContextSeparation verifies client and allocator contexts
// never share a key from the same seed.
func TestNodeKeyFromSeedContextSeparation(t *testing.T) {
	seed := []byte("persistent-identity-seed-0123456789abcdef")
	clientKey, err := NodeKeyFromSeed(seed, ClientNodeKeyContext)
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	allocatorKey, err := NodeKeyFromSeed(seed, AllocatorNodeKeyContext)
	if err != nil {
		t.Fatalf("allocator: %v", err)
	}
	if bytes.Equal(clientKey, allocatorKey) {
		t.Fatal("client and allocator node keys collided")
	}
	if bytes.Equal(clientKey.Public().(ed25519.PublicKey), allocatorKey.Public().(ed25519.PublicKey)) {
		t.Fatal("client and allocator node public keys collided")
	}
}

// TestNodeKeyFromSeedRequiresMaterial verifies an empty seed is rejected.
func TestNodeKeyFromSeedRequiresMaterial(t *testing.T) {
	if _, err := NodeKeyFromSeed(nil, ClientNodeKeyContext); err == nil {
		t.Fatal("derivation with empty seed succeeded")
	}
}

// TestPublicKeySizeWithinContract verifies the derived node public key fits the
// transport-neutral MaxPeerKeySize bound in the grant contract.
func TestPublicKeySizeWithinContract(t *testing.T) {
	seed := []byte("persistent-identity-seed-0123456789abcdef")
	key, err := NodeKeyFromSeed(seed, ClientNodeKeyContext)
	if err != nil {
		t.Fatalf("NodeKeyFromSeed: %v", err)
	}
	if n := len(key.Public().(ed25519.PublicKey)); n > 64 {
		t.Fatalf("derived public key size %d exceeds the 64-byte wire bound", n)
	}
}
