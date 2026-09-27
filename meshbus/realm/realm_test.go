package realm

import (
	"bytes"
	"errors"
	"testing"
)

func TestRealmDerivesIDAndBindsMembershipProof(t *testing.T) {
	key := bytes.Repeat([]byte{1}, KeySize)
	opened, err := Open(Config{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(opened.ID(), key) || len(opened.ID()) != 32 {
		t.Fatal("realm ID is not a public domain-separated digest")
	}

	nonce := bytes.Repeat([]byte{2}, NonceSize)
	challenger := bytes.Repeat([]byte{3}, 16)
	responder := bytes.Repeat([]byte{4}, 16)
	proof, err := opened.Proof(nonce, challenger, responder)
	if err != nil {
		t.Fatal(err)
	}
	if !opened.Verify(proof, nonce, challenger, responder) {
		t.Fatal("valid membership proof was rejected")
	}
	if opened.Verify(proof, nonce, responder, challenger) {
		t.Fatal("proof does not bind peer identity roles")
	}
	otherNonce := bytes.Clone(nonce)
	otherNonce[0]++
	if opened.Verify(proof, otherNonce, challenger, responder) {
		t.Fatal("proof does not bind the challenge nonce")
	}
}

func TestExplicitDomainsPreserveExistingProtocols(t *testing.T) {
	key := bytes.Repeat([]byte{1}, KeySize)
	first, err := Open(Config{Key: key, IDDomain: "existing-id-v1", AuthenticationDomain: "existing-auth-v1"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Open(Config{Key: key, IDDomain: "existing-id-v1", AuthenticationDomain: "existing-auth-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.ID(), second.ID()) {
		t.Fatal("explicit realm domains are not deterministic")
	}
}

func TestRealmRejectsInvalidKeyAndProofInputs(t *testing.T) {
	if _, err := Open(Config{Key: []byte("short")}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("Open() error = %v, want ErrInvalidKey", err)
	}
	opened, err := Open(Config{Key: bytes.Repeat([]byte{1}, KeySize)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := opened.Proof([]byte("short"), []byte{1}, []byte{2}); !errors.Is(err, ErrInvalidProof) {
		t.Fatalf("Proof() error = %v, want ErrInvalidProof", err)
	}
}

func TestGenerateKeyProducesIndependentMaterial(t *testing.T) {
	first, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	second, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != KeySize || len(second) != KeySize || bytes.Equal(first, second) {
		t.Fatal("generated realm keys are not independent 256-bit values")
	}
}
