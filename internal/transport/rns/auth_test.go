package rns

import (
	"bytes"
	"testing"
)

func TestAuthProofBindsNonceAndBothIdentities(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	nonce := bytes.Repeat([]byte{2}, authNonceSize)
	challenger := bytes.Repeat([]byte{3}, 16)
	responder := bytes.Repeat([]byte{4}, 16)
	proof := authProof(key, nonce, challenger, responder)
	if !bytes.Equal(proof, authProof(key, nonce, challenger, responder)) {
		t.Fatal("proof is not deterministic")
	}
	if bytes.Equal(proof, authProof(key, nonce, responder, challenger)) {
		t.Fatal("proof does not bind identity roles")
	}
	otherNonce := bytes.Clone(nonce)
	otherNonce[0]++
	if bytes.Equal(proof, authProof(key, otherNonce, challenger, responder)) {
		t.Fatal("proof does not bind nonce")
	}
}

func TestAuthMessageRoundTrip(t *testing.T) {
	want := &authMessage{kind: authKindResponse, nonce: bytes.Repeat([]byte{2}, authNonceSize), proof: bytes.Repeat([]byte{3}, authProofSize)}
	data, err := want.Pack()
	if err != nil {
		t.Fatal(err)
	}
	var got authMessage
	if err := got.Unpack(data); err != nil {
		t.Fatal(err)
	}
	if got.kind != want.kind || !bytes.Equal(got.nonce, want.nonce) || !bytes.Equal(got.proof, want.proof) {
		t.Fatalf("unpacked = %+v", got)
	}
}
