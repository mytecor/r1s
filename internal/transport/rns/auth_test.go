package rns

import (
	"bytes"
	"testing"
)

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
