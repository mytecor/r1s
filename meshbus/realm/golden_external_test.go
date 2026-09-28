package realm_test

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/mytecor/meshbus/realm"
)

func TestRealmV1GoldenVector(t *testing.T) {
	key := make([]byte, realm.KeySize)
	nonce := make([]byte, realm.NonceSize)
	for index := range key {
		key[index] = byte(index)
		nonce[index] = byte(index + 32)
	}
	challenger, _ := hex.DecodeString("00112233445566778899aabbccddeeff")
	responder, _ := hex.DecodeString("ffeeddccbbaa99887766554433221100")
	opened, err := realm.Open(realm.Config{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	wantID, _ := hex.DecodeString("274ba59d390f9a2615b1c2c9bf111a84fd126312d3c71036695eac2c93c35f96")
	if got := opened.ID(); !bytes.Equal(got, wantID) {
		t.Fatalf("realm ID = %x, want %x", got, wantID)
	}
	proof, err := opened.Proof(nonce, challenger, responder)
	if err != nil {
		t.Fatal(err)
	}
	wantProof, _ := hex.DecodeString("da3a7a5a0ee5e00f11a9db87e0b558af9fbf3f8ac6e249e87880c7882d3ada61")
	if !bytes.Equal(proof, wantProof) {
		t.Fatalf("proof = %x, want %x", proof, wantProof)
	}
}
