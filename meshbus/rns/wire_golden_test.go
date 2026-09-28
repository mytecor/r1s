package rns

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestAuthV1GoldenVector(t *testing.T) {
	nonce := make([]byte, authNonceSize)
	for index := range nonce {
		nonce[index] = byte(index + 32)
	}
	proof, _ := hex.DecodeString("da3a7a5a0ee5e00f11a9db87e0b558af9fbf3f8ac6e249e87880c7882d3ada61")
	wire, err := (&authMessage{kind: authKindResponse, nonce: nonce, proof: proof}).Pack()
	if err != nil {
		t.Fatal(err)
	}
	want, _ := hex.DecodeString("02202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3fda3a7a5a0ee5e00f11a9db87e0b558af9fbf3f8ac6e249e87880c7882d3ada61")
	if !bytes.Equal(wire, want) {
		t.Fatalf("auth wire = %x, want %x", wire, want)
	}
}

func FuzzAuthMessageUnpack(f *testing.F) {
	seed, _ := hex.DecodeString("02202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3fda3a7a5a0ee5e00f11a9db87e0b558af9fbf3f8ac6e249e87880c7882d3ada61")
	f.Add(seed)
	f.Add([]byte{authKindChallenge})
	f.Fuzz(func(t *testing.T, data []byte) {
		var message authMessage
		_ = message.Unpack(data)
	})
}
