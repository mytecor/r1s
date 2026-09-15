package cluster

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestTokenIDAndStateRoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte{0x42}, KeySize)
	token, err := Token(key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseToken(token)
	if err != nil || !bytes.Equal(parsed, key) {
		t.Fatalf("ParseToken() = %x, %v", parsed, err)
	}
	firstID, err := ID(key)
	if err != nil {
		t.Fatal(err)
	}
	secondID, _ := ID(parsed)
	if !bytes.Equal(firstID, secondID) || bytes.Equal(firstID, key) {
		t.Fatal("cluster ID is not a stable domain-separated derivation")
	}
	path := filepath.Join(t.TempDir(), "state", "cluster")
	if err := SaveNew(path, key); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(path)
	if err != nil || !bytes.Equal(loaded, key) {
		t.Fatalf("Load() = %x, %v", loaded, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %o, want 600", info.Mode().Perm())
	}
}

func TestGenerateProducesKeySizedRandomMaterial(t *testing.T) {
	first, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	second, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != KeySize || len(second) != KeySize || bytes.Equal(first, second) {
		t.Fatal("generated keys are missing independent 256-bit random material")
	}
}

func TestStateCannotBeSilentlyReplaced(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster")
	first := bytes.Repeat([]byte{1}, KeySize)
	if err := SaveNew(path, first); err != nil {
		t.Fatal(err)
	}
	if err := SaveNew(path, first); err != nil {
		t.Fatalf("idempotent save: %v", err)
	}
	if err := SaveNew(path, bytes.Repeat([]byte{2}, KeySize)); !errors.Is(err, ErrStateExists) {
		t.Fatalf("replacement error = %v", err)
	}
}

func TestRejectsInvalidTokensAndStates(t *testing.T) {
	for _, value := range []string{"", "r1s1:", "r1s1:not-base64", "other:abcd"} {
		if _, err := ParseToken(value); !errors.Is(err, ErrInvalidToken) {
			t.Errorf("ParseToken(%q) error = %v", value, err)
		}
	}
	path := filepath.Join(t.TempDir(), "cluster")
	if err := os.WriteFile(path, []byte(`{"version":2,"key":"r1s1:bad"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Load() error = %v", err)
	}
	if err := os.WriteFile(path, []byte(`{"version":1,"key":"r1s1:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"} trailing`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("Load() trailing-data error = %v", err)
	}
}
