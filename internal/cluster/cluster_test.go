package cluster

import (
	"bytes"
	"errors"
	"fmt"
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
	directory := filepath.Join(t.TempDir(), "clusters")
	idText, err := SaveCredential(directory, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, idText)
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

func TestDefaultDirectoryUsesCurrentUserConfigDirectory(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	path, err := DefaultDirectory()
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(home, DefaultRelPath)
	if path != want {
		t.Fatalf("DefaultDirectory() = %q, want %q", path, want)
	}
}

func TestCredentialStoreListsAndResolvesUniquePrefixes(t *testing.T) {
	directory := t.TempDir()
	first := bytes.Repeat([]byte{1}, KeySize)
	firstID, err := SaveCredential(directory, first)
	if err != nil {
		t.Fatal(err)
	}
	if duplicateID, err := SaveCredential(directory, first); err != nil || duplicateID != firstID {
		t.Fatalf("idempotent save: %v", err)
	}
	second := bytes.Repeat([]byte{2}, KeySize)
	secondID, err := SaveCredential(directory, second)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := List(directory)
	if err != nil || len(ids) != 2 || ids[0] > ids[1] {
		t.Fatalf("List() = %v, %v", ids, err)
	}
	selector := firstID
	for length := 1; length < len(firstID); length++ {
		if firstID[:length] != secondID[:length] {
			selector = firstID[:length]
			break
		}
	}
	loaded, resolvedID, err := Resolve(directory, selector)
	if err != nil || resolvedID != firstID || !bytes.Equal(loaded, first) {
		t.Fatalf("Resolve(%q) = %x, %q, %v", selector, loaded, resolvedID, err)
	}
	if _, _, err := Resolve(directory, "r1s1:not-a-selector"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("token selector error = %v, want ErrNotFound", err)
	}
	if _, _, err := Resolve(directory, ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty selector error = %v, want ErrNotFound", err)
	}
	info, err := os.Stat(directory)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode = %v, %v", info.Mode().Perm(), err)
	}
	for _, id := range []string{firstID, secondID} {
		info, err := os.Stat(filepath.Join(directory, id))
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("credential %s mode = %v, %v", id, info.Mode().Perm(), err)
		}
	}
}

func TestResolveRejectsAmbiguousPrefixAndIgnoresLegacyFile(t *testing.T) {
	root := t.TempDir()
	directory := filepath.Join(root, "clusters")
	byPrefix := make(map[byte]string)
	for value := byte(1); value != 0; value++ {
		key := bytes.Repeat([]byte{value}, KeySize)
		id, err := SaveCredential(directory, key)
		if err != nil {
			t.Fatal(err)
		}
		prefix := id[0]
		if _, exists := byPrefix[prefix]; exists {
			if _, _, err := Resolve(directory, id[:1]); !errors.Is(err, ErrAmbiguous) {
				t.Fatalf("Resolve(%q) error = %v, want ErrAmbiguous", id[:1], err)
			}
			break
		}
		byPrefix[prefix] = id
	}
	legacyPath := filepath.Join(root, "cluster")
	if err := SaveNew(legacyPath, bytes.Repeat([]byte{0xee}, KeySize)); err != nil {
		t.Fatal(err)
	}
	legacyKey, _ := Load(legacyPath)
	legacyIDBytes, _ := ID(legacyKey)
	legacyID := fmt.Sprintf("%x", legacyIDBytes)
	if _, _, err := Resolve(directory, legacyID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("legacy credential unexpectedly selected: %v", err)
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
