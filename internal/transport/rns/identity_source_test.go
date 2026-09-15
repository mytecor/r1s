package rns

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"quad4/reticulum-go/pkg/identity"
	"quad4/reticulum-go/pkg/rnsutil"
)

func TestLoadOrCreateIdentityAcceptsRNSPrivateKeyEncodings(t *testing.T) {
	original, err := identity.New()
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	privateKey, err := original.GetPrivateKey()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for index := range privateKey {
			privateKey[index] = 0
		}
	}()

	sources := map[string]string{
		"hex":    rnsutil.EncodeBytes(privateKey, rnsutil.EncHex),
		"base32": rnsutil.EncodeBytes(privateKey, rnsutil.EncBase32),
		"base64": rnsutil.EncodeBytes(privateKey, rnsutil.EncBase64),
	}
	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			if !IsInlineIdentitySource(source) {
				t.Fatal("encoded private identity was classified as a path")
			}
			loaded, err := loadOrCreateIdentity(source)
			if err != nil {
				t.Fatal(err)
			}
			defer loaded.Close()
			if !bytes.Equal(loaded.Hash(), original.Hash()) {
				t.Fatalf("identity hash = %x, want %x", loaded.Hash(), original.Hash())
			}
		})
	}
}

func TestLoadOrCreateIdentityPreservesFileBehavior(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "identity")
	created, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	wantHash := bytes.Clone(created.Hash())
	created.Close()
	if IsInlineIdentitySource(path) {
		t.Fatal("identity file was classified as inline data")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != privateIdentitySize {
		t.Fatalf("identity file size = %d, want %d", info.Size(), privateIdentitySize)
	}

	loaded, err := loadOrCreateIdentity(path)
	if err != nil {
		t.Fatal(err)
	}
	defer loaded.Close()
	if !bytes.Equal(loaded.Hash(), wantHash) {
		t.Fatalf("reloaded identity hash = %x, want %x", loaded.Hash(), wantHash)
	}
}

func TestLongNonIdentityValueRemainsAFilePath(t *testing.T) {
	source := filepath.Join(t.TempDir(), strings.Repeat("z", privateIdentitySize*2))
	if IsInlineIdentitySource(source) {
		t.Fatal("long path was classified as an encoded private identity")
	}
	loaded, err := loadOrCreateIdentity(source)
	if err != nil {
		t.Fatal(err)
	}
	loaded.Close()
}
