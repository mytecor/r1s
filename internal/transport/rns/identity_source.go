package rns

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Quad4-Software/Reticulum-Go/pkg/rnsutil"
)

const privateIdentitySize = 64

// IsInlineIdentitySource reports whether source is encoded private RNS
// identity material rather than a file path. This is r1s command policy, not
// part of the meshbus adapter contract.
func IsInlineIdentitySource(source string) bool {
	if _, err := os.Stat(source); err == nil || !errors.Is(err, os.ErrNotExist) {
		return false
	}
	_, recognized := privateIdentityEncoding(source)
	return recognized
}

// IdentitySeed returns the persistent RNS private identity material used by
// r1s to derive its tunnel identity.
func IdentitySeed(source string) ([]byte, error) {
	loaded, err := rnsutil.LoadIdentity(source)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		encoding, recognized := privateIdentityEncoding(source)
		if recognized {
			loaded, err = rnsutil.ImportPrivateIdentity(strings.TrimSpace(source), encoding)
			if err != nil {
				return nil, fmt.Errorf("invalid encoded private RNS identity: %w", err)
			}
		} else {
			if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
				return nil, err
			}
			loaded, err = rnsutil.GenerateIdentity(source)
			if err != nil {
				return nil, err
			}
		}
	}
	defer loaded.Close()
	return loaded.GetPrivateKey()
}

func privateIdentityEncoding(source string) (rnsutil.Encoding, bool) {
	value := strings.TrimSpace(source)
	for _, encoding := range []rnsutil.Encoding{rnsutil.EncHex, rnsutil.EncBase64, rnsutil.EncBase32} {
		decoded, err := rnsutil.DecodeBytes(value, encoding)
		valid := err == nil && len(decoded) == privateIdentitySize
		for index := range decoded {
			decoded[index] = 0
		}
		if valid {
			return encoding, true
		}
	}
	return 0, false
}
