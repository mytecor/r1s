// Package rns holds the private tunnel Reticulum data plane (F21-01). It is a
// deliberately minimal, self-contained transport: a separate Reticulum-Go
// stack that runs over the system yggdrasil service only as an IP/TCP underlay,
// never over the control-plane RNS. The main control RNS is used exclusively
// for the control plane.
//
// The package does not reuse internal/transport/rns (whose endpoint is tuned
// for the control plane: announces, allocator discovery, protobuf Envelope,
// cluster HMAC challenge, connection registry). It is a fresh implementation
// that holds Link / Channel / Buffer as the only tunnel data primitives and
// enforces the "one local TCP connection = one RNS Link = one tunnel stream"
// model.
package rns

import (
	"crypto/hkdf"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/Quad4-Software/Reticulum-Go/pkg/identity"
	"github.com/Quad4-Software/Reticulum-Go/pkg/rnsutil"
)

// privateIdentitySize is the byte length of an RNS private identity, used to
// recognise inline encoded identities. It mirrors the control-plane transport.
const privateIdentitySize = 64

// Tunnel app name and aspect used to derive the tunnel destination hash. Both
// edges agree on these so the allocator's tunnel destination is a pure function
// of the persistent identity; the destination hash is what the control plane
// advertises (F21-02) and what the client dials.
const (
	tunnelAppName = "r1s"
	tunnelAspect  = "tunnel"
)

// loadOrCreateIdentity loads the persistent RNS identity from an existing file
// or an inline encoded private identity, creating a new identity file if the
// source path does not exist. This mirrors the control-plane identity source
// handling but is kept local so the tunnel package never imports
// internal/transport/rns. The returned identity MUST be Closed by the caller.
func loadOrCreateIdentity(source string) (*identity.Identity, error) {
	loaded, err := rnsutil.LoadIdentity(source)
	if err == nil {
		return loaded, nil
	}
	if !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "no such file") {
		return nil, err
	}

	loaded, recognized, importErr := importPrivateIdentity(source)
	if recognized {
		return loaded, importErr
	}

	if err := os.MkdirAll(filepath.Dir(source), 0o700); err != nil {
		return nil, err
	}
	return rnsutil.GenerateIdentity(source)
}

// importPrivateIdentity decodes an inline encoded private identity. The bool
// distinguishes an encoded candidate from an ordinary path.
func importPrivateIdentity(source string) (*identity.Identity, bool, error) {
	encoding, recognized := privateIdentityEncoding(source)
	if !recognized {
		return nil, false, nil
	}
	loaded, err := rnsutil.ImportPrivateIdentity(strings.TrimSpace(source), encoding)
	if err != nil {
		return nil, true, fmt.Errorf("invalid encoded private RNS identity: %w", err)
	}
	return loaded, true, nil
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

// ifacNetworkName and ifacHKDFContext are the shared-cluster IFAC derivation
// parameters. The IFAC only proves early cluster membership on the underlay
// (a weak gate that both edges must already share the cluster secret to pass);
// it is never a substitute for owner authorization, which the allocator
// performs from the identified remote RNS identity at Open time (F21-03).
const (
	ifacNetworkName = "r1s-tunnel"
	ifacHKDFContext = "r1s/tunnel-ifac/v1"
)

// deriveIFACPassphrase derives the Backbone/TCP interface passphrase from the
// cluster secret via HKDF-SHA256. The derivation is deterministic, so all
// cluster members derive the same Interface Access Code and only members can
// create the private tunnel underlay. The salt is nil: the cluster secret is
// already shared-secret entropy.
func deriveIFACPassphrase(clusterKey []byte) ([]byte, error) {
	if len(clusterKey) == 0 {
		return nil, fmt.Errorf("tunnel RNS stack: cluster key is required to derive the interface access code")
	}
	passphrase, err := hkdf.Key(sha256.New, clusterKey, nil, ifacHKDFContext, 32)
	if err != nil {
		return nil, fmt.Errorf("derive tunnel IFAC passphrase: %w", err)
	}
	return passphrase, nil
}
