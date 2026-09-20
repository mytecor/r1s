package yggdrasil

import (
	"crypto/ed25519"
	"fmt"
	"io"
	"net/url"

	"github.com/gologme/log"
	"github.com/yggdrasil-network/yggdrasil-go/src/config"
	"github.com/yggdrasil-network/yggdrasil-go/src/core"
)

// Logger is the node diagnostic sink type yggdrasil-go's Core expects.
type Logger = core.Logger

// startCore starts the embedded yggdrasil node over the derived private key.
// The TLS certificate is generated in memory from the same key (yggdrasil-go
// requires an ed25519 tls.Certificate for its node identity); nothing is
// persisted. Peers are joined after startup so a private peer set stays edge
// configuration.
func startCore(key ed25519.PrivateKey, options NodeOptions) (*core.Core, error) {
	if len(key) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("yggdrasil node: derived key has wrong size %d", len(key))
	}
	nodeConfig := config.GenerateConfig()
	nodeConfig.PrivateKey = config.KeyBytes(key)
	if err := nodeConfig.GenerateSelfSignedCertificate(); err != nil {
		return nil, fmt.Errorf("yggdrasil node: build certificate: %w", err)
	}
	logger := options.LogSink
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	nodeCore, err := core.New(nodeConfig.Certificate, logger)
	if err != nil {
		return nil, fmt.Errorf("yggdrasil node: %w", err)
	}
	if err := applyPeers(nodeCore, options.Peers); err != nil {
		nodeCore.Stop()
		return nil, err
	}
	return nodeCore, nil
}

// applyPeers joins the configured bootstrap peers. A malformed peer URI is a
// configuration error; a peer that cannot be reached yet is not: the mesh
// reconnects on its own schedule and a session dial fails fast while no path
// exists.
func applyPeers(nodeCore *core.Core, peers []string) error {
	for _, uri := range peers {
		parsed, err := url.Parse(uri)
		if err != nil {
			return fmt.Errorf("yggdrasil peer %q: %w", uri, err)
		}
		if err := nodeCore.AddPeer(parsed, ""); err != nil {
			return fmt.Errorf("yggdrasil peer %q: %w", uri, err)
		}
	}
	return nil
}
