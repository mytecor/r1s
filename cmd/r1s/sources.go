package main

import (
	"path/filepath"

	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
)

// identityDataDirectory returns the directory that should own state derived
// from an identity source: beside the identity file for a file-based source,
// or the default cluster directory for an inline identity.
func identityDataDirectory(source string) (string, error) {
	if !rns.IsInlineIdentitySource(source) {
		return filepath.Dir(source), nil
	}
	defaultClusterDirectory, err := cluster.DefaultDirectory()
	if err != nil {
		return "", err
	}
	return filepath.Dir(defaultClusterDirectory), nil
}
