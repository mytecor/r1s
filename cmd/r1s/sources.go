package main

import (
	"path/filepath"
	"strings"

	"github.com/mytecor/r1s/internal/cluster"
	"github.com/mytecor/r1s/internal/transport/rns"
)

// clientClusterSource resolves the cluster membership source for the r1s
// client commands, defaulting to the standard cluster state path.
func clientClusterSource(value string) (string, error) {
	if strings.TrimSpace(value) != "" {
		return value, nil
	}
	return cluster.DefaultPath()
}

// identityDataDirectory returns the directory that should own state derived
// from an identity source: beside the identity file for a file-based source,
// or the default cluster directory for an inline identity.
func identityDataDirectory(source string) (string, error) {
	if !rns.IsInlineIdentitySource(source) {
		return filepath.Dir(source), nil
	}
	defaultClusterPath, err := cluster.DefaultPath()
	if err != nil {
		return "", err
	}
	return filepath.Dir(defaultClusterPath), nil
}
