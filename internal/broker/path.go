package broker

import (
	"fmt"
	"os"
	"path/filepath"
)

const socketEnvironment = "R1S_SOCKET"

// DefaultAddress returns the per-user authority-broker endpoint. R1S_SOCKET is
// intentionally supported so a mounted broker socket can appear at a stable
// path inside a container without copying cluster credentials into it.
func DefaultAddress() (string, error) {
	if value := os.Getenv(socketEnvironment); value != "" {
		return value, nil
	}
	return platformDefaultAddress()
}

func unixDefaultAddress() (string, error) {
	if runtimeDirectory := os.Getenv("XDG_RUNTIME_DIR"); runtimeDirectory != "" {
		return filepath.Join(runtimeDirectory, "r1s", "cluster.sock"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".local", "state", "r1s", "cluster.sock"), nil
}
