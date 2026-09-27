//go:build !windows

package broker

import (
	"errors"
	"net"
	"os"
	"path/filepath"
)

func platformDefaultAddress() (string, error) { return unixDefaultAddress() }

func listen(address string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(address), 0o700); err != nil {
		return nil, err
	}
	if _, err := os.Lstat(address); err == nil {
		connection, dialErr := net.Dial("unix", address)
		if dialErr == nil {
			_ = connection.Close()
			return nil, errors.New("broker socket is already active")
		}
		if err := os.Remove(address); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", address)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(address, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(address)
		return nil, err
	}
	return listener, nil
}

func dial(address string) (net.Conn, error) { return net.Dial("unix", address) }

func removeAddress(address string) { _ = os.Remove(address) }
