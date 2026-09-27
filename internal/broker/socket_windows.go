//go:build windows

package broker

import (
	"net"
	"time"

	"github.com/Microsoft/go-winio"
)

func platformDefaultAddress() (string, error) { return `\\.\pipe\r1s-cluster`, nil }

func listen(address string) (net.Listener, error) {
	return winio.ListenPipe(address, &winio.PipeConfig{SecurityDescriptor: "D:P(A;;GA;;;OW)"})
}

func dial(address string) (net.Conn, error) {
	return winio.DialPipe(address, ptrDuration(5*time.Second))
}

func ptrDuration(value time.Duration) *time.Duration { return &value }

func removeAddress(string) {}
