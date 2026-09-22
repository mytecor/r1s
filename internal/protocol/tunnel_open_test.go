package protocol_test

import (
	"errors"
	"strings"
	"testing"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/protocol"
)

func TestValidateTunnelOpen(t *testing.T) {
	valid := &r1sv1.TunnelOpen{ExecutionId: "execution", TargetPort: 8080}
	if err := protocol.ValidateTunnelOpen(valid); err != nil {
		t.Fatalf("valid open rejected: %v", err)
	}
	if err := protocol.ValidateTunnelOpen(nil); !errors.Is(err, protocol.ErrInvalidEnvelope) {
		t.Fatalf("ValidateTunnelOpen(nil) error = %v, want ErrInvalidEnvelope", err)
	}
	tests := map[string]func(*r1sv1.TunnelOpen){
		"missing execution": func(open *r1sv1.TunnelOpen) { open.ExecutionId = "" },
		"zero port":         func(open *r1sv1.TunnelOpen) { open.TargetPort = 0 },
		"overflow port":     func(open *r1sv1.TunnelOpen) { open.TargetPort = 65536 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			open := &r1sv1.TunnelOpen{ExecutionId: "execution", TargetPort: 8080}
			mutate(open)
			if err := protocol.ValidateTunnelOpen(open); !errors.Is(err, protocol.ErrInvalidEnvelope) {
				t.Fatalf("ValidateTunnelOpen() error = %v, want ErrInvalidEnvelope", err)
			}
		})
	}
}

func TestValidateTunnelOpenResult(t *testing.T) {
	ok := &r1sv1.TunnelOpenResult{Ok: true}
	if err := protocol.ValidateTunnelOpenResult(ok); err != nil {
		t.Fatalf("valid ok result rejected: %v", err)
	}
	for _, errValue := range []r1sv1.TunnelOpenError{
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_UNAUTHORIZED,
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_NOT_RUNNING,
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_INVALID_PORT,
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_NO_ENDPOINT,
		r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_MESH_UNREACHABLE,
	} {
		result := &r1sv1.TunnelOpenResult{Ok: false, Error: errValue, Detail: "why"}
		if err := protocol.ValidateTunnelOpenResult(result); err != nil {
			t.Fatalf("valid rejected result for %v rejected: %v", errValue, err)
		}
	}
	// An ok result must not carry an error classification.
	bad := &r1sv1.TunnelOpenResult{Ok: true, Error: r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_UNAUTHORIZED}
	if err := protocol.ValidateTunnelOpenResult(bad); !errors.Is(err, protocol.ErrInvalidEnvelope) {
		t.Fatalf("ok result with error = %v, want ErrInvalidEnvelope", err)
	}
	// A rejected result must carry a known classification.
	if err := protocol.ValidateTunnelOpenResult(&r1sv1.TunnelOpenResult{Ok: false}); !errors.Is(err, protocol.ErrInvalidEnvelope) {
		t.Fatalf("rejected result without error = %v, want ErrInvalidEnvelope", err)
	}
}

func TestClassifyTunnelOpenResult(t *testing.T) {
	status, err := protocol.ClassifyTunnelOpenResult(&r1sv1.TunnelOpenResult{Ok: true})
	if err != nil || !status.OK {
		t.Fatalf("ok classification = (%+v, %v), want OK", status, err)
	}
	status, err = protocol.ClassifyTunnelOpenResult(&r1sv1.TunnelOpenResult{
		Ok: false, Error: r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_UNAUTHORIZED, Detail: "not owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	if status.OK || status.Error != r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_UNAUTHORIZED || status.ErrorDetail != "not owner" {
		t.Fatalf("rejected classification = %+v", status)
	}
	if !strings.Contains(strings.ToLower(status.Message()), "unauthorized") {
		t.Fatalf("classified message = %q, want it to name the error", status.Message())
	}
	if !status.Is(errors.New("any")) {
		t.Fatalf("rejected status must Is(customary error)")
	}
	if _, err := protocol.ClassifyTunnelOpenResult(&r1sv1.TunnelOpenResult{Ok: true, Error: r1sv1.TunnelOpenError_TUNNEL_OPEN_ERROR_UNAUTHORIZED}); err == nil {
		t.Fatal("invalid hybrid result accepted")
	}
}
