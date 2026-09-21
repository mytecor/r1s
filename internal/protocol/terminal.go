// Package protocol defines message validation, compatibility, and the
// protocol-level domain vocabulary shared by the client, the allocator, and
// the local API. Everything here is transport-independent core.
package protocol

import r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"

// Terminal reports whether an execution phase ends the execution's lifetime:
// no further lease renewals, no further commands, and retained metadata only.
func Terminal(phase r1sv1.ExecutionPhase) bool {
	return phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_CANCELLED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED || phase == r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED
}
