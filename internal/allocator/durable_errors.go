package allocator

import (
	"errors"
	"fmt"
)

// These codes are the stable persistence boundary for classifiable command
// failures. Go error strings remain diagnostic rather than becoming identity.
func classifyError(err error) string {
	for code, sentinel := range durableErrors() {
		if errors.Is(err, sentinel) {
			return code
		}
	}
	return ""
}

func restoreError(code, message string) error {
	if sentinel := durableErrors()[code]; sentinel != nil {
		return fmt.Errorf("%w: %s", sentinel, message)
	}
	return errors.New(message)
}

func durableErrors() map[string]error {
	return map[string]error{
		"result_expired": ErrResultExpired, "command_expired": ErrCommandExpired,
		"admission":           ErrAdmission,
		"offer_released":      ErrOfferReleased,
		"unsupported_message": ErrUnsupportedMessage,
		"capacity_exhausted":  ErrCapacityExhausted,
		"offer_not_found":     ErrOfferNotFound,
		"offer_expired":       ErrOfferExpired,
		"offer_assigned":      ErrOfferAlreadyAssigned,
		"execution_not_found": ErrExecutionNotFound,
		"execution_conflict":  ErrExecutionConflict,
		"unauthorized":        ErrUnauthorized,
		"invalid_transition":  ErrInvalidTransition,
		"runtime_start":       ErrRuntimeStart,
		"runtime_stop":        ErrRuntimeStop,
		"id_collision":        ErrIDCollision,
		"replay_capacity":     ErrReplayCapacity,
		"replay_conflict":     ErrReplayConflict,
		"store":               ErrStore,
	}
}
