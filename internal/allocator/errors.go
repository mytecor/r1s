package allocator

import "errors"

var (
	ErrInvalidConfig        = errors.New("invalid allocator configuration")
	ErrUnsupportedMessage   = errors.New("unsupported allocator message")
	ErrCapacityExhausted    = errors.New("allocator capacity exhausted")
	ErrOfferNotFound        = errors.New("offer not found")
	ErrOfferExpired         = errors.New("offer expired")
	ErrOfferAlreadyAssigned = errors.New("offer already assigned")
	ErrExecutionNotFound    = errors.New("execution not found")
	ErrExecutionConflict    = errors.New("execution conflicts with existing execution")
	ErrUnauthorized         = errors.New("sender is not authorized")
	ErrInvalidTransition    = errors.New("invalid execution transition")
	ErrRuntimeStart         = errors.New("runtime start failed")
	ErrRuntimeStop          = errors.New("runtime stop failed")
	ErrIDCollision          = errors.New("generated identifier already exists")
	ErrReplayCapacity       = errors.New("replay capacity exhausted")
	ErrReplayConflict       = errors.New("message ID reused with different content")
	ErrStore                = errors.New("allocator state store failed")
	ErrRecoveryUnsupported  = errors.New("runtime does not support recovery")
)
