package client

import "errors"

var (
	ErrInvalidConfig      = errors.New("invalid client configuration")
	ErrInvalidAllocator   = errors.New("invalid allocator")
	ErrUnsupportedMessage = errors.New("unsupported client message")
	ErrRequestNotFound    = errors.New("request not found")
	ErrExecutionNotFound  = errors.New("execution not found")
	ErrNoOffer            = errors.New("no usable offer")
	ErrConflict           = errors.New("client state conflict")
	ErrUnauthorized       = errors.New("unauthorized allocator")
)
