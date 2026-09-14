package runtime

import (
	"context"
	"errors"
)

var ErrLogsMissing = errors.New("logs are missing or expired")
var ErrLogOffset = errors.New("log offset is outside retained data")

type LogChunk struct {
	Data           []byte
	NextOffset     uint64
	EOF, Truncated bool
}
type LogStore interface {
	Read(context.Context, string, string, uint64, uint32) (LogChunk, error)
	Remove(context.Context, string) error
}
