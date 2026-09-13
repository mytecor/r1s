package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestUnavailableRuntime(t *testing.T) {
	runtime := Unavailable{}
	if err := runtime.Start(context.Background(), StartRequest{}, nil); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Start() error = %v", err)
	}
	if err := runtime.Stop(context.Background(), "execution"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Stop() error = %v", err)
	}
}
