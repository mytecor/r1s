package logstore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

func TestCaptureBoundedRetentionAndTruncation(t *testing.T) {
	root := t.TempDir()
	store, err := New(root, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	directory, limit, err := store.Reserve("execution")
	if err != nil {
		t.Fatal(err)
	}
	if limit != 16 {
		t.Fatalf("limit = %d, want 16", limit)
	}
	stdout := strings.NewReader("12345678901234567890") // 20 > 16
	stderr := strings.NewReader("short")
	if err := Capture(directory, limit, stdout, stderr, func() error { return nil }); err != nil {
		t.Fatal(err)
	}

	chunk, err := store.Read(context.Background(), "execution", "stdout", 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunk.Data, []byte("1234567890123456")) || !chunk.EOF || !chunk.Truncated {
		t.Fatalf("stdout chunk = %+v, want first 16 bytes truncated", chunk)
	}
	if chunk.NextOffset != 16 {
		t.Fatalf("next offset = %d, want 16", chunk.NextOffset)
	}
	// A bounded noisy stream must not grow past its cap: reading at the cap is EOF.
	chunk, err = store.Read(context.Background(), "execution", "stdout", 16, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(chunk.Data) != 0 || !chunk.EOF {
		t.Fatalf("post-cap read = %+v", chunk)
	}
	stderrChunk, err := store.Read(context.Background(), "execution", "stderr", 0, 16)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stderrChunk.Data, []byte("short")) || stderrChunk.Truncated {
		t.Fatalf("stderr chunk = %+v", stderrChunk)
	}
}

func TestReadOffsetValidationAndRemove(t *testing.T) {
	store, err := New(t.TempDir(), 32, 128)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Reserve("execution"); err != nil {
		t.Fatal(err)
	}
	if err := Capture(store.directory("execution"), 32, strings.NewReader("abcdef"), strings.NewReader(""), func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	// Offsets beyond the retained data are an explicit conflict, not silent truncation.
	if _, err := store.Read(context.Background(), "execution", "stdout", 99, 4); !errors.Is(err, r1sruntime.ErrLogOffset) {
		t.Fatalf("offset beyond file = %v, want ErrLogOffset", err)
	}
	// Read a bounded middle range.
	chunk, err := store.Read(context.Background(), "execution", "stdout", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(chunk.Data, []byte("cde")) || chunk.NextOffset != 5 || chunk.EOF {
		t.Fatalf("intermediate chunk = %+v", chunk)
	}
	// Unknown stream is an explicit conflict.
	if _, err := store.Read(context.Background(), "execution", "combined", 0, 4); !errors.Is(err, r1sruntime.ErrLogOffset) {
		t.Fatalf("unknown stream = %v", err)
	}
	// Removing the reservation makes subsequent reads report the logs as missing.
	if err := store.Remove(context.Background(), "execution"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(context.Background(), "execution", "stdout", 0, 4); !errors.Is(err, r1sruntime.ErrLogsMissing) {
		t.Fatalf("after Remove = %v, want ErrLogsMissing", err)
	}
	if _, err := os.Stat(filepath.Join(store.directory("execution"), "metadata.json")); !os.IsNotExist(err) {
		t.Fatalf("reservation directory not removed: %v", err)
	}
}

func TestReserveAfterRestartKeepsLimitAndBudget(t *testing.T) {
	root := t.TempDir()
	first, err := New(root, 10, 50)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.Reserve("execution-1"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := first.Reserve("execution-2"); err != nil {
		t.Fatal(err)
	}
	// A new store over the same directory sees prior reservations, so an
	// exec-3 reservation that would exceed the aggregate budget is refused.
	second, err := New(root, 10, 50)
	if err != nil {
		t.Fatal(err)
	}
	one := second.directory("execution-1")
	two := second.directory("execution-2")
	if _, limit, err := second.Reserve("execution-1"); err != nil || limit != 10 {
		t.Fatalf("re-reserve limit=%d err=%v", limit, err)
	}
	if _, _, err := second.Reserve("execution-3"); err == nil {
		t.Fatal("budget-exhausted reservation accepted")
	}
	_ = one
	_ = two
}
