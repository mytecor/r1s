package bolt

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestStorePersistsCommittedSnapshot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "allocator.db")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(state) != "second" {
		t.Fatalf("state = %q, want second", state)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if permissions := info.Mode().Perm(); permissions&0o077 != 0 {
		t.Fatalf("database permissions = %o, want mode 0600", permissions)
	}
}

func TestStoreHonorsCancelledContext(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "allocator.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Save(ctx, []byte("state")); err == nil {
		t.Fatal("Save() succeeded with cancelled context")
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("Load() succeeded with cancelled context")
	}
}
