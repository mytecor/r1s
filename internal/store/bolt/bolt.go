// Package bolt provides a local atomic allocator state store backed by bbolt.
package bolt

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	goBolt "go.etcd.io/bbolt"
)

var (
	bucketName = []byte("allocator")
	stateKey   = []byte("state")
)

// Store owns one bbolt database handle.
type Store struct {
	database *goBolt.DB
}

// Open creates or opens a durable state database with mode 0600.
func Open(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("state database path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}
	database, err := goBolt.Open(path, 0o600, &goBolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}
	return &Store{database: database}, nil
}

// Load returns a copy of the last committed snapshot.
func (s *Store) Load(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var state []byte
	err := s.database.View(func(transaction *goBolt.Tx) error {
		bucket := transaction.Bucket(bucketName)
		if bucket == nil {
			return nil
		}
		state = append(state, bucket.Get(stateKey)...)
		return nil
	})
	return state, err
}

// Save commits a complete snapshot in one transaction.
func (s *Store) Save(ctx context.Context, state []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.database.Update(func(transaction *goBolt.Tx) error {
		bucket, err := transaction.CreateBucketIfNotExists(bucketName)
		if err != nil {
			return err
		}
		return bucket.Put(stateKey, append([]byte(nil), state...))
	})
}

// Close flushes and closes the database.
func (s *Store) Close() error { return s.database.Close() }
