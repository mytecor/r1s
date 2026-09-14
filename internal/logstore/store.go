// Package logstore retains bounded local logs. It has no network dependencies.
package logstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	r1sruntime "github.com/mytecor/r1s/internal/runtime"
)

type Store struct {
	root                    string
	streamBytes, totalBytes int64
	mu                      sync.Mutex
}
type metadata struct {
	Execution string `json:"execution"`
	Limit     int64  `json:"limit"`
}

func New(root string, streamBytes, totalBytes int64) (*Store, error) {
	if root == "" || streamBytes < 1 || streamBytes > 1<<30 || totalBytes < 2*streamBytes {
		return nil, fmt.Errorf("invalid local log budget")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(root, 0700); err != nil {
		return nil, err
	}
	return &Store{root: root, streamBytes: streamBytes, totalBytes: totalBytes}, nil
}

func (s *Store) directory(id string) string {
	h := sha256.Sum256([]byte(id))
	return filepath.Join(s.root, hex.EncodeToString(h[:]))
}

// Reserve charges the full two-stream budget, including active offline writers.
func (s *Store) Reserve(id string) (string, int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.directory(id)
	if meta, err := readMetadata(dir); err == nil {
		if meta.Execution != id {
			return "", 0, fmt.Errorf("log identity conflict")
		}
		return dir, meta.Limit, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", 0, err
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return "", 0, err
	}
	var reserved int64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		m, err := readMetadata(filepath.Join(s.root, entry.Name()))
		if err != nil {
			return "", 0, fmt.Errorf("invalid log reservation %s: %w", entry.Name(), err)
		}
		reserved += 2 * m.Limit
	}
	if reserved > s.totalBytes-2*s.streamBytes {
		return "", 0, fmt.Errorf("local log budget exhausted")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return "", 0, err
	}
	data, _ := json.Marshal(metadata{Execution: id, Limit: s.streamBytes})
	f, err := os.OpenFile(filepath.Join(dir, "metadata.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return "", 0, err
	}
	_, err = f.Write(data)
	err = errors.Join(err, f.Sync(), f.Close())
	return dir, s.streamBytes, err
}

func readMetadata(dir string) (metadata, error) {
	var m metadata
	data, err := os.ReadFile(filepath.Join(dir, "metadata.json"))
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(data, &m)
	if err == nil && (m.Execution == "" || m.Limit < 1 || m.Limit > 1<<30) {
		err = fmt.Errorf("invalid log metadata")
	}
	return m, err
}

func (s *Store) Read(ctx context.Context, id, stream string, offset uint64, limit uint32) (r1sruntime.LogChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var chunk r1sruntime.LogChunk
	if err := ctx.Err(); err != nil {
		return chunk, err
	}
	if (stream != "stdout" && stream != "stderr") || limit == 0 || limit > 1<<20 {
		return chunk, r1sruntime.ErrLogOffset
	}
	dir := s.directory(id)
	m, err := readMetadata(dir)
	if errors.Is(err, os.ErrNotExist) {
		return chunk, r1sruntime.ErrLogsMissing
	}
	if err != nil {
		return chunk, err
	}
	if m.Execution != id {
		return chunk, r1sruntime.ErrLogsMissing
	}
	f, err := os.Open(filepath.Join(dir, stream))
	if errors.Is(err, os.ErrNotExist) {
		return chunk, r1sruntime.ErrLogsMissing
	}
	if err != nil {
		return chunk, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return chunk, err
	}
	if offset > uint64(info.Size()) {
		return chunk, r1sruntime.ErrLogOffset
	}
	data := make([]byte, min(uint64(limit), uint64(info.Size())-offset))
	n, err := f.ReadAt(data, int64(offset))
	if err != nil && err != io.EOF {
		return chunk, err
	}
	_, truncatedErr := os.Stat(filepath.Join(dir, stream+".truncated"))
	return r1sruntime.LogChunk{Data: data[:n], NextOffset: offset + uint64(n), EOF: offset+uint64(n) == uint64(info.Size()), Truncated: truncatedErr == nil}, nil
}

func (s *Store) Remove(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.RemoveAll(s.directory(id))
}

// Capture is run by a shim-owned process. It drains excess bytes without writing
// them, so a noisy or disconnected workload cannot grow its files beyond the cap.
func Capture(directory string, limit int64, stdout, stderr io.Reader, ready func() error) error {
	m, err := readMetadata(directory)
	if err != nil {
		return err
	}
	if m.Limit != limit {
		return fmt.Errorf("log limit mismatch")
	}
	files := make([]*os.File, 2)
	for i, stream := range []string{"stdout", "stderr"} {
		f, err := os.OpenFile(filepath.Join(directory, stream), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			for _, old := range files {
				if old != nil {
					old.Close()
				}
			}
			return err
		}
		files[i] = f
	}
	defer files[0].Close()
	defer files[1].Close()
	if err := ready(); err != nil {
		return err
	}
	errs := make(chan error, 2)
	for i, source := range []io.Reader{stdout, stderr} {
		go func() {
			f := files[i]
			info, err := f.Stat()
			if err != nil {
				errs <- err
				return
			}
			remaining := max(int64(0), limit-info.Size())
			buffer := make([]byte, 32<<10)
			truncated := false
			for {
				n, readErr := source.Read(buffer)
				if n > 0 {
					keep := min(int64(n), remaining)
					if keep > 0 {
						if _, err := f.Write(buffer[:keep]); err != nil {
							errs <- err
							return
						}
						remaining -= keep
					}
					if int64(n) > keep && !truncated {
						truncated = true
						name := []string{"stdout", "stderr"}[i]
						if err := os.WriteFile(filepath.Join(directory, name+".truncated"), []byte{1}, 0600); err != nil {
							errs <- err
							return
						}
					}
				}
				if readErr != nil {
					if readErr == io.EOF {
						readErr = nil
					}
					errs <- errors.Join(readErr, f.Sync())
					return
				}
			}
		}()
	}
	return errors.Join(<-errs, <-errs)
}
