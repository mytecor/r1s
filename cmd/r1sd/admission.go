package main

import (
	"fmt"
	"io"
	"os"

	"github.com/mytecor/r1s/internal/allocator"
)

// readAdmission loads the local admission policy from an optional file (or
// stdin when the path is empty) and grounds it against the configured
// capacity.
func readAdmission(path string, capacity map[string]uint32) (allocator.AdmissionPolicy, error) {
	var reader io.Reader
	if path != "" {
		file, err := os.Open(path)
		if err != nil {
			return allocator.AdmissionPolicy{}, err
		}
		defer file.Close()
		reader = file
	}
	admission, err := allocator.ReadAdmission(reader, capacity)
	if err != nil {
		return allocator.AdmissionPolicy{}, fmt.Errorf("admission policy: %w", err)
	}
	return admission, nil
}
