package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mytecor/r1s/internal/broker"
	"github.com/mytecor/r1s/internal/cluster"
)

func TestParseClusterUseDefaultsToForeground(t *testing.T) {
	for _, test := range []struct {
		arguments []string
		detach    bool
	}{
		{arguments: []string{"abcd"}},
		{arguments: []string{"-d", "abcd"}, detach: true},
		{arguments: []string{"--detach", "abcd"}, detach: true},
	} {
		options, err := parseClusterUse(test.arguments, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("parseClusterUse(%v): %v", test.arguments, err)
		}
		if options.selector != "abcd" || options.detach != test.detach {
			t.Fatalf("parseClusterUse(%v) = %+v", test.arguments, options)
		}
	}
}

func TestClusterUseForegroundBlocksUntilCanceled(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	address := filepath.Join(os.TempDir(), fmt.Sprintf("r1s-cluster-use-foreground-%d.sock", os.Getpid()))
	_ = os.Remove(address)
	t.Cleanup(func() { _ = os.Remove(address) })
	t.Setenv("R1S_SOCKET", address)
	directory, err := cluster.DefaultDirectory()
	if err != nil {
		t.Fatal(err)
	}
	id, err := cluster.SaveCredential(directory, bytes.Repeat([]byte{0x65}, cluster.KeySize))
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runClusterSession(ctx, []string{"use", id}, io.Discard, &bytes.Buffer{})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		current, statusErr := broker.Status(address)
		if statusErr == nil {
			if current != id {
				t.Fatalf("current cluster = %q, want %q", current, id)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("foreground broker did not become ready: %v", statusErr)
		}
		time.Sleep(10 * time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("cluster use returned before cancellation: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cluster use cancellation = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("foreground cluster use did not stop after cancellation")
	}
}
