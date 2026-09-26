package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"strings"
	"testing"

	"github.com/mytecor/r1s/internal/cluster"
)

func TestDecodeRequestJSON(t *testing.T) {
	request, err := decodeRequestJSON(`{
  "workload": {
    "image": "example.test/app@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "command": ["/bin/sh", "-c"],
    "args": ["echo hello"],
    "environment": {"MODE": "test"},
    "workingDirectory": "/work"
  },
  "policy": {}
}`)
	if err != nil {
		t.Fatal(err)
	}
	if request.GetRequestId() != "" || request.GetResourceClass() != "default" || request.GetWorkload().GetImage() == "" {
		t.Fatalf("request = %v", request)
	}
}

func TestDecodeRequestJSONRejectsManagedAndUnknownFields(t *testing.T) {
	for name, input := range map[string]string{
		"managed request ID": `{"requestId":"caller-selected"}`,
		"unknown field":      `{"unexpected":true}`,
		"retired retention":  `{"policy":{"resultRetention":"86400s"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeRequestJSON(input); err == nil {
				t.Fatal("decodeRequestJSON() succeeded")
			}
		})
	}
}

func TestDecodeRequestJSONIsBounded(t *testing.T) {
	if _, err := decodeRequestJSON(strings.Repeat("x", maxRequestJSONBytes+1)); err == nil {
		t.Fatal("oversized request succeeded")
	}
}

func TestHelpUsesCanonicalDoubleDashFlags(t *testing.T) {
	var output bytes.Buffer
	flags := newFlagSet("test", &output)
	flags.String("identity", "", "identity")
	if err := flags.Parse([]string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("Parse() error = %v", err)
	}
	if !strings.Contains(output.String(), "--identity value") {
		t.Fatalf("help = %q", output.String())
	}
}

func TestRNSConfigFlagIsRemoved(t *testing.T) {
	_, err := parseCommandLine([]string{"--rns-config", "unused", "run", "abcd", `{}`}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("parseCommandLine() error = %v, want removed flag diagnostic", err)
	}
}

// TestLegacyClientFlagsAreGone locks in the F22-07 cutover: the legacy
// identity/state/socket/allocator/keep-alive surfaces no longer exist as
// top-level or workflow options, and the removed workflow commands are
// rejected outright.
func TestLegacyClientFlagsAreGone(t *testing.T) {
	for _, args := range [][]string{
		{"--identity", "x", "run", "abcd", `{}`},
		{"--state", "s.db", "run", "abcd", `{}`},
		{"--socket", "c.sock", "run", "abcd", `{}`},
		{"--keep-alive", "run", "abcd", `{}`},
		{"--allocator", "d", "run", "abcd", `{}`},
	} {
		if _, err := parseCommandLine(args, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
			t.Fatalf("legacy flags not rejected in %v: %v", args, err)
		}
	}
	for _, command := range []string{"request", "serve", "list", "inspect", "cancel", "result", "logs", "tunnel"} {
		if _, err := parseCommandLine([]string{command}, &bytes.Buffer{}); err == nil {
			t.Fatalf("removed command %q was accepted", command)
		}
	}
}

func TestVersionFlagPrintsVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--version"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(stdout.String()); got != version {
		t.Fatalf("r1s --version = %q, want %q", got, version)
	}
	if stderr.Len() != 0 {
		t.Fatalf("r1s --version wrote to stderr: %q", stderr.String())
	}
}

func TestClusterInitDoesNotRequireNetworkFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := run(context.Background(), []string{"cluster", "init"}, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "Join token: r1s1:") {
		t.Fatalf("output = %q", stdout.String())
	}
}

func TestAmbiguousClusterFailsBeforeTransportStartup(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	directory, err := cluster.DefaultDirectory()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[byte]string)
	selector := ""
	for value := byte(1); value != 0; value++ {
		id, err := cluster.SaveCredential(directory, bytes.Repeat([]byte{value}, cluster.KeySize))
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := seen[id[0]]; exists {
			selector = id[:1]
			break
		}
		seen[id[0]] = id
	}
	if selector == "" {
		t.Fatal("failed to construct ambiguous cluster prefix")
	}
	_, err = openRunApplication(context.Background(), commandLine{
		clusterSelector: selector,
	}, &bytes.Buffer{})
	if !errors.Is(err, cluster.ErrAmbiguous) {
		t.Fatalf("openRunApplication() error = %v, want ErrAmbiguous", err)
	}
}
