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
  "policy": {
    "resultRetention": "86400s"
  }
}`)
	if err != nil {
		t.Fatal(err)
	}
	if request.GetRequestId() != "" || request.GetResourceClass() != "default" || request.GetWorkload().GetImage() == "" {
		t.Fatalf("request = %v", request)
	}
	if got := request.GetPolicy().GetResultRetention().AsDuration().Seconds(); got != 86400 {
		t.Fatalf("result retention = %v", got)
	}
}

func TestDecodeRequestJSONRejectsManagedAndUnknownFields(t *testing.T) {
	for name, input := range map[string]string{
		"managed request ID": `{"requestId":"caller-selected"}`,
		"unknown field":      `{"unexpected":true}`,
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
	_, err := parseCommandLine([]string{"--rns-config", "unused", "list"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("parseCommandLine() error = %v, want removed flag diagnostic", err)
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

func TestInlineClusterTokenErrorIsRedacted(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{
		"--cluster", "r1s1:not-base64",
		"--identity", "unused",
		"list",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "cluster identifier must be a hexadecimal prefix") {
		t.Fatalf("error = %v, want invalid selector diagnostic", err)
	}
	if strings.Contains(err.Error(), "not-base64") {
		t.Fatalf("error exposed inline token: %v", err)
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
	_, err = openApplication(context.Background(), commandLine{
		identitySource:  "unused",
		clusterSelector: selector,
	}, &bytes.Buffer{})
	if !errors.Is(err, cluster.ErrAmbiguous) {
		t.Fatalf("openApplication() error = %v, want ErrAmbiguous", err)
	}
}
