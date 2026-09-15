package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"path/filepath"
	"strings"
	"testing"
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
    "maxRuntime": "600s",
    "resultRetention": "86400s"
  }
}`)
	if err != nil {
		t.Fatal(err)
	}
	if request.GetRequestId() != "" || request.GetResourceClass() != "default" || request.GetWorkload().GetImage() == "" {
		t.Fatalf("request = %v", request)
	}
	if got := request.GetPolicy().GetMaxRuntime().AsDuration().Seconds(); got != 600 {
		t.Fatalf("max runtime = %v", got)
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
	flags.String("rns-config", "", "configuration")
	if err := flags.Parse([]string{"--help"}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("Parse() error = %v", err)
	}
	if !strings.Contains(output.String(), "--rns-config value") {
		t.Fatalf("help = %q", output.String())
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
	path := filepath.Join(t.TempDir(), "cluster")
	if err := run(context.Background(), []string{"--cluster", path, "cluster", "init"}, &stdout, &stderr); err != nil {
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
		"--rns-config", "unused",
		"--identity", "unused",
		"list",
	}, &stdout, &stderr)
	if err == nil || !strings.Contains(err.Error(), "inline join token") {
		t.Fatalf("error = %v, want inline token diagnostic", err)
	}
	if strings.Contains(err.Error(), "not-base64") {
		t.Fatalf("error exposed inline token: %v", err)
	}
}
