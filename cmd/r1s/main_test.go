package main

import (
	"bytes"
	"errors"
	"flag"
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
