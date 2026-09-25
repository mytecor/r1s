package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"testing"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
	"github.com/mytecor/r1s/internal/cluster"
)

func TestRunCommandUsesPositionalClusterWithoutPersistentIdentity(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	directory, err := cluster.DefaultDirectory()
	if err != nil {
		t.Fatal(err)
	}
	id, err := cluster.SaveCredential(directory, bytes.Repeat([]byte{0x71}, cluster.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	options, err := parseCommandLine([]string{"run", id[:12], "--offer-wait", "1s", `{"workload":{"image":"example.test/image@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if options.clusterSelector != id[:12] || options.identitySource != "" || options.statePath != "" {
		t.Fatalf("run options = %+v", options)
	}
	first, err := openRunApplication(context.Background(), options, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	defer first.close()
	second, err := openRunApplication(context.Background(), options, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	defer second.close()
	if first.store != nil || second.store != nil {
		t.Fatal("run application opened a durable client store")
	}
	if bytes.Equal(first.identity, second.identity) {
		t.Fatalf("separate runs reused ephemeral identity %x", first.identity)
	}
}

func TestRunRejectsLegacyClusterFlag(t *testing.T) {
	for _, arguments := range [][]string{
		{"--cluster", "abcd", "run", "abcd", `{}`},
		{"--identity", "identity", "run", "abcd", `{}`},
		{"--state", "state.db", "run", "abcd", `{}`},
		{"--socket", "client.sock", "run", "abcd", `{}`},
	} {
		if _, err := parseCommandLine(arguments, &bytes.Buffer{}); err == nil {
			t.Fatalf("run accepted legacy options in %v", arguments)
		}
	}
}

func TestRunHelpWorksWithOrWithoutCluster(t *testing.T) {
	for _, arguments := range [][]string{{"run", "--help"}, {"run", "abcd", "--help"}} {
		var output bytes.Buffer
		if err := run(context.Background(), arguments, &bytes.Buffer{}, &output); !errors.Is(err, flag.ErrHelp) {
			t.Fatalf("run(%v) error = %v, want help", arguments, err)
		}
		if output.Len() == 0 {
			t.Fatalf("run(%v) produced no help", arguments)
		}
	}
}

func TestWorkloadStatus(t *testing.T) {
	zero := int32(0)
	seven := int32(7)
	tests := []struct {
		name  string
		state *r1sv1.ExecutionState
		want  int
	}{
		{name: "completed", state: &r1sv1.ExecutionState{Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED, ExitCode: &zero}},
		{name: "workload status", state: &r1sv1.ExecutionState{Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_COMPLETED, ExitCode: &seven}, want: 7},
		{name: "failed without status", state: &r1sv1.ExecutionState{Phase: r1sv1.ExecutionPhase_EXECUTION_PHASE_FAILED}, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := workloadStatus(test.state)
			if test.want == 0 {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var status interface{ ExitStatus() int }
			if !errors.As(err, &status) || status.ExitStatus() != test.want {
				t.Fatalf("workloadStatus() = %v, want exit %d", err, test.want)
			}
		})
	}
}

func TestRunAcceptsDetachFlagsAndKeepsJSON(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	directory, err := cluster.DefaultDirectory()
	if err != nil {
		t.Fatal(err)
	}
	id, err := cluster.SaveCredential(directory, bytes.Repeat([]byte{0x42}, cluster.KeySize))
	if err != nil {
		t.Fatal(err)
	}
	json := `{"workload":{"image":"example.test/i@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`
	for _, flag := range []string{"-d", "--detach"} {
		options, err := parseCommandLine([]string{"run", id[:12], flag, "--offer-wait", "1s", json}, &bytes.Buffer{})
		if err != nil {
			t.Fatalf("parseCommandLine(%s) error: %v", flag, err)
		}
		if options.clusterSelector != id[:12] {
			t.Fatalf("cluster selector = %q", options.clusterSelector)
		}
		found := false
		for _, argument := range options.arguments {
			if argument == flag {
				found = true
			}
		}
		if !found {
			t.Fatalf("argument %s not preserved in %v", flag, options.arguments)
		}
	}
}

func TestContainsDetachFlag(t *testing.T) {
	if !containsDetachFlag([]string{"-d"}) || !containsDetachFlag([]string{"--detach"}) {
		t.Fatal("detach flags not recognized")
	}
	if containsDetachFlag([]string{"request"}) || containsDetachFlag([]string{"--log-file", "x"}) {
		t.Fatal("non-detach args misdetected")
	}
}
