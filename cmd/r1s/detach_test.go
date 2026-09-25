package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestRunDirectoryUnderHomeState(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	dir, err := runDirectory("run-abc123")
	if err != nil {
		t.Fatal(err)
	}
	if dir != filepath.Join(home, ".local/state/r1s/runs", "run-abc123") {
		t.Fatalf("runDirectory = %q", dir)
	}
}

func TestParseReadyLine(t *testing.T) {
	runID, pid, logPath, runDir, err := parseReadyLine("run-1\t1234\t/tmp/x/log\t/tmp/x/runs/run-1\n")
	if err != nil {
		t.Fatal(err)
	}
	if runID != "run-1" || pid != 1234 || logPath != "/tmp/x/log" || runDir != "/tmp/x/runs/run-1" {
		t.Fatalf("parse = %q %d %q %q", runID, pid, logPath, runDir)
	}
	for _, line := range map[string]string{
		"empty":       "",
		"too few":     "run-1\t1234\t/tmp/x/log",
		"blank runid": "\t1234\t/tmp/x/log\t/tmp/x/d",
		"non-numeric": "run-1\tabc\t/tmp/x/log\t/tmp/x/d",
		"zero pid":    "run-1\t0\t/tmp/x/log\t/tmp/x/d",
	} {
		if _, _, _, _, err := parseReadyLine(line); err == nil {
			t.Fatalf("parseReadyLine(%q) succeeded", line)
		}
	}
}

func TestWriteAndRemovePIDMarker(t *testing.T) {
	runDir := t.TempDir()
	if err := writePIDMarker(runDir); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(runDir, "pid"))
	if err != nil {
		t.Fatal(err)
	}
	if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err != nil || pid != os.Getpid() {
		t.Fatalf("pid marker = %q, want this process pid", strings.TrimSpace(string(data)))
	}
	info, err := os.Stat(filepath.Join(runDir, "pid"))
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Fatalf("pid marker perms %04o, want owner-only", mode)
	}
	if err := removePIDMarker(runDir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "pid")); !os.IsNotExist(err) {
		t.Fatalf("pid marker not removed: %v", err)
	}
}

func writeMarker(t *testing.T, runDir string, pid int) {
	t.Helper()
	if err := os.MkdirAll(runDir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "pid"), []byte(fmt.Sprintf("%d\n", pid)), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyDetachedPaths(t *testing.T) {
	runDir := filepath.Join(t.TempDir(), "runs", "run-1")
	if err := writePIDMarker(runDir); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(runDir, "output.log")
	if err := os.WriteFile(logPath, []byte("out"), 0600); err != nil {
		t.Fatal(err)
	}
	pid := 0
	data, err := os.ReadFile(filepath.Join(runDir, "pid"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid)
	if err := verifyDetachedPaths(runDir, pid, logPath); err != nil {
		t.Fatalf("consistent state rejected: %v", err)
	}
	// A stale marker (different child pid) must be rejected: the parent refuses
	// to report a live run when the pid does not match the handshake.
	if err := verifyDetachedPaths(runDir, pid+1, logPath); err == nil {
		t.Fatal("stale pid accepted")
	}
	// A missing log path is rejected.
	if err := verifyDetachedPaths(runDir, pid, filepath.Join(runDir, "missing.log")); err == nil {
		t.Fatal("missing log accepted")
	}
}

func TestVerifyDetachedPathsRequiresOwnerOnlyPerms(t *testing.T) {
	runDir := t.TempDir()
	writeMarker(t, runDir, 4321)
	if err := os.Chmod(runDir, 0o755); err != nil {
		t.Fatal(err)
	}
	logPath := filepath.Join(runDir, "output.log")
	if err := os.WriteFile(logPath, []byte("out"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := verifyDetachedPaths(runDir, 4321, logPath); err == nil {
		t.Fatal("owner-only violation accepted")
	}
}

func TestLaunchDetachedRunStartFailureNeverReportsLiveRun(t *testing.T) {
	var out bytes.Buffer
	err := launchDetachedRun(context.Background(), filepath.Join(t.TempDir(), "no-such-binary"), []string{"run"}, &out)
	if err == nil || out.Len() != 0 {
		t.Fatalf("start failure: err=%v out=%q", err, out.String())
	}
}

func TestLaunchDetachedRunExitBeforeHandshakeNeverReportsLiveRun(t *testing.T) {
	script := filepath.Join(t.TempDir(), "exit-immediately.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 7\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	err := launchDetachedRun(context.Background(), script, []string{"run"}, &out)
	if err == nil || out.Len() != 0 {
		t.Fatalf("exit-before-ownership: err=%v out=%q", err, out.String())
	}
}

// TestLaunchDetachedRunSuccessHandshake drives the parent orchestration against
// a scripted child that, like the real child, creates the run directory with
// owner-only perms, writes its own PID marker, creates the output log, and only
// then writes the machine-ready handshake line to the inherited fd 3. The
// parent must print the run ID, PID, and log path and report no error.
func TestLaunchDetachedRunSuccessHandshake(t *testing.T) {
	base := t.TempDir()
	runDir := filepath.Join(base, "runs", "run-success")
	logPath := filepath.Join(runDir, "output.log")
	script := filepath.Join(base, "child.sh")
	body := "#!/bin/sh\n" +
		"mkdir -p \"$1\" && chmod 700 \"$1\"\n" +
		"echo \"$$\" > \"$1/pid\" && chmod 600 \"$1/pid\"\n" +
		": > \"$1/output.log\" && chmod 600 \"$1/output.log\"\n" +
		"printf 'run-success\\t%s\\t%s\\t%s\\n' \"$$\" \"$1/output.log\" \"$1\" >&3\n" +
		"exit 0\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := launchDetachedRun(context.Background(), script, []string{runDir}, &out); err != nil {
		t.Fatalf("handshake failed: %v", err)
	}
	if !strings.Contains(out.String(), "run=run-success pid=") {
		t.Fatalf("output = %q, missing run=run-success", out.String())
	}
	if !strings.Contains(out.String(), "log="+logPath) {
		t.Fatalf("output = %q, missing log path", out.String())
	}
}
