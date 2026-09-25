package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	r1sv1 "github.com/mytecor/r1s/api/gen/r1s/v1"
)

// Detached run ownership (F22-05): `r1s run -d` hands the lease-holding run to
// a background child. The parent never builds an RNS node; it spawns the child,
// waits for a short ownership handshake, and only then prints the run ID, PID,
// and log path. A child that fails before the handshake is reported as a failed
// detach with its exit status, never as a live run.

// runStateRelPath locates the per-user run record directory. Runs live under
// ~/.local/state/r1s/runs/<run-id>/ with a pid marker while active and an
// output.log after creation. No cluster secret or secret workload field is
// copied into this directory.
const runStateRelPath = ".local/state/r1s/runs"

// detachHandshakeFD is the file descriptor the child inherits (via ExtraFiles)
// and writes its ready line to. It is the first and only ExtraFile, so it lands
// at fd 3 after exec.
const detachHandshakeFD = 3

// childHandshakeTimeout bounds how long the parent waits for the child to take
// ownership. The child's first request can take up to offer-wait plus RNS
// connect time, so this is deliberately generous.
const childHandshakeTimeout = 120 * time.Second

// runStateDir returns the per-user run record root.
func runStateDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, runStateRelPath), nil
}

// runDirectory returns the state directory for one logical run.
func runDirectory(runID string) (string, error) {
	root, err := runStateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, runID), nil
}

// launchDetached is the parent half of `r1s run -d`. It re-executes this same
// binary as the lease-holding child (never building its own RNS node), waits
// for the child's ownership handshake, and only then prints the run ID, PID,
// and log path. A child that fails or exits before the handshake is reported
// as a failed detach, never as a live run.
func (a *application) launchDetached(clusterID, workloadJSON string, offerWait time.Duration, logFile string, stderr io.Writer) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("detach: resolve executable: %w", err)
	}
	// The child is re-invoked with the same run command minus -d. The internal
	// --r1s-child marker and handshake pipe fd tell it to run as the detached
	// holder; --r1s-log-file carries the resolved detached output path (which
	// the child cannot resolve until it knows the run ID and this must be
	// decided before the handshake reports it). When no --log-file was given,
	// the child resolves the default under the run state directory itself.
	childArgs := []string{"run", clusterID, workloadJSON, "--offer-wait", offerWait.String()}
	if logFile != "" {
		// The parent already knows an explicit --log-file; carry it to the child
		// as an internal flag so the child uses exactly this path for the output
		// and the parent can validate it after the handshake.
		childArgs = append(childArgs, "--r1s-log-file", logFile)
	}
	childArgs = append(childArgs, "--r1s-child")
	return launchDetachedRun(a.ctx, executable, childArgs, a.stdout)
}

// launchDetachedRun spawns `executable childArgs...`, reads the child's
// ownership handshake, and prints `run=... pid=... log=...` to stdout only
// after the reported paths exist with owner-only permissions. A child that
// exits before the handshake returns its exit error instead.
func launchDetachedRun(ctx context.Context, executable string, childArgs []string, stdout io.Writer) error {
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("detach: create handshake pipe: %w", err)
	}
	defer readEnd.Close()

	// The child becomes a session leader so a terminal interrupt aimed at the
	// foreground parent never reaches the detached run.
	cmd := exec.Command(executable, childArgs...)
	cmd.ExtraFiles = []*os.File{writeEnd}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := cmd.Start(); err != nil {
		writeEnd.Close()
		return fmt.Errorf("detach: start run child: %w", err)
	}
	// The parent's copy of the write end is closed so EOF on the read end means
	// the child has either finished its handshake or exited.
	writeEnd.Close()

	reading := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(readEnd).ReadString('\n')
		reading <- line
	}()

	timer := time.NewTimer(childHandshakeTimeout)
	defer timer.Stop()
	var line string
	select {
	case <-ctx.Done():
		cmd.Process.Kill()
		return ctx.Err()
	case <-timer.C:
		cmd.Process.Kill()
		return fmt.Errorf("detach: child did not take ownership within %s", childHandshakeTimeout)
	case result := <-reading:
		// A complete line is a valid handshake even if the child closed the pipe
		// right after writing it (io.EOF accompanies the data). Only an empty
		// read means the child failed before signaling ownership.
		line = strings.TrimSpace(result)
		if line == "" {
			// The child closed the pipe without a ready line: it failed before
			// ownership. Reap it and report the failure, never a live run.
			waitErr := cmd.Wait()
			if waitErr != nil {
				return fmt.Errorf("detach: child failed before taking ownership: %w", waitErr)
			}
			return fmt.Errorf("detach: child exited before the ownership handshake")
		}
	}

	runID, pid, logPath, runDir, err := parseReadyLine(line)
	if err != nil {
		return fmt.Errorf("detach: invalid child handshake: %w", err)
	}
	// The child reports paths that must already exist with owner-only
	// permissions; the parent refuses to print a live run otherwise.
	if err := verifyDetachedPaths(runDir, pid, logPath); err != nil {
		return fmt.Errorf("detach: child reported unreachable state: %w", err)
	}
	fmt.Fprintf(stdout, "run=%s pid=%d log=%s\n", runID, pid, logPath)
	return nil
}

// parseReadyLine parses the child's machine-ready handshake line. The four
// tab-separated fields are run ID, PID, output log path, and run directory.
func parseReadyLine(line string) (runID string, pid int, logPath, runDir string, err error) {
	fields := strings.Split(line, "\t")
	if len(fields) != 4 || strings.TrimSpace(fields[0]) == "" || strings.TrimSpace(fields[2]) == "" || strings.TrimSpace(fields[3]) == "" {
		return "", 0, "", "", fmt.Errorf("expected <run-id>\\t<pid>\\t<log>\\t<dir>")
	}
	pid, err = strconv.Atoi(strings.TrimSpace(fields[1]))
	if err != nil || pid <= 0 {
		return "", 0, "", "", fmt.Errorf("invalid pid %q", fields[1])
	}
	return strings.TrimSpace(fields[0]), pid, strings.TrimSpace(fields[2]), strings.TrimSpace(fields[3]), nil
}

// verifyDetachedPaths confirms the run directory, its pid marker, and the
// output log exist with owner-only permissions, and that the pid marker names
// the child pid the parent already learned from the handshake.
func verifyDetachedPaths(runDir string, pid int, logPath string) error {
	info, err := os.Stat(runDir)
	if err != nil {
		return fmt.Errorf("run directory %s: %w", runDir, err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return fmt.Errorf("run directory %s has owner-only violation: %04o", runDir, mode)
	}
	data, err := os.ReadFile(filepath.Join(runDir, "pid"))
	if err != nil {
		return fmt.Errorf("pid marker: %w", err)
	}
	markerPID, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || markerPID != pid {
		return fmt.Errorf("pid marker %q does not match handshake pid %d", strings.TrimSpace(string(data)), pid)
	}
	for _, path := range []string{
		filepath.Join(runDir, "pid"),
		logPath,
	} {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("path %s: %w", path, err)
		}
		if mode := info.Mode().Perm(); mode&0o077 != 0 {
			return fmt.Errorf("path %s has owner-only violation: %04o", path, mode)
		}
	}
	return nil
}

// writePIDMarker records the child's own PID in the run directory with owner-only
// permissions. Multiple concurrent children of one run ID never clobber each
// other's marker.
func writePIDMarker(runDir string) error {
	if err := os.MkdirAll(runDir, 0700); err != nil {
		return fmt.Errorf("create run state directory: %w", err)
	}
	path := filepath.Join(runDir, "pid")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("write pid marker: %w", err)
	}
	_, writeErr := fmt.Fprintf(file, "%d\n", os.Getpid())
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("write pid marker: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close pid marker: %w", closeErr)
	}
	return nil
}

// removePIDMarker clears the marker on clean exit, leaving the output log.
func removePIDMarker(runDir string) error {
	if runDir == "" {
		return nil
	}
	if err := os.Remove(filepath.Join(runDir, "pid")); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// detachedOutputPath resolves where a detached run's output lands: the
// explicit --log-file when given, otherwise output.log in the run's state
// directory.
func detachedOutputPath(logFile, runDir string) string {
	if logFile != "" {
		return logFile
	}
	return filepath.Join(runDir, "output.log")
}

// runDetachedChild is the lease-holding child half of `r1s run -d`. It runs the
// initial assignment, records its own PID marker and opens the output file,
// signals ownership to the parent only then, and finally holds the run while a
// concurrent tail appends every stream and reschedule marker to the same file.
// On clean exit the PID marker is removed and the output file is retained.
func (a *application) runDetachedChild(workloadJSON string, offerWait time.Duration, handshakeFD int, logFile string) error {
	created, executionID, _, err := a.runRequestJSON(workloadJSON, offerWait)
	if err != nil {
		return err
	}
	runID := created.GetRunId()
	runDir, err := runDirectory(runID)
	if err != nil {
		return err
	}
	if err := writePIDMarker(runDir); err != nil {
		return fmt.Errorf("detached run %s: %w", runID, err)
	}
	defer removePIDMarker(runDir)

	outputPath := detachedOutputPath(logFile, runDir)
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("detached run %s: open output: %w", runID, err)
	}
	defer file.Close()

	// Signal ownership to the parent only now, when the reported paths already
	// exist. The parent prints run/pid/log and returns; from here the child
	// keeps the lease itself.
	if err := signalDetachReady(handshakeFD, runID, outputPath, runDir); err != nil {
		return err
	}

	tail := newDetachedTail(a, file)
	tail.SetActive(executionID, runID, created.GetAttempt())
	stop := make(chan struct{})
	go tail.run(a.ctx, stop)

	activeID, state, err := a.holdRun(a.ctx, executionID, offerWait, func(id string, request *r1sv1.ExecutionRequest) {
		tail.SetActive(id, request.GetRunId(), request.GetAttempt())
	}, nil)
	close(stop)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			a.bestEffortRunCancel(activeID)
		}
		return err
	}
	return workloadStatus(state)
}

// signalDetachReady writes the machine-ready handshake line to the child's
// inherited pipe and closes it. The four tab-separated fields are run ID, the
// child's PID, the output log path, and the run state directory.
func signalDetachReady(handshakeFD int, runID, outputPath, runDir string) error {
	pipe := os.NewFile(uintptr(handshakeFD), "r1s-detach-handshake")
	if pipe == nil {
		return fmt.Errorf("detached run %s: invalid handshake fd %d", runID, handshakeFD)
	}
	_, err := fmt.Fprintf(pipe, "%s\t%d\t%s\t%s\n", runID, os.Getpid(), outputPath, runDir)
	closeErr := pipe.Close()
	if err != nil {
		return fmt.Errorf("detached run %s: write handshake: %w", runID, err)
	}
	if closeErr != nil {
		return fmt.Errorf("detached run %s: close handshake: %w", runID, closeErr)
	}
	return nil
}
