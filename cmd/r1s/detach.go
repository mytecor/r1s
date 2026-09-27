package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	r1sclient "github.com/mytecor/r1s/client"
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
func (a *application) launchDetached(workloadJSON string, offerWait time.Duration, logFile string, publishes []portMapping, stderr io.Writer) error {
	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("detach: resolve executable: %w", err)
	}
	// The child is re-invoked with the same run command minus -d. The internal
	// --r1s-child marker tells it to run as the detached
	// holder; --r1s-log-file carries the resolved detached output path (which
	// the child cannot resolve until it knows the run ID and this must be
	// decided before the handshake reports it). When no --log-file was given,
	// the child resolves the default under the run state directory itself.
	childArgs := []string{"run", workloadJSON, "--offer-wait", offerWait.String()}
	if len(publishes) > 0 {
		var bits []string
		for _, m := range publishes {
			bits = append(bits, fmt.Sprintf("%d:%d", m.host, m.container))
		}
		childArgs = append(childArgs, "--r1s-publish", strings.Join(bits, ","))
	}
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
	return launchDetachedProcess(ctx, executable, childArgs, detachedProcess{
		label: "detach run", timeout: childHandshakeTimeout,
		validate: func(line string) error {
			runID, pid, logPath, runDir, err := parseReadyLine(line)
			if err != nil {
				return err
			}
			if err := verifyDetachedPaths(runDir, pid, logPath); err != nil {
				return fmt.Errorf("reported unreachable state: %w", err)
			}
			_, err = fmt.Fprintf(stdout, "run=%s pid=%d log=%s\n", runID, pid, logPath)
			return err
		},
	})
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
func (a *application) runDetachedChild(workloadJSON string, offerWait time.Duration, logFile string, publishes []portMapping) error {
	request, err := decodeRequestJSON(workloadJSON)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancelCause(a.ctx)
	defer cancel(nil)
	var runID, runDir, outputPath string
	var file *os.File
	var publisher *runPublisher
	var tail *runTail
	stop := make(chan struct{})
	var initErr error
	result, err := a.controller.Run(runCtx, request, r1sclient.RunOptions{
		OfferWait: offerWait,
		OnEvent: func(event r1sclient.Event) {
			if event.Kind != r1sclient.EventAttemptAssigned {
				return
			}
			if tail != nil {
				tail.SetActive(event.Attempt.ExecutionID, event.Attempt.RunID, event.Attempt.Number)
				if publisher != nil {
					publisher.SetActive(event.Attempt.ExecutionID)
				}
				return
			}
			runID = event.Attempt.RunID
			runDir, initErr = runDirectory(runID)
			if initErr == nil {
				initErr = writePIDMarker(runDir)
			}
			if initErr == nil {
				outputPath = detachedOutputPath(logFile, runDir)
				file, initErr = os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
			}
			if initErr == nil && len(publishes) > 0 {
				initErr = a.ensureRunTunnelEdge()
				if initErr == nil {
					publisher, initErr = newRunPublisher(runCtx, a, publishes)
				}
				if initErr == nil {
					publisher.SetActive(event.Attempt.ExecutionID)
					publisher.start()
				}
			}
			if initErr == nil {
				initErr = signalDetachReady(a.stdout, runID, outputPath, runDir)
			}
			if initErr != nil {
				cancel(initErr)
				return
			}
			tail = newDetachedTail(a, file)
			tail.SetActive(event.Attempt.ExecutionID, runID, event.Attempt.Number)
			go tail.run(runCtx, stop)
		},
	})
	if tail != nil {
		close(stop)
	}
	if publisher != nil {
		publisher.close()
	}
	if file != nil {
		_ = file.Close()
	}
	if runDir != "" {
		removePIDMarker(runDir)
	}
	if initErr != nil {
		return fmt.Errorf("detached run %s: %w", runID, initErr)
	}
	if err != nil {
		return err
	}
	return workloadStatus(result.State)
}

// signalDetachReady writes the common stdout readiness line. The four
// tab-separated fields are run ID, PID, output log path, and run directory.
func signalDetachReady(output io.Writer, runID, outputPath, runDir string) error {
	_, err := fmt.Fprintf(output, "%s\t%d\t%s\t%s\n", runID, os.Getpid(), outputPath, runDir)
	if err != nil {
		return fmt.Errorf("detached run %s: write handshake: %w", runID, err)
	}
	return nil
}
