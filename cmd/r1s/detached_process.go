package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

// detachedProcess describes the common re-exec and readiness contract used by
// both `run -d` and `cluster use -d`. The child writes exactly one readiness
// line to stdout; the caller validates its command-specific payload.
type detachedProcess struct {
	label    string
	timeout  time.Duration
	validate func(string) error
}

func launchDetachedProcess(ctx context.Context, executable string, arguments []string, options detachedProcess) error {
	if options.label == "" || options.timeout <= 0 || options.validate == nil {
		return errors.New("detach: invalid process launch configuration")
	}
	cmd := exec.Command(executable, arguments...)
	cmd.SysProcAttr = detachedSysProcAttr()
	ready, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("%s: create readiness pipe: %w", options.label, err)
	}
	defer ready.Close()
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("%s: start child: %w", options.label, err)
	}
	reading := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(ready).ReadString('\n')
		reading <- strings.TrimSpace(line)
	}()
	timer := time.NewTimer(options.timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		killAndWait(cmd)
		return ctx.Err()
	case <-timer.C:
		killAndWait(cmd)
		return fmt.Errorf("%s: child did not become ready within %s", options.label, options.timeout)
	case line := <-reading:
		if line == "" {
			return awaitFailedDetachedChild(ctx, timer.C, cmd, options.label)
		}
		if err := options.validate(line); err != nil {
			killAndWait(cmd)
			return fmt.Errorf("%s: invalid readiness handshake: %w", options.label, err)
		}
		if err := cmd.Process.Release(); err != nil {
			return fmt.Errorf("%s: release child: %w", options.label, err)
		}
		return nil
	}
}

func awaitFailedDetachedChild(ctx context.Context, timeout <-chan time.Time, cmd *exec.Cmd, label string) error {
	waited := make(chan error, 1)
	go func() { waited <- cmd.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			return fmt.Errorf("%s: child failed before readiness: %w", label, err)
		}
		return fmt.Errorf("%s: child exited before readiness", label)
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		<-waited
		return ctx.Err()
	case <-timeout:
		_ = cmd.Process.Kill()
		<-waited
		return fmt.Errorf("%s: child did not become ready before timeout", label)
	}
}

func killAndWait(cmd *exec.Cmd) {
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}
