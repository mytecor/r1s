//go:build linux

package containerd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"

	containerdclient "github.com/containerd/containerd/v2/client"
	"golang.org/x/sys/unix"
)

func (b *clientBackend) dialExecution(ctx context.Context, executionID, fingerprint string, port uint16) (net.Conn, error) {
	if !strings.HasPrefix(b.address, "/") && !strings.HasPrefix(b.address, "unix://") {
		return nil, fmt.Errorf("execution tunnels require a local containerd Unix socket")
	}
	if port == 0 {
		return nil, fmt.Errorf("%w: port is required", ErrInvalidRequest)
	}
	ctx = b.context(ctx)
	container, err := b.client.LoadContainer(ctx, containerID(executionID))
	if err != nil {
		return nil, err
	}
	if err := verifyLabels(ctx, container, executionID, fingerprint); err != nil {
		return nil, err
	}
	task, err := container.Task(ctx, nil)
	if err != nil {
		return nil, err
	}
	status, err := task.Status(ctx)
	if err != nil {
		return nil, err
	}
	if status.Status != containerdclient.Running || task.Pid() == 0 {
		return nil, ErrExecutionMissing
	}

	// Pin task identity before opening /proc so a reused PID cannot supply a
	// different process's network namespace while the task exits.
	pidfd, err := unix.PidfdOpen(int(task.Pid()), 0)
	if err != nil {
		return nil, fmt.Errorf("pin execution process: %w", err)
	}
	defer unix.Close(pidfd)
	namespace, err := os.Open(fmt.Sprintf("/proc/%d/ns/net", task.Pid()))
	if err != nil {
		return nil, fmt.Errorf("open execution network namespace: %w", err)
	}
	defer namespace.Close()
	if err := unix.PidfdSendSignal(pidfd, 0, nil, 0); err != nil {
		return nil, ErrExecutionMissing
	}
	// Recheck after opening the namespace, keeping the namespace FD pinned while
	// dialing. A task that stopped during lookup must not authorize a connection.
	status, err = task.Status(ctx)
	if err != nil {
		return nil, err
	}
	if status.Status != containerdclient.Running {
		return nil, ErrExecutionMissing
	}
	return dialNamespace(ctx, namespace, port)
}

// dialNamespace uses a dedicated locked thread so no other goroutine observes
// the execution namespace. If restoring fails, the goroutine exits still locked
// and Go discards its thread instead of returning it to the scheduler.
func dialNamespace(ctx context.Context, namespace *os.File, port uint16) (net.Conn, error) {
	type result struct {
		conn net.Conn
		err  error
	}
	done := make(chan result, 1)
	go func() {
		runtime.LockOSThread()
		restored := true
		defer func() {
			if restored {
				runtime.UnlockOSThread()
			}
		}()
		original, err := os.Open(fmt.Sprintf("/proc/self/task/%d/ns/net", unix.Gettid()))
		if err != nil {
			done <- result{err: err}
			return
		}
		defer original.Close()
		targetInfo, err := namespace.Stat()
		if err != nil {
			done <- result{err: err}
			return
		}
		originalInfo, err := original.Stat()
		if err != nil {
			done <- result{err: err}
			return
		}
		if os.SameFile(targetInfo, originalInfo) {
			done <- result{err: fmt.Errorf("refusing tunnel into allocator network namespace")}
			return
		}
		if err := unix.Setns(int(namespace.Fd()), unix.CLONE_NEWNET); err != nil {
			done <- result{err: fmt.Errorf("enter execution network namespace: %w", err)}
			return
		}
		restored = false
		// A numeric IPv4 address with tcp4 avoids DNS and parallel-family dialing.
		var conn net.Conn
		dialErr := enableLoopback()
		if dialErr == nil {
			conn, dialErr = (&net.Dialer{}).DialContext(ctx, "tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(int(port))))
		}
		restoreErr := unix.Setns(int(original.Fd()), unix.CLONE_NEWNET)
		restored = restoreErr == nil
		if restoreErr != nil {
			if conn != nil {
				conn.Close()
				conn = nil
			}
			dialErr = errors.Join(dialErr, fmt.Errorf("restore allocator network namespace: %w", restoreErr))
		}
		done <- result{conn: conn, err: dialErr}
	}()
	r := <-done
	return r.conn, r.err
}

// Fresh OCI network namespaces have loopback down. Bring up only that
// interface, inside the already verified execution namespace.
func enableLoopback() error {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	req, err := unix.NewIfreq("lo")
	if err != nil {
		return err
	}
	if err := unix.IoctlIfreq(fd, unix.SIOCGIFFLAGS, req); err != nil {
		return err
	}
	if req.Uint16()&unix.IFF_UP != 0 {
		return nil
	}
	req.SetUint16(req.Uint16() | unix.IFF_UP)
	if err := unix.IoctlIfreq(fd, unix.SIOCSIFFLAGS, req); err != nil {
		return fmt.Errorf("enable execution loopback: %w", err)
	}
	return nil
}
