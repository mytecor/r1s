//go:build !windows

package main

import "syscall"

// detachedSysProcAttr returns process attributes that make the detached child
// a session leader, so a terminal interrupt aimed at the foreground parent
// never reaches the background run. Windows has no session/process-group
// concept here, so it leaves the attributes nil (see detach_windows.go).
func detachedSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}
