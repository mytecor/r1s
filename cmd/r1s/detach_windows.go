//go:build windows

package main

import "syscall"

// detachedSysProcAttr returns the process attributes for the detached child on
// Windows. syscall.SysProcAttr has no Setsid field there, and Windows has no
// POSIX session concept, so we leave the attributes nil and rely on the child
// process being independently spawned.
func detachedSysProcAttr() *syscall.SysProcAttr {
	return nil
}
