package benchmark

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
)

// gitShortCommit returns the current repository's short commit hash, or an
// error when not inside a git repository or git is unavailable. It runs `git
// rev-parse --short HEAD` in the current working directory. Failing to find a
// commit is non-fatal for a benchmark report (rows stay identifiable by
// host/go/OS even without VCS context).
func gitShortCommit() (string, error) {
	cmd := exec.Command("git", "rev-parse", "--short", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// cpuModel returns a short CPU model string where the platform exposes one.
// It is best-effort; empty and false when unknown (e.g. some macOS builds).
func cpuModel() (string, bool) {
	switch runtime.GOOS {
	case "darwin":
		return readSysctl("machdep.cpu.brand_string"), true
	case "linux":
		data, err := os.ReadFile("/proc/cpuinfo")
		if err != nil {
			return "", false
		}
		for _, line := range strings.Split(string(data), "\n") {
			if after, ok := strings.CutPrefix(line, "model name"); ok {
				return strings.TrimSpace(strings.TrimPrefix(after, " :")), true
			}
		}
		return "", false
	default:
		return "", false
	}
}

// readSysctl reads one sysctl key via the /usr/sbin/sysctl binary.
func readSysctl(key string) string {
	out, err := exec.Command("/usr/sbin/sysctl", "-n", key).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}
