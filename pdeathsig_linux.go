//go:build linux

package pepper

import (
	"os/exec"
	"syscall"
)

// setPdeathsig sets SIGKILL on the child process when the parent dies.
// Linux only — Pdeathsig is a Linux-specific field in SysProcAttr.
func setPdeathsig(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
}
