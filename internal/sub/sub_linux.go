//go:build linux

package sub

import (
	"os"
	"os/exec"
	"syscall"
)

type unixManager struct{}

func newManager() (Manager, error) { return &unixManager{}, nil }

// ConfigureCommand sets SIGKILL on the child when the parent process dies.
// Pdeathsig is a Linux-only kernel feature; this file is build-tagged linux.
func (m *unixManager) ConfigureCommand(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Pdeathsig = syscall.SIGKILL
	return nil
}

func (m *unixManager) AddChildProcess(p *os.Process) error { return nil }
func (m *unixManager) Dispose() error                      { return nil }
