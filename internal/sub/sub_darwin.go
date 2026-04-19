//go:build darwin || freebsd || openbsd || netbsd

package sub

import (
	"os"
	"os/exec"
	"sync"
	"syscall"
)

type unixManager struct {
	mu   sync.Mutex
	pids []int // tracked child PIDs
}

func newManager() (Manager, error) { return &unixManager{}, nil }

// ConfigureCommand puts the child into its own process group (Setpgid).
// On macOS/BSD, Pdeathsig does not exist. Setpgid means the child's PGID
// equals its PID, so Dispose() can kill the whole group with Kill(-pid, SIGKILL).
// For normal shutdown, exec.CommandContext handles it via the process directly.
func (m *unixManager) ConfigureCommand(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
	return nil
}

// AddChildProcess registers a started process for cleanup on abnormal parent exit.
// It does NOT kill the process — that only happens in Dispose().
func (m *unixManager) AddChildProcess(p *os.Process) error {
	m.mu.Lock()
	m.pids = append(m.pids, p.Pid)
	m.mu.Unlock()
	return nil
}

// Dispose kills all tracked process groups. Called when the parent is shutting
// down abnormally (bgCtx cancelled). Normal shutdown is handled by
// exec.CommandContext which sends SIGKILL to the process directly.
func (m *unixManager) Dispose() error {
	m.mu.Lock()
	pids := m.pids
	m.pids = nil
	m.mu.Unlock()
	for _, pid := range pids {
		// -pid targets the process group (pgid == pid because Setpgid was set).
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	}
	return nil
}
