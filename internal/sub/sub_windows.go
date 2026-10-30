//go:build windows

package sub

import (
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/windows"
)

type windowsManager struct {
	exitGroup windows.Handle
	mu        sync.Mutex
	closed    atomic.Bool
}

func newManager() (Manager, error) {
	handle, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	if _, err := windows.SetInformationJobObject(
		handle,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info))); err != nil {
		return nil, err
	}

	return &windowsManager{exitGroup: handle}, nil
}

func (m *windowsManager) ConfigureCommand(cmd *exec.Cmd) error { return nil }

func (m *windowsManager) AddChildProcess(p *os.Process) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed.Load() || p == nil {
		return nil
	}
	// `os.Process` no longer exposes its handle as a raw uintptr (since Go 1.25
	// it holds a pointer to an OS-specific struct), so re-open the child by PID
	// instead of casting the struct layout. The requested rights are the minimum
	// AssignProcessToJobObject needs.
	h, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
		false,
		uint32(p.Pid),
	)
	if err != nil {
		return err
	}
	assignErr := windows.AssignProcessToJobObject(m.exitGroup, h)
	_ = windows.CloseHandle(h)
	return assignErr
}

// Dispose closes the Job Object handle. Safe to call multiple times —
// subsequent calls are no-ops. CloseHandle on an already-closed handle
// returns ERROR_INVALID_HANDLE on Windows, so we guard with an atomic flag.
func (m *windowsManager) Dispose() error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return windows.CloseHandle(m.exitGroup)
}
