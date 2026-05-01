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

type process struct {
	Pid    int
	Handle uintptr
}

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
	if m.closed.Load() {
		return nil
	}
	return windows.AssignProcessToJobObject(
		m.exitGroup,
		windows.Handle((*process)(unsafe.Pointer(p)).Handle))
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
