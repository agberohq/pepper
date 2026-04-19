//go:build windows

package sub

import (
	"os"
	"os/exec"
	"sync"
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
	return windows.AssignProcessToJobObject(
		m.exitGroup,
		windows.Handle((*process)(unsafe.Pointer(p)).Handle))
}

func (m *windowsManager) Dispose() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return windows.CloseHandle(m.exitGroup)
}
