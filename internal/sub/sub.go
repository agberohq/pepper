package sub

import (
	"os"
	"os/exec"
	"sync"
)

// Manager defines the cross-platform interface for child process lifecycle management.
type Manager interface {
	// ConfigureCommand prepares the cmd before starting (e.g., setting SysProcAttr).
	ConfigureCommand(cmd *exec.Cmd) error
	// AddChildProcess registers a started process to be killed when the parent dies.
	AddChildProcess(p *os.Process) error
	// Dispose cleans up OS resources (e.g., closing Job Object handles on Windows,
	// killing tracked process groups on macOS).
	Dispose() error
}

// instance-level API: each Pepper runtime owns its own Manager so concurrent
// tests don't race on a shared global.

// New creates a new platform-specific Manager. Callers own the returned value
// and must call Dispose() when done.
func New() (Manager, error) {
	return newManager()
}

// package-level convenience API (used by runtime.go)
// A single process-wide manager is still available for the common single-instance
// case, protected by a mutex so concurrent tests don't race on Init/Dispose.

var (
	globalMu      sync.Mutex
	globalManager Manager
)

func Init() error {
	globalMu.Lock()
	defer globalMu.Unlock()
	m, err := newManager()
	if err != nil {
		return err
	}
	globalManager = m
	return nil
}

func Configure(cmd *exec.Cmd) error {
	globalMu.Lock()
	m := globalManager
	globalMu.Unlock()
	return m.ConfigureCommand(cmd)
}

func Track(p *os.Process) error {
	globalMu.Lock()
	m := globalManager
	globalMu.Unlock()
	return m.AddChildProcess(p)
}

func Dispose() error {
	globalMu.Lock()
	m := globalManager
	globalManager = nil
	globalMu.Unlock()
	if m == nil {
		return nil
	}
	return m.Dispose()
}
