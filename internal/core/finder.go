package core

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/agberohq/pepper/internal/registry"
)

// RuntimeFinder handles discovery of Python runtime files
type RuntimeFinder struct {
	cache   atomic.Value // stores string
	homeEnv string
	pathEnv string
}

// NewRuntimeFinder creates a new runtime finder instance
func NewRuntimeFinder() *RuntimeFinder {
	return &RuntimeFinder{
		homeEnv: os.Getenv("PEEPER_HOME"),
		pathEnv: os.Getenv("PATH"),
	}
}

// Python locates the Python interpreter
func (f *RuntimeFinder) Python() string {
	// Windows: check for python.exe, py.exe
	if runtime.GOOS == "windows" {
		for _, name := range []string{"python.exe", "python3.exe", "py.exe"} {
			if p, err := exec.LookPath(name); err == nil {
				return p
			}
		}
		return "python.exe"
	}

	// Unix-like systems
	for _, name := range []string{"python3", "python"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return "python3"
}

// Runtime locates runtime.py with validation
func (f *RuntimeFinder) Runtime(specs []*registry.Spec) string {
	// Check cache
	if cached := f.cache.Load(); cached != nil {
		if path := cached.(string); path != "" && f.fileExists(path) {
			return path
		}
		f.cache.Store((*string)(nil))
	}

	// Search in priority order
	searchers := []func() string{
		f.peeperHome,
		f.currentDir,
		f.path,
		f.executableDir,
		f.systemPaths,
		func() string { return f.legacyFallback(specs) },
	}

	for _, search := range searchers {
		if path := search(); path != "" {
			f.cache.Store(path)
			return path
		}
	}

	return ""
}

// ClearCache forces re-scanning on next call
func (f *RuntimeFinder) ClearCache() {
	f.cache.Store((*string)(nil))
}

// Private search methods
func (f *RuntimeFinder) peeperHome() string {
	if f.homeEnv == "" {
		return ""
	}

	subdirs := []string{"", "python", "runtime", "runtime/python"}
	for _, sub := range subdirs {
		dir := filepath.Join(f.homeEnv, sub)
		if f.validRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.py")
		}
	}
	return ""
}

func (f *RuntimeFinder) currentDir() string {
	dir, err := filepath.Abs(".")
	if err != nil {
		return ""
	}

	for i := 0; i <= 3; i++ {
		if f.validRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.py")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func (f *RuntimeFinder) path() string {
	for _, dir := range filepath.SplitList(f.pathEnv) {
		if f.validRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.py")
		}

		for _, sub := range []string{"peeper", "python", "runtime"} {
			subDir := filepath.Join(dir, sub)
			if f.validRuntimeDir(subDir) {
				return filepath.Join(subDir, "runtime.py")
			}
		}
	}
	return ""
}

func (f *RuntimeFinder) executableDir() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}

	dirs := []string{
		filepath.Dir(exe),
		filepath.Dir(filepath.Dir(exe)),
	}

	for _, dir := range dirs {
		if f.validRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.py")
		}
	}
	return ""
}

func (f *RuntimeFinder) systemPaths() string {
	var paths []string

	if runtime.GOOS == "windows" {
		paths = []string{
			`C:\Program Files\peeper`,
			`C:\Program Files\peeper\python`,
			`C:\ProgramData\peeper`,
			`%PROGRAMFILES%\peeper`,
			`%LOCALAPPDATA%\peeper`,
		}
		// Expand env vars in paths
		for i, p := range paths {
			paths[i] = os.ExpandEnv(p)
		}
	} else {
		paths = []string{
			"/usr/local/bin",
			"/usr/bin",
			"/opt/peeper",
			"/opt/peeper/python",
			"/usr/local/share/peeper",
			"/usr/share/peeper",
		}
	}

	for _, dir := range paths {
		if f.validRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.py")
		}
	}
	return ""
}

func (f *RuntimeFinder) validRuntimeDir(dir string) bool {
	runtime := filepath.Join(dir, "runtime.py")
	cap := filepath.Join(dir, "cap.py")
	return f.fileExists(runtime) && f.fileExists(cap)
}

func (f *RuntimeFinder) legacyFallback(specs []*registry.Spec) string {
	// Original logic preserved exactly
	if f.fileExists("internal/runtime/python/runtime.py") {
		return "internal/runtime/python/runtime.py"
	}
	if f.fileExists("../internal/runtime/python/runtime.py") {
		return "../internal/runtime/python/runtime.py"
	}

	for _, s := range specs {
		if s.Source == "" {
			continue
		}
		dir := filepath.Dir(s.Source)
		for _, c := range []string{
			filepath.Join(dir, "runtime.py"),
			filepath.Join(filepath.Dir(dir), "runtime.py"),
		} {
			if _, err := os.Stat(c); err == nil {
				return c
			}
		}
	}

	if exe, err := os.Executable(); err == nil {
		if c := filepath.Join(filepath.Dir(exe), "runtime.py"); f.fileExists(c) {
			return c
		}
	}

	if f.fileExists("runtime.py") {
		return "runtime.py"
	}
	return ""
}

func (f *RuntimeFinder) fileExists(path string) bool { _, err := os.Stat(path); return err == nil }
