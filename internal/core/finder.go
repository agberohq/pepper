package core

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/agberohq/pepper/internal/registry"
)

// RuntimeFinder handles discovery of Python, Julia, and Node.js runtime files.
type RuntimeFinder struct {
	cache      atomic.Value // stores string — path to runtime.py
	juliaCache atomic.Value // stores string — path to runtime.jl
	nodeCache  atomic.Value // stores string — path to runtime.js
	homeEnv    string
	pathEnv    string
}

// NewRuntimeFinder creates a new runtime finder instance
func NewRuntimeFinder() *RuntimeFinder {
	return &RuntimeFinder{
		homeEnv: os.Getenv("PEEPER_HOME"),
		pathEnv: os.Getenv("PATH"),
	}
}

// Python locates the Python interpreter.
func (f *RuntimeFinder) Python() string {
	if runtime.GOOS == "windows" {
		for _, name := range []string{"python.exe", "python3.exe", "py.exe"} {
			if p, err := exec.LookPath(name); err == nil {
				return p
			}
		}
		return "python.exe"
	}
	for _, name := range []string{"python3", "python"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return "python3"
}

// Julia locates the Julia interpreter.
func (f *RuntimeFinder) Julia() string {
	if runtime.GOOS == "windows" {
		for _, name := range []string{"julia.exe"} {
			if p, err := exec.LookPath(name); err == nil {
				return p
			}
		}
		// Common Windows install locations
		for _, dir := range []string{
			os.ExpandEnv(`%LOCALAPPDATA%\Programs\Julia\bin`),
			`C:\Julia\bin`,
		} {
			candidate := filepath.Join(dir, "julia.exe")
			if f.fileExists(candidate) {
				return candidate
			}
		}
		return "julia.exe"
	}
	if p, err := exec.LookPath("julia"); err == nil {
		return p
	}
	// Common Unix install locations
	for _, candidate := range []string{
		"/usr/local/bin/julia",
		"/usr/bin/julia",
		"/opt/julia/bin/julia",
	} {
		if f.fileExists(candidate) {
			return candidate
		}
	}
	return "julia"
}

// Runtime locates runtime.py with validation.
func (f *RuntimeFinder) Runtime(specs []*registry.Spec) string {
	if cached := f.cache.Load(); cached != nil {
		if path := cached.(string); path != "" && f.fileExists(path) {
			return path
		}
		f.cache.Store((*string)(nil))
	}

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

// JuliaRuntime locates runtime.jl with the same search strategy as runtime.py.
func (f *RuntimeFinder) JuliaRuntime(specs []*registry.Spec) string {
	if cached := f.juliaCache.Load(); cached != nil {
		if path := cached.(string); path != "" && f.fileExists(path) {
			return path
		}
		f.juliaCache.Store((*string)(nil))
	}

	searchers := []func() string{
		f.juliaHome,
		f.juliaCurrentDir,
		f.juliaExecutableDir,
		f.juliaSystemPaths,
		func() string { return f.juliaLegacyFallback(specs) },
	}

	for _, search := range searchers {
		if path := search(); path != "" {
			f.juliaCache.Store(path)
			return path
		}
	}
	return ""
}

// HasPython checks if a Python interpreter is available on the system.
func (f *RuntimeFinder) HasPython() bool {
	return exec.Command(f.Python(), "--version").Run() == nil
}

// HasPythonMsgpack checks if the Python interpreter has msgpack available.
func (f *RuntimeFinder) HasPythonMsgpack() bool {
	return exec.Command(f.Python(), "-c", "import msgpack").Run() == nil
}

// HasPythonRedis checks if Python has redis client library available.
func (f *RuntimeFinder) HasPythonRedis() bool {
	return exec.Command(f.Python(), "-c", "import redis").Run() == nil
}

// HasPythonNats checks if Python has nats client library available.
func (f *RuntimeFinder) HasPythonNats() bool {
	return exec.Command(f.Python(), "-c", "import nats").Run() == nil
}

// HasJulia checks if a Julia interpreter is available on the system.
func (f *RuntimeFinder) HasJulia() bool {
	return exec.Command(f.Julia(), "--version").Run() == nil
}

// HasJuliaMsgpack checks if Julia has MsgPack.jl available.
func (f *RuntimeFinder) HasJuliaMsgpack() bool {
	return exec.Command(f.Julia(), "--startup-file=no", "-e",
		`using MsgPack`).Run() == nil
}

// ClearCache forces re-scanning on next call.
func (f *RuntimeFinder) ClearCache() {
	f.cache.Store((*string)(nil))
	f.juliaCache.Store((*string)(nil))
	f.nodeCache.Store((*string)(nil))
}

// Node locates the Node.js interpreter.
func (f *RuntimeFinder) Node() string {
	if runtime.GOOS == "windows" {
		for _, name := range []string{"node.exe", "nodejs.exe"} {
			if p, err := exec.LookPath(name); err == nil {
				return p
			}
		}
		return "node.exe"
	}
	for _, name := range []string{"node", "nodejs"} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return "node"
}

// NodeRuntime locates runtime.js with the same search strategy as runtime.py.
func (f *RuntimeFinder) NodeRuntime(specs []*registry.Spec) string {
	if cached := f.nodeCache.Load(); cached != nil {
		if path := cached.(string); path != "" && f.fileExists(path) {
			return path
		}
		f.nodeCache.Store((*string)(nil))
	}
	searchers := []func() string{
		f.nodeHome,
		f.nodeCurrentDir,
		f.nodeExecutableDir,
		f.nodeSystemPaths,
		func() string { return f.nodeLegacyFallback(specs) },
	}
	for _, search := range searchers {
		if path := search(); path != "" {
			f.nodeCache.Store(path)
			return path
		}
	}
	return ""
}

// HasNode checks if Node.js is available.
func (f *RuntimeFinder) HasNode() bool {
	return exec.Command(f.Node(), "--version").Run() == nil
}

// HasNodeMsgpack checks if @msgpack/msgpack is available.
func (f *RuntimeFinder) HasNodeMsgpack() bool {
	return exec.Command(f.Node(), "-e", `require("@msgpack/msgpack")`).Run() == nil
}

// Python search methods

func (f *RuntimeFinder) peeperHome() string {
	if f.homeEnv == "" {
		return ""
	}
	for _, sub := range []string{"", "python", "runtime", "runtime/python"} {
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
	for _, dir := range []string{filepath.Dir(exe), filepath.Dir(filepath.Dir(exe))} {
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
			os.ExpandEnv(`%PROGRAMFILES%\peeper`),
			os.ExpandEnv(`%PROGRAMFILES%\peeper\python`),
			os.ExpandEnv(`%LOCALAPPDATA%\peeper`),
		}
	} else {
		paths = []string{
			"/usr/local/bin", "/usr/bin",
			"/opt/peeper", "/opt/peeper/python",
			"/usr/local/share/peeper", "/usr/share/peeper",
		}
	}
	for _, dir := range paths {
		if f.validRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.py")
		}
	}
	return ""
}

func (f *RuntimeFinder) legacyFallback(specs []*registry.Spec) string {
	if f.fileExists("runtime/python/runtime.py") {
		return "runtime/python/runtime.py"
	}
	if f.fileExists("../runtime/python/runtime.py") {
		return "../runtime/python/runtime.py"
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

func (f *RuntimeFinder) validRuntimeDir(dir string) bool {
	return f.fileExists(filepath.Join(dir, "runtime.py")) &&
		f.fileExists(filepath.Join(dir, "cap.py"))
}

// Julia search methods

func (f *RuntimeFinder) juliaHome() string {
	if f.homeEnv == "" {
		return ""
	}
	for _, sub := range []string{"", "julia", "runtime", "runtime/julia"} {
		dir := filepath.Join(f.homeEnv, sub)
		if f.validJuliaRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.jl")
		}
	}
	return ""
}

func (f *RuntimeFinder) juliaCurrentDir() string {
	dir, err := filepath.Abs(".")
	if err != nil {
		return ""
	}
	for i := 0; i <= 3; i++ {
		if f.validJuliaRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.jl")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func (f *RuntimeFinder) juliaExecutableDir() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	for _, dir := range []string{filepath.Dir(exe), filepath.Dir(filepath.Dir(exe))} {
		if f.validJuliaRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.jl")
		}
	}
	return ""
}

func (f *RuntimeFinder) juliaSystemPaths() string {
	var paths []string
	if runtime.GOOS == "windows" {
		paths = []string{
			os.ExpandEnv(`%PROGRAMFILES%\peeper`),
			os.ExpandEnv(`%PROGRAMFILES%\peeper\julia`),
			os.ExpandEnv(`%LOCALAPPDATA%\peeper`),
		}
	} else {
		paths = []string{
			"/usr/local/bin", "/usr/bin",
			"/opt/peeper", "/opt/peeper/julia",
			"/usr/local/share/peeper", "/usr/share/peeper",
		}
	}
	for _, dir := range paths {
		if f.validJuliaRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.jl")
		}
	}
	return ""
}

func (f *RuntimeFinder) juliaLegacyFallback(specs []*registry.Spec) string {
	for _, candidate := range []string{
		"runtime/julia/runtime.jl",
		"../runtime/julia/runtime.jl",
	} {
		if f.fileExists(candidate) {
			return candidate
		}
	}
	for _, s := range specs {
		if s.Source == "" || s.Runtime != registry.RuntimeJulia {
			continue
		}
		dir := filepath.Dir(s.Source)
		for _, c := range []string{
			filepath.Join(dir, "runtime.jl"),
			filepath.Join(filepath.Dir(dir), "runtime.jl"),
		} {
			if f.fileExists(c) {
				return c
			}
		}
	}
	if exe, err := os.Executable(); err == nil {
		if c := filepath.Join(filepath.Dir(exe), "runtime.jl"); f.fileExists(c) {
			return c
		}
	}
	if f.fileExists("runtime.jl") {
		return "runtime.jl"
	}
	return ""
}

func (f *RuntimeFinder) validJuliaRuntimeDir(dir string) bool {
	return f.fileExists(filepath.Join(dir, "runtime.jl")) &&
		f.fileExists(filepath.Join(dir, "cap.jl"))
}

// Node.js search methods

func (f *RuntimeFinder) nodeHome() string {
	if f.homeEnv == "" {
		return ""
	}
	for _, sub := range []string{"", "node", "runtime", "runtime/node"} {
		dir := filepath.Join(f.homeEnv, sub)
		if f.validNodeRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.js")
		}
	}
	return ""
}

func (f *RuntimeFinder) nodeCurrentDir() string {
	dir, err := filepath.Abs(".")
	if err != nil {
		return ""
	}
	for i := 0; i <= 3; i++ {
		if f.validNodeRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.js")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return ""
}

func (f *RuntimeFinder) nodeExecutableDir() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	for _, dir := range []string{filepath.Dir(exe), filepath.Dir(filepath.Dir(exe))} {
		if f.validNodeRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.js")
		}
	}
	return ""
}

func (f *RuntimeFinder) nodeSystemPaths() string {
	var paths []string
	if runtime.GOOS == "windows" {
		paths = []string{
			os.ExpandEnv(`%PROGRAMFILES%\peeper`),
			os.ExpandEnv(`%LOCALAPPDATA%\peeper`),
		}
	} else {
		paths = []string{
			"/usr/local/bin", "/usr/bin",
			"/opt/peeper", "/opt/peeper/node",
			"/usr/local/share/peeper", "/usr/share/peeper",
		}
	}
	for _, dir := range paths {
		if f.validNodeRuntimeDir(dir) {
			return filepath.Join(dir, "runtime.js")
		}
	}
	return ""
}

func (f *RuntimeFinder) nodeLegacyFallback(specs []*registry.Spec) string {
	for _, candidate := range []string{"runtime/node/runtime.js", "../runtime/node/runtime.js"} {
		if f.fileExists(candidate) {
			return candidate
		}
	}
	for _, s := range specs {
		if s.Source == "" || s.Runtime != registry.RuntimeNode {
			continue
		}
		dir := filepath.Dir(s.Source)
		for _, c := range []string{
			filepath.Join(dir, "runtime.js"),
			filepath.Join(filepath.Dir(dir), "runtime.js"),
		} {
			if f.fileExists(c) {
				return c
			}
		}
	}
	if exe, err := os.Executable(); err == nil {
		if c := filepath.Join(filepath.Dir(exe), "runtime.js"); f.fileExists(c) {
			return c
		}
	}
	if f.fileExists("runtime.js") {
		return "runtime.js"
	}
	return ""
}

func (f *RuntimeFinder) validNodeRuntimeDir(dir string) bool {
	return f.fileExists(filepath.Join(dir, "runtime.js")) &&
		f.fileExists(filepath.Join(dir, "cap.js"))
}

func (f *RuntimeFinder) fileExists(path string) bool { _, err := os.Stat(path); return err == nil }
