package blob

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	FilePrefix   = "pepper_blob_"
	OrphanMaxAge = 24 * time.Hour
)

type Ref struct {
	IsPepperBlob bool   `msgpack:"_pepper_blob"`
	ID           string `msgpack:"id"`
	Path         string `msgpack:"path"`
	Size         int64  `msgpack:"size"`
	DType        string `msgpack:"dtype"`
	Shape        []int  `msgpack:"shape"`
	Format       string `msgpack:"format"`
}

// Scheme returns the URL scheme of Path.
// Returns "file" for bare filesystem paths (no "://" present).
func (r Ref) Scheme() string {
	if i := strings.Index(r.Path, "://"); i > 0 {
		return r.Path[:i]
	}
	return "file"
}

// IsLocal reports whether the blob lives on the local filesystem.
func (r Ref) IsLocal() bool { return r.Scheme() == "file" }

// IsRemote reports whether the blob lives on a remote object store (e.g. s3://).
func (r Ref) IsRemote() bool { return !r.IsLocal() }

type Blob struct {
	ref     Ref
	manager *Manager

	mu     sync.Mutex
	closed bool
}

func (b *Blob) Ref() Ref     { return b.ref }
func (b *Blob) ID() string   { return b.ref.ID }
func (b *Blob) Path() string { return b.ref.Path }

func (b *Blob) KeepAlive(newDeadlineMs int64) {
	if b.manager != nil {
		b.manager.keepAlive(b.ref.ID, time.UnixMilli(newDeadlineMs))
	}
}

// Close deletes the blob file immediately.
// Safe to call multiple times. Remove errors are silently ignored;
// ScanOrphans cleans up any leftovers at next startup.
func (b *Blob) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	if b.manager != nil {
		b.manager.remove(b.ref.ID)
		return nil
	}
	_ = os.Remove(b.ref.Path)
	return nil
}

func PlatformDefaultDir() string {
	if isLinux() {
		if info, err := os.Stat("/dev/shm"); err == nil && info.IsDir() {
			dir := filepath.Join("/dev/shm", "pepper")
			_ = os.MkdirAll(dir, 0700)
			return dir
		}
	}
	dir := filepath.Join(os.TempDir(), "pepper")
	_ = os.MkdirAll(dir, 0700)
	return dir
}

func isLinux() bool {
	_, err := os.Stat("/proc/self")
	return err == nil
}

type writeConfig struct {
	dtype  string
	shape  []int
	format string
}

type WriteOption func(*writeConfig)

func WithDType(dtype string) WriteOption {
	return func(c *writeConfig) { c.dtype = dtype }
}

func WithShape(shape ...int) WriteOption {
	return func(c *writeConfig) { c.shape = shape }
}

func WithFormat(format string) WriteOption {
	return func(c *writeConfig) { c.format = format }
}
