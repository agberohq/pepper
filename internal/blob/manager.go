package blob

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/jack"
)

type Manager struct {
	dir    string
	reaper *jack.Reaper

	mu    sync.RWMutex
	blobs map[string]string
}

func NewManager(dir string, reaper *jack.Reaper) (*Manager, error) {
	if dir == "" {
		dir = PlatformDefaultDir()
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("blob.NewManager: cannot create blob dir %q: %w", dir, err)
	}
	return &Manager{
		dir:    dir,
		reaper: reaper,
		blobs:  make(map[string]string),
	}, nil
}

func (m *Manager) Write(data []byte, ttl time.Duration, opts ...WriteOption) (*Blob, error) {
	cfg := writeConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	blobID := "blob_" + ulid.Make().String()
	path := filepath.Join(m.dir, FilePrefix+blobID+".dat")

	if err := os.WriteFile(path, data, 0600); err != nil {
		return nil, fmt.Errorf("blob.Write: %w", err)
	}
	return m.register(blobID, path, int64(len(data)), ttl, cfg), nil
}

// WriteFile copies a file from disk into the blob directory.
// The original file is not modified.
func (m *Manager) WriteFile(srcPath string, ttl time.Duration, opts ...WriteOption) (*Blob, error) {
	f, err := os.Open(srcPath)
	if err != nil {
		return nil, fmt.Errorf("blob.WriteFile: open %q: %w", srcPath, err)
	}
	defer f.Close()
	return m.WriteFromReader(f, ttl, opts...)
}

func (m *Manager) WriteFromReader(r io.Reader, ttl time.Duration, opts ...WriteOption) (*Blob, error) {
	cfg := writeConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	blobID := "blob_" + ulid.Make().String()
	path := filepath.Join(m.dir, FilePrefix+blobID+".dat")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return nil, fmt.Errorf("blob.WriteFromReader: create: %w", err)
	}
	size, err := io.Copy(f, r)
	f.Close()
	if err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("blob.WriteFromReader: copy: %w", err)
	}
	return m.register(blobID, path, size, ttl, cfg), nil
}

func (m *Manager) register(blobID, path string, size int64, ttl time.Duration, cfg writeConfig) *Blob {
	ref := Ref{
		IsPepperBlob: true,
		ID:           blobID,
		Path:         path,
		Size:         size,
		DType:        cfg.dtype,
		Shape:        cfg.shape,
		Format:       cfg.format,
	}

	m.mu.Lock()
	m.blobs[blobID] = path
	m.mu.Unlock()

	m.reaper.TouchAt(blobID, time.Now().Add(ttl))

	return &Blob{ref: ref, manager: m}
}

func (m *Manager) keepAlive(blobID string, newExpiry time.Time) {
	m.reaper.TouchAt(blobID, newExpiry)
}

func (m *Manager) remove(blobID string) {
	m.mu.Lock()
	path, ok := m.blobs[blobID]
	if ok {
		delete(m.blobs, blobID)
	}
	m.mu.Unlock()

	if ok {
		_ = os.Remove(path)
	}
	m.reaper.Remove(blobID)
}

func (m *Manager) Reap(blobID string) {
	m.remove(blobID)
}

func (m *Manager) ScanOrphans() (int, error) {
	entries, err := os.ReadDir(m.dir)
	if err != nil {
		return 0, fmt.Errorf("blob.ScanOrphans: %w", err)
	}
	cutoff := time.Now().Add(-OrphanMaxAge)
	removed := 0
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), FilePrefix) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			if os.Remove(filepath.Join(m.dir, entry.Name())) == nil {
				removed++
			}
		}
	}
	return removed, nil
}

func (m *Manager) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.blobs)
}
