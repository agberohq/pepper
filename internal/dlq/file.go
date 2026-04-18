package dlq

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// File writes DLQ entries to a directory on disk.
// Each entry creates:
//   - {origin_id}.json    — entry metadata
//   - {origin_id}.msgpack — raw request payload (if provided via WriteWithPayload)
type File struct {
	dir string
	mu  sync.Mutex
}

// NewFile creates a File DLQ writing to dir.
// The directory is created if it does not exist.
func NewFile(dir string) *File {
	_ = os.MkdirAll(dir, 0700)
	return &File{dir: dir}
}

func (d *File) Write(entry Entry) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("dlq.File.Write: marshal: %w", err)
	}
	path := filepath.Join(d.dir, sanitize(entry.OriginID)+".json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("dlq.File.Write: write %q: %w", path, err)
	}
	return nil
}

// WriteWithPayload records an entry and saves the raw payload bytes separately.
func (d *File) WriteWithPayload(entry Entry, payload []byte) error {
	if len(payload) > 0 {
		payloadPath := filepath.Join(d.dir, sanitize(entry.OriginID)+".msgpack")
		if err := os.WriteFile(payloadPath, payload, 0600); err == nil {
			entry.PayloadPath = payloadPath
		}
	}
	return d.Write(entry)
}

// List returns all DLQ origin IDs present on disk.
func (d *File) List() ([]string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".json") {
			ids = append(ids, name[:len(name)-len(".json")])
		}
	}
	return ids, nil
}
