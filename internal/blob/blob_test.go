package blob

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/olekukonko/jack"
)

func newManager(t *testing.T) *Manager {
	t.Helper()
	reaper := jack.NewReaper(0,
		jack.ReaperWithShards(4),
		jack.ReaperWithHandler(func(_ context.Context, _ string) {}),
	)
	t.Cleanup(reaper.Stop)
	m, err := NewManager(t.TempDir(), reaper)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return m
}

func TestPlatformDefaultDirNonEmpty(t *testing.T) {
	dir := PlatformDefaultDir()
	if dir == "" {
		t.Error("PlatformDefaultDir returned empty string")
	}
}

func TestPlatformDefaultDirIsAbsolute(t *testing.T) {
	dir := PlatformDefaultDir()
	if !filepath.IsAbs(dir) {
		t.Errorf("PlatformDefaultDir = %q is not absolute", dir)
	}
}

func TestPlatformDefaultDirContainsPepper(t *testing.T) {
	dir := PlatformDefaultDir()
	if !strings.Contains(dir, "pepper") {
		t.Errorf("PlatformDefaultDir = %q does not contain 'pepper'", dir)
	}
}

func TestNewManagerCreatesDir(t *testing.T) {
	reaper := jack.NewReaper(0,
		jack.ReaperWithShards(4),
		jack.ReaperWithHandler(func(_ context.Context, _ string) {}),
	)
	t.Cleanup(reaper.Stop)

	nested := filepath.Join(t.TempDir(), "nested", "blobs")
	m, err := NewManager(nested, reaper)
	if err != nil {
		t.Fatalf("NewManager on nested path: %v", err)
	}
	if m == nil {
		t.Fatal("NewManager returned nil")
	}
	if _, err := os.Stat(nested); os.IsNotExist(err) {
		t.Errorf("NewManager did not create dir %q", nested)
	}
}

func TestWriteCreatesFile(t *testing.T) {
	m := newManager(t)
	b, err := m.Write([]byte("hello audio"), time.Minute)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if b == nil {
		t.Fatal("Write returned nil blob")
	}
	if _, err := os.Stat(b.Path()); os.IsNotExist(err) {
		t.Errorf("blob file not created at %q", b.Path())
	}
}

func TestWriteFileContents(t *testing.T) {
	m := newManager(t)
	data := []byte{0x00, 0x01, 0xFE, 0xFF}
	b, err := m.Write(data, time.Minute)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := os.ReadFile(b.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("file contents = %v, want %v", got, data)
	}
}

func TestWriteRefFields(t *testing.T) {
	m := newManager(t)
	data := make([]byte, 1024)
	b, err := m.Write(data, time.Minute,
		WithDType("float32"),
		WithShape(1, 32, 32),
		WithFormat("image/jpeg"),
	)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	ref := b.Ref()
	if !ref.IsPepperBlob {
		t.Error("IsPepperBlob should be true")
	}
	if ref.ID == "" {
		t.Error("ID should not be empty")
	}
	if !strings.HasPrefix(ref.ID, "blob_") {
		t.Errorf("ID = %q, want blob_ prefix", ref.ID)
	}
	if ref.Path == "" {
		t.Error("Path should not be empty")
	}
	if ref.Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", ref.Size, len(data))
	}
	if ref.DType != "float32" {
		t.Errorf("DType = %q, want float32", ref.DType)
	}
	if len(ref.Shape) != 3 || ref.Shape[0] != 1 || ref.Shape[1] != 32 || ref.Shape[2] != 32 {
		t.Errorf("Shape = %v, want [1,32,32]", ref.Shape)
	}
	if ref.Format != "image/jpeg" {
		t.Errorf("Format = %q, want image/jpeg", ref.Format)
	}
}

func TestWriteTracksLen(t *testing.T) {
	m := newManager(t)
	if m.Len() != 0 {
		t.Fatalf("Len should be 0 before Write")
	}
	m.Write([]byte("a"), time.Minute)
	m.Write([]byte("b"), time.Minute)
	if m.Len() != 2 {
		t.Errorf("Len = %d, want 2", m.Len())
	}
}

func TestWriteFromReader(t *testing.T) {
	m := newManager(t)
	data := []byte("streaming audio content")
	b, err := m.WriteFromReader(bytes.NewReader(data), time.Minute)
	if err != nil {
		t.Fatalf("WriteFromReader: %v", err)
	}
	got, err := os.ReadFile(b.Path())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Error("WriteFromReader: contents mismatch")
	}
	if b.Ref().Size != int64(len(data)) {
		t.Errorf("Size = %d, want %d", b.Ref().Size, len(data))
	}
}

func TestCloseDeletesFile(t *testing.T) {
	m := newManager(t)
	b, _ := m.Write([]byte("temp"), time.Minute)
	path := b.Path()

	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should be deleted after Close")
	}
}

func TestCloseDecreasesLen(t *testing.T) {
	m := newManager(t)
	b, _ := m.Write([]byte("x"), time.Minute)
	b.Close()
	if m.Len() != 0 {
		t.Errorf("Len = %d after Close, want 0", m.Len())
	}
}

func TestCloseIdempotent(t *testing.T) {
	m := newManager(t)
	b, _ := m.Write([]byte("x"), time.Minute)
	b.Close()
	if err := b.Close(); err != nil {
		t.Errorf("second Close returned error: %v", err)
	}
}

func TestKeepAliveDoesNotPanic(t *testing.T) {
	m := newManager(t)
	b, _ := m.Write([]byte("data"), time.Minute)
	defer b.Close()
	b.KeepAlive(time.Now().Add(2 * time.Minute).UnixMilli())
}

func TestKeepAliveAfterCloseDoesNotPanic(t *testing.T) {
	m := newManager(t)
	b, _ := m.Write([]byte("data"), time.Minute)
	b.Close()
	b.KeepAlive(time.Now().Add(time.Minute).UnixMilli())
}

func TestRefIsPepperBlobMarker(t *testing.T) {
	m := newManager(t)
	b, _ := m.Write([]byte("data"), time.Minute)
	defer b.Close()
	if !b.Ref().IsPepperBlob {
		t.Error("IsPepperBlob must be true for wire routing")
	}
}

func TestRefPathMatchesBlobPath(t *testing.T) {
	m := newManager(t)
	b, _ := m.Write([]byte("data"), time.Minute)
	defer b.Close()
	if b.Ref().Path != b.Path() {
		t.Errorf("Ref.Path = %q, Blob.Path = %q — must match", b.Ref().Path, b.Path())
	}
}

func TestScanOrphansRemovesOldFiles(t *testing.T) {
	dir := t.TempDir()
	reaper := jack.NewReaper(0,
		jack.ReaperWithShards(4),
		jack.ReaperWithHandler(func(_ context.Context, _ string) {}),
	)
	t.Cleanup(reaper.Stop)
	m, _ := NewManager(dir, reaper)

	orphanPath := filepath.Join(dir, FilePrefix+"orphan01.dat")
	os.WriteFile(orphanPath, []byte("orphaned"), 0600)
	old := time.Now().Add(-25 * time.Hour)
	os.Chtimes(orphanPath, old, old)

	n, err := m.ScanOrphans()
	if err != nil {
		t.Fatalf("ScanOrphans: %v", err)
	}
	if n != 1 {
		t.Errorf("ScanOrphans removed %d orphans, want 1", n)
	}
	if _, err := os.Stat(orphanPath); !os.IsNotExist(err) {
		t.Error("orphan file should be deleted")
	}
}

func TestScanOrphansKeepsRecentFiles(t *testing.T) {
	dir := t.TempDir()
	reaper := jack.NewReaper(0,
		jack.ReaperWithShards(4),
		jack.ReaperWithHandler(func(_ context.Context, _ string) {}),
	)
	t.Cleanup(reaper.Stop)
	m, _ := NewManager(dir, reaper)

	recentPath := filepath.Join(dir, FilePrefix+"recent01.dat")
	os.WriteFile(recentPath, []byte("recent"), 0600)

	n, err := m.ScanOrphans()
	if err != nil {
		t.Fatalf("ScanOrphans: %v", err)
	}
	if n != 0 {
		t.Errorf("ScanOrphans removed %d files, want 0", n)
	}
	if _, err := os.Stat(recentPath); os.IsNotExist(err) {
		t.Error("recent file should NOT be deleted")
	}
}

func TestScanOrphansIgnoresNonBlobFiles(t *testing.T) {
	dir := t.TempDir()
	reaper := jack.NewReaper(0,
		jack.ReaperWithShards(4),
		jack.ReaperWithHandler(func(_ context.Context, _ string) {}),
	)
	t.Cleanup(reaper.Stop)
	m, _ := NewManager(dir, reaper)

	otherPath := filepath.Join(dir, "other-file.dat")
	os.WriteFile(otherPath, []byte("other"), 0600)
	old := time.Now().Add(-25 * time.Hour)
	os.Chtimes(otherPath, old, old)

	n, err := m.ScanOrphans()
	if err != nil {
		t.Fatalf("ScanOrphans: %v", err)
	}
	if n != 0 {
		t.Errorf("ScanOrphans removed %d files, want 0 (non-blob file)", n)
	}
	if _, err := os.Stat(otherPath); os.IsNotExist(err) {
		t.Error("non-blob file should NOT be deleted")
	}
}

func TestReaperDeletesExpiredBlob(t *testing.T) {
	dir := t.TempDir()
	deletedCh := make(chan string, 1)

	var m *Manager
	reaper := jack.NewReaper(50*time.Millisecond,
		jack.ReaperWithShards(4),
		jack.ReaperWithHandler(func(_ context.Context, id string) {
			if m != nil {
				m.Reap(id)
			}
			deletedCh <- id
		}),
	)
	t.Cleanup(reaper.Stop)

	m, _ = NewManager(dir, reaper)

	b, _ := m.Write([]byte("short-lived"), 80*time.Millisecond)
	path := b.Path()

	select {
	case <-deletedCh:
		// Reaper fired and Reap completed
	case <-time.After(500 * time.Millisecond):
		t.Fatal("reaper did not fire within 500ms")
	}

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Errorf("blob file should be deleted after TTL, still exists at %q", path)
	}
	if m.Len() != 0 {
		t.Errorf("Len = %d after TTL expiry, want 0", m.Len())
	}
}
