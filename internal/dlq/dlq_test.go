package dlq

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Nop

func TestNopDiscards(t *testing.T) {
	n := Nop{}
	err := n.Write(Entry{
		OriginID:     "01HTEST",
		Cap:          "audio.denoise",
		CrashCount:   2,
		LastWorkerID: "w-2",
		DeclaredAt:   time.Now(),
	})
	if err != nil {
		t.Errorf("Nop.Write returned error: %v", err)
	}
}

func TestNopImplementsBackend(t *testing.T) {
	var _ Backend = Nop{}
}

// File

func TestFileWriteCreatesJSONFile(t *testing.T) {
	dir := t.TempDir()
	f := NewFile(dir)

	entry := Entry{
		OriginID:     "01HPOISONID",
		Cap:          "audio.denoise",
		CrashCount:   2,
		LastWorkerID: "w-2",
		DeclaredAt:   time.Now(),
	}
	if err := f.Write(entry); err != nil {
		t.Fatalf("Write: %v", err)
	}

	path := filepath.Join(dir, entry.OriginID+".json")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Errorf("expected JSON file at %s", path)
	}
}

func TestFileWriteJSONIsReadable(t *testing.T) {
	dir := t.TempDir()
	f := NewFile(dir)

	entry := Entry{
		OriginID:   "01HREADABLE",
		Cap:        "doc.parse",
		CrashCount: 3,
		DeclaredAt: time.Now(),
	}
	if err := f.Write(entry); err != nil {
		t.Fatalf("Write: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(dir, entry.OriginID+".json"))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if !bytes.Contains(data, []byte(entry.OriginID)) {
		t.Errorf("JSON file does not contain origin_id %q\n%s", entry.OriginID, data)
	}
	if !bytes.Contains(data, []byte(entry.Cap)) {
		t.Errorf("JSON file does not contain cap %q\n%s", entry.Cap, data)
	}
}

func TestFileWriteMultipleEntries(t *testing.T) {
	dir := t.TempDir()
	f := NewFile(dir)

	for i, id := range []string{"01HAAA", "01HBBB", "01HCCC"} {
		err := f.Write(Entry{
			OriginID:   id,
			Cap:        "cap.x",
			CrashCount: i + 1,
			DeclaredAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("Write %q: %v", id, err)
		}
	}

	ids, err := f.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(ids) != 3 {
		t.Errorf("List returned %d IDs, want 3: %v", len(ids), ids)
	}
}

func TestFileWriteWithPayload(t *testing.T) {
	dir := t.TempDir()
	f := NewFile(dir)

	entry := Entry{
		OriginID:   "01HWITHPAYLOAD",
		Cap:        "audio.denoise",
		CrashCount: 2,
		DeclaredAt: time.Now(),
	}
	payload := []byte{0x01, 0x02, 0x03, 0x04}

	if err := f.WriteWithPayload(entry, payload); err != nil {
		t.Fatalf("WriteWithPayload: %v", err)
	}

	if _, err := os.Stat(filepath.Join(dir, entry.OriginID+".json")); os.IsNotExist(err) {
		t.Error("expected .json metadata file")
	}
	payloadPath := filepath.Join(dir, entry.OriginID+".msgpack")
	written, err := os.ReadFile(payloadPath)
	if err != nil {
		t.Fatalf("ReadFile payload: %v", err)
	}
	if !bytes.Equal(written, payload) {
		t.Errorf("payload = %v, want %v", written, payload)
	}
}

func TestFileListEmptyDir(t *testing.T) {
	dir := t.TempDir()
	f := NewFile(dir)
	ids, err := f.List()
	if err != nil {
		t.Fatalf("List on empty dir: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("expected empty list, got %v", ids)
	}
}

func TestFileListSkipsNonJSON(t *testing.T) {
	dir := t.TempDir()
	f := NewFile(dir)

	_ = f.Write(Entry{OriginID: "01HKEEP", Cap: "x", DeclaredAt: time.Now()})
	_ = os.WriteFile(filepath.Join(dir, "other.txt"), []byte("noise"), 0600)
	_ = os.WriteFile(filepath.Join(dir, "foo.JSON"), []byte("uppercase"), 0600)

	ids, err := f.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(ids) != 1 || ids[0] != "01HKEEP" {
		t.Errorf("List = %v, want [01HKEEP]", ids)
	}
}

func TestFileCreatesDirectoryIfMissing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "dlq")
	f := NewFile(dir)
	entry := Entry{
		OriginID:   "01HNESTED",
		Cap:        "x",
		DeclaredAt: time.Now(),
	}
	if err := f.Write(entry); err != nil {
		t.Fatalf("Write to nested dir: %v", err)
	}
}

func TestFileConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	f := NewFile(dir)

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			id := fmt.Sprintf("01HCONC%02d", n)
			_ = f.Write(Entry{OriginID: id, Cap: "x", DeclaredAt: time.Now()})
		}(i)
	}
	wg.Wait()

	ids, err := f.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(ids) != 50 {
		t.Errorf("List returned %d IDs, want 50", len(ids))
	}
}

func TestFileWriteWithPayloadEmptyPayload(t *testing.T) {
	dir := t.TempDir()
	f := NewFile(dir)

	entry := Entry{OriginID: "01HNOPL", Cap: "x", DeclaredAt: time.Now()}
	if err := f.WriteWithPayload(entry, nil); err != nil {
		t.Fatalf("WriteWithPayload(nil): %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "01HNOPL.msgpack")); !os.IsNotExist(err) {
		t.Error("expected no payload file for empty payload")
	}
}

func TestFileImplementsBackend(t *testing.T) {
	var _ Backend = NewFile(t.TempDir())
}

// Entry fields

func TestEntryPayloadPathOptional(t *testing.T) {
	e := Entry{
		OriginID:    "01H",
		PayloadPath: "",
	}
	if e.PayloadPath != "" {
		t.Error("PayloadPath should default to empty")
	}
}
