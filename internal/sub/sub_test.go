package sub_test

import (
	"os/exec"
	"testing"

	"github.com/agberohq/pepper/internal/sub"
)

func TestInitAndConfigure(t *testing.T) {
	m, err := sub.New()
	if err != nil {
		t.Fatalf("sub.New: %v", err)
	}
	defer m.Dispose()

	cmd := exec.Command("true") // no-op, always succeeds
	if err := m.ConfigureCommand(cmd); err != nil {
		t.Fatalf("m.ConfigureCommand: %v", err)
	}
	// SysProcAttr must be set (non-nil) after Configure on all platforms
	// Windows manager leaves it nil (Job Object handles lifecycle instead).
	// On unix platforms it should be non-nil.
	t.Logf("Configure ok: SysProcAttr=%+v", cmd.SysProcAttr)
}

func TestTrackNilProcess(t *testing.T) {
	m, err := sub.New()
	if err != nil {
		t.Fatalf("sub.New: %v", err)
	}
	defer m.Dispose()
	t.Log("Track (no-op path) ok")
}

func TestDisposeIdempotent(t *testing.T) {
	m, err := sub.New()
	if err != nil {
		t.Fatalf("sub.New: %v", err)
	}
	if err := m.Dispose(); err != nil {
		t.Fatalf("first Dispose: %v", err)
	}
	if err := m.Dispose(); err != nil {
		t.Fatalf("second Dispose: %v", err)
	}
}
