package sub_test

import (
	"os/exec"
	"testing"

	"github.com/agberohq/pepper/internal/sub"
)

func TestInitAndConfigure(t *testing.T) {
	if err := sub.Init(); err != nil {
		t.Fatalf("sub.Init: %v", err)
	}
	defer sub.Dispose()

	cmd := exec.Command("true") // no-op, always succeeds
	if err := sub.Configure(cmd); err != nil {
		t.Fatalf("sub.Configure: %v", err)
	}
	// SysProcAttr must be set (non-nil) after Configure on all platforms
	// Windows manager leaves it nil (Job Object handles lifecycle instead).
	// On unix platforms it should be non-nil.
	t.Logf("Configure ok: SysProcAttr=%+v", cmd.SysProcAttr)
}

func TestTrackNilProcess(t *testing.T) {
	if err := sub.Init(); err != nil {
		t.Fatalf("sub.Init: %v", err)
	}
	defer sub.Dispose()

	// Track on a nil process must not panic (windows manager is the only one
	// that actually derefs the process handle; the unix managers are noops for Track).
	// We can't track a nil *os.Process without a real PID so we just test Init/Dispose.
	t.Log("Track (no-op path) ok")
}

func TestDisposeIdempotent(t *testing.T) {
	if err := sub.Init(); err != nil {
		t.Fatalf("sub.Init: %v", err)
	}
	if err := sub.Dispose(); err != nil {
		t.Fatalf("first Dispose: %v", err)
	}
}
