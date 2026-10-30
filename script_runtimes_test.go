package pepper

import (
	"testing"
)

// TestRustWorkerExeName pins the name cargo emits for the runtime binary.
//
// Cargo appends the platform executable suffix, so a Windows build produces
// `pepper-worker.exe`. Looking for the bare name made `Start()` fail with
// "cargo build succeeded but <dir>/pepper-worker is missing" on windows CI
// runners even though the build had worked.
func TestRustWorkerExeName(t *testing.T) {
	cases := []struct {
		goos string
		want string
	}{
		{"windows", "pepper-worker.exe"},
		{"linux", "pepper-worker"},
		{"darwin", "pepper-worker"},
	}
	for _, tc := range cases {
		if got := rustWorkerExeName(tc.goos); got != tc.want {
			t.Errorf("rustWorkerExeName(%q) = %q, want %q", tc.goos, got, tc.want)
		}
	}
}
