//go:build windows

package pepper

import "os/exec"

// setPdeathsig is a no-op on Windows.
// Windows zombie protection is handled via Job Objects at the process manager level.
// When the Go process exits, the OS kills all processes in its Job Object.
func setPdeathsig(cmd *exec.Cmd) {}
