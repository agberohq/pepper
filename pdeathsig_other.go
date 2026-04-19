//go:build !linux && !windows

package pepper

import "os/exec"

// setPdeathsig is a no-op on non-Linux Unix platforms (macOS, FreeBSD, etc.).
// The Pdeathsig field does not exist on these platforms.
// Zombie worker protection on macOS relies on the process group relationship.
func setPdeathsig(cmd *exec.Cmd) {}
