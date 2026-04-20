package pepper

import (
	"time"

	"github.com/agberohq/pepper/internal/registry"
)

// CapOption modifies a registry.Spec during registration.
// Pass to pp.Register() to override defaults.
type CapOption func(*registry.Spec)

// Version sets the capability semver.
func Version(v string) CapOption {
	return func(s *registry.Spec) { s.Version = v }
}

// Groups sets the worker groups this capability is dispatched to.
func Groups(groups ...string) CapOption {
	return func(s *registry.Spec) { s.Groups = groups }
}

// WithConfig sets the config dict passed to setup().
func WithConfig(cfg map[string]any) CapOption {
	return func(s *registry.Spec) { s.Config = cfg }
}

// Timeout sets the per-request timeout for this capability.
func Timeout(d time.Duration) CapOption {
	return func(s *registry.Spec) { s.Timeout = d }
}
