package pepper

import (
	"context"
	"time"
)

// Session is a stateful handle bound to a named session ID.
// Obtained via pp.Session("user-123").
type Session struct {
	id string
	pp *Pepper
}

// Do executes a capability with this session's context injected.
func (s *Session) Do(ctx context.Context, cap string, in In, opts ...CallOption) (Result, error) {
	opts = append([]CallOption{WithSession(s.id)}, opts...)
	return s.pp.Do(ctx, cap, in, opts...)
}

// Set stores a key-value pair in the session.
func (s *Session) Set(key string, value any) error {
	return s.pp.sessions.Set(s.id, key, value)
}

// Get retrieves a value from the session.
// Returns zero value and false if the key is not found.
func (s *Session) Get(key string) (any, bool) {
	return s.pp.sessions.Get(s.id, key)
}

// GetAll returns all key-value pairs in the session.
func (s *Session) GetAll() (map[string]any, bool) {
	return s.pp.sessions.GetAll(s.id)
}

// Clear deletes all data in the session.
func (s *Session) Clear() error {
	return s.pp.sessions.Clear(s.id)
}

// TTL resets the session's idle expiry timer.
// Useful when you want to explicitly extend a long-lived session.
func (s *Session) TTL(_ time.Duration) error {
	// Touch resets the TTL managed by jack.Lifetime in the store.
	return s.pp.sessions.Touch(s.id)
}

// Exists reports whether this session has any stored data.
func (s *Session) Exists() bool {
	return s.pp.sessions.Exists(s.id)
}
