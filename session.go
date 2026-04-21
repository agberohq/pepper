package pepper

import (
	"context"

	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/runtime/goruntime"
)

// Session is a stateful handle bound to a named session ID.
// Obtain via pp.NewSession() to generate a new ID, or pp.Session(id)
// to bind to an existing one (e.g. from a request header or JWT).
type Session struct {
	id string
	pp *Pepper
}

// ID returns the session identifier.
// Store this in a cookie, JWT, or request context to resume the session later:
//
//	sess := pp.NewSession()
//	token := jwt.Sign(Claims{SessionID: sess.ID()})
//	// later:
//	sess := pp.Session(claims.SessionID)
func (s *Session) ID() string { return s.id }

// Do executes a capability with this session's context injected.
// For typed output use Call[Out].DoSession(ctx, sess) instead.
func (s *Session) Do(ctx context.Context, cap string, in core.In, opts ...CallOption) (Result, error) {
	opts = append([]CallOption{WithCallSession(s.id)}, opts...)
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

// Touch resets the session's idle expiry timer.
// Useful when you want to explicitly extend a long-lived session.
func (s *Session) Touch() error {
	return s.pp.sessions.Touch(s.id)
}

// Exists reports whether this session has any stored data.
func (s *Session) Exists() bool {
	return s.pp.sessions.Exists(s.id)
}

// Context helpers

// SessionFromContext retrieves the session ID injected into ctx by the pepper
// runtime when a capability is invoked through a Session.
// Returns "" if no session ID is present (e.g. the call was not session-scoped).
//
// Use this inside any Go Func capability to access the current session:
//
//	func(ctx context.Context, in TextIn) (TextOut, error) {
//	    sessID := pepper.SessionFromContext(ctx)  // "" if no session
//	    if sessID != "" {
//	        prefs, _ := myStore.Get(sessID)
//	    }

// }
func SessionFromContext(ctx context.Context) string {
	v, _ := ctx.Value(goruntime.SessionIDKey()).(string)
	return v
}
