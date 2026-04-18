package storage

// Store is the pluggable session persistence interface.
type Store interface {
	// Set stores a key-value pair in a session, resetting the TTL.
	Set(sessionID, key string, value any) error

	// Get retrieves a value from a session. Returns zero value and false if not found.
	Get(sessionID, key string) (any, bool)

	// GetAll returns all key-value pairs in a session.
	GetAll(sessionID string) (map[string]any, bool)

	// Merge applies a map of updates atomically. Used by the router for
	// session_updates received in worker response meta.
	Merge(sessionID string, updates map[string]any) error

	// Touch resets the TTL for a session without modifying its data.
	Touch(sessionID string) error

	// Clear deletes all data in a session and cancels its TTL timer.
	Clear(sessionID string) error

	// Exists reports whether a session has any stored data.
	Exists(sessionID string) bool
}

// Wrap returns s unchanged. Convenience for embedding a custom Store.
func Wrap(s Store) Store { return s }

// TODO: RedisStore — implement using jack.Lifetime for TTL tracking.
// func NewRedis(url string, ttl time.Duration) Store { ... }
