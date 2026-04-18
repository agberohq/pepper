package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/olekukonko/jack"
)

// memory is the default in-memory session store.
type memory struct {
	mu       sync.RWMutex
	sessions map[string]map[string]any
	lifetime *jack.Lifetime
	ttl      time.Duration
}

// NewMemory returns an in-memory Store backed by jack.Lifetime for TTL.
// Sessions expire after ttl of inactivity.
func NewMemory(ttl time.Duration) Store {
	lm := jack.NewLifetime(
		jack.LifetimeWithShards(8),
	)
	m := &memory{
		sessions: make(map[string]map[string]any),
		lifetime: lm,
		ttl:      ttl,
	}
	return m
}

func (m *memory) Set(sessionID, key string, value any) error {
	m.mu.Lock()
	if _, ok := m.sessions[sessionID]; !ok {
		m.sessions[sessionID] = make(map[string]any)
	}
	m.sessions[sessionID][key] = value
	m.mu.Unlock()

	// Schedule (or reset) expiry via jack.Lifetime
	m.lifetime.ScheduleTimed(context.Background(), sessionID,
		jack.CallbackCtx(func(_ context.Context, id string) {
			m.evict(id)
		}),
		m.ttl,
	)
	return nil
}

func (m *memory) Get(sessionID, key string) (any, bool) {
	m.mu.RLock()
	sess, ok := m.sessions[sessionID]
	if !ok {
		m.mu.RUnlock()
		return nil, false
	}
	v, found := sess[key]
	m.mu.RUnlock()

	if found {
		// Touch resets TTL on access
		m.lifetime.ResetTimed(sessionID)
	}
	return v, found
}

func (m *memory) GetAll(sessionID string) (map[string]any, bool) {
	m.mu.RLock()
	sess, ok := m.sessions[sessionID]
	if !ok {
		m.mu.RUnlock()
		return nil, false
	}
	// Return a shallow copy so caller cannot mutate internal state
	out := make(map[string]any, len(sess))
	for k, v := range sess {
		out[k] = v
	}
	m.mu.RUnlock()
	return out, true
}

func (m *memory) Merge(sessionID string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	m.mu.Lock()
	if _, ok := m.sessions[sessionID]; !ok {
		m.sessions[sessionID] = make(map[string]any)
	}
	for k, v := range updates {
		m.sessions[sessionID][k] = v
	}
	m.mu.Unlock()
	m.lifetime.ResetTimed(sessionID)
	return nil
}

func (m *memory) Touch(sessionID string) error {
	m.mu.RLock()
	_, ok := m.sessions[sessionID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}
	m.lifetime.ResetTimed(sessionID)
	return nil
}

func (m *memory) Clear(sessionID string) error {
	m.mu.Lock()
	delete(m.sessions, sessionID)
	m.mu.Unlock()
	m.lifetime.CancelTimed(sessionID)
	return nil
}

func (m *memory) Exists(sessionID string) bool {
	m.mu.RLock()
	_, ok := m.sessions[sessionID]
	m.mu.RUnlock()
	return ok
}

// evict removes a session when its TTL fires.
func (m *memory) evict(sessionID string) {
	m.mu.Lock()
	delete(m.sessions, sessionID)
	m.mu.Unlock()
}
