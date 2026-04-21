package session

import (
	"fmt"
	"sync"
	"time"
)

// NewMemory returns an in-process session Store with per-session TTL eviction.
// TTL is reset on every Set, Merge, or Touch call.
func NewMemory(ttl time.Duration) Store {
	s := &memStore{
		ttl:    ttl,
		data:   make(map[string]map[string]any),
		timers: make(map[string]*time.Timer),
	}
	return s
}

type memStore struct {
	mu     sync.RWMutex
	ttl    time.Duration
	data   map[string]map[string]any
	timers map[string]*time.Timer
}

// resetTimer resets (or creates) the expiry timer for a session.
// Must be called with mu held for write.
func (s *memStore) resetTimer(sessionID string) {
	if s.ttl <= 0 {
		return
	}
	if t, ok := s.timers[sessionID]; ok {
		t.Reset(s.ttl)
		return
	}
	s.timers[sessionID] = time.AfterFunc(s.ttl, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.data, sessionID)
		delete(s.timers, sessionID)
	})
}

func (s *memStore) Set(sessionID, key string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[sessionID]; !ok {
		s.data[sessionID] = make(map[string]any)
	}
	s.data[sessionID][key] = value
	s.resetTimer(sessionID)
	return nil
}

func (s *memStore) Get(sessionID, key string) (any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.data[sessionID]
	if !ok {
		return nil, false
	}
	v, ok := sess[key]
	return v, ok
}

func (s *memStore) GetAll(sessionID string) (map[string]any, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.data[sessionID]
	if !ok || len(sess) == 0 {
		return nil, false
	}
	out := make(map[string]any, len(sess))
	for k, v := range sess {
		out[k] = v
	}
	return out, true
}

func (s *memStore) Merge(sessionID string, updates map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[sessionID]; !ok {
		s.data[sessionID] = make(map[string]any)
	}
	for k, v := range updates {
		s.data[sessionID][k] = v
	}
	s.resetTimer(sessionID)
	return nil
}

func (s *memStore) Touch(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.data[sessionID]; !ok {
		return fmt.Errorf("session %q not found", sessionID)
	}
	s.resetTimer(sessionID)
	return nil
}

func (s *memStore) Clear(sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.timers[sessionID]; ok {
		t.Stop()
		delete(s.timers, sessionID)
	}
	delete(s.data, sessionID)
	return nil
}

func (s *memStore) Exists(sessionID string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.data[sessionID]
	return ok && len(sess) > 0
}
