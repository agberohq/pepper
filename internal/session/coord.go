package session

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/agberohq/pepper/internal/coord"
)

// NewCoordStore returns a Store backed by a coord.Store.
// Sessions are stored under "pepper:session:{id}" as JSON-encoded hash maps.
// TTL is refreshed on every write/touch via coord.Store's ttlSeconds parameter.
//
// This replaces NewMemory for multi-node deployments:
//
//	c, _ := coord.NewRedis("redis://localhost:6379")
//	pepper.New(pepper.WithStorage(storage.NewCoordStore(c, 24*time.Hour)))
func NewCoordStore(c coord.Store, ttl time.Duration) Store {
	return &coordStore{coord: c, ttl: ttl}
}

type coordStore struct {
	coord coord.Store
	ttl   time.Duration
}

func (s *coordStore) ttlSecs() int64 {
	return int64(s.ttl.Seconds())
}

func sessionKey(sessionID string) string { return "pepper:session:" + sessionID }
func fieldKey(sessionID, key string) string {
	return "pepper:session:" + sessionID + ":" + key
}

func (s *coordStore) Set(sessionID, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("storage.coordStore.Set marshal: %w", err)
	}
	return s.coord.Set(context.Background(), fieldKey(sessionID, key), data, s.ttlSecs())
}

func (s *coordStore) Get(sessionID, key string) (any, bool) {
	data, ok, err := s.coord.Get(context.Background(), fieldKey(sessionID, key))
	if err != nil || !ok {
		return nil, false
	}
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return string(data), true
	}
	return v, true
}

func (s *coordStore) GetAll(sessionID string) (map[string]any, bool) {
	prefix := sessionKey(sessionID) + ":"
	keys, err := s.coord.List(context.Background(), prefix)
	if err != nil || len(keys) == 0 {
		return nil, false
	}
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		field := strings.TrimPrefix(k, prefix)
		if v, ok := s.Get(sessionID, field); ok {
			out[field] = v
		}
	}
	return out, len(out) > 0
}

func (s *coordStore) Merge(sessionID string, updates map[string]any) error {
	for k, v := range updates {
		if err := s.Set(sessionID, k, v); err != nil {
			return err
		}
	}
	return nil
}

func (s *coordStore) Touch(sessionID string) error {
	// Re-write the session prefix marker to reset TTL.
	return s.coord.Set(context.Background(), sessionKey(sessionID), []byte("1"), s.ttlSecs())
}

func (s *coordStore) Clear(sessionID string) error {
	prefix := sessionKey(sessionID)
	keys, err := s.coord.List(context.Background(), prefix)
	if err != nil {
		return err
	}
	ctx := context.Background()
	for _, k := range keys {
		_ = s.coord.Delete(ctx, k)
	}
	return nil
}

func (s *coordStore) Exists(sessionID string) bool {
	keys, err := s.coord.List(context.Background(), sessionKey(sessionID))
	return err == nil && len(keys) > 0
}
