package coord

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

func NewMemory() Store {
	m := &memStore{
		kv:     make(map[string]memEntry),
		queues: make(map[string]chan []byte),
		stopCh: make(chan struct{}),
	}
	go m.reaperLoop()
	return m
}

type memEntry struct {
	value     []byte
	expiresAt time.Time
}

type memSub struct {
	prefix    string
	ch        chan Event
	closeOnce sync.Once
}

func (s *memSub) close() {
	s.closeOnce.Do(func() { close(s.ch) })
}

type memStore struct {
	mu   sync.RWMutex
	kv   map[string]memEntry
	subs []*memSub

	qmu    sync.Mutex
	queues map[string]chan []byte

	stopCh chan struct{}
	closed sync.Once
}

func (m *memStore) Set(_ context.Context, key string, value []byte, ttlSeconds int64) error {
	var exp time.Time
	if ttlSeconds > 0 {
		exp = time.Now().Add(time.Duration(ttlSeconds) * time.Second)
	}
	cp := make([]byte, len(value))
	copy(cp, value)

	m.mu.Lock()
	m.kv[key] = memEntry{value: cp, expiresAt: exp}
	subs := m.snapshotSubs(key)
	m.mu.Unlock()

	m.fanout(subs, Event{Key: key, Value: cp})
	return nil
}

func (m *memStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	m.mu.RLock()
	e, ok := m.kv[key]
	m.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	if !e.expiresAt.IsZero() && time.Now().After(e.expiresAt) {
		return nil, false, nil
	}
	cp := make([]byte, len(e.value))
	copy(cp, e.value)
	return cp, true, nil
}

func (m *memStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	_, existed := m.kv[key]
	delete(m.kv, key)
	var subs []*memSub
	if existed {
		subs = m.snapshotSubs(key)
	}
	m.mu.Unlock()
	if existed {
		m.fanout(subs, Event{Key: key, Deleted: true})
	}
	return nil
}

func (m *memStore) List(_ context.Context, prefix string) ([]string, error) {
	now := time.Now()
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k, e := range m.kv {
		if strings.HasPrefix(k, prefix) {
			if e.expiresAt.IsZero() || now.Before(e.expiresAt) {
				keys = append(keys, k)
			}
		}
	}
	return keys, nil
}

func (m *memStore) Publish(_ context.Context, channel string, payload []byte) error {
	m.mu.RLock()
	subs := m.snapshotSubsByChannel(channel)
	m.mu.RUnlock()

	cp := make([]byte, len(payload))
	copy(cp, payload)
	m.fanout(subs, Event{Channel: channel, Value: cp})
	return nil
}

func (m *memStore) Subscribe(ctx context.Context, channelPrefix string) (<-chan Event, error) {
	ch := make(chan Event, 64)
	sub := &memSub{prefix: channelPrefix, ch: ch}

	m.mu.Lock()
	m.subs = append(m.subs, sub)
	m.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-m.stopCh:
		}
		m.mu.Lock()
		for i, s := range m.subs {
			if s == sub {
				m.subs = append(m.subs[:i], m.subs[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		sub.close()
	}()

	return ch, nil
}

func (m *memStore) Push(_ context.Context, queue string, payload []byte) error {
	q := m.getOrCreateQueue(queue)
	cp := make([]byte, len(payload))
	copy(cp, payload)

	select {
	case q <- cp:
		return nil
	default:
		return fmt.Errorf("coord/memory: Push queue %q full", queue)
	}
}

func (m *memStore) Pull(ctx context.Context, queue string) ([]byte, error) {
	q := m.getOrCreateQueue(queue)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.stopCh:
		return nil, fmt.Errorf("coord/memory: closed")
	case data := <-q:
		return data, nil
	}
}

func (m *memStore) getOrCreateQueue(name string) chan []byte {
	m.qmu.Lock()
	defer m.qmu.Unlock()
	if q, ok := m.queues[name]; ok {
		return q
	}
	q := make(chan []byte, 512)
	m.queues[name] = q
	return q
}

func (m *memStore) Close() error {
	m.closed.Do(func() { close(m.stopCh) })
	return nil
}

func (m *memStore) snapshotSubsByChannel(channel string) []*memSub {
	var out []*memSub
	for _, s := range m.subs {
		prefix := s.prefix

		if strings.HasSuffix(prefix, "*") {
			base := strings.TrimSuffix(prefix, "*")
			if strings.HasPrefix(channel, base) {
				out = append(out, s)
			}
			continue
		}

		if strings.HasSuffix(prefix, ">") {
			base := strings.TrimSuffix(prefix, ">")
			if strings.HasPrefix(channel, base) {
				out = append(out, s)
			}
			continue
		}

		if strings.HasPrefix(channel, prefix) {
			out = append(out, s)
		}
	}
	return out
}

func (m *memStore) snapshotSubs(key string) []*memSub {
	var out []*memSub
	for _, s := range m.subs {
		prefix := s.prefix

		if strings.HasSuffix(prefix, "*") {
			base := strings.TrimSuffix(prefix, "*")
			if strings.HasPrefix(key, base) {
				out = append(out, s)
			}
			continue
		}

		if strings.HasSuffix(prefix, ">") {
			base := strings.TrimSuffix(prefix, ">")
			if strings.HasPrefix(key, base) {
				out = append(out, s)
			}
			continue
		}

		if strings.HasPrefix(key, prefix) {
			out = append(out, s)
		}
	}
	return out
}

func (m *memStore) fanout(subs []*memSub, ev Event) {
	for _, s := range subs {
		select {
		case s.ch <- ev:
		default:
		}
	}
}

func (m *memStore) reaperLoop() {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-t.C:
			now := time.Now()
			m.mu.Lock()
			for k, e := range m.kv {
				if !e.expiresAt.IsZero() && now.After(e.expiresAt) {
					delete(m.kv, k)
				}
			}
			m.mu.Unlock()
		}
	}
}

var _ Store = (*memStore)(nil)
