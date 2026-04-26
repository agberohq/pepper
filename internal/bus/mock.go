package bus

import (
	"context"
	"sync"

	"github.com/agberohq/pepper/internal/coord"
)

// Mock is a fully functional in-memory Bus for unit tests.
// It is backed by coord.NewMemory() — no mocking, no stubs, real behaviour.
//
// In addition to the standard Bus interface, Mock exposes PopPush so tests
// can synchronously inspect messages that were delivered via PushOne without
// starting a goroutine to pull from the queue.
type Mock struct {
	mu   sync.Mutex
	push map[string][][]byte // topic → queued payloads (in order)

	inner Bus // memory-backed coord bus for pub/sub
	store coord.Store
}

// NewMock returns an in-memory Bus suitable for use in tests.
// It shares the real coord.Memory backend so Publish / Subscribe / PushOne
// all work correctly without any extra plumbing.
func NewMock() *Mock {
	s := coord.NewMemory()
	m := &Mock{
		push:  make(map[string][][]byte),
		store: s,
	}
	// inner bus handles Publish / Subscribe / SubscribePrefix.
	// PushOne is intercepted so tests can call PopPush.
	m.inner = NewCoordBus(s, "mock://memory")
	return m
}

// Publish sends data to all subscribers on topic (PUB/SUB).
func (m *Mock) Publish(topic string, data []byte) error {
	return m.inner.Publish(topic, data)
}

// Subscribe returns a channel that receives messages on topic.
func (m *Mock) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	return m.inner.Subscribe(ctx, topic)
}

// SubscribePrefix returns a channel receiving messages on any topic starting
// with prefix.
func (m *Mock) SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error) {
	return m.inner.SubscribePrefix(ctx, prefix)
}

// PushOne records the payload in an internal queue keyed by topic.
// Tests inspect the queue with PopPush; no blocking occurs.
func (m *Mock) PushOne(_ context.Context, topic string, data []byte) error {
	cp := make([]byte, len(data))
	copy(cp, data)
	m.mu.Lock()
	m.push[topic] = append(m.push[topic], cp)
	m.mu.Unlock()
	return nil
}

// PopPush dequeues and returns the oldest payload that was delivered via
// PushOne for topic. Returns (nil, false) if the queue is empty.
func (m *Mock) PopPush(topic string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.push[topic]
	if len(q) == 0 {
		return nil, false
	}
	data := q[0]
	m.push[topic] = q[1:]
	return data, true
}

// Addr returns a stable mock address.
func (m *Mock) Addr() string { return "mock://memory" }

// Close shuts down the underlying coord store.
func (m *Mock) Close() error {
	return m.store.Close()
}

var _ Bus = (*Mock)(nil)
