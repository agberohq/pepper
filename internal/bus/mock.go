package bus

import (
	"context"
	"sync"
)

// Mock is a test-only in-memory bus implementation.
type Mock struct {
	mu         sync.RWMutex
	subs       map[string][]chan Message
	prefixSubs []struct {
		prefix string
		ch     chan Message
	}
	pushQs map[string]chan []byte
	closed bool
	addr   string
}

// NewMock creates a new mock bus for testing.
func NewMock() *Mock {
	return &Mock{
		subs:   make(map[string][]chan Message),
		pushQs: make(map[string]chan []byte),
		addr:   "mock://localhost",
	}
}

func (m *Mock) Publish(topic string, data []byte) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil
	}

	msg := Message{Topic: topic, Data: data}

	// Exact subscribers
	for _, ch := range m.subs[topic] {
		select {
		case ch <- msg:
		default:
		}
	}

	// Prefix subscribers
	for _, ps := range m.prefixSubs {
		if hasPrefix(topic, ps.prefix) {
			select {
			case ps.ch <- msg:
			default:
			}
		}
	}

	return nil
}

func (m *Mock) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan Message, 64)
	m.subs[topic] = append(m.subs[topic], ch)

	go func() {
		<-ctx.Done()
		m.mu.Lock()
		subs := m.subs[topic]
		for i, s := range subs {
			if s == ch {
				m.subs[topic] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		close(ch)
	}()

	return ch, nil
}

func (m *Mock) SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan Message, 64)
	m.prefixSubs = append(m.prefixSubs, struct {
		prefix string
		ch     chan Message
	}{prefix, ch})

	go func() {
		<-ctx.Done()
		m.mu.Lock()
		for i, ps := range m.prefixSubs {
			if ps.ch == ch {
				m.prefixSubs = append(m.prefixSubs[:i], m.prefixSubs[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		close(ch)
	}()

	return ch, nil
}

func (m *Mock) PushOne(ctx context.Context, topic string, data []byte) error {
	m.mu.RLock()
	q, ok := m.pushQs[topic]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		q = make(chan []byte, 64)
		m.pushQs[topic] = q
		m.mu.Unlock()
	}

	select {
	case q <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Mock) Addr() string {
	return m.addr
}

func (m *Mock) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}
	m.closed = true

	for _, subs := range m.subs {
		for _, ch := range subs {
			close(ch)
		}
	}
	for _, ps := range m.prefixSubs {
		close(ps.ch)
	}
	for _, q := range m.pushQs {
		close(q)
	}

	return nil
}

// PopPush returns the next message from a push queue (for testing).
func (m *Mock) PopPush(topic string) ([]byte, bool) {
	m.mu.RLock()
	q, ok := m.pushQs[topic]
	m.mu.RUnlock()

	if !ok {
		return nil, false
	}

	select {
	case data := <-q:
		return data, true
	default:
		return nil, false
	}
}

// Ensure Mock implements Bus.
var _ Bus = (*Mock)(nil)
