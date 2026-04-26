package pending

import (
	"fmt"
	"sync/atomic"

	"github.com/olekukonko/mappo"
)

// Map is the in-flight request registry. Lock-free reads via mappo.Sharded.
type Map struct {
	entries *mappo.Concurrent[string, *entry]
	len     atomic.Int64
}

// New returns an empty pending Map.
func New() *Map {
	return &Map{
		entries: mappo.NewConcurrent[string, *entry](),
	}
}

// Register allocates a response channel for corr_id.
func (m *Map) Register(corrID string) (<-chan Response, error) {
	ch := make(chan Response, 1)
	e := &entry{ch: ch}
	if _, loaded := m.entries.SetIfAbsent(corrID, e); loaded {
		return nil, fmt.Errorf("pepper/pending: corr_id %q already registered", corrID)
	}
	m.len.Add(1)
	return ch, nil
}

// RegisterStream allocates a streaming channel for corr_id.
func (m *Map) RegisterStream(corrID string, bufSize int) (<-chan Response, error) {
	ch := make(chan Response, bufSize)
	e := &entry{stream: ch}
	if _, loaded := m.entries.SetIfAbsent(corrID, e); loaded {
		return nil, fmt.Errorf("pepper/pending: corr_id %q already registered", corrID)
	}
	m.len.Add(1)
	return ch, nil
}

// Resolve delivers a response to the goroutine waiting on corrID.
// Safe to call from multiple goroutines (only the first succeeds).
func (m *Map) Resolve(corrID string, resp Response) {
	val, ok := m.entries.Get(corrID)
	if !ok {
		return
	}
	if !val.done.CompareAndSwap(false, true) {
		return
	}
	m.entries.Delete(corrID)
	m.len.Add(-1)

	if val.ch != nil {
		val.ch <- resp
	}
	if val.stream != nil {
		val.stream <- resp
		close(val.stream)
	}
}

// Chunk delivers one streaming chunk to the goroutine waiting on corrID.
// Does NOT mark the entry done — the stream stays open for more chunks.
func (m *Map) Chunk(corrID string, resp Response) {
	val, ok := m.entries.Get(corrID)
	if !ok {
		return
	}
	if val.done.Load() || val.stream == nil {
		return
	}
	select {
	case val.stream <- resp:
	default:
		// Stream buffer full — drop chunk (backpressure should prevent this)
	}
}

// EndStream closes the streaming channel for corrID, signalling no more chunks.
func (m *Map) EndStream(corrID string) {
	val, ok := m.entries.Get(corrID)
	if !ok {
		return
	}
	if !val.done.CompareAndSwap(false, true) {
		return
	}
	m.entries.Delete(corrID)
	m.len.Add(-1)
	if val.stream != nil {
		close(val.stream)
	}
}

// Fail delivers an error response to the goroutine waiting on corrID.
func (m *Map) Fail(corrID string, err error) {
	m.Resolve(corrID, Response{Err: err})
}

// CancelByIDs cancels all in-flight entries whose corrID is in the list.
func (m *Map) CancelByIDs(corrIDs []string, err error) {
	for _, id := range corrIDs {
		m.Fail(id, err)
	}
}

// Len returns the number of in-flight requests.
func (m *Map) Len() int {
	return int(m.len.Load())
}

// Has reports whether corrID is currently in-flight.
func (m *Map) Has(corrID string) bool {
	return m.entries.Has(corrID)
}
