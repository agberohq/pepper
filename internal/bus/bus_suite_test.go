package bus

import (
	"testing"

	"github.com/agberohq/pepper/internal/coord"
)

// NewMulaBus creates a Bus backed by a single-node Mula coord.Store.
// url must be "tcp://host:port" or "" for a random port.
// This is the canonical factory for callers that need a self-contained Bus
// without managing a coord.Store separately.
func NewMulaBus(url string) (Bus, error) {
	if url == "" {
		url = "tcp://127.0.0.1:0"
	}
	s, err := coord.NewMula(url)
	if err != nil {
		return nil, err
	}
	return NewCoordBus(s, coord.MulaAddr(s)), nil
}

// TestNewMulaBus verifies the factory creates a working bus.
func TestNewMulaBus(t *testing.T) {
	b, err := NewMulaBus("")
	if err != nil {
		t.Fatalf("NewMulaBus: %v", err)
	}
	defer b.Close()

	if b.Addr() == "" {
		t.Error("Addr() should not be empty")
	}
	if err := b.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	// Idempotent close.
	if err := b.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}
