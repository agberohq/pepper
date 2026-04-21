package coord

import (
	"os"
	"testing"
)

func TestNATS(t *testing.T) {
	addr := os.Getenv("PEPPER_NATS_URL")
	if addr == "" {
		t.Skip("set PEPPER_NATS_URL=nats://host:4222 to run NATS coord tests")
	}
	s := &Suite{
		Name: "NATS",
		NewStore: func(t *testing.T) Store {
			store, err := NewNATS(addr)
			if err != nil {
				t.Fatalf("NewNATS: %v", err)
			}
			return store
		},
	}
	s.Run(t)
}

func BenchmarkNATSSetGet(b *testing.B) {
	addr := os.Getenv("PEPPER_NATS_URL")
	if addr == "" {
		b.Skip("set PEPPER_NATS_URL=nats://host:4222 to run NATS coord benchmarks")
	}
	store, err := NewNATS(addr)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	runBenchSetGet(b, store)
}
