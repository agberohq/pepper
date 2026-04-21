package coord

import (
	"os"
	"testing"
)

func TestRedis(t *testing.T) {
	addr := os.Getenv("PEPPER_REDIS_URL")
	if addr == "" {
		t.Skip("set PEPPER_REDIS_URL=redis://host:port to run Redis coord tests")
	}
	s := &Suite{
		Name: "Redis",
		NewStore: func(t *testing.T) Store {
			store, err := NewRedis(addr)
			if err != nil {
				t.Fatalf("NewRedis: %v", err)
			}
			return store
		},
	}
	s.Run(t)
}

func BenchmarkRedisSetGet(b *testing.B) {
	addr := os.Getenv("PEPPER_REDIS_URL")
	if addr == "" {
		b.Skip("set PEPPER_REDIS_URL=redis://host:port to run Redis coord benchmarks")
	}
	store, err := NewRedis(addr)
	if err != nil {
		b.Fatal(err)
	}
	defer store.Close()
	runBenchSetGet(b, store)
}
