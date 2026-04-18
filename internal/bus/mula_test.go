package bus

import (
	"context"
	"testing"
)

func TestMula(t *testing.T) {
	suite := &TestBusInterface{
		Name: "Mula",
		NewBus: func(cfg Config) (Bus, error) {
			if cfg.URL == "" {
				cfg = Config{URL: "tcp://127.0.0.1:0"}
			}
			return NewMula(cfg)
		},
	}
	suite.Run(t)
}

func TestMulaTopicHelpers(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		expected string
		fn       func(string) string
	}{
		{"TopicPush", "gpu", "pepper.push.gpu", TopicPush},
		{"TopicPub", "gpu", "pepper.pub.gpu", TopicPub},
		{"TopicRes", "origin123", "pepper.res.origin123", TopicRes},
		{"TopicPipe", "asr", "pepper.pipe.asr", TopicPipe},
		{"TopicStream", "corr456", "pepper.stream.corr456", TopicStream},
		{"TopicCb", "corr789", "pepper.cb.corr789", TopicCb},
		{"TopicHB", "worker1", "pepper.hb.worker1", TopicHB},
		{"TopicControl", "worker1", "pepper.control.worker1", TopicControl},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.fn(tt.topic)
			if got != tt.expected {
				t.Errorf("%s(%q) = %q, want %q", tt.name, tt.topic, got, tt.expected)
			}
		})
	}
}

func TestMulaIsBroadcastTopic(t *testing.T) {
	tests := []struct {
		topic    string
		expected bool
	}{
		{"pepper.pub.gpu", true},
		{"pepper.broadcast", true},
		{"pepper.push.gpu", false},
		{"pepper.res.abc", false},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := IsBroadcastTopic(tt.topic)
			if got != tt.expected {
				t.Errorf("IsBroadcastTopic(%q) = %v, want %v", tt.topic, got, tt.expected)
			}
		})
	}
}

func TestMulaIsPushTopic(t *testing.T) {
	tests := []struct {
		topic    string
		expected bool
	}{
		{"pepper.push.gpu", true},
		{"pepper.pub.gpu", false},
		{"pepper.broadcast", false},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := IsPushTopic(tt.topic)
			if got != tt.expected {
				t.Errorf("IsPushTopic(%q) = %v, want %v", tt.topic, got, tt.expected)
			}
		})
	}
}

func TestMulaGroupExtractors(t *testing.T) {
	tests := []struct {
		topic    string
		expected string
		fn       func(string) string
	}{
		{"pepper.push.gpu", "gpu", GroupFromPushTopic},
		{"pepper.pub.asr", "asr", GroupFromPubTopic},
	}

	for _, tt := range tests {
		t.Run(tt.topic, func(t *testing.T) {
			got := tt.fn(tt.topic)
			if got != tt.expected {
				t.Errorf("extract group from %q = %q, want %q", tt.topic, got, tt.expected)
			}
		})
	}
}

func TestMulaWorkerEnvVars(t *testing.T) {
	vars := WorkerEnvVars("w-1", "tcp://127.0.0.1:7731", "msgpack", []string{"gpu", "asr"}, 5000, 8, "/dev/shm/pepper")

	expected := map[string]string{
		"PEPPER_WORKER_ID":      "w-1",
		"PEPPER_BUS_URL":        "tcp://127.0.0.1:7731",
		"PEPPER_CODEC":          "msgpack",
		"PEPPER_GROUPS":         "gpu,asr",
		"PEPPER_HEARTBEAT_MS":   "5000",
		"PEPPER_MAX_CONCURRENT": "8",
		"PEPPER_BLOB_DIR":       "/dev/shm/pepper",
	}

	for _, v := range vars {
		for k, expectedVal := range expected {
			if len(v) > len(k) && v[:len(k)+1] == k+"=" {
				val := v[len(k)+1:]
				if val != expectedVal {
					t.Errorf("%s = %q, want %q", k, val, expectedVal)
				}
			}
		}
	}
}

func TestMulaClose(t *testing.T) {
	bus, err := NewMula(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		t.Fatalf("NewMula: %v", err)
	}

	// Close should be idempotent
	if err := bus.Close(); err != nil {
		t.Errorf("first Close: %v", err)
	}
	if err := bus.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}

	// Operations after close should fail
	err = bus.Publish("test", []byte("data"))
	if err == nil {
		t.Error("Publish after close should fail")
	}
}

func BenchmarkMulaPublish(b *testing.B) {
	bus, err := NewMula(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		b.Fatal(err)
	}
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "bench.topic"
	subCh, err := bus.Subscribe(ctx, topic)
	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for range subCh {
		}
	}()

	data := []byte("benchmark data")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := bus.Publish(topic, data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMulaSubscribePrefix(b *testing.B) {
	bus, err := NewMula(Config{URL: "tcp://127.0.0.1:0"})
	if err != nil {
		b.Fatal(err)
	}
	defer bus.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	subCh, err := bus.SubscribePrefix(ctx, "pepper.res.")
	if err != nil {
		b.Fatal(err)
	}

	go func() {
		for range subCh {
		}
	}()

	data := []byte("benchmark")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bus.Publish("pepper.res."+string(rune(i%26+'a')), data)
	}
}
