package coord

import (
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/agberohq/pepper/internal/wire"
	"github.com/nats-io/nats.go"
)

const (
	natsKVBucket   = "pepper-coord"
	natsWorkStream = "pepper-work"
)

func NewNATS(url string) (Store, error) {
	nc, err := nats.Connect(url, nats.Timeout(5*time.Second))
	if err != nil {
		return nil, fmt.Errorf("coord/nats: connect: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("coord/nats: jetstream: %w", err)
	}

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  natsKVBucket,
		History: 1,
		Storage: nats.FileStorage,
	})
	if err != nil {
		kv, err = js.KeyValue(natsKVBucket)
		if err != nil {
			nc.Close()
			return nil, fmt.Errorf("coord/nats: kv bucket: %w", err)
		}
	}

	// Delete and recreate the work stream to purge any stale durable consumers
	// left by previous runs. Stale consumers that were created without a deliver
	// group prevent new queue-group push subscriptions from succeeding.
	// Recreating the stream is simpler than enumerating consumers and handles
	// all nats.go API versions without type assertions.
	_ = js.DeleteStream(natsWorkStream)
	if _, err = js.AddStream(&nats.StreamConfig{
		Name:      natsWorkStream,
		Subjects:  []string{wire.TopicPushPrefix + ">"},
		Retention: nats.WorkQueuePolicy,
		Storage:   nats.FileStorage,
	}); err != nil {
		nc.Close()
		return nil, fmt.Errorf("coord/nats: recreate work stream: %w", err)
	}

	return &natsStore{
		nc:       nc,
		js:       js,
		kv:       kv,
		stopCh:   make(chan struct{}),
		pullSubs: make(map[string]*nats.Subscription),
	}, nil
}

func isNATSAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "already in use") ||
		strings.Contains(s, "already exists") ||
		strings.Contains(s, "name is already in use")
}

type natsStore struct {
	nc     *nats.Conn
	js     nats.JetStreamContext
	kv     nats.KeyValue
	stopCh chan struct{}
	closed sync.Once
	// pullSubs caches persistent pull subscriptions per queue to avoid
	// creating a new PullSubscribe on every Pull() call.
	pullSubsMu sync.Mutex
	pullSubs   map[string]*nats.Subscription
}

func encodeKey(key string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func decodeKey(key string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(key)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (s *natsStore) Set(_ context.Context, key string, value []byte, _ int64) error {
	_, err := s.kv.Put(encodeKey(key), value)
	return err
}

func (s *natsStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	entry, err := s.kv.Get(encodeKey(key))
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	return entry.Value(), true, nil
}

func (s *natsStore) Delete(_ context.Context, key string) error {
	return s.kv.Delete(encodeKey(key))
}

func (s *natsStore) List(_ context.Context, prefix string) ([]string, error) {
	lister, err := s.kv.ListKeys()
	if err != nil {
		return nil, err
	}
	defer lister.Stop()

	var keys []string
	for encoded := range lister.Keys() {
		decoded, err := decodeKey(encoded)
		if err != nil {
			continue
		}
		if strings.HasPrefix(decoded, prefix) {
			keys = append(keys, decoded)
		}
	}
	return keys, nil
}

func (s *natsStore) Publish(_ context.Context, channel string, payload []byte) error {
	return s.nc.Publish(channel, payload)
}

func (s *natsStore) Subscribe(ctx context.Context, channelPrefix string) (<-chan Event, error) {
	ch := make(chan Event, 64)

	// Use prefix-based subscriptions instead of ">" to avoid receiving all
	// cluster traffic. For prefix patterns (ending with "." or ":"), subscribe
	// to both the exact topic and the wildcard pattern.
	var subs []*nats.Subscription

	handler := func(m *nats.Msg) {
		if !natsChannelMatches(m.Subject, channelPrefix) {
			return
		}
		select {
		case ch <- Event{Channel: m.Subject, Value: m.Data}:
		case <-ctx.Done():
		case <-s.stopCh:
		}
	}

	if strings.HasSuffix(channelPrefix, ".") || strings.HasSuffix(channelPrefix, ":") {
		// Prefix subscription: use NATS wildcard pattern for prefix matching
		wildcard := strings.TrimSuffix(channelPrefix, ".") + ".>"
		if strings.HasSuffix(channelPrefix, ":") {
			wildcard = strings.TrimSuffix(channelPrefix, ":") + ":>"
		}
		sub, err := s.nc.Subscribe(wildcard, handler)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	} else if strings.HasSuffix(channelPrefix, "*") {
		// Wildcard * converted to > for multi-level matching
		wildcard := strings.TrimSuffix(channelPrefix, "*") + ">"
		sub, err := s.nc.Subscribe(wildcard, handler)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	} else if strings.HasSuffix(channelPrefix, ">") {
		// Already a wildcard pattern
		sub, err := s.nc.Subscribe(channelPrefix, handler)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	} else {
		// Exact topic subscription, but also subscribe to the wildcard pattern
		// so that sub-topics like "pepper.control.w-1" are received when
		// subscribing to "pepper.control".
		sub1, err := s.nc.Subscribe(channelPrefix, handler)
		if err != nil {
			return nil, err
		}
		subs = append(subs, sub1)

		wildcard := channelPrefix + ".>"
		sub2, err := s.nc.Subscribe(wildcard, handler)
		if err != nil {
			sub1.Unsubscribe()
			return nil, err
		}
		subs = append(subs, sub2)
	}

	go func() {
		select {
		case <-ctx.Done():
		case <-s.stopCh:
		}
		for _, sub := range subs {
			sub.Unsubscribe()
		}
		close(ch)
	}()

	return ch, nil
}

func (s *natsStore) Push(_ context.Context, queue string, payload []byte) error {
	// Use core NATS publish (not JetStream) so Python workers can receive
	// via plain SUB subscriptions. JetStream messages are invisible to core
	// NATS subscribers — using js.Publish here would silently drop messages
	// to Python workers that use SUB pepper.push.<group>.
	return s.nc.Publish(queue, payload)
}

func (s *natsStore) Pull(ctx context.Context, queue string) ([]byte, error) {
	cname := consumerName(queue)

	// Ensure the consumer exists
	_, err := s.js.ConsumerInfo(natsWorkStream, cname)
	if err != nil {
		_, err = s.js.AddConsumer(natsWorkStream, &nats.ConsumerConfig{
			Durable:       cname,
			FilterSubject: queue,
			AckPolicy:     nats.AckExplicitPolicy,
			DeliverPolicy: nats.DeliverAllPolicy,
			MaxAckPending: 1,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
		})
		if err != nil && !isNATSAlreadyExists(err) {
			return nil, fmt.Errorf("coord/nats: consumer: %w", err)
		}
	}

	// Use cached persistent subscription to avoid creating one per call
	s.pullSubsMu.Lock()
	sub, ok := s.pullSubs[queue]
	if !ok {
		sub, err = s.js.PullSubscribe(queue, cname, nats.BindStream(natsWorkStream))
		if err != nil {
			s.pullSubsMu.Unlock()
			return nil, err
		}
		s.pullSubs[queue] = sub
	}
	s.pullSubsMu.Unlock()

	msgs, err := sub.Fetch(1, nats.Context(ctx))
	if err != nil {
		if err == context.DeadlineExceeded || err == context.Canceled {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, ctx.Err()
	}

	msg := msgs[0]
	if err := msg.Ack(); err != nil {
		return nil, err
	}
	return msg.Data, nil
}

func (s *natsStore) Close() error {
	var err error
	s.closed.Do(func() {
		close(s.stopCh)
		s.pullSubsMu.Lock()
		for _, sub := range s.pullSubs {
			sub.Unsubscribe()
		}
		s.pullSubsMu.Unlock()
		err = s.nc.Drain()
	})
	return err
}

func consumerName(queue string) string {
	return "c-" + strings.NewReplacer(".", "_").Replace(queue)
}

func natsChannelMatches(channel, prefix string) bool {
	switch {
	case strings.HasSuffix(prefix, "*"):
		return strings.HasPrefix(channel, strings.TrimSuffix(prefix, "*"))
	case strings.HasSuffix(prefix, ">"):
		return strings.HasPrefix(channel, strings.TrimSuffix(prefix, ">"))
	case strings.HasSuffix(prefix, "."), strings.HasSuffix(prefix, ":"):
		return strings.HasPrefix(channel, prefix)
	default:
		if channel == prefix {
			return true
		}
		return strings.HasPrefix(channel, prefix+".") || strings.HasPrefix(channel, prefix+":")
	}
}

var _ Store = (*natsStore)(nil)
