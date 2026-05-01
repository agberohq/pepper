package coord

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func NewRedis(url string) (Store, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("coord/redis: parse url: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("coord/redis: connect: %w", err)
	}

	return &redisStore{
		client: client,
		stopCh: make(chan struct{}),
	}, nil
}

type redisStore struct {
	client *redis.Client
	stopCh chan struct{}
	closed sync.Once
}

func (s *redisStore) Set(ctx context.Context, key string, value []byte, ttlSeconds int64) error {
	ttl := time.Duration(ttlSeconds) * time.Second
	if ttlSeconds <= 0 {
		ttl = 0
	}
	return s.client.Set(ctx, key, value, ttl).Err()
}

func (s *redisStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	val, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (s *redisStore) Delete(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

func (s *redisStore) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	iter := s.client.Scan(ctx, 0, prefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (s *redisStore) Publish(ctx context.Context, channel string, payload []byte) error {
	return s.client.Publish(ctx, channel, payload).Err()
}

func (s *redisStore) Subscribe(ctx context.Context, channelPrefix string) (<-chan Event, error) {
	// A large channel buffer ensures bursts of cluster throughput don't block the go-redis reader.
	ch := make(chan Event, 1024)
	pattern := channelPrefix + "*"
	pubsub := s.client.PSubscribe(ctx, pattern)

	go func() {
		defer close(ch)
		defer pubsub.Close()

		// WithChannelSize expands the internal go-redis buffer (default 100).
		msgCh := pubsub.Channel(redis.WithChannelSize(1024))
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-msgCh:
				if !ok {
					return
				}
				select {
				case ch <- Event{Channel: msg.Channel, Value: []byte(msg.Payload)}:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}

func (s *redisStore) Push(ctx context.Context, queue string, payload []byte) error {
	return s.client.LPush(ctx, queue, payload).Err()
}

func (s *redisStore) Pull(ctx context.Context, queue string) ([]byte, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("coord/redis: closed")
		default:
		}

		result, err := s.client.BRPop(ctx, 2*time.Second, queue).Result()
		if err == redis.Nil {
			continue
		}
		if err != nil {
			if err == context.DeadlineExceeded || err == context.Canceled {
				return nil, ctx.Err()
			}
			return nil, err
		}
		if len(result) < 2 {
			continue
		}
		return []byte(result[1]), nil
	}
}

func (s *redisStore) Close() error {
	var err error
	s.closed.Do(func() {
		close(s.stopCh)
		err = s.client.Close()
	})
	return err
}

var _ Store = (*redisStore)(nil)
