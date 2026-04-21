package coord

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// NewRedis returns a Redis-backed Store.
//
//	c, err := coord.NewRedis("redis://127.0.0.1:6379")
//	pepper.New(pepper.WithCoord(c))
func NewRedis(url string) (Store, error) {
	addr := strings.TrimPrefix(url, "redis://")
	s := &redisStore{
		addr:   addr,
		stopCh: make(chan struct{}),
	}
	c, err := respDial(addr)
	if err != nil {
		return nil, fmt.Errorf("coord/redis: connect %q: %w", url, err)
	}
	c.Close()
	return s, nil
}

type redisStore struct {
	addr string
	mu   sync.Mutex // guards conn pool (single persistent conn for KV ops)
	conn net.Conn

	closed atomic.Bool
	stopCh chan struct{}
}

func (s *redisStore) dial() (net.Conn, error) {
	return respDial(s.addr)
}

// conn returns a live connection, re-dialing if needed.
func (s *redisStore) getConn() (net.Conn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		// Ping to verify liveness.
		if err := respSend(s.conn, "PING"); err == nil {
			if reply, err := respRead(s.conn); err == nil && reply == "PONG" {
				return s.conn, nil
			}
		}
		s.conn.Close()
		s.conn = nil
	}
	c, err := respDial(s.addr)
	if err != nil {
		return nil, err
	}
	s.conn = c
	return c, nil
}

func (s *redisStore) Set(_ context.Context, key string, value []byte, ttlSeconds int64) error {
	c, err := s.getConn()
	if err != nil {
		return err
	}
	encoded := hex.EncodeToString(value)
	if ttlSeconds > 0 {
		_, err = respSendRead(c, "SET", key, encoded, "EX", strconv.FormatInt(ttlSeconds, 10))
	} else {
		_, err = respSendRead(c, "SET", key, encoded)
	}
	return err
}

func (s *redisStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	c, err := s.getConn()
	if err != nil {
		return nil, false, err
	}
	reply, err := respSendRead(c, "GET", key)
	if err != nil {
		return nil, false, err
	}
	if reply == nil {
		return nil, false, nil
	}
	encoded, ok := reply.(string)
	if !ok {
		return nil, false, fmt.Errorf("coord/redis: GET unexpected reply type")
	}
	decoded, err := hex.DecodeString(encoded)
	if err != nil {
		return nil, false, fmt.Errorf("coord/redis: GET decode: %w", err)
	}
	return decoded, true, nil
}

func (s *redisStore) Delete(_ context.Context, key string) error {
	c, err := s.getConn()
	if err != nil {
		return err
	}
	_, err = respSendRead(c, "DEL", key)
	return err
}

func (s *redisStore) List(_ context.Context, prefix string) ([]string, error) {
	c, err := s.dial()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	br := bufio.NewReader(c)
	var keys []string
	cursor := "0"
	for {
		if err := respSend(c, "SCAN", cursor, "MATCH", prefix+"*", "COUNT", "100"); err != nil {
			return nil, err
		}
		reply, err := readRESP(br)
		if err != nil {
			return nil, err
		}
		arr, ok := reply.([]any)
		if !ok || len(arr) < 2 {
			break
		}
		cursor, _ = arr[0].(string)
		items, _ := arr[1].([]any)
		for _, item := range items {
			if k, ok := item.(string); ok {
				keys = append(keys, k)
			}
		}
		if cursor == "0" {
			break
		}
	}
	return keys, nil
}

func (s *redisStore) Publish(_ context.Context, channel string, payload []byte) error {
	c, err := s.getConn()
	if err != nil {
		return err
	}
	_, err = respSendRead(c, "PUBLISH", channel, hex.EncodeToString(payload))
	return err
}

// Subscribe listens on channels matching channelPrefix using Redis PSUBSCRIBE.
// Each call opens a dedicated connection — Redis pub/sub connections are stateful.
func (s *redisStore) Subscribe(ctx context.Context, channelPrefix string) (<-chan Event, error) {
	c, err := respDial(s.addr)
	if err != nil {
		return nil, fmt.Errorf("coord/redis: subscribe dial: %w", err)
	}

	if err := respSend(c, "PSUBSCRIBE", channelPrefix+"*"); err != nil {
		c.Close()
		return nil, fmt.Errorf("coord/redis: PSUBSCRIBE: %w", err)
	}

	ch := make(chan Event, 64)
	go s.readPubSub(ctx, c, ch)
	return ch, nil
}

func (s *redisStore) readPubSub(ctx context.Context, c net.Conn, ch chan Event) {
	defer close(ch)
	defer c.Close()

	br := bufio.NewReader(c)
	done := ctx.Done()

	for {
		select {
		case <-done:
			return
		case <-s.stopCh:
			return
		default:
		}

		reply, err := readRESP(br)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			return
		}

		arr, ok := reply.([]any)
		if !ok || len(arr) < 4 {
			continue
		}
		kind, _ := arr[0].(string)
		if kind != "pmessage" {
			continue
		}
		channel, _ := arr[2].(string)
		payload, _ := arr[3].(string)
		decoded, err := hex.DecodeString(payload)
		if err != nil {
			continue
		}
		select {
		case ch <- Event{Channel: channel, Value: decoded}:
		case <-done:
			return
		}
	}
}

func (s *redisStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stopCh)
	s.mu.Lock()
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.mu.Unlock()
	return nil
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	ne, ok := err.(net.Error)
	return ok && ne.Timeout()
}

const (
	redisBRPOPBlock = 2 // BRPOP block duration in seconds; loop retries on timeout
)

// Push enqueues payload on queue using Redis LPUSH.
// Exactly one Pull caller (across all nodes) will dequeue it via BRPOP.
func (s *redisStore) Push(_ context.Context, queue string, payload []byte) error {
	c, err := s.getConn()
	if err != nil {
		return err
	}
	_, err = respSendRead(c, "LPUSH", queue, hex.EncodeToString(payload))
	return err
}

// Pull dequeues the next item from queue using Redis BRPOP.
// Blocks until an item is available or ctx is cancelled.
// Opens a dedicated connection per call — BRPOP occupies the connection
// for the duration of the block, so it cannot share the KV connection.
func (s *redisStore) Pull(ctx context.Context, queue string) ([]byte, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("coord/redis: closed")
		default:
		}

		c, err := s.dial()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}

		blockSecs := strconv.Itoa(redisBRPOPBlock)
		_ = c.SetDeadline(time.Now().Add(time.Duration(redisBRPOPBlock+1) * time.Second))
		reply, err := respSendRead(c, "BRPOP", queue, blockSecs)
		c.Close()

		if err != nil {
			if isTimeout(err) {
				continue // block expired, loop
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}

		arr, ok := reply.([]any)
		if !ok || len(arr) < 2 {
			continue // nil reply = timeout, loop
		}
		encoded, ok := arr[1].(string)
		if !ok {
			continue
		}
		decoded, err := hex.DecodeString(encoded)
		if err != nil {
			return nil, fmt.Errorf("coord/redis: Pull decode: %w", err)
		}
		return decoded, nil
	}
}

var _ Store = (*redisStore)(nil)
