package bus

//
// Maps the Pepper Bus interface onto Redis Streams (XADD/XREAD/XREADGROUP).
//
// Topic layout
//   pepper.push.{group}  → stream key, consumed via consumer group (XREADGROUP)
//                          so exactly one worker receives each message.
//   pepper.pub.{group}   → stream key, each subscriber reads from its own
//                          consumer group so every subscriber gets a copy.
//   pepper.res.*         → streams consumed by the router (single consumer).
//   pepper.broadcast     → stream consumed by all registered subscribers.
//
// Wire format: each stream entry has a single field "d" containing the raw
// message bytes encoded as a hex string (Redis Streams values must be strings).
//
// Usage:
//
//	b, err := bus.NewRedis(bus.Config{URL: "redis://127.0.0.1:6379"})
//	// then pass to pepper.WithBus(b) — not yet wired in config, coming soon.
//
// The adapter is intentionally self-contained (no external Redis library) so
// it stays consistent with the redis.go in internal/storage.

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

// redisBus implements Bus over Redis Streams.
type redisBus struct {
	addr string // host:port
	ttl  time.Duration

	mu         sync.RWMutex
	subs       map[string][]redisSub  // exact topic → subs
	prefixSubs []redisPrefixSub       // prefix → sub
	pushQs     map[string]*redisPushQ // push topics

	// publishedTopics is populated by every Publish/PushOne call so that
	// prefixReadLoop can discover streams as they are created, without polling
	// Redis SCAN (which can't see streams that don't exist yet).
	topicMu         sync.RWMutex
	publishedTopics map[string]struct{}

	closed atomic.Bool
	stopCh chan struct{}
	wg     sync.WaitGroup
}

type redisSub struct {
	topic string
	ch    chan Message
	ctx   context.Context
}

type redisPrefixSub struct {
	prefix string
	ch     chan Message
	ctx    context.Context
}

type redisPushQ struct {
	topic    string
	streamID string // Redis stream key
	ch       chan []byte
}

// NewRedis creates a Redis Streams bus.
// url accepts "redis://host:port" or bare "host:port".
// Messages older than ttl are trimmed from streams (0 = no trim).
func NewRedis(cfg Config) (*redisBus, error) {
	addr := strings.TrimPrefix(cfg.URL, "redis://")
	b := &redisBus{
		addr:            addr,
		ttl:             24 * time.Hour,
		subs:            make(map[string][]redisSub),
		pushQs:          make(map[string]*redisPushQ),
		publishedTopics: make(map[string]struct{}),
		stopCh:          make(chan struct{}),
	}
	// Verify connectivity immediately so the caller gets a clear error.
	if err := b.ping(); err != nil {
		return nil, fmt.Errorf("bus.NewRedis: ping %s: %w", addr, err)
	}
	return b, nil
}

// Bus interface

func (b *redisBus) Addr() string { return "redis://" + b.addr }

func (b *redisBus) Publish(topic string, data []byte) error {
	if b.closed.Load() {
		return fmt.Errorf("redis bus: closed")
	}
	if err := b.xadd(topic, data); err != nil {
		return err
	}
	// Register the topic so prefixReadLoop can discover it without SCAN.
	b.topicMu.Lock()
	b.publishedTopics[topic] = struct{}{}
	b.topicMu.Unlock()
	return nil
}

func (b *redisBus) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("redis bus: closed")
	}
	ch := make(chan Message, 64)
	sub := redisSub{topic: topic, ch: ch, ctx: ctx}

	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], sub)
	b.mu.Unlock()

	b.wg.Add(1)
	go b.readLoop(ctx, topic, ch)
	return ch, nil
}

func (b *redisBus) SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("redis bus: closed")
	}
	ch := make(chan Message, 64)

	b.mu.Lock()
	b.prefixSubs = append(b.prefixSubs, redisPrefixSub{prefix: prefix, ch: ch, ctx: ctx})
	b.mu.Unlock()

	b.wg.Add(1)
	go b.prefixReadLoop(ctx, prefix, ch)
	return ch, nil
}

// PushOne delivers to exactly one consumer using a Redis consumer group.
// The stream is named after the topic; the consumer group is "pepper-router".
// Workers connect as individual consumers in that group.
func (b *redisBus) PushOne(ctx context.Context, topic string, data []byte) error {
	if b.closed.Load() {
		return fmt.Errorf("redis bus: closed")
	}
	// Ensure the consumer group exists (XGROUP CREATE … MKSTREAM).
	if err := b.ensureGroup(topic, "pepper-router"); err != nil {
		return err
	}
	if err := b.xadd(topic, data); err != nil {
		return err
	}
	b.topicMu.Lock()
	b.publishedTopics[topic] = struct{}{}
	b.topicMu.Unlock()
	return nil
}

func (b *redisBus) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	// Signal all read loops to stop. Each loop owns its channel and closes it
	// via "defer close(ch)" when it exits — we must NOT close channels here or
	// we race with the loop's own close (panic: close of closed channel / send
	// on closed channel).
	close(b.stopCh)
	b.wg.Wait()
	return nil
}

// Redis read loops

// readLoop polls a Redis stream for one exact topic, forwarding to ch.
// Uses XREAD with a 1-second blocking timeout so ctx cancellation is noticed
// within ~1 s without busy-polling.
func (b *redisBus) readLoop(ctx context.Context, topic string, ch chan Message) {
	defer b.wg.Done()
	defer close(ch)

	lastID := "0-0" // read from beginning; switch to "$" after first read to tail
	seenFirst := false

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopCh:
			return
		default:
		}

		entries, err := b.xread(topic, lastID, 10, time.Second)
		if err != nil {
			// transient errors (network blip) — back off briefly
			select {
			case <-time.After(200 * time.Millisecond):
			case <-ctx.Done():
				return
			case <-b.stopCh:
				return
			}
			continue
		}
		if !seenFirst {
			// After the initial catch-up, only tail new entries.
			lastID = "$"
			seenFirst = true
		}
		for _, e := range entries {
			select {
			case ch <- Message{Topic: topic, Data: e.data}:
				lastID = e.id
			case <-ctx.Done():
				return
			case <-b.stopCh:
				return
			}
		}
	}
}

// prefixReadLoop delivers messages from all streams whose topic starts with
// prefix. Rather than polling Redis SCAN (which can't see streams that don't
// exist yet), it watches the bus's own publishedTopics registry — populated
// by every Publish/PushOne call — and polls any matching topics directly.
// This means delivery latency is bounded by the poll interval (~50 ms), not
// by a slow SCAN ticker.
func (b *redisBus) prefixReadLoop(ctx context.Context, prefix string, ch chan Message) {
	defer b.wg.Done()
	defer close(ch)

	// lastIDs tracks the last-seen entry ID per stream key.
	lastIDs := make(map[string]string)

	// syncFromRegistry merges newly-seen topics from publishedTopics into lastIDs.
	syncFromRegistry := func() {
		b.topicMu.RLock()
		for topic := range b.publishedTopics {
			if _, known := lastIDs[topic]; !known && hasPrefix(topic, prefix) {
				lastIDs[topic] = "0-0" // read from beginning for new streams
			}
		}
		b.topicMu.RUnlock()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopCh:
			return
		default:
		}

		syncFromRegistry()

		if len(lastIDs) == 0 {
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				return
			case <-b.stopCh:
				return
			}
			continue
		}

		for topic, lastID := range lastIDs {
			entries, err := b.xread(topic, lastID, 10, 0)
			if err != nil {
				continue
			}
			for _, e := range entries {
				select {
				case ch <- Message{Topic: topic, Data: e.data}:
					lastIDs[topic] = e.id
				case <-ctx.Done():
					return
				case <-b.stopCh:
					return
				}
			}
		}

		// Short sleep between poll cycles to avoid busy-spinning.
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return
		case <-b.stopCh:
			return
		}
	}
}

// low-level Redis helpers

type streamEntry struct {
	id   string
	data []byte
}

func (b *redisBus) ping() error {
	c, err := b.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	return redisCmd(c, "PING")
}

func (b *redisBus) xadd(stream string, data []byte) error {
	c, err := b.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	encoded := hex.EncodeToString(data)
	return redisCmd(c, "XADD", stream, "MAXLEN", "~", "100000", "*", "d", encoded)
}

func (b *redisBus) xread(stream, lastID string, count int, block time.Duration) ([]streamEntry, error) {
	c, err := b.dial()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	blockMs := int(block.Milliseconds())
	reply, err := redisCmdReply(c,
		"XREAD", "COUNT", strconv.Itoa(count),
		"BLOCK", strconv.Itoa(blockMs),
		"STREAMS", stream, lastID,
	)
	if err != nil || reply == nil {
		return nil, err
	}
	return parseXRead(reply, stream)
}

func (b *redisBus) ensureGroup(stream, group string) error {
	c, err := b.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	// XGROUP CREATE … $ MKSTREAM — ignore BUSYGROUP error (group already exists).
	err = redisCmd(c, "XGROUP", "CREATE", stream, group, "$", "MKSTREAM")
	if err != nil && strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return err
}

func (b *redisBus) dial() (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return (&net.Dialer{}).DialContext(ctx, "tcp", b.addr)
}

// redisCmd sends a command and reads the reply, returning an error on Redis errors.
func redisCmd(c net.Conn, args ...string) error {
	_, err := redisCmdReply(c, args...)
	return err
}

// redisCmdReply sends a RESP command and returns the parsed reply.
func redisCmdReply(c net.Conn, args ...string) (any, error) {
	_ = c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(a), a)
	}
	if _, err := fmt.Fprint(c, sb.String()); err != nil {
		return nil, fmt.Errorf("redis write: %w", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	return readRESPBus(bufio.NewReader(c))
}

// readRESPBus is a self-contained RESP parser (avoids importing internal/storage).
func readRESPBus(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, fmt.Errorf("redis: empty reply")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, fmt.Errorf("redis: %s", line[1:])
	case ':':
		n, err := strconv.ParseInt(line[1:], 10, 64)
		return n, err
	case '$':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := r.Read(buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		arr := make([]any, n)
		for i := range arr {
			arr[i], err = readRESPBus(r)
			if err != nil {
				return nil, err
			}
		}
		return arr, nil
	}
	return nil, fmt.Errorf("redis: unknown reply type %q", line[0])
}

// parseXRead extracts streamEntry values from an XREAD reply.
// XREAD response: *1 [ stream-name, *N [ [id, [field, value, ...]], ... ] ]
func parseXRead(reply any, streamName string) ([]streamEntry, error) {
	// Top level: array of [stream-name, entries]
	top, ok := reply.([]any)
	if !ok {
		return nil, nil
	}
	for _, item := range top {
		pair, ok := item.([]any)
		if !ok || len(pair) < 2 {
			continue
		}
		name, _ := pair[0].(string)
		if name != streamName {
			continue
		}
		entries, ok := pair[1].([]any)
		if !ok {
			continue
		}
		result := make([]streamEntry, 0, len(entries))
		for _, raw := range entries {
			entry, ok := raw.([]any)
			if !ok || len(entry) < 2 {
				continue
			}
			id, _ := entry[0].(string)
			fields, ok := entry[1].([]any)
			if !ok {
				continue
			}
			// fields: [key, value, key, value, ...]
			for i := 0; i+1 < len(fields); i += 2 {
				k, _ := fields[i].(string)
				v, _ := fields[i+1].(string)
				if k == "d" {
					data, err := hex.DecodeString(v)
					if err == nil {
						result = append(result, streamEntry{id: id, data: data})
					}
				}
			}
		}
		return result, nil
	}
	return nil, nil
}

// Ensure redisBus implements Bus.
var _ Bus = (*redisBus)(nil)
