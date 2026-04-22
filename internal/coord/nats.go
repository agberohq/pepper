package coord

import (
	"bufio"
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	natsWriteTimeout   = 3 * time.Second
	natsReadTimeout    = 5 * time.Second
	natsConnectTimeout = 3 * time.Second
	natsCoordKVBucket  = "pepper-coord"
	// Queue group name used for Pull subscribers across all nodes.
	// NATS delivers each message to exactly one member of the group.
	natsPullQueueGroup = "pepper-workers"
)

// NewNATS returns a NATS-backed Store using JetStream KV for key-value ops
// and core NATS pub/sub and queue groups for messaging.
//
//	c, err := coord.NewNATS("nats://127.0.0.1:4222")
//	pepper.New(pepper.WithCoord(c))
func NewNATS(url string) (Store, error) {
	addr := strings.TrimPrefix(url, "nats://")
	s := &natsStore{addr: addr, stopCh: make(chan struct{})}
	c, err := s.dial()
	if err != nil {
		return nil, fmt.Errorf("coord/nats: connect %q: %w", url, err)
	}
	if err := natsEnsureBucket(c); err != nil {
		c.Close()
		return nil, fmt.Errorf("coord/nats: ensure KV bucket: %w", err)
	}
	c.Close()
	return s, nil
}

type natsStore struct {
	addr   string
	stopCh chan struct{}
	closed atomic.Bool
}

// dial opens a raw NATS connection, drains the INFO banner, sends CONNECT {},
// then drains the server's initial PING so the connection is fully ready.
// NATS sends a PING immediately after CONNECT to verify liveness; if it goes
// unanswered the server closes the connection within ~2 seconds.
func (s *natsStore) dial() (net.Conn, error) {
	d := &net.Dialer{Timeout: natsConnectTimeout}
	c, err := d.Dial("tcp", s.addr)
	if err != nil {
		return nil, err
	}
	br := bufio.NewReader(c)
	// Drain lines until INFO is seen.
	for {
		_ = c.SetReadDeadline(time.Now().Add(natsReadTimeout))
		line, err := br.ReadString('\n')
		if err != nil {
			c.Close()
			return nil, err
		}
		if strings.HasPrefix(line, "INFO") {
			break
		}
	}
	_ = c.SetWriteDeadline(time.Now().Add(natsWriteTimeout))
	if _, err := fmt.Fprint(c, "CONNECT {}\r\n"); err != nil {
		c.Close()
		return nil, err
	}
	// Drain any immediate server messages (typically a PING right after CONNECT).
	// Respond to PING with PONG; stop at the first non-PING line or on timeout.
	_ = c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			// Timeout means no more immediate messages — connection is ready.
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "PING" {
			_ = c.SetWriteDeadline(time.Now().Add(natsWriteTimeout))
			if _, werr := fmt.Fprint(c, "PONG\r\n"); werr != nil {
				c.Close()
				return nil, werr
			}
		}
		// Any non-PING line (e.g. +OK) — stop draining.
		if line != "PING" {
			break
		}
	}
	_ = c.SetDeadline(time.Time{}) // clear deadline; callers set their own
	return c, nil
}

func natsWrite(c net.Conn, s string) error {
	_ = c.SetWriteDeadline(time.Now().Add(natsWriteTimeout))
	_, err := fmt.Fprint(c, s)
	return err
}

func natsReadLine(br *bufio.Reader, c net.Conn) (string, error) {
	_ = c.SetReadDeadline(time.Now().Add(natsReadTimeout))
	line, err := br.ReadString('\n')
	return strings.TrimRight(line, "\r\n"), err
}

// natsEnsureBucket creates the JetStream KV bucket if absent.
// Only fields supported by all NATS server versions are sent; in particular
// "history" is not a valid stream-create field and causes an -ERR on some builds.
func natsEnsureBucket(c net.Conn) error {
	br := bufio.NewReader(c)
	sid := 1
	inbox := fmt.Sprintf("_INBOX.coord.%d", sid)
	if err := natsWrite(c, fmt.Sprintf("SUB %s %d\r\n", inbox, sid)); err != nil {
		return err
	}
	payload := `{"num_replicas":1}`
	msg := fmt.Sprintf("PUB $JS.API.STREAM.CREATE.KV_%s %s %d\r\n%s\r\n",
		natsCoordKVBucket, inbox, len(payload), payload)
	if err := natsWrite(c, msg); err != nil {
		return err
	}
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		line, err := natsReadLine(br, c)
		if err != nil {
			return nil // timeout reading response is acceptable
		}
		if line == "PING" {
			// Server keepalive during the wait — answer and keep reading.
			_ = natsWrite(c, "PONG\r\n")
			continue
		}
		if strings.HasPrefix(line, "MSG") {
			parts := strings.Fields(line)
			if len(parts) >= 4 {
				n, _ := strconv.Atoi(parts[3])
				buf := make([]byte, n+2)
				_, _ = br.Read(buf)
			}
			return nil
		}
		if strings.HasPrefix(line, "-ERR") || strings.HasPrefix(line, "+OK") {
			return nil
		}
	}
}

func kvSubject(key string) string {
	return fmt.Sprintf("$KV.%s.%s", natsCoordKVBucket, key)
}

func (s *natsStore) Set(_ context.Context, key string, value []byte, _ int64) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	encoded := hex.EncodeToString(value)
	subj := kvSubject(key)
	return natsWrite(c, fmt.Sprintf("PUB %s %d\r\n%s\r\n", subj, len(encoded), encoded))
}

func (s *natsStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	c, err := s.dial()
	if err != nil {
		return nil, false, err
	}
	defer c.Close()
	br := bufio.NewReader(c)

	sid := 1
	inbox := fmt.Sprintf("_INBOX.coord.get.%d", sid)
	if err := natsWrite(c, fmt.Sprintf("SUB %s %d\r\n", inbox, sid)); err != nil {
		return nil, false, err
	}
	body := fmt.Sprintf(`{"last_by_subj":"%s"}`, kvSubject(key))
	apiSubj := fmt.Sprintf("$JS.API.DIRECT.GET.KV_%s", natsCoordKVBucket)
	msg := fmt.Sprintf("PUB %s %s %d\r\n%s\r\n", apiSubj, inbox, len(body), body)
	if err := natsWrite(c, msg); err != nil {
		return nil, false, err
	}

	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := natsReadLine(br, c)
		if err != nil {
			return nil, false, nil
		}
		if line == "PING" {
			_ = natsWrite(c, "PONG\r\n")
			continue
		}
		if !strings.HasPrefix(line, "MSG") {
			return nil, false, nil
		}
		parts := strings.Fields(line)
		if len(parts) < 4 {
			return nil, false, nil
		}
		n, _ := strconv.Atoi(parts[3])
		buf := make([]byte, n+2)
		if _, err := br.Read(buf); err != nil {
			return nil, false, err
		}
		decoded, err := hex.DecodeString(strings.TrimSpace(string(buf[:n])))
		if err != nil {
			return nil, false, err
		}
		return decoded, true, nil
	}
}

func (s *natsStore) Delete(_ context.Context, key string) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	subj := kvSubject(key)
	hdr := "NATS/1.0\r\nKV-Operation: DEL\r\n\r\n"
	return natsWrite(c, fmt.Sprintf("HPUB %s %d 0\r\n%s\r\n", subj, len(hdr), hdr))
}

func (s *natsStore) List(_ context.Context, prefix string) ([]string, error) {
	c, err := s.dial()
	if err != nil {
		return nil, err
	}
	defer c.Close()
	br := bufio.NewReader(c)

	sid := 1
	inbox := fmt.Sprintf("_INBOX.coord.list.%d", sid)
	if err := natsWrite(c, fmt.Sprintf("SUB %s %d\r\n", inbox, sid)); err != nil {
		return nil, err
	}
	pattern := kvSubject(prefix) + ">"
	body := fmt.Sprintf(`{"filter":"%s"}`, pattern)
	apiSubj := fmt.Sprintf("$JS.API.STREAM.NAMES.KV_%s", natsCoordKVBucket)
	if err := natsWrite(c, fmt.Sprintf("PUB %s %s %d\r\n%s\r\n", apiSubj, inbox, len(body), body)); err != nil {
		return nil, err
	}

	var keys []string
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := natsReadLine(br, c)
		if err != nil {
			break
		}
		if line == "PING" {
			_ = natsWrite(c, "PONG\r\n")
			continue
		}
		if !strings.HasPrefix(line, "MSG") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}
		n, _ := strconv.Atoi(parts[3])
		buf := make([]byte, n+2)
		if _, err := br.Read(buf); err != nil {
			break
		}
		if len(parts) >= 2 {
			subj := parts[1]
			rawKey := strings.TrimPrefix(subj, fmt.Sprintf("$KV.%s.", natsCoordKVBucket))
			if strings.HasPrefix(rawKey, prefix) {
				keys = append(keys, rawKey)
			}
		}
	}
	return keys, nil
}

func (s *natsStore) Publish(_ context.Context, channel string, payload []byte) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	encoded := hex.EncodeToString(payload)
	return natsWrite(c, fmt.Sprintf("PUB %s %d\r\n%s\r\n", channel, len(encoded), encoded))
}

// Subscribe listens on channels matching channelPrefix.
// Each call opens a dedicated connection with a plain SUB.
// Handles both exact topics (pepper.control.w-1) and prefix patterns (pepper.control.*).
func (s *natsStore) Subscribe(ctx context.Context, channelPrefix string) (<-chan Event, error) {
	c, err := s.dial()
	if err != nil {
		return nil, fmt.Errorf("coord/nats: subscribe dial: %w", err)
	}

	ch := make(chan Event, 64)

	// NATS wildcards: * (single token), > (multi-token suffix, must be dot-prefixed)
	// If caller passes a Redis-style pattern with *, convert to NATS >
	subj := channelPrefix
	if strings.HasSuffix(subj, "*") {
		subj = strings.TrimSuffix(subj, "*") + ">"
	}

	// Subscribe to the computed subject
	sid := 1
	if err := natsWrite(c, fmt.Sprintf("SUB %s %d\r\n", subj, sid)); err != nil {
		c.Close()
		return nil, err
	}

	// If the original was an exact topic (no wildcard), also subscribe to its prefix
	// to match Redis PSUBSCRIBE behavior. Use a DIFFERENT sid.
	if !strings.ContainsAny(channelPrefix, "*>") {
		sid++
		prefixSubj := channelPrefix + ".>"
		if err := natsWrite(c, fmt.Sprintf("SUB %s %d\r\n", prefixSubj, sid)); err != nil {
			c.Close()
			return nil, err
		}
	}

	go s.readMessages(ctx, c, ch)
	return ch, nil
}

func (s *natsStore) readMessages(ctx context.Context, c net.Conn, ch chan Event) {
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
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, err := natsReadLine(br, c)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			return
		}
		// Respond to server keepalive PINGs so the connection stays alive.
		if line == "PING" {
			_ = natsWrite(c, "PONG\r\n")
			continue
		}
		if !strings.HasPrefix(line, "MSG") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}
		channel := parts[1]
		n, _ := strconv.Atoi(parts[3])
		buf := make([]byte, n+2)
		if _, err := br.Read(buf); err != nil {
			return
		}
		decoded, err := hex.DecodeString(strings.TrimSpace(string(buf[:n])))
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

// Push publishes payload to a NATS queue subject.
// Pull subscribers using the shared queue group receive exactly one copy.
// Subject format: pepper.queue.{queue}
func (s *natsStore) Push(_ context.Context, queue string, payload []byte) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	encoded := hex.EncodeToString(payload)
	return natsWrite(c, fmt.Sprintf("PUB %s %d\r\n%s\r\n", queue, len(encoded), encoded))
}

// Pull subscribes to the queue subject with a shared queue group and blocks
// until one message arrives or ctx is cancelled.
// All nodes calling Pull on the same queue compete — NATS delivers to exactly one.
func (s *natsStore) Pull(ctx context.Context, queue string) ([]byte, error) {
	c, err := s.dial()
	if err != nil {
		return nil, fmt.Errorf("coord/nats: Pull dial: %w", err)
	}
	defer c.Close()

	subj := queue
	sid := 1
	// Queue-group subscription: all nodes subscribe with the same group name.
	// NATS selects exactly one subscriber per message.
	if err := natsWrite(c, fmt.Sprintf("SUB %s %s %d\r\n", subj, natsPullQueueGroup, sid)); err != nil {
		return nil, err
	}

	br := bufio.NewReader(c)
	done := ctx.Done()
	for {
		select {
		case <-done:
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("coord/nats: closed")
		default:
		}
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, err := natsReadLine(br, c)
		if err != nil {
			if isTimeout(err) {
				continue
			}
			return nil, err
		}
		if line == "PING" {
			_ = natsWrite(c, "PONG\r\n")
			continue
		}
		if !strings.HasPrefix(line, "MSG") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}
		n, _ := strconv.Atoi(parts[3])
		buf := make([]byte, n+2)
		if _, err := br.Read(buf); err != nil {
			return nil, err
		}
		decoded, err := hex.DecodeString(strings.TrimSpace(string(buf[:n])))
		if err != nil {
			return nil, fmt.Errorf("coord/nats: Pull decode: %w", err)
		}
		return decoded, nil
	}
}

func (s *natsStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stopCh)
	return nil
}

var _ Store = (*natsStore)(nil)
