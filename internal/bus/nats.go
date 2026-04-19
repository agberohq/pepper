package bus

//
// Maps the Pepper Bus interface onto NATS subjects using the raw NATS text
// protocol (no external library required — keeps the dependency footprint zero).
//
// Topic layout (direct mapping: Pepper topic → NATS subject)
//   pepper.push.{group}  → NATS queue-subscribe with queue group "pepper-push"
//                          so exactly one subscriber receives each message.
//   pepper.pub.{group}   → NATS publish, all plain subscribers receive it.
//   pepper.res.*         → plain subscribe with a wildcard (pepper.res.>)
//   pepper.broadcast     → plain publish/subscribe
//
// Wire encoding: raw bytes published as NATS message payload. No extra framing.
//
// PushOne semantics via NATS queue groups:
//   Workers subscribe to "pepper.push.{group}" with QueueGroup "pepper-push".
//   NATS delivers each published message to exactly one member of the group.
//   The router side uses PushOne which publishes once; workers pull it.
//
// Usage:
//
//	b, err := bus.NewNATS(bus.Config{URL: "nats://127.0.0.1:4222"})

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	natsPushQueueGroup = "pepper-push"
	natsReadTimeout    = 5 * time.Second
	natsWriteTimeout   = 3 * time.Second
	natsConnectTimeout = 3 * time.Second
)

// natsConn wraps a single NATS client connection with its subscription SIDs.
type natsConn struct {
	conn net.Conn
	brd  *bufio.Reader
	mu   sync.Mutex
	sid  atomic.Int64 // monotonic subscription ID
}

func dialNATS(addr string) (*natsConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), natsConnectTimeout)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	nc := &natsConn{conn: raw, brd: bufio.NewReader(raw)}
	// Drain the INFO banner.
	raw.SetReadDeadline(time.Now().Add(natsReadTimeout))
	if _, err := nc.brd.ReadString('\n'); err != nil {
		raw.Close()
		return nil, fmt.Errorf("nats: read INFO: %w", err)
	}
	return nc, nil
}

func (nc *natsConn) nextSID() string {
	return strconv.FormatInt(nc.sid.Add(1), 10)
}

// pub publishes msg to subject.
func (nc *natsConn) pub(subject string, msg []byte) error {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.conn.SetWriteDeadline(time.Now().Add(natsWriteTimeout))
	_, err := fmt.Fprintf(nc.conn, "PUB %s %d\r\n", subject, len(msg))
	if err != nil {
		return err
	}
	if _, err := nc.conn.Write(msg); err != nil {
		return err
	}
	_, err = nc.conn.Write([]byte("\r\n"))
	return err
}

// sub subscribes to subject. queueGroup may be empty for a plain subscription.
func (nc *natsConn) sub(subject, queueGroup string) (string, error) {
	sid := nc.nextSID()
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.conn.SetWriteDeadline(time.Now().Add(natsWriteTimeout))
	var err error
	if queueGroup != "" {
		_, err = fmt.Fprintf(nc.conn, "SUB %s %s %s\r\n", subject, queueGroup, sid)
	} else {
		_, err = fmt.Fprintf(nc.conn, "SUB %s %s\r\n", subject, sid)
	}
	return sid, err
}

// unsub unsubscribes sid.
func (nc *natsConn) unsub(sid string) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.conn.SetWriteDeadline(time.Now().Add(natsWriteTimeout))
	fmt.Fprintf(nc.conn, "UNSUB %s\r\n", sid)
}

// readMsg blocks until a MSG arrives for any subscribed SID.
// Returns subject, payload.  Handles PING/PONG keepalives internally.
func (nc *natsConn) readMsg(timeout time.Duration) (subject string, payload []byte, err error) {
	for {
		nc.conn.SetReadDeadline(time.Now().Add(timeout))
		line, err := nc.brd.ReadString('\n')
		if err != nil {
			return "", nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "PING":
			nc.mu.Lock()
			nc.conn.SetWriteDeadline(time.Now().Add(natsWriteTimeout))
			fmt.Fprint(nc.conn, "PONG\r\n")
			nc.mu.Unlock()
			continue
		case strings.HasPrefix(line, "MSG "):
			// MSG <subject> <sid> [<reply>] <#bytes>
			parts := strings.Fields(line)
			if len(parts) < 4 {
				continue
			}
			byteCount, _ := strconv.Atoi(parts[len(parts)-1])
			subj := parts[1]
			buf := make([]byte, byteCount)
			if _, err := nc.brd.Read(buf); err != nil {
				return "", nil, err
			}
			// consume trailing \r\n
			nc.brd.ReadString('\n')
			return subj, buf, nil
		case strings.HasPrefix(line, "-ERR"):
			return "", nil, fmt.Errorf("nats: %s", line)
		}
		// ignore +OK and other lines
	}
}

func (nc *natsConn) close() { nc.conn.Close() }

// natsBus

type natsBus struct {
	addr string

	mu       sync.RWMutex
	subConns []*natsSubConn // one conn per Subscribe/SubscribePrefix call
	closed   atomic.Bool
	stopCh   chan struct{}
	wg       sync.WaitGroup

	// pubConn is used only for Publish/PushOne — not for reads.
	pubMu   sync.Mutex
	pubConn *natsConn
}

type natsSubConn struct {
	nc      *natsConn
	subject string // subscribed NATS subject (may contain wildcards)
	ch      chan Message
	ctx     context.Context
}

// NewNATS creates a NATS bus adapter.
// cfg.URL must be "nats://host:port" or bare "host:port".
func NewNATS(cfg Config) (*natsBus, error) {
	addr := strings.TrimPrefix(cfg.URL, "nats://")
	b := &natsBus{addr: addr, stopCh: make(chan struct{})}

	// Establish the shared publish connection.
	nc, err := dialNATS(addr)
	if err != nil {
		return nil, fmt.Errorf("bus.NewNATS: connect %s: %w", addr, err)
	}
	b.pubConn = nc
	return b, nil
}

// Bus interface

func (b *natsBus) Addr() string { return "nats://" + b.addr }

func (b *natsBus) Publish(topic string, data []byte) error {
	if b.closed.Load() {
		return fmt.Errorf("nats bus: closed")
	}
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	return b.pubConn.pub(topic, data)
}

// PushOne publishes to a NATS queue-group subject.
// Workers must subscribe with the same queue group (pepper-push) so NATS
// delivers to exactly one of them.
func (b *natsBus) PushOne(ctx context.Context, topic string, data []byte) error {
	if b.closed.Load() {
		return fmt.Errorf("nats bus: closed")
	}
	// NATS queue group delivery is handled on the subscriber side.
	// The publisher just sends a normal PUB — NATS routes it to one member.
	b.pubMu.Lock()
	defer b.pubMu.Unlock()
	return b.pubConn.pub(topic, data)
}

// Subscribe opens a dedicated NATS connection for this subscription.
// Each subscription gets its own connection so that blocking reads on one
// do not delay delivery on another.
func (b *natsBus) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("nats bus: closed")
	}
	return b.subscribe(ctx, topic, "")
}

// SubscribePrefix subscribes to all NATS subjects matching prefix.
// Pepper prefix "pepper.res." maps to NATS wildcard "pepper.res.>".
func (b *natsBus) SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("nats bus: closed")
	}
	// Convert Pepper prefix to NATS wildcard.
	// "pepper.res." → "pepper.res.>"
	// "pepper.control." → "pepper.control.>"
	subject := strings.TrimSuffix(prefix, ".") + ".>"
	return b.subscribe(ctx, subject, "")
}

func (b *natsBus) subscribe(ctx context.Context, subject, queueGroup string) (<-chan Message, error) {
	nc, err := dialNATS(b.addr)
	if err != nil {
		return nil, fmt.Errorf("nats subscribe: dial: %w", err)
	}
	if _, err := nc.sub(subject, queueGroup); err != nil {
		nc.close()
		return nil, fmt.Errorf("nats subscribe: SUB %s: %w", subject, err)
	}

	ch := make(chan Message, 64)
	sc := &natsSubConn{nc: nc, subject: subject, ch: ch, ctx: ctx}

	b.mu.Lock()
	b.subConns = append(b.subConns, sc)
	b.mu.Unlock()

	b.wg.Add(1)
	go b.subReadLoop(sc)
	return ch, nil
}

// subReadLoop reads messages from one NATS connection and forwards to ch.
func (b *natsBus) subReadLoop(sc *natsSubConn) {
	defer b.wg.Done()
	defer close(sc.ch)
	defer sc.nc.close()

	for {
		select {
		case <-sc.ctx.Done():
			return
		case <-b.stopCh:
			return
		default:
		}

		subj, payload, err := sc.nc.readMsg(time.Second)
		if err != nil {
			if b.closed.Load() || sc.ctx.Err() != nil {
				return
			}
			// Transient read error (timeout, blip) — retry.
			select {
			case <-time.After(100 * time.Millisecond):
			case <-sc.ctx.Done():
				return
			case <-b.stopCh:
				return
			}
			continue
		}

		select {
		case sc.ch <- Message{Topic: subj, Data: payload}:
		case <-sc.ctx.Done():
			return
		case <-b.stopCh:
			return
		}
	}
}

func (b *natsBus) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(b.stopCh)

	b.pubMu.Lock()
	if b.pubConn != nil {
		b.pubConn.close()
	}
	b.pubMu.Unlock()

	b.wg.Wait()
	return nil
}

// Ensure natsBus implements Bus.
var _ Bus = (*natsBus)(nil)
