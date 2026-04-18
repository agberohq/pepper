package bus

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Mula struct {
	cfg        Config
	baseAddr   string
	mu         sync.RWMutex
	subs       map[string][]*subscriber
	localSubs  map[string][]chan Message
	prefixSubs []prefixSub
	pushQs     map[string]*pushQueue
	closed     atomic.Bool
	listener   net.Listener
	port       int
	stopCh     chan struct{}
}

type subscriber struct {
	conn   net.Conn
	topics []string
	sendCh chan []byte
	closed atomic.Bool
}

type prefixSub struct {
	prefix string
	ch     chan Message
}

type pushQueue struct {
	ch chan []byte
}

func NewMula(cfg Config) (*Mula, error) {
	host, port, err := parseAddr(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("bus.NewMula: %w", err)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, fmt.Errorf("bus.NewMula: listen %s:%d: %w", host, port, err)
	}
	b := &Mula{
		cfg:       cfg,
		baseAddr:  host,
		subs:      make(map[string][]*subscriber),
		localSubs: make(map[string][]chan Message),
		pushQs:    make(map[string]*pushQueue),
		listener:  ln,
		port:      ln.Addr().(*net.TCPAddr).Port,
		stopCh:    make(chan struct{}),
	}
	go b.acceptLoop()
	return b, nil
}

func (b *Mula) Addr() string {
	return fmt.Sprintf("tcp://%s:%d", b.baseAddr, b.port)
}

func (b *Mula) Publish(topic string, data []byte) error {
	if b.closed.Load() {
		return fmt.Errorf("bus: closed")
	}
	msg := encodeMsg(topic, data)

	b.mu.RLock()
	remoteSubs := make([]*subscriber, len(b.subs[topic]))
	copy(remoteSubs, b.subs[topic])
	localChans := make([]chan Message, len(b.localSubs[topic]))
	copy(localChans, b.localSubs[topic])
	prefixCopy := make([]prefixSub, len(b.prefixSubs))
	copy(prefixCopy, b.prefixSubs)
	b.mu.RUnlock()

	for _, sub := range remoteSubs {
		if sub.closed.Load() {
			continue
		}
		select {
		case sub.sendCh <- msg:
		case <-time.After(50 * time.Millisecond):
		}
	}

	m := Message{Topic: topic, Data: data}
	for _, ch := range localChans {
		_ = sendWithTimeout(ch, m, 50*time.Millisecond)
	}
	for _, ps := range prefixCopy {
		if strings.HasPrefix(topic, ps.prefix) {
			_ = sendWithTimeout(ps.ch, m, 50*time.Millisecond)
		}
	}
	return nil
}

func (b *Mula) PushOne(ctx context.Context, topic string, data []byte) error {
	if b.closed.Load() {
		return fmt.Errorf("bus: closed")
	}
	q := b.getOrCreatePushQ(topic)
	select {
	case q.ch <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Mula) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	return b.subscribe(ctx, topic)
}

func (b *Mula) SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("bus: closed")
	}
	bufSize := b.cfg.RecvBuf
	if bufSize <= 0 {
		bufSize = 64
	}
	ch := make(chan Message, bufSize)
	b.mu.Lock()
	b.prefixSubs = append(b.prefixSubs, prefixSub{prefix: prefix, ch: ch})
	b.mu.Unlock()
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		for i, ps := range b.prefixSubs {
			if ps.ch == ch {
				b.prefixSubs = append(b.prefixSubs[:i], b.prefixSubs[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		close(ch)
	}()
	return ch, nil
}

func (b *Mula) subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("bus: closed")
	}
	bufSize := b.cfg.RecvBuf
	if bufSize <= 0 {
		bufSize = 64
	}
	ch := make(chan Message, bufSize)
	b.mu.Lock()
	b.localSubs[topic] = append(b.localSubs[topic], ch)
	b.mu.Unlock()
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		subs := b.localSubs[topic]
		for i, s := range subs {
			if s == ch {
				b.localSubs[topic] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		close(ch)
	}()
	return ch, nil
}

func (b *Mula) Close() error {
	if !b.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(b.stopCh)
	b.listener.Close()
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, subs := range b.subs {
		for _, sub := range subs {
			sub.conn.Close()
		}
	}
	for _, lsubs := range b.localSubs {
		for _, ch := range lsubs {
			close(ch)
		}
	}
	for _, ps := range b.prefixSubs {
		close(ps.ch)
	}
	return nil
}

func (b *Mula) acceptLoop() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.stopCh:
				return
			default:
				time.Sleep(5 * time.Millisecond)
				continue
			}
		}
		go b.handleConn(conn)
	}
}

func (b *Mula) handleConn(conn net.Conn) {
	defer conn.Close()
	helloData, err := readFrame(conn)
	if err != nil {
		return
	}
	topicStr := string(helloData)
	topics := strings.Split(topicStr, "|")
	if len(topics) > 0 && strings.HasPrefix(topics[0], "pull:") {
		pullTopic := strings.TrimPrefix(topics[0], "pull:")
		b.servePuller(conn, pullTopic)
		return
	}
	sub := &subscriber{
		conn:   conn,
		topics: topics,
		sendCh: make(chan []byte, b.cfg.SendBuf),
	}
	b.mu.Lock()
	for _, t := range topics {
		b.subs[t] = append(b.subs[t], sub)
	}
	b.mu.Unlock()
	go func() {
		defer sub.closed.Store(true)
		for msg := range sub.sendCh {
			if err := writeFrame(conn, msg); err != nil {
				return
			}
		}
	}()
	for {
		data, err := readFrame(conn)
		if err != nil {
			break
		}
		topic, payload := decodeMsg(data)
		b.deliverToLocal(topic, payload)
	}
	sub.closed.Store(true)
	b.mu.Lock()
	for _, t := range topics {
		subs := b.subs[t]
		for i, s := range subs {
			if s == sub {
				b.subs[t] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}
	b.mu.Unlock()
}

func (b *Mula) servePuller(conn net.Conn, topic string) {
	q := b.getOrCreatePushQ(topic)
	for {
		select {
		case <-b.stopCh:
			return
		case data := <-q.ch:
			if err := writeFrame(conn, data); err != nil {
				select {
				case q.ch <- data:
				default:
				}
				return
			}
		}
	}
}

func (b *Mula) deliverToLocal(topic string, data []byte) {
	b.mu.RLock()
	exactSubs := make([]chan Message, len(b.localSubs[topic]))
	copy(exactSubs, b.localSubs[topic])
	prefixSubs := make([]prefixSub, len(b.prefixSubs))
	copy(prefixSubs, b.prefixSubs)
	b.mu.RUnlock()

	msg := Message{Topic: topic, Data: data}
	for _, ch := range exactSubs {
		_ = sendWithTimeout(ch, msg, 50*time.Millisecond)
	}
	for _, ps := range prefixSubs {
		if strings.HasPrefix(topic, ps.prefix) {
			_ = sendWithTimeout(ps.ch, msg, 50*time.Millisecond)
		}
	}
}

func (b *Mula) getOrCreatePushQ(topic string) *pushQueue {
	b.mu.RLock()
	q, ok := b.pushQs[topic]
	b.mu.RUnlock()
	if ok {
		return q
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if q, ok = b.pushQs[topic]; ok {
		return q
	}
	q = &pushQueue{ch: make(chan []byte, 256)}
	b.pushQs[topic] = q
	return q
}

func writeFrame(conn net.Conn, data []byte) error {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	_, err := conn.Write(data)
	return err
}

func readFrame(conn net.Conn) ([]byte, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(hdr)
	if size == 0 {
		return nil, nil
	}
	if size > 64*1024*1024 {
		return nil, fmt.Errorf("bus: frame too large: %d bytes", size)
	}
	buf := make([]byte, size)
	_, err := io.ReadFull(conn, buf)
	return buf, err
}

func encodeMsg(topic string, data []byte) []byte {
	tb := []byte(topic)
	out := make([]byte, 2+len(tb)+len(data))
	binary.BigEndian.PutUint16(out[:2], uint16(len(tb)))
	copy(out[2:], tb)
	copy(out[2+len(tb):], data)
	return out
}

func decodeMsg(frame []byte) (topic string, data []byte) {
	if len(frame) < 2 {
		return "", frame
	}
	tl := binary.BigEndian.Uint16(frame[:2])
	if int(tl) > len(frame)-2 {
		return "", frame
	}
	return string(frame[2 : 2+tl]), frame[2+tl:]
}

func parseAddr(rawURL string) (host string, port int, err error) {
	rawURL = strings.TrimSpace(rawURL)
	switch {
	case strings.HasPrefix(rawURL, "tcp://"):
		addr := strings.TrimPrefix(rawURL, "tcp://")
		h, p, e := net.SplitHostPort(addr)
		if e != nil {
			return "", 0, fmt.Errorf("invalid tcp URL %q: %w", rawURL, e)
		}
		port, err = strconv.Atoi(p)
		if err != nil {
			return "", 0, fmt.Errorf("invalid port in %q: %w", rawURL, err)
		}
		return h, port, nil
	case strings.HasPrefix(rawURL, "ipc://"):
		return "127.0.0.1", 7731, nil
	case rawURL == "" || rawURL == "auto":
		return "127.0.0.1", 0, nil
	default:
		return "", 0, fmt.Errorf("unsupported bus URL %q — use tcp:// or ipc://", rawURL)
	}
}

// sendWithTimeout sends to a channel with a short timeout to avoid silent drops in tests.
func sendWithTimeout(ch chan<- Message, msg Message, timeout time.Duration) bool {
	select {
	case ch <- msg:
		return true
	case <-time.After(timeout):
		return false
	}
}

func WorkerConnectAddr(busAddr string) string { return busAddr }
func GetPID() int                             { return os.Getpid() }

var _ Bus = (*Mula)(nil)
