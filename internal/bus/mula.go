package bus

// Mula is the default intra-node transport connecting the Go router to
// Python/CLI/HTTP worker subprocesses on the same machine.
//
// Wire format: [4-byte big-endian length][payload]
// Message encoding: [2-byte topic-len][topic][data]

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// MulaConfig holds configuration for the Mula transport.
type MulaConfig struct {
	URL     string // tcp://host:port, port 0 = OS assigns
	SendBuf int
	RecvBuf int
}

// DefaultMulaConfig returns safe defaults.
func DefaultMulaConfig() MulaConfig {
	return MulaConfig{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128}
}

// Mula is a Bus implementation using Pepper's custom TCP framing protocol.
type Mula struct {
	cfg        MulaConfig
	baseAddr   string
	mu         sync.RWMutex
	subs       map[string][]*mulaRemoteSub
	localSubs  map[string][]*mulaLocalSub
	prefixSubs []mulaPrefixSub
	pushQs     map[string]*mulaPushQ
	closed     atomic.Bool
	listener   net.Listener
	port       int
	stopCh     chan struct{}
}

type mulaRemoteSub struct {
	conn     net.Conn
	topics   []string
	sendCh   chan []byte
	closed   atomic.Bool
	closedCh chan struct{}
}

type mulaPrefixSub struct {
	prefix    string
	ch        chan Message
	closeOnce *sync.Once
}

type mulaLocalSub struct {
	ch        chan Message
	closeOnce sync.Once
}

type mulaPushQ struct {
	ch chan []byte
}

// MulaMessage is a message received from the Mula bus.
type MulaMessage struct {
	Topic string
	Data  []byte
}

// NewMula creates a new Mula bus bound to the address in cfg.URL.
// It accepts either a Config or MulaConfig — callers may use Config for
// consistency with the Bus interface conventions.
func NewMula(cfg Config) (*Mula, error) {
	mcfg := MulaConfig{URL: cfg.URL, SendBuf: cfg.SendBuf, RecvBuf: cfg.RecvBuf}
	return newMulaFromMulaConfig(mcfg)
}

// NewMulaFromMulaConfig creates a Mula from a MulaConfig directly.
func NewMulaFromMulaConfig(cfg MulaConfig) (*Mula, error) {
	return newMulaFromMulaConfig(cfg)
}

func newMulaFromMulaConfig(cfg MulaConfig) (*Mula, error) {
	host, port, err := parseMulaAddr(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("coord.NewMula: %w", err)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, fmt.Errorf("coord.NewMula: listen %s:%d: %w", host, port, err)
	}
	b := &Mula{
		cfg:       cfg,
		baseAddr:  host,
		subs:      make(map[string][]*mulaRemoteSub),
		localSubs: make(map[string][]*mulaLocalSub),
		pushQs:    make(map[string]*mulaPushQ),
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
		return fmt.Errorf("mula: closed")
	}
	msg := mulaMarshal(topic, data)

	b.mu.RLock()
	remote := make([]*mulaRemoteSub, len(b.subs[topic]))
	copy(remote, b.subs[topic])
	local := make([]*mulaLocalSub, len(b.localSubs[topic]))
	copy(local, b.localSubs[topic])
	prefix := make([]mulaPrefixSub, len(b.prefixSubs))
	copy(prefix, b.prefixSubs)
	b.mu.RUnlock()

	for _, sub := range remote {
		if sub.closed.Load() {
			continue
		}
		select {
		case sub.sendCh <- msg:
		default:
		}
	}
	m := Message{Topic: topic, Data: data}
	for _, ls := range local {
		select {
		case ls.ch <- m:
		default:
		}
	}
	for _, ps := range prefix {
		if strings.HasPrefix(topic, ps.prefix) {
			select {
			case ps.ch <- m:
			default:
			}
		}
	}
	return nil
}

func (b *Mula) PushOne(ctx context.Context, topic string, data []byte) error {
	if b.closed.Load() {
		return fmt.Errorf("mula: closed")
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
		return nil, fmt.Errorf("mula: closed")
	}
	bufSize := b.cfg.RecvBuf
	if bufSize <= 0 {
		bufSize = 64
	}
	ch := make(chan Message, bufSize)
	once := &sync.Once{}
	ps := mulaPrefixSub{prefix: prefix, ch: ch, closeOnce: once}
	b.mu.Lock()
	b.prefixSubs = append(b.prefixSubs, ps)
	b.mu.Unlock()
	go func() {
		<-ctx.Done()
		b.mu.Lock()
		for i, s := range b.prefixSubs {
			if s.ch == ch {
				b.prefixSubs = append(b.prefixSubs[:i], b.prefixSubs[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		once.Do(func() { close(ch) })
	}()
	return ch, nil
}

func (b *Mula) subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	if b.closed.Load() {
		return nil, fmt.Errorf("mula: closed")
	}
	bufSize := b.cfg.RecvBuf
	if bufSize <= 0 {
		bufSize = 64
	}
	ls := &mulaLocalSub{ch: make(chan Message, bufSize)}
	b.mu.Lock()
	b.localSubs[topic] = append(b.localSubs[topic], ls)
	b.mu.Unlock()

	if strings.HasPrefix(topic, "pepper.push.") {
		go func() {
			q := b.getOrCreatePushQ(topic)
			for {
				select {
				case <-b.stopCh:
					return
				case <-ctx.Done():
					return
				case data := <-q.ch:
					select {
					case ls.ch <- Message{Topic: topic, Data: data}:
					default:
					}
				}
			}
		}()
	}

	go func() {
		<-ctx.Done()
		b.mu.Lock()
		subs := b.localSubs[topic]
		for i, s := range subs {
			if s == ls {
				b.localSubs[topic] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		ls.closeOnce.Do(func() { close(ls.ch) })
	}()
	return ls.ch, nil
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
		for _, ls := range lsubs {
			ls.closeOnce.Do(func() { close(ls.ch) })
		}
	}
	for i := range b.prefixSubs {
		ch := b.prefixSubs[i].ch
		b.prefixSubs[i].closeOnce.Do(func() { close(ch) })
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
				continue
			}
		}
		go b.handleConn(conn)
	}
}

func (b *Mula) handleConn(conn net.Conn) {
	defer conn.Close()
	helloData, err := mulaReadFrame(conn)
	if err != nil {
		return
	}
	topicStr := string(helloData)
	topics := strings.Split(topicStr, "|")
	if len(topics) > 0 && strings.HasPrefix(topics[0], "pull:") {
		b.servePuller(conn, strings.TrimPrefix(topics[0], "pull:"))
		return
	}
	sub := &mulaRemoteSub{
		conn:     conn,
		topics:   topics,
		sendCh:   make(chan []byte, b.cfg.SendBuf),
		closedCh: make(chan struct{}),
	}
	b.mu.Lock()
	for _, t := range topics {
		b.subs[t] = append(b.subs[t], sub)
	}
	b.mu.Unlock()
	go func() {
		defer func() {
			sub.closed.Store(true)
			close(sub.closedCh)
		}()
		for msg := range sub.sendCh {
			if err := mulaWriteFrame(conn, msg); err != nil {
				return
			}
		}
	}()
	for _, t := range topics {
		if strings.HasPrefix(t, "pepper.push.") {
			go b.drainPushToSub(sub, t)
		}
	}
	for {
		data, err := mulaReadFrame(conn)
		if err != nil {
			break
		}
		topic, payload := mulaUnmarshal(data)
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

func (b *Mula) drainPushToSub(sub *mulaRemoteSub, topic string) {
	q := b.getOrCreatePushQ(topic)
	for {
		select {
		case <-b.stopCh:
			return
		case <-sub.closedCh:
			return
		case data := <-q.ch:
			select {
			case sub.sendCh <- mulaMarshal(topic, data):
			case <-sub.closedCh:
				select {
				case q.ch <- data:
				default:
				}
				return
			case <-b.stopCh:
				select {
				case q.ch <- data:
				default:
				}
				return
			}
		}
	}
}

func (b *Mula) servePuller(conn net.Conn, topic string) {
	q := b.getOrCreatePushQ(topic)
	for {
		select {
		case <-b.stopCh:
			return
		case data := <-q.ch:
			if err := mulaWriteFrame(conn, data); err != nil {
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
	exact := make([]*mulaLocalSub, len(b.localSubs[topic]))
	copy(exact, b.localSubs[topic])
	prefix := make([]mulaPrefixSub, len(b.prefixSubs))
	copy(prefix, b.prefixSubs)
	b.mu.RUnlock()

	m := Message{Topic: topic, Data: data}
	for _, ls := range exact {
		select {
		case ls.ch <- m:
		default:
		}
	}
	for _, ps := range prefix {
		if strings.HasPrefix(topic, ps.prefix) {
			select {
			case ps.ch <- m:
			default:
			}
		}
	}
}

func (b *Mula) getOrCreatePushQ(topic string) *mulaPushQ {
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
	q = &mulaPushQ{ch: make(chan []byte, 256)}
	b.pushQs[topic] = q
	return q
}

// AsBus returns b as a Bus. Since *Mula now directly implements Bus,
// this is a convenience method for callers that need the interface type.
func (b *Mula) AsBus() Bus { return b }

// Wire encoding helpers.

func mulaWriteFrame(conn net.Conn, data []byte) error {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(data)))
	if _, err := conn.Write(hdr); err != nil {
		return err
	}
	_, err := conn.Write(data)
	return err
}

func mulaReadFrame(conn net.Conn) ([]byte, error) {
	hdr := make([]byte, 4)
	if _, err := io.ReadFull(conn, hdr); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(hdr)
	if size == 0 {
		return nil, nil
	}
	if size > 64*1024*1024 {
		return nil, fmt.Errorf("mula: frame too large: %d bytes", size)
	}
	buf := make([]byte, size)
	_, err := io.ReadFull(conn, buf)
	return buf, err
}

func mulaMarshal(topic string, data []byte) []byte {
	tb := []byte(topic)
	out := make([]byte, 2+len(tb)+len(data))
	binary.BigEndian.PutUint16(out[:2], uint16(len(tb)))
	copy(out[2:], tb)
	copy(out[2+len(tb):], data)
	return out
}

func mulaUnmarshal(frame []byte) (topic string, data []byte) {
	if len(frame) < 2 {
		return "", frame
	}
	tl := binary.BigEndian.Uint16(frame[:2])
	if int(tl) > len(frame)-2 {
		return "", frame
	}
	return string(frame[2 : 2+tl]), frame[2+tl:]
}

func parseMulaAddr(rawURL string) (host string, port int, err error) {
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
	case rawURL == "" || rawURL == "auto":
		return "127.0.0.1", 0, nil
	default:
		return "", 0, fmt.Errorf("unsupported mula URL %q — use tcp://host:port", rawURL)
	}
}
