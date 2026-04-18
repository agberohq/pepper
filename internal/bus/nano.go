package bus

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/pull"
	"go.nanomsg.org/mangos/v3/protocol/push"
	"go.nanomsg.org/mangos/v3/protocol/sub"
	// Import all transports
	_ "go.nanomsg.org/mangos/v3/transport/inproc"
	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

// freePort returns an available TCP port.
func freePort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// Nano implements the Bus interface using mangos (nanomsg).
type Nano struct {
	cfg Config
	// Sockets
	pullSock mangos.Socket // receives all responses (pepper.res.*, pepper.hb.*, pepper.cb.*)
	pubSock  mangos.Socket // broadcast socket (pepper.broadcast)
	// Per-group PUB sockets (created on demand)
	mu       sync.RWMutex
	pubSocks map[string]mangos.Socket // group -> PUB socket
	// Local subscribers (in-process channels)
	localSubs  map[string][]chan Message
	prefixSubs []prefixSub
	localMu    sync.RWMutex
	// PUSH queues for DispatchAny
	pushQs map[string]chan []byte
	pushMu sync.RWMutex
	// Subscriber sockets for local delivery
	subSocks   map[string]mangos.Socket // topic -> SUB socket for local delivery
	subSocksMu sync.RWMutex
	closed     atomic.Bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	addr       string
	port       int
}

// setDeadlineOption sets a socket deadline option, ignoring if unsupported.
// Some mangos socket types/transports don't support deadline options.
func setDeadlineOption(sock mangos.Socket, opt string, val time.Duration) {
	_ = sock.SetOption(opt, val) // ignore error - deadline is optional optimization
}

// NewNano creates a new mangos-based bus.
func NewNano(cfg Config) (*Nano, error) {
	if cfg.SendBuf <= 0 {
		cfg.SendBuf = 128
	}
	if cfg.RecvBuf <= 0 {
		cfg.RecvBuf = 128
	}
	n := &Nano{
		cfg:       cfg,
		pubSocks:  make(map[string]mangos.Socket),
		pushQs:    make(map[string]chan []byte),
		localSubs: make(map[string][]chan Message),
		subSocks:  make(map[string]mangos.Socket),
		stopCh:    make(chan struct{}),
	}

	// Create PULL socket for receiving responses
	pullSock, err := pull.NewSocket()
	if err != nil {
		return nil, fmt.Errorf("nano: create pull socket: %w", err)
	}
	if err := pullSock.SetOption(mangos.OptionReadQLen, cfg.RecvBuf); err != nil {
		pullSock.Close()
		return nil, fmt.Errorf("nano: set read q len: %w", err)
	}
	// Set receive deadline (optional - ignore if unsupported)
	setDeadlineOption(pullSock, mangos.OptionRecvDeadline, 100*time.Millisecond)

	// Parse and bind address
	host, port, err := parseAddr(cfg.URL)
	if err != nil {
		pullSock.Close()
		return nil, fmt.Errorf("nano: parse addr: %w", err)
	}
	if host == "" {
		host = "127.0.0.1"
	}
	if port == 0 {
		port = freePort()
	}
	bindAddr := fmt.Sprintf("tcp://%s:%d", host, port)
	if err := pullSock.Listen(bindAddr); err != nil {
		pullSock.Close()
		return nil, fmt.Errorf("nano: listen %s: %w", bindAddr, err)
	}
	n.pullSock = pullSock
	n.addr = fmt.Sprintf("tcp://%s:%d", host, port)
	n.port = port

	// Create broadcast PUB socket
	pubSock, err := pub.NewSocket()
	if err != nil {
		n.Close()
		return nil, fmt.Errorf("nano: create pub socket: %w", err)
	}
	if err := pubSock.SetOption(mangos.OptionWriteQLen, cfg.SendBuf); err != nil {
		n.Close()
		return nil, fmt.Errorf("nano: set write q len: %w", err)
	}
	// Set send deadline (optional - ignore if unsupported)
	setDeadlineOption(pubSock, mangos.OptionSendDeadline, 100*time.Millisecond)
	n.pubSock = pubSock

	// Start receive loops
	n.wg.Add(1)
	go n.receiveLoop()

	return n, nil
}

// receiveLoop receives messages from the PULL socket and delivers to local subscribers.
func (n *Nano) receiveLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}
		msg, err := n.pullSock.Recv()
		if err != nil {
			if n.closed.Load() {
				return
			}
			// Timeout is expected, continue
			continue
		}
		topic, data := decodeMsg(msg)
		n.deliverToLocal(topic, data)
	}
}

// Publish sends data to all subscribers on a topic (PUB/SUB semantics).
func (n *Nano) Publish(topic string, data []byte) error {
	if n.closed.Load() {
		return fmt.Errorf("bus: closed")
	}
	msg := encodeMsg(topic, data)
	var sock mangos.Socket
	var err error
	if topic == TopicBroadcast {
		sock = n.pubSock
	} else if IsPubTopic(topic) {
		sock, err = n.getOrCreatePubSocket(topic)
		if err != nil {
			return err
		}
	} else {
		// For non-pub topics, just deliver locally
		n.deliverToLocal(topic, data)
		return nil
	}
	// Send to mangos subscribers
	if err := sock.Send(msg); err != nil {
		// If send fails, still try local delivery
		n.deliverToLocal(topic, data)
		return fmt.Errorf("nano: publish: %w", err)
	}
	// Also deliver to local subscribers
	n.deliverToLocal(topic, data)
	return nil
}

// IsPubTopic reports whether a topic should use PUB/SUB.
func IsPubTopic(topic string) bool {
	return hasPrefix(topic, "pepper.pub.") || topic == TopicBroadcast
}

// Subscribe returns a channel receiving all messages on an exact topic.
func (n *Nano) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	if n.closed.Load() {
		return nil, fmt.Errorf("bus: closed")
	}
	ch := make(chan Message, n.cfg.RecvBuf)
	n.localMu.Lock()
	n.localSubs[topic] = append(n.localSubs[topic], ch)
	n.localMu.Unlock()

	go func() {
		<-ctx.Done()
		n.localMu.Lock()
		subs := n.localSubs[topic]
		for i, s := range subs {
			if s == ch {
				n.localSubs[topic] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		n.localMu.Unlock()
		close(ch)
	}()
	return ch, nil
}

// ensureSubSocket creates a SUB socket for local delivery of a topic.
func (n *Nano) ensureSubSocket(topic string) error {
	n.subSocksMu.Lock()
	defer n.subSocksMu.Unlock()
	if _, exists := n.subSocks[topic]; exists {
		return nil
	}
	sock, err := sub.NewSocket()
	if err != nil {
		return fmt.Errorf("nano: create sub socket: %w", err)
	}
	if err := sock.SetOption(mangos.OptionSubscribe, []byte(topic)); err != nil {
		sock.Close()
		return fmt.Errorf("nano: subscribe %s: %w", topic, err)
	}
	// Set recv deadline (optional - ignore if unsupported)
	setDeadlineOption(sock, mangos.OptionRecvDeadline, 100*time.Millisecond)
	if err := sock.Dial(n.addr); err != nil {
		sock.Close()
		return fmt.Errorf("nano: dial %s: %w", n.addr, err)
	}
	n.subSocks[topic] = sock
	// Start a goroutine to receive from this socket
	n.wg.Add(1)
	go n.subReceiveLoop(topic, sock)
	return nil
}

// subReceiveLoop receives from a SUB socket and delivers locally.
func (n *Nano) subReceiveLoop(topic string, sock mangos.Socket) {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}
		msg, err := sock.Recv()
		if err != nil {
			if n.closed.Load() {
				return
			}
			// Timeout is expected
			continue
		}
		recvTopic, data := decodeMsg(msg)
		n.deliverToLocal(recvTopic, data)
	}
}

// SubscribePrefix returns a channel receiving messages on any topic that starts with prefix.
func (n *Nano) SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error) {
	if n.closed.Load() {
		return nil, fmt.Errorf("bus: closed")
	}
	ch := make(chan Message, n.cfg.RecvBuf)
	n.localMu.Lock()
	n.prefixSubs = append(n.prefixSubs, prefixSub{prefix: prefix, ch: ch})
	n.localMu.Unlock()
	go func() {
		<-ctx.Done()
		n.localMu.Lock()
		for i, ps := range n.prefixSubs {
			if ps.ch == ch {
				n.prefixSubs = append(n.prefixSubs[:i], n.prefixSubs[i+1:]...)
				break
			}
		}
		n.localMu.Unlock()
		close(ch)
	}()
	return ch, nil
}

// PushOne sends data to exactly one subscriber (PUSH/PULL semantics).
func (n *Nano) PushOne(ctx context.Context, topic string, data []byte) error {
	if n.closed.Load() {
		return fmt.Errorf("bus: closed")
	}
	q := n.getOrCreatePushQ(topic)
	select {
	case q <- data:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Addr returns the transport address workers use to connect.
func (n *Nano) Addr() string {
	return n.addr
}

// Close shuts down all sockets.
func (n *Nano) Close() error {
	if !n.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(n.stopCh)
	n.wg.Wait()
	if n.pullSock != nil {
		n.pullSock.Close()
	}
	if n.pubSock != nil {
		n.pubSock.Close()
	}
	n.mu.Lock()
	for _, sock := range n.pubSocks {
		sock.Close()
	}
	n.pubSocks = nil
	n.mu.Unlock()
	n.subSocksMu.Lock()
	for _, sock := range n.subSocks {
		sock.Close()
	}
	n.subSocks = nil
	n.subSocksMu.Unlock()
	// Close local subscriber channels
	n.localMu.Lock()
	for _, subs := range n.localSubs {
		for _, ch := range subs {
			close(ch)
		}
	}
	for _, ps := range n.prefixSubs {
		close(ps.ch)
	}
	n.localSubs = nil
	n.prefixSubs = nil
	n.localMu.Unlock()
	// Close push queues
	n.pushMu.Lock()
	for _, q := range n.pushQs {
		close(q)
	}
	n.pushQs = nil
	n.pushMu.Unlock()
	return nil
}

// getOrCreatePubSocket returns a PUB socket for the given topic's group.
func (n *Nano) getOrCreatePubSocket(topic string) (mangos.Socket, error) {
	group := GroupFromPubTopic(topic)
	if group == "" {
		group = topic
	}
	n.mu.RLock()
	sock, ok := n.pubSocks[group]
	n.mu.RUnlock()
	if ok {
		return sock, nil
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if sock, ok = n.pubSocks[group]; ok {
		return sock, nil
	}
	sock, err := pub.NewSocket()
	if err != nil {
		return nil, fmt.Errorf("nano: create pub socket for %s: %w", group, err)
	}
	if err := sock.SetOption(mangos.OptionWriteQLen, n.cfg.SendBuf); err != nil {
		sock.Close()
		return nil, fmt.Errorf("nano: set write q len: %w", err)
	}
	// Set send deadline (optional - ignore if unsupported)
	setDeadlineOption(sock, mangos.OptionSendDeadline, 100*time.Millisecond)
	n.pubSocks[group] = sock
	return sock, nil
}

// getOrCreatePushQ returns a queue channel for PUSH/PULL delivery.
func (n *Nano) getOrCreatePushQ(topic string) chan []byte {
	n.pushMu.RLock()
	q, ok := n.pushQs[topic]
	n.pushMu.RUnlock()
	if ok {
		return q
	}
	n.pushMu.Lock()
	defer n.pushMu.Unlock()
	if q, ok = n.pushQs[topic]; ok {
		return q
	}
	q = make(chan []byte, 256)
	n.pushQs[topic] = q
	n.wg.Add(1)
	go n.pushWorker(topic, q)
	return q
}

// pushWorker pulls messages from the queue and sends them via a PUSH socket.
func (n *Nano) pushWorker(topic string, q chan []byte) {
	defer n.wg.Done()
	sock, err := push.NewSocket()
	if err != nil {
		return
	}
	defer sock.Close()
	if err := sock.SetOption(mangos.OptionWriteQLen, n.cfg.SendBuf); err != nil {
		return
	}
	// Set send deadline (optional - ignore if unsupported)
	setDeadlineOption(sock, mangos.OptionSendDeadline, 100*time.Millisecond)
	if err := sock.Dial(n.addr); err != nil {
		return
	}
	for {
		select {
		case <-n.stopCh:
			return
		case data, ok := <-q:
			if !ok {
				return
			}
			msg := encodeMsg(topic, data)
			_ = sock.Send(msg)
		}
	}
}

// deliverToLocal sends a message to all local subscribers.
func (n *Nano) deliverToLocal(topic string, data []byte) {
	n.localMu.RLock()
	exactSubs := make([]chan Message, len(n.localSubs[topic]))
	copy(exactSubs, n.localSubs[topic])
	prefixSubs := make([]prefixSub, len(n.prefixSubs))
	copy(prefixSubs, n.prefixSubs)
	n.localMu.RUnlock()

	msg := Message{Topic: topic, Data: data}
	// Exact matches
	for _, ch := range exactSubs {
		select {
		case ch <- msg:
		default:
		}
	}
	// Prefix matches
	for _, ps := range prefixSubs {
		if hasPrefix(topic, ps.prefix) {
			select {
			case ps.ch <- msg:
			default:
			}
		}
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// WorkerConnectAddr returns the address workers should connect to.
func (n *Nano) WorkerConnectAddr() string {
	return n.addr
}

// Ensure Nano implements Bus.
var _ Bus = (*Nano)(nil)

// WorkerDial creates a SUB socket for a worker and subscribes to topics.
func WorkerDial(busAddr string, topics []string) (mangos.Socket, error) {
	sock, err := sub.NewSocket()
	if err != nil {
		return nil, fmt.Errorf("worker dial: create sub socket: %w", err)
	}
	for _, topic := range topics {
		if err := sock.SetOption(mangos.OptionSubscribe, []byte(topic)); err != nil {
			sock.Close()
			return nil, fmt.Errorf("worker dial: subscribe %s: %w", topic, err)
		}
	}
	if err := sock.SetOption(mangos.OptionSubscribe, []byte(TopicBroadcast)); err != nil {
		sock.Close()
		return nil, fmt.Errorf("worker dial: subscribe broadcast: %w", err)
	}
	if err := sock.Dial(busAddr); err != nil {
		sock.Close()
		return nil, fmt.Errorf("worker dial: connect to %s: %w", busAddr, err)
	}
	return sock, nil
}

// WorkerPush creates a PUSH socket for a worker to send responses.
func WorkerPush(busAddr string) (mangos.Socket, error) {
	sock, err := push.NewSocket()
	if err != nil {
		return nil, fmt.Errorf("worker push: create socket: %w", err)
	}
	if err := sock.Dial(busAddr); err != nil {
		sock.Close()
		return nil, fmt.Errorf("worker push: connect to %s: %w", busAddr, err)
	}
	return sock, nil
}

// WorkerPull creates a PULL socket for a worker to receive DispatchAny requests.
func WorkerPull(busAddr string) (mangos.Socket, error) {
	sock, err := pull.NewSocket()
	if err != nil {
		return nil, fmt.Errorf("worker pull: create socket: %w", err)
	}
	if err := sock.Dial(busAddr); err != nil {
		sock.Close()
		return nil, fmt.Errorf("worker pull: connect to %s: %w", busAddr, err)
	}
	return sock, nil
}
