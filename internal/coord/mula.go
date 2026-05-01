package coord

//
// Two modes:
//
// Single-node (NewMula): workers connect over a local TCP socket.
//     Wire: [4-byte big-endian length][frame]
//     Frame: [2-byte topic-len][topic-bytes][payload-bytes]
//
// Multi-node (NewMulaCluster): same TCP framing between Go and Python,
//     plus HashiCorp memberlist gossip for peer discovery and failure detection.
//     KV and pub/sub are forwarded to all peers; Push/Pull uses consistent
//     hashing so exactly one node owns each queue.

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/hashicorp/memberlist"
)

// Single-node constructor

// NewMula creates a single-node Mula coord.Store.
// url must be "tcp://host:port" or "" for a random port on 127.0.0.1.
func NewMula(url string) (Store, error) {
	host, port, err := parseMulaURL(url)
	if err != nil {
		return nil, fmt.Errorf("coord/mula: %w", err)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", host, port))
	if err != nil {
		return nil, fmt.Errorf("coord/mula: listen: %w", err)
	}
	m := newMulaCore(host, ln)
	go m.acceptLoop()
	return m, nil
}

// Multi-node constructor

// NewMulaCluster creates a multi-node Mula coord.Store backed by HashiCorp
// memberlist for peer discovery and failure detection.
//
//   - bindAddr: IP to bind for both memberlist gossip and RPC.
//   - gossipPort: UDP port for memberlist (0 = random).
//   - rpcPort: TCP port for inter-node coordination RPC (0 = random).
//   - seeds: existing cluster members in host:gossipPort format.
func NewMulaCluster(bindAddr string, gossipPort, rpcPort int, seeds []string) (Store, error) {
	// RPC listener for inter-node forwarding.
	ln, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindAddr, rpcPort))
	if err != nil {
		return nil, fmt.Errorf("coord/mula: rpc listen: %w", err)
	}

	m := newMulaCore(bindAddr, ln)

	// memberlist for peer discovery.
	conf := memberlist.DefaultLANConfig()
	conf.Name = ln.Addr().String() // stable identifier = RPC address
	conf.BindAddr = bindAddr
	conf.BindPort = gossipPort
	conf.AdvertiseAddr = bindAddr
	conf.AdvertisePort = gossipPort
	conf.Delegate = &mulaDelegate{rpcAddr: ln.Addr().String()}
	conf.LogOutput = io.Discard // suppress memberlist noise

	ml, err := memberlist.Create(conf)
	if err != nil {
		ln.Close()
		return nil, fmt.Errorf("coord/mula: memberlist: %w", err)
	}
	m.ml = ml

	if len(seeds) > 0 {
		if _, err := ml.Join(seeds); err != nil {
			// Non-fatal: start as single-node, rejoin later.
			_ = err
		}
	}

	go m.acceptLoop()
	return m, nil
}

// MulaAddr returns the tcp:// address a Store is listening on.
// Returns "" for non-Mula backends.
func MulaAddr(s Store) string {
	if m, ok := s.(*mulaStore); ok {
		return fmt.Sprintf("tcp://%s:%d", m.addr, m.port)
	}
	return ""
}

// MulaGossipPort returns the memberlist gossip port, or 0 for single-node.
func MulaGossipPort(s Store) int {
	if m, ok := s.(*mulaStore); ok && m.ml != nil {
		return int(m.ml.LocalNode().Port)
	}
	return 0
}

// Core implementation

type mulaStore struct {
	addr string
	port int
	ln   net.Listener
	ml   *memberlist.Memberlist // nil in single-node mode

	mu         sync.RWMutex
	remoteSubs map[string][]*mulaRemoteSub
	prefixSubs []*mulaPrefixSub
	localSubs  map[string][]*mulaLocalSub

	kvMu sync.RWMutex
	kv   map[string][]byte

	qmu    sync.Mutex
	pushQs map[string]chan []byte

	stopCh chan struct{}
	closed atomic.Bool
}

func newMulaCore(addr string, ln net.Listener) *mulaStore {
	return &mulaStore{
		addr:       addr,
		port:       ln.Addr().(*net.TCPAddr).Port,
		ln:         ln,
		remoteSubs: make(map[string][]*mulaRemoteSub),
		localSubs:  make(map[string][]*mulaLocalSub),
		kv:         make(map[string][]byte),
		pushQs:     make(map[string]chan []byte),
		stopCh:     make(chan struct{}),
	}
}

// KV store

func (m *mulaStore) Set(ctx context.Context, key string, value []byte, _ int64) error {
	if m.closed.Load() {
		return fmt.Errorf("coord/mula: closed")
	}
	// Multi-node: forward to the owner.
	if owner := m.keyOwner(key); owner != nil {
		return m.rpcSet(ctx, owner, key, value)
	}
	cp := make([]byte, len(value))
	copy(cp, value)
	m.kvMu.Lock()
	m.kv[key] = cp
	m.kvMu.Unlock()
	return nil
}

func (m *mulaStore) Get(ctx context.Context, key string) ([]byte, bool, error) {
	if owner := m.keyOwner(key); owner != nil {
		return m.rpcGet(ctx, owner, key)
	}
	m.kvMu.RLock()
	v, ok := m.kv[key]
	m.kvMu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	cp := make([]byte, len(v))
	copy(cp, v)
	return cp, true, nil
}

func (m *mulaStore) Delete(ctx context.Context, key string) error {
	if owner := m.keyOwner(key); owner != nil {
		return m.rpcDelete(ctx, owner, key)
	}
	m.kvMu.Lock()
	delete(m.kv, key)
	m.kvMu.Unlock()
	return nil
}

func (m *mulaStore) List(_ context.Context, prefix string) ([]string, error) {
	m.kvMu.RLock()
	defer m.kvMu.RUnlock()
	var keys []string
	for k := range m.kv {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

// Pub/sub

func (m *mulaStore) Publish(ctx context.Context, channel string, payload []byte) error {
	if m.closed.Load() {
		return fmt.Errorf("coord/mula: closed")
	}
	// Multi-node: broadcast to all peers.
	if m.ml != nil {
		for _, peer := range m.peers() {
			go m.rpcPublish(ctx, peer, channel, payload) //nolint:errcheck
		}
	}
	m.localPublish(channel, payload)
	return nil
}

func (m *mulaStore) localPublish(channel string, payload []byte) {
	frame := mulaMarshal(channel, payload)

	m.mu.RLock()
	remote := make([]*mulaRemoteSub, len(m.remoteSubs[channel]))
	copy(remote, m.remoteSubs[channel])
	prefix := make([]*mulaPrefixSub, len(m.prefixSubs))
	copy(prefix, m.prefixSubs)
	local := make([]*mulaLocalSub, len(m.localSubs[channel]))
	copy(local, m.localSubs[channel])
	m.mu.RUnlock()

	for _, sub := range remote {
		if !sub.closed.Load() {
			select {
			case sub.sendCh <- frame:
			default:
			}
		}
	}
	ev := Event{Channel: channel, Value: payload}
	for _, ls := range local {
		ls.mu.RLock()
		if !ls.closed {
			select {
			case ls.ch <- ev:
			default:
			}
		}
		ls.mu.RUnlock()
	}
	for _, ps := range prefix {
		if strings.HasPrefix(channel, ps.prefix) {
			ps.mu.RLock()
			if !ps.closed {
				select {
				case ps.ch <- ev:
				default:
				}
			}
			ps.mu.RUnlock()
		}
	}
}

func (m *mulaStore) Subscribe(ctx context.Context, channelPrefix string) (<-chan Event, error) {
	if m.closed.Load() {
		return nil, fmt.Errorf("coord/mula: closed")
	}

	if strings.HasSuffix(channelPrefix, ".") || strings.HasSuffix(channelPrefix, ":") {
		return m.subscribePrefixInternal(ctx, channelPrefix)
	}
	return m.subscribeExactInternal(ctx, channelPrefix)
}

// subscribePrefixInternal handles prefix subscriptions (topic ends with "." or ":").
func (m *mulaStore) subscribePrefixInternal(ctx context.Context, prefix string) (<-chan Event, error) {
	ps := &mulaPrefixSub{
		prefix: prefix,
		ch:     make(chan Event, 256),
	}

	m.mu.Lock()
	m.prefixSubs = append(m.prefixSubs, ps)
	m.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-m.stopCh:
		}
		// Remove from prefixSubs first so no new sends arrive, then close.
		m.mu.Lock()
		for i, s := range m.prefixSubs {
			if s == ps {
				m.prefixSubs = append(m.prefixSubs[:i], m.prefixSubs[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		// Acquire write-lock: no sender can be mid-send once we hold it.
		ps.mu.Lock()
		ps.closed = true
		close(ps.ch)
		ps.mu.Unlock()
	}()
	return ps.ch, nil
}

// subscribeExactInternal handles exact-topic subscriptions.
func (m *mulaStore) subscribeExactInternal(ctx context.Context, topic string) (<-chan Event, error) {
	ls := &mulaLocalSub{
		ch: make(chan Event, 256),
	}

	m.mu.Lock()
	m.localSubs[topic] = append(m.localSubs[topic], ls)
	m.mu.Unlock()

	// For push topics, bridge the coord push queue directly into ls.ch so that
	// in-process Go workers receive PushOne'd messages (not just Publish'd ones).
	// Pre-create the queue channel synchronously to guarantee coord.Push and
	// drainPushToLocalSub share the same channel regardless of scheduling order.
	if strings.HasPrefix(topic, "pepper.push.") {
		_ = m.getOrCreateQueue(topic)
		go m.drainPushToLocalSub(ctx, ls, topic)
	}

	go func() {
		select {
		case <-ctx.Done():
		case <-m.stopCh:
		}
		// Remove from localSubs first so no new sends arrive, then close.
		m.mu.Lock()
		subs := m.localSubs[topic]
		for i, s := range subs {
			if s == ls {
				m.localSubs[topic] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		m.mu.Unlock()
		// Acquire write-lock: no sender can be mid-send once we hold it.
		ls.mu.Lock()
		ls.closed = true
		close(ls.ch)
		ls.mu.Unlock()
	}()
	return ls.ch, nil
}

// Work queue

func (m *mulaStore) Push(ctx context.Context, queue string, payload []byte) error {
	if m.closed.Load() {
		return fmt.Errorf("coord/mula: closed")
	}
	// In cluster mode, push queues are worker-transport channels: Python workers
	// subscribe to their personal queue (pepper.push.<workerID>) and to the group
	// queue (pepper.push.<group>) via a long-lived TCP connection to whichever
	// node they started on. Consistent-hash routing sends ~50% of items to the
	// wrong node (the one without the subscribing worker), causing items to sit
	// unread until the request deadline. Instead, broadcast the item to every
	// node in the cluster so it lands in the local queue of the node that has a
	// drainPushToSub goroutine (i.e. a subscribed worker) for this topic.
	//
	// Exactly-once semantics are preserved because drainPushToSub uses a channel
	// receive (one goroutine wins) and only one node will have a remote subscriber
	// for a given worker-specific queue. For group queues, workers on different
	// nodes compete and the first to receive wins — identical to Redis BRPOP.
	if m.ml != nil {
		for _, peer := range m.peers() {
			go m.rpcPush(ctx, &peer, queue, payload) //nolint:errcheck
		}
	}
	q := m.getOrCreateQueue(queue)
	cp := make([]byte, len(payload))
	copy(cp, payload)
	select {
	case q <- cp:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mulaStore) Pull(ctx context.Context, queue string) ([]byte, error) {
	// Push broadcasts to all peers, so every node has the item locally.
	// Pull always reads from the local queue — no cross-node routing needed.
	q := m.getOrCreateQueue(queue)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-m.stopCh:
		return nil, fmt.Errorf("coord/mula: closed")
	case data := <-q:
		return data, nil
	}
}

func (m *mulaStore) getOrCreateQueue(name string) chan []byte {
	m.qmu.Lock()
	defer m.qmu.Unlock()
	if q, ok := m.pushQs[name]; ok {
		return q
	}
	q := make(chan []byte, 512)
	m.pushQs[name] = q
	return q
}

// Lifecycle

func (m *mulaStore) Close() error {
	if !m.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(m.stopCh)
	if m.ml != nil {
		m.ml.Leave(0)
		m.ml.Shutdown()
	}
	m.ln.Close()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, subs := range m.remoteSubs {
		for _, sub := range subs {
			sub.conn.Close()
		}
	}
	// Local and prefix sub cleanup goroutines select on stopCh and will run
	// their close-under-WLock logic once stopCh fires. Nothing to do here.
	return nil
}

// TCP accept loop

func (m *mulaStore) acceptLoop() {
	for {
		conn, err := m.ln.Accept()
		if err != nil {
			select {
			case <-m.stopCh:
				return
			default:
				continue
			}
		}
		go m.handleConn(conn)
	}
}

func (m *mulaStore) handleConn(conn net.Conn) {
	defer conn.Close()
	helloData, err := mulaReadFrame(conn)
	if err != nil {
		return
	}
	topicStr := string(helloData)

	// Internal RPC connection (inter-node forwarding).
	if topicStr == "rpc" {
		m.serveRPC(conn)
		return
	}

	topics := strings.Split(topicStr, "|")

	// Pull-only connection (Python BRPOP-style).
	if len(topics) > 0 && strings.HasPrefix(topics[0], "pull:") {
		m.servePuller(conn, strings.TrimPrefix(topics[0], "pull:"))
		return
	}

	sub := &mulaRemoteSub{
		conn:     conn,
		topics:   topics,
		sendCh:   make(chan []byte, 256),
		closedCh: make(chan struct{}),
	}
	m.mu.Lock()
	for _, t := range topics {
		m.remoteSubs[t] = append(m.remoteSubs[t], sub)
	}
	m.mu.Unlock()

	go func() {
		defer func() {
			sub.closed.Store(true)
			close(sub.closedCh)
		}()
		for frame := range sub.sendCh {
			if err := mulaWriteFrame(conn, frame); err != nil {
				return
			}
		}
	}()

	for _, t := range topics {
		if strings.HasPrefix(t, "pepper.push.") {
			go m.drainPushToSub(sub, t)
		}
	}

	for {
		data, err := mulaReadFrame(conn)
		if err != nil {
			break
		}
		topic, payload := mulaUnmarshal(data)
		// Use Publish (not localPublish) so that in multi-node mode the message
		// is broadcast to all peer stores via rpcPublish. This is essential for
		// cross-node visibility: when a Python worker on nodeA sends worker_hello
		// or a heartbeat, coordA must fan it out to coordB so that nodeB's router
		// can discover the worker and populate its cap-affinity table.
		// serveRPC's rpcOpPublish handler correctly calls localPublish at the
		// destination, so there is no re-broadcast loop.
		_ = m.Publish(context.Background(), topic, payload)
	}

	sub.closed.Store(true)
	m.mu.Lock()
	for _, t := range topics {
		subs := m.remoteSubs[t]
		for i, s := range subs {
			if s == sub {
				m.remoteSubs[t] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}
	m.mu.Unlock()
}

func (m *mulaStore) drainPushToSub(sub *mulaRemoteSub, topic string) {
	q := m.getOrCreateQueue(topic)
	for {
		select {
		case <-m.stopCh:
			return
		case <-sub.closedCh:
			return
		case data := <-q:
			frame := mulaMarshal(topic, data)
			select {
			case sub.sendCh <- frame:
			case <-sub.closedCh:
				select {
				case q <- data:
				default:
				}
				return
			case <-m.stopCh:
				select {
				case q <- data:
				default:
				}
				return
			}
		}
	}
}

// drainPushToLocalSub bridges the push queue for topic into a local in-process
// subscriber channel. It mirrors drainPushToSub (which does the same for remote
// TCP subscribers) so that Go workers using bus.Subscribe on a push topic receive
// messages delivered via PushOne / coord.Push — not only those sent via Publish.
//
// If the sub's channel closes before a dequeued item can be delivered, the item
// is returned to the queue so another subscriber (or a later reconnect) can pull it.
func (m *mulaStore) drainPushToLocalSub(ctx context.Context, ls *mulaLocalSub, topic string) {
	q := m.getOrCreateQueue(topic)
	for {
		select {
		case <-m.stopCh:
			return
		case <-ctx.Done():
			return
		case data := <-q:
			ev := Event{Channel: topic, Value: data}
			ls.mu.RLock()
			dropped := ls.closed
			if !dropped {
				select {
				case ls.ch <- ev:
				default:
					dropped = true // buffer full; treat as dropped
				}
			}
			ls.mu.RUnlock()
			if dropped {
				// Return item to queue so another subscriber or future drain can pick it up.
				select {
				case q <- data:
				default:
				}
				return
			}
		}
	}
}

func (m *mulaStore) servePuller(conn net.Conn, topic string) {
	q := m.getOrCreateQueue(topic)
	for {
		select {
		case <-m.stopCh:
			return
		case data := <-q:
			if err := mulaWriteFrame(conn, data); err != nil {
				select {
				case q <- data:
				default:
				}
				return
			}
		}
	}
}

// Multi-node peer routing

// peers returns all cluster members except self.
func (m *mulaStore) peers() []string {
	if m.ml == nil {
		return nil
	}
	self := m.ml.LocalNode().Name
	var out []string
	for _, node := range m.ml.Members() {
		if node.Name == self {
			continue
		}
		// Meta carries the RPC TCP address (bare host:port).
		if addr := string(node.Meta); addr != "" {
			if !strings.HasPrefix(addr, "tcp://") {
				addr = "tcp://" + addr
			}
			out = append(out, addr)
		}
	}
	return out
}

// allNodes returns self + all peers as RPC addresses, sorted for consistency.
func (m *mulaStore) allNodes() []string {
	self := fmt.Sprintf("tcp://%s:%d", m.addr, m.port)
	nodes := []string{self}
	nodes = append(nodes, m.peers()...)
	sort.Strings(nodes)
	return nodes
}

func (m *mulaStore) hashNode(key string) string {
	nodes := m.allNodes()
	if len(nodes) == 0 {
		return ""
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return nodes[h.Sum32()%uint32(len(nodes))]
}

func (m *mulaStore) selfAddr() string {
	return fmt.Sprintf("tcp://%s:%d", m.addr, m.port)
}

// keyOwner returns the RPC address of the node that owns a KV key,
// or nil if this node is the owner (or single-node mode).
func (m *mulaStore) keyOwner(key string) *string {
	if m.ml == nil {
		return nil
	}
	owner := m.hashNode(key)
	if owner == m.selfAddr() {
		return nil
	}
	return &owner
}

// queueOwner returns the RPC address of the node owning a push queue,
// or nil if this node is the owner.
func (m *mulaStore) queueOwner(queue string) *string {
	return m.keyOwner(queue)
}

// Inter-node RPC

const (
	rpcOpSet     = byte(1)
	rpcOpGet     = byte(2)
	rpcOpDelete  = byte(3)
	rpcOpPublish = byte(4)
	rpcOpPush    = byte(5)
)

func (m *mulaStore) rpcDial(addr string) (net.Conn, error) {
	conn, err := net.Dial("tcp", strings.TrimPrefix(addr, "tcp://"))
	if err != nil {
		return nil, err
	}
	// Identify as internal RPC connection.
	if err := mulaWriteFrame(conn, []byte("rpc")); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (m *mulaStore) serveRPC(conn net.Conn) {
	for {
		frame, err := mulaReadFrame(conn)
		if err != nil {
			return
		}
		if len(frame) < 1 {
			continue
		}
		op := frame[0]
		rest := frame[1:]
		switch op {
		case rpcOpSet:
			kl := binary.BigEndian.Uint16(rest)
			key := string(rest[2 : 2+kl])
			val := rest[2+kl:]
			m.kvMu.Lock()
			cp := make([]byte, len(val))
			copy(cp, val)
			m.kv[key] = cp
			m.kvMu.Unlock()
			mulaWriteFrame(conn, []byte{0}) //nolint:errcheck
		case rpcOpGet:
			key := string(rest)
			m.kvMu.RLock()
			v, ok := m.kv[key]
			m.kvMu.RUnlock()
			if !ok {
				mulaWriteFrame(conn, []byte{1}) //nolint:errcheck
			} else {
				resp := append([]byte{0}, v...)
				mulaWriteFrame(conn, resp) //nolint:errcheck
			}
		case rpcOpDelete:
			key := string(rest)
			m.kvMu.Lock()
			delete(m.kv, key)
			m.kvMu.Unlock()
			mulaWriteFrame(conn, []byte{0}) //nolint:errcheck
		case rpcOpPublish:
			kl := binary.BigEndian.Uint16(rest)
			channel := string(rest[2 : 2+kl])
			payload := rest[2+kl:]
			m.localPublish(channel, payload)
			mulaWriteFrame(conn, []byte{0}) //nolint:errcheck
		case rpcOpPush:
			kl := binary.BigEndian.Uint16(rest)
			queue := string(rest[2 : 2+kl])
			payload := rest[2+kl:]
			q := m.getOrCreateQueue(queue)
			cp := make([]byte, len(payload))
			copy(cp, payload)
			select {
			case q <- cp:
			default:
			}
			mulaWriteFrame(conn, []byte{0}) //nolint:errcheck
		}
	}
}

func rpcEncodeKV(op byte, key string, value []byte) []byte {
	kb := []byte(key)
	frame := make([]byte, 1+2+len(kb)+len(value))
	frame[0] = op
	binary.BigEndian.PutUint16(frame[1:], uint16(len(kb)))
	copy(frame[3:], kb)
	copy(frame[3+len(kb):], value)
	return frame
}

func (m *mulaStore) rpcSet(ctx context.Context, addr *string, key string, value []byte) error {
	conn, err := m.rpcDial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := mulaWriteFrame(conn, rpcEncodeKV(rpcOpSet, key, value)); err != nil {
		return err
	}
	_, err = mulaReadFrame(conn)
	return err
}

func (m *mulaStore) rpcGet(ctx context.Context, addr *string, key string) ([]byte, bool, error) {
	conn, err := m.rpcDial(*addr)
	if err != nil {
		return nil, false, err
	}
	defer conn.Close()
	frame := append([]byte{rpcOpGet}, []byte(key)...)
	if err := mulaWriteFrame(conn, frame); err != nil {
		return nil, false, err
	}
	resp, err := mulaReadFrame(conn)
	if err != nil || len(resp) == 0 {
		return nil, false, err
	}
	if resp[0] == 1 {
		return nil, false, nil
	}
	return resp[1:], true, nil
}

func (m *mulaStore) rpcDelete(ctx context.Context, addr *string, key string) error {
	conn, err := m.rpcDial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	frame := append([]byte{rpcOpDelete}, []byte(key)...)
	if err := mulaWriteFrame(conn, frame); err != nil {
		return err
	}
	_, err = mulaReadFrame(conn)
	return err
}

func (m *mulaStore) rpcPublish(ctx context.Context, addr string, channel string, payload []byte) error {
	conn, err := m.rpcDial(addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := mulaWriteFrame(conn, rpcEncodeKV(rpcOpPublish, channel, payload)); err != nil {
		return err
	}
	_, err = mulaReadFrame(conn)
	return err
}

func (m *mulaStore) rpcPullFrom(ctx context.Context, ownerAddr string, queue string) ([]byte, error) {
	conn, err := net.Dial("tcp", strings.TrimPrefix(ownerAddr, "tcp://"))
	if err != nil {
		return nil, fmt.Errorf("coord/mula: rpcPullFrom dial %s: %w", ownerAddr, err)
	}
	defer conn.Close()

	// Send the pull handshake: the owner's handleConn routes "pull:topic" to servePuller.
	if err := mulaWriteFrame(conn, []byte("pull:"+queue)); err != nil {
		return nil, fmt.Errorf("coord/mula: rpcPullFrom write: %w", err)
	}

	// servePuller blocks until a queue item is available, then writes it.
	// Apply the caller's context deadline to the TCP read.
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetReadDeadline(deadline) //nolint:errcheck
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close() // unblocks mulaReadFrame
		case <-done:
		}
	}()

	data, err := mulaReadFrame(conn)
	close(done)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("coord/mula: rpcPullFrom read: %w", err)
	}
	return data, nil
}

func (m *mulaStore) rpcPush(ctx context.Context, addr *string, queue string, payload []byte) error {
	conn, err := m.rpcDial(*addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if err := mulaWriteFrame(conn, rpcEncodeKV(rpcOpPush, queue, payload)); err != nil {
		return err
	}
	_, err = mulaReadFrame(conn)
	return err
}

// Internal types

type mulaRemoteSub struct {
	conn     net.Conn
	topics   []string
	sendCh   chan []byte
	closed   atomic.Bool
	closedCh chan struct{}
}

// mulaLocalSub is a local (in-process) pub/sub subscriber.
//
// Thread-safety: mu serialises close vs send. Senders acquire RLock, check
// closed, send. The cleanup goroutine acquires WLock, sets closed=true, closes ch.
// This avoids the close-vs-send race without adding goroutine hops: messages go
// directly from localPublish into ch with no intermediate relay or owner goroutine.
type mulaLocalSub struct {
	mu     sync.RWMutex
	closed bool
	ch     chan Event // callers read here; closed under WLock
}

// mulaPrefixSub follows the same mutex-protected pattern as mulaLocalSub.
type mulaPrefixSub struct {
	prefix string
	mu     sync.RWMutex
	closed bool
	ch     chan Event // callers read here; closed under WLock
}

// mulaDelegate provides memberlist with this node's RPC address via Meta.
type mulaDelegate struct {
	rpcAddr string
}

func (d *mulaDelegate) NodeMeta(limit int) []byte         { return []byte(d.rpcAddr) }
func (d *mulaDelegate) NotifyMsg([]byte)                  {}
func (d *mulaDelegate) GetBroadcasts(_, _ int) [][]byte   { return nil }
func (d *mulaDelegate) LocalState(_ bool) []byte          { return nil }
func (d *mulaDelegate) MergeRemoteState(_ []byte, _ bool) {}

// Wire helpers

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
		return nil, fmt.Errorf("coord/mula: frame too large: %d bytes", size)
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

func parseMulaURL(rawURL string) (host string, port int, err error) {
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

var _ Store = (*mulaStore)(nil)
