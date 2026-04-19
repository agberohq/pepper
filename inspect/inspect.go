// Package inspect implements the Pepper live wire inspector.
//
// The inspector mirrors every decoded message to a read-only socket
// without affecting the running process. Attach with:
//
//	pepper inspect /tmp/pepper-tap.sock
//	pepper inspect /tmp/pepper-tap.sock --filter cap=face.recognize
//	pepper inspect /tmp/pepper-tap.sock --filter hop=">0"
//	pepper inspect /tmp/pepper-tap.sock --format json
package inspect

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

// Inspector mirrors wire events to connected inspector clients.
type Inspector struct {
	path     string
	listener net.Listener
	mu       sync.RWMutex
	clients  map[net.Conn]*client
	eventCh  chan Event
	stopCh   chan struct{}
}

type client struct {
	conn   net.Conn
	filter Filter
	format string // "text" | "json"
}

// New creates an Inspector that listens on the given Unix socket path.
func New(path string) (*Inspector, error) {
	// Remove stale socket
	_ = os.Remove(path)

	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("inspect.New: listen %q: %w", path, err)
	}
	// Restrict access to owner only
	_ = os.Chmod(path, 0600)

	ins := &Inspector{
		path:     path,
		listener: ln,
		clients:  make(map[net.Conn]*client),
		eventCh:  make(chan Event, 1024),
		stopCh:   make(chan struct{}),
	}
	go ins.acceptLoop()
	go ins.broadcastLoop()
	return ins, nil
}

// Emit sends an event to all connected inspector clients.
// Non-blocking — drops events if the buffer is full.
func (ins *Inspector) Emit(e Event) {
	e.Time = time.Now()
	select {
	case ins.eventCh <- e:
	default:
		// Drop — never block the hot path for observability
	}
}

// Close shuts down the inspector.
func (ins *Inspector) Close() error {
	close(ins.stopCh)
	return ins.listener.Close()
}

func (ins *Inspector) acceptLoop() {
	for {
		conn, err := ins.listener.Accept()
		if err != nil {
			select {
			case <-ins.stopCh:
				return
			default:
				continue
			}
		}
		go ins.handleClient(conn)
	}
}

func (ins *Inspector) handleClient(conn net.Conn) {
	// Read filter config from client: one JSON line then stream events
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)

	c := &client{conn: conn, format: "text", filter: Filter{MinHop: -1}}
	if err == nil && n > 0 {
		var cfg struct {
			Filter Filter `json:"filter"`
			Format string `json:"format"`
		}
		if json.Unmarshal(buf[:n], &cfg) == nil {
			c.filter = cfg.Filter
			if cfg.Format != "" {
				c.format = cfg.Format
			}
		}
	}

	ins.mu.Lock()
	ins.clients[conn] = c
	ins.mu.Unlock()

	// Write a welcome line
	fmt.Fprintf(conn, "# pepper inspector — %s\n", time.Now().Format(time.RFC3339))

	// Keep alive — wait for disconnect
	discard := make([]byte, 1)
	for {
		_, err := conn.Read(discard)
		if err != nil {
			break
		}
	}

	ins.mu.Lock()
	delete(ins.clients, conn)
	ins.mu.Unlock()
	conn.Close()
}

func (ins *Inspector) broadcastLoop() {
	for {
		select {
		case <-ins.stopCh:
			return
		case e := <-ins.eventCh:
			ins.broadcast(e)
		}
	}
}

func (ins *Inspector) broadcast(e Event) {
	ins.mu.RLock()
	clients := make([]*client, 0, len(ins.clients))
	for _, c := range ins.clients {
		clients = append(clients, c)
	}
	ins.mu.RUnlock()

	for _, c := range clients {
		if !c.filter.Matches(e) {
			continue
		}
		var line string
		if c.format == "json" {
			data, _ := json.Marshal(e)
			line = string(data)
		} else {
			line = e.Format()
		}
		fmt.Fprintln(c.conn, line)
	}
}
