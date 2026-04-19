package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// NewRedis returns a Redis-backed Store.
// url must be a Redis address in the form "host:port" or "redis://host:port".
// ttl sets the session idle expiry (applied via EXPIRE on every write/touch).
//
//	pepper.WithSessionStore(storage.NewRedis("localhost:6379", 24*time.Hour))
func NewRedis(url string, ttl time.Duration) Store {
	addr := strings.TrimPrefix(url, "redis://")
	return &redisStore{addr: addr, ttl: ttl}
}

type redisStore struct {
	addr string
	ttl  time.Duration
	mu   sync.Mutex
	conn net.Conn
}

// Store interface

func (r *redisStore) Set(sessionID, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("redis.Set marshal: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c, err := r.dial()
	if err != nil {
		return err
	}
	hkey := hkey(sessionID)
	if err := r.send(c, "HSET", hkey, key, string(data)); err != nil {
		return err
	}
	if _, err := r.readReply(c); err != nil {
		return err
	}
	return r.expire(c, hkey)
}

func (r *redisStore) Get(sessionID, key string) (any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, err := r.dial()
	if err != nil {
		return nil, false
	}
	if err := r.send(c, "HGET", hkey(sessionID), key); err != nil {
		return nil, false
	}
	reply, err := r.readReply(c)
	if err != nil || reply == nil {
		return nil, false
	}
	raw, ok := reply.(string)
	if !ok {
		return nil, false
	}
	var v any
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		return raw, true
	}
	return v, true
}

func (r *redisStore) GetAll(sessionID string) (map[string]any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, err := r.dial()
	if err != nil {
		return nil, false
	}
	if err := r.send(c, "HGETALL", hkey(sessionID)); err != nil {
		return nil, false
	}
	reply, err := r.readReply(c)
	if err != nil {
		return nil, false
	}
	arr, ok := reply.([]any)
	if !ok || len(arr) == 0 {
		return nil, false
	}
	out := make(map[string]any, len(arr)/2)
	for i := 0; i+1 < len(arr); i += 2 {
		k, _ := arr[i].(string)
		v, _ := arr[i+1].(string)
		var decoded any
		if json.Unmarshal([]byte(v), &decoded) == nil {
			out[k] = decoded
		} else {
			out[k] = v
		}
	}
	return out, true
}

func (r *redisStore) Merge(sessionID string, updates map[string]any) error {
	if len(updates) == 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c, err := r.dial()
	if err != nil {
		return err
	}
	args := []string{hkey(sessionID)}
	for k, v := range updates {
		data, _ := json.Marshal(v)
		args = append(args, k, string(data))
	}
	cmdArgs := make([]any, len(args))
	for i, a := range args {
		cmdArgs[i] = a
	}
	if err := r.send(c, "HSET", args...); err != nil {
		return err
	}
	if _, err := r.readReply(c); err != nil {
		return err
	}
	return r.expire(c, args[0])
}

func (r *redisStore) Touch(sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, err := r.dial()
	if err != nil {
		return err
	}
	return r.expire(c, hkey(sessionID))
}

func (r *redisStore) Clear(sessionID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, err := r.dial()
	if err != nil {
		return err
	}
	if err := r.send(c, "DEL", hkey(sessionID)); err != nil {
		return err
	}
	_, err = r.readReply(c)
	return err
}

func (r *redisStore) Exists(sessionID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, err := r.dial()
	if err != nil {
		return false
	}
	if err := r.send(c, "EXISTS", hkey(sessionID)); err != nil {
		return false
	}
	reply, err := r.readReply(c)
	if err != nil {
		return false
	}
	n, _ := reply.(int64)
	return n > 0
}

// internals

func hkey(sessionID string) string { return "pepper:session:" + sessionID }

func (r *redisStore) dial() (net.Conn, error) {
	if r.conn != nil {
		// Ping to check liveness
		if err := r.send(r.conn, "PING"); err == nil {
			if reply, err := r.readReply(r.conn); err == nil && reply == "PONG" {
				return r.conn, nil
			}
		}
		r.conn.Close()
		r.conn = nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	d := net.Dialer{}
	conn, err := d.DialContext(ctx, "tcp", r.addr)
	if err != nil {
		return nil, fmt.Errorf("redis: connect %q: %w", r.addr, err)
	}
	r.conn = conn
	return conn, nil
}

func (r *redisStore) expire(c net.Conn, key string) error {
	secs := int64(r.ttl.Seconds())
	if secs <= 0 {
		return nil
	}
	if err := r.send(c, "EXPIRE", key, strconv.FormatInt(secs, 10)); err != nil {
		return err
	}
	_, err := r.readReply(c)
	return err
}

// send writes a RESP command to the connection.
func (r *redisStore) send(c net.Conn, cmd string, args ...string) error {
	_ = c.SetWriteDeadline(time.Now().Add(3 * time.Second))
	parts := append([]string{cmd}, args...)
	var sb strings.Builder
	fmt.Fprintf(&sb, "*%d\r\n", len(parts))
	for _, p := range parts {
		fmt.Fprintf(&sb, "$%d\r\n%s\r\n", len(p), p)
	}
	_, err := fmt.Fprint(c, sb.String())
	return err
}

// readReply reads one RESP reply from the connection.
func (r *redisStore) readReply(c net.Conn) (any, error) {
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	br := bufio.NewReader(c)
	return readRESP(br)
}

func readRESP(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, fmt.Errorf("redis: empty reply")
	}
	switch line[0] {
	case '+': // simple string
		return line[1:], nil
	case '-': // error
		return nil, fmt.Errorf("redis: %s", line[1:])
	case ':': // integer
		n, err := strconv.ParseInt(line[1:], 10, 64)
		return n, err
	case '$': // bulk string
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil // nil bulk
		}
		buf := make([]byte, n+2)
		if _, err := r.Read(buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*': // array
		n, err := strconv.Atoi(line[1:])
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		arr := make([]any, n)
		for i := range arr {
			arr[i], err = readRESP(r)
			if err != nil {
				return nil, err
			}
		}
		return arr, nil
	}
	return nil, fmt.Errorf("redis: unknown reply type %q", line[0])
}
