package coord

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
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
	natsKVBucket       = "pepper-coord"
	natsWorkStream     = "pepper-work"
)

func NewNATS(url string) (Store, error) {
	addr := strings.TrimPrefix(url, "nats://")
	s := &natsStore{addr: addr, stopCh: make(chan struct{})}
	c, err := s.dial()
	if err != nil {
		return nil, fmt.Errorf("coord/nats: connect %q: %w", url, err)
	}
	defer c.Close()

	if err := natsEnsureKVBucket(c); err != nil {
		return nil, fmt.Errorf("coord/nats: ensure KV bucket: %w", err)
	}
	if err := natsEnsureWorkStream(c); err != nil {
		return nil, fmt.Errorf("coord/nats: ensure work stream: %w", err)
	}

	return s, nil
}

type natsStore struct {
	addr   string
	stopCh chan struct{}
	closed atomic.Bool
}

// dial opens a TCP connection and completes the NATS protocol handshake.
//
// headers:true — required so the server accepts HPUB frames (used by Delete).
// Without it the server rejects HPUB with "message headers not supported".
//
// no_responders:true — server immediately sends a 404 status when a JetStream
// fetch request finds no messages, rather than holding the connection open.
func (s *natsStore) dial() (net.Conn, error) {
	d := &net.Dialer{Timeout: natsConnectTimeout}
	c, err := d.Dial("tcp", s.addr)
	if err != nil {
		return nil, err
	}

	// Read INFO banner byte-by-byte. NATS 2.x sends PING right after INFO;
	// reading one byte at a time ensures we don't over-consume into that PING.
	_ = c.SetReadDeadline(time.Now().Add(natsReadTimeout))
	var buf []byte
	b := make([]byte, 1)
	for {
		if _, err := c.Read(b); err != nil {
			c.Close()
			return nil, err
		}
		buf = append(buf, b[0])
		if b[0] == '\n' {
			break
		}
	}
	if !strings.HasPrefix(string(buf), "INFO") {
		c.Close()
		return nil, fmt.Errorf("coord/nats: expected INFO, got %q", string(buf))
	}

	_ = c.SetWriteDeadline(time.Now().Add(natsWriteTimeout))
	if _, err := fmt.Fprint(c, `CONNECT {"headers":true,"no_responders":true}`+"\r\n"); err != nil {
		c.Close()
		return nil, err
	}

	_ = c.SetDeadline(time.Time{})
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
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// natsReadMsg reads exactly one MSG or HMSG frame from br, strips any NATS
// header section, and returns the body bytes and the reply subject.
//
// Wire formats:
//
//	MSG  <subj> <sid> [reply] <bytes>\r\n<body(\r\n)>
//	HMSG <subj> <sid> [reply] <hdrLen> <totalLen>\r\n<header(hdrLen)><body>\r\n
//
// We use io.ReadFull instead of br.Read to avoid short reads that would
// corrupt the connection's parse state for subsequent frames.
func natsReadMsg(line string, br *bufio.Reader) (body []byte, replySubj string, err error) {
	parts := strings.Fields(line)

	if strings.HasPrefix(line, "HMSG") {
		// HMSG <subj> <sid> [reply] <hdrLen> <totalLen>
		if len(parts) < 5 {
			return nil, "", fmt.Errorf("coord/nats: malformed HMSG: %q", line)
		}
		totalLen, _ := strconv.Atoi(parts[len(parts)-1])
		hdrLen, _ := strconv.Atoi(parts[len(parts)-2])
		if len(parts) == 6 { // reply present
			replySubj = parts[3]
		}
		buf := make([]byte, totalLen+2) // +2 for trailing \r\n
		if _, err := io.ReadFull(br, buf); err != nil {
			return nil, replySubj, err
		}
		// body is everything after the header section, up to totalLen
		return buf[hdrLen:totalLen], replySubj, nil
	}

	// Plain MSG: <subj> <sid> [reply] <len>
	if len(parts) < 4 {
		return nil, "", fmt.Errorf("coord/nats: malformed MSG: %q", line)
	}
	totalLen, _ := strconv.Atoi(parts[len(parts)-1])
	if len(parts) == 5 { // reply present
		replySubj = parts[3]
	}
	buf := make([]byte, totalLen+2)
	if _, err := io.ReadFull(br, buf); err != nil {
		return nil, replySubj, err
	}
	return buf[:totalLen], replySubj, nil
}

// natsJSRequest sends one JetStream API request and returns the response body.
// It subscribes on inbox, publishes the request with inbox as reply-to, then
// reads until it gets a MSG/HMSG (skipping PING, +OK, -ERR).
func natsJSRequest(c net.Conn, apiSubj, inbox string, sid int, payload []byte) ([]byte, error) {
	br := bufio.NewReader(c)

	if err := natsWrite(c, fmt.Sprintf("SUB %s %d\r\n", inbox, sid)); err != nil {
		return nil, err
	}
	if err := natsWrite(c, fmt.Sprintf("PUB %s %s %d\r\n%s\r\n", apiSubj, inbox, len(payload), payload)); err != nil {
		return nil, err
	}

	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		line, err := natsReadLine(br, c)
		if err != nil {
			return nil, err
		}
		switch {
		case line == "PING":
			_ = natsWrite(c, "PONG\r\n")
		case strings.HasPrefix(line, "+OK"), strings.HasPrefix(line, "-ERR"):
			// skip
		case strings.HasPrefix(line, "MSG"), strings.HasPrefix(line, "HMSG"):
			body, _, err := natsReadMsg(line, br)
			if err != nil {
				return nil, err
			}
			return body, nil
		}
	}
}

// natsEnsureKVBucket creates (or is a no-op for existing) the JetStream KV
// stream. "history" is intentionally absent — it is a KV-layer concept;
// max_msgs_per_subject=1 is the correct stream-level equivalent.
func natsEnsureKVBucket(c net.Conn) error {
	config := map[string]any{
		"name":                 "KV_" + natsKVBucket,
		"subjects":             []string{"$KV." + natsKVBucket + ".>"},
		"max_msgs_per_subject": 1,
		"storage":              "file",
		"retention":            "limits",
		"num_replicas":         1,
		"discard":              "new",
	}
	payload, _ := json.Marshal(config)
	inbox := fmt.Sprintf("_INBOX.kv.%d", time.Now().UnixNano())
	_, _ = natsJSRequest(c, "$JS.API.STREAM.CREATE.KV_"+natsKVBucket, inbox, 1, payload)
	return nil
}

// natsEnsureWorkStream creates the durable JetStream stream that backs
// Push/Pull. workqueue retention means each message is delivered to exactly
// one consumer and then removed once acked.
func natsEnsureWorkStream(c net.Conn) error {
	config := map[string]any{
		"name":         natsWorkStream,
		"subjects":     []string{"pepper.push.>"},
		"storage":      "file",
		"retention":    "workqueue",
		"num_replicas": 1,
		"discard":      "old",
	}
	payload, _ := json.Marshal(config)
	inbox := fmt.Sprintf("_INBOX.work.%d", time.Now().UnixNano())
	_, _ = natsJSRequest(c, "$JS.API.STREAM.CREATE."+natsWorkStream, inbox, 2, payload)
	return nil
}

func kvSubject(key string) string {
	return fmt.Sprintf("$KV.%s.%s", natsKVBucket, key)
}

// consumerName makes a stable, dot-free JetStream consumer name from a queue
// subject (consumer names must not contain dots).
func consumerName(queue string) string {
	return "c-" + strings.NewReplacer(".", "_").Replace(queue)
}

// Key-Value

// Set publishes value to the KV stream.
//
// We hex-encode value so all 256 byte values survive the NATS/JetStream
// base64 round-trip without any escaping issues (NATS stores raw bytes, but
// the STREAM.MSG.GET API returns message.data as standard base64; Get must
// decode that layer first, yielding our hex string, then hex-decode to get
// the original bytes back).
//
// We wait for a JetStream PUB ack (+OK from JS) so that a subsequent List or
// Get on the same connection sees the key — without this the operation is
// fire-and-forget and races.
func (s *natsStore) Set(_ context.Context, key string, value []byte, _ int64) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()
	br := bufio.NewReader(c)

	encoded := hex.EncodeToString(value)
	subj := kvSubject(key)
	inbox := fmt.Sprintf("_INBOX.set.%d", time.Now().UnixNano())

	// Subscribe to inbox so we get the JetStream PUB ack.
	if err := natsWrite(c, fmt.Sprintf("SUB %s 1\r\n", inbox)); err != nil {
		return err
	}
	// PUB with reply-to inbox triggers a JetStream ack response.
	if err := natsWrite(c, fmt.Sprintf("PUB %s %s %d\r\n%s\r\n", subj, inbox, len(encoded), encoded)); err != nil {
		return err
	}

	// Wait for the JetStream ack (a MSG back to inbox containing the seq/stream).
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	for {
		line, err := natsReadLine(br, c)
		if err != nil {
			return err
		}
		switch {
		case line == "PING":
			_ = natsWrite(c, "PONG\r\n")
		case strings.HasPrefix(line, "+OK"), strings.HasPrefix(line, "-ERR"):
			// skip
		case strings.HasPrefix(line, "MSG"), strings.HasPrefix(line, "HMSG"):
			_, _, _ = natsReadMsg(line, br) // consume ack body, ignore content
			return nil
		}
	}
}

// Get retrieves the value for key.
//
// STREAM.MSG.GET returns message.data as standard base64 (this is the NATS
// JetStream API contract — the stream stores raw bytes, the API wraps them in
// base64 for JSON transport). We base64-decode that field first to recover our
// hex string, then hex-decode to get the original bytes back.
func (s *natsStore) Get(_ context.Context, key string) ([]byte, bool, error) {
	c, err := s.dial()
	if err != nil {
		return nil, false, err
	}
	defer c.Close()

	req, _ := json.Marshal(map[string]string{"last_by_subj": kvSubject(key)})
	inbox := fmt.Sprintf("_INBOX.get.%d", time.Now().UnixNano())
	resp, err := natsJSRequest(c, "$JS.API.STREAM.MSG.GET.KV_"+natsKVBucket, inbox, 1, req)
	if err != nil {
		if isTimeout(err) {
			return nil, false, nil
		}
		return nil, false, err
	}

	var result map[string]any
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, false, nil
	}
	if errMap, ok := result["error"].(map[string]any); ok {
		if code, _ := errMap["code"].(float64); code == 404 || code == 503 {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("coord/nats: Get: %v", errMap)
	}
	msgData, _ := result["message"].(map[string]any)
	if msgData == nil {
		return nil, false, nil
	}
	// message.data is base64(raw-stream-bytes). raw-stream-bytes == hex(value).
	b64str, _ := msgData["data"].(string)
	if b64str == "" {
		return nil, false, nil
	}
	hexBytes, err := base64.StdEncoding.DecodeString(b64str)
	if err != nil {
		return nil, false, fmt.Errorf("coord/nats: Get base64: %w", err)
	}
	decoded, err := hex.DecodeString(string(hexBytes))
	if err != nil {
		return nil, false, fmt.Errorf("coord/nats: Get hex: %w", err)
	}
	return decoded, true, nil
}

// Delete writes a KV tombstone via HPUB. The CONNECT frame negotiates
// headers:true so the server accepts this command without error.
func (s *natsStore) Delete(_ context.Context, key string) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	subj := kvSubject(key)
	hdr := "NATS/1.0\r\nKV-Operation: DEL\r\n\r\n"
	// HPUB <subject> <hdr-len> <total-len>\r\n<headers>
	// total-len == hdr-len when there is no body (0 body bytes).
	return natsWrite(c, fmt.Sprintf("HPUB %s %d %d\r\n%s\r\n", subj, len(hdr), len(hdr), hdr))
}

// List returns all keys whose names begin with prefix.
//
// We use the $JS.API.STREAM.SUBJECTS endpoint (available since NATS 2.10)
// which accepts a subject filter and returns a map of subject→count. This is
// the canonical KV-key-listing API; STREAM.INFO's state.subjects field is
// only populated for very small streams and requires a separate flag.
func (s *natsStore) List(_ context.Context, prefix string) ([]string, error) {
	c, err := s.dial()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	// Filter covers all keys in the bucket; we apply the logical prefix locally.
	filterSubj := fmt.Sprintf("$KV.%s.>", natsKVBucket)
	req, _ := json.Marshal(map[string]any{"filter": filterSubj})
	inbox := fmt.Sprintf("_INBOX.list.%d", time.Now().UnixNano())
	resp, err := natsJSRequest(c, "$JS.API.STREAM.SUBJECTS.KV_"+natsKVBucket, inbox, 1, req)
	if err != nil {
		if isTimeout(err) {
			return nil, nil
		}
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(resp, &result); err != nil {
		return nil, nil
	}
	if errMap, ok := result["error"].(map[string]any); ok {
		// Empty stream or stream not found — return empty list, not an error.
		_ = errMap
		return nil, nil
	}

	basePrefix := fmt.Sprintf("$KV.%s.", natsKVBucket)
	var keys []string
	if subjects, ok := result["subjects"].(map[string]any); ok {
		for subj := range subjects {
			key := strings.TrimPrefix(subj, basePrefix)
			if strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
	}
	return keys, nil
}

// Pub/Sub

func (s *natsStore) Publish(_ context.Context, channel string, payload []byte) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	encoded := hex.EncodeToString(payload)
	return natsWrite(c, fmt.Sprintf("PUB %s %d\r\n%s\r\n", channel, len(encoded), encoded))
}

func (s *natsStore) Subscribe(ctx context.Context, channelPrefix string) (<-chan Event, error) {
	c, err := s.dial()
	if err != nil {
		return nil, fmt.Errorf("coord/nats: subscribe dial: %w", err)
	}
	ch := make(chan Event, 64)

	// Build NATS subject(s) to subscribe to.
	//
	// NATS wildcards (* and >) only tokenise on dots. Colon-separated channel
	// names cannot be wildcarded — "pepper:test:pub:>" is NOT a wildcard in
	// NATS; it is a literal subject containing colons. For any prefix that
	// contains a colon we subscribe to > (all subjects) and rely on client-side
	// prefix matching in readMessages. For purely dot-separated prefixes we use
	// narrow NATS wildcards for efficiency.
	var subjects []string
	if strings.Contains(channelPrefix, ":") {
		subjects = []string{">"}
	} else {
		switch {
		case strings.HasSuffix(channelPrefix, "*"):
			subjects = []string{strings.TrimSuffix(channelPrefix, "*") + ">"}
		case strings.HasSuffix(channelPrefix, ">"):
			subjects = []string{channelPrefix}
		case strings.HasSuffix(channelPrefix, "."):
			subjects = []string{channelPrefix + ">"}
		default:
			subjects = []string{channelPrefix, channelPrefix + ".>"}
		}
	}

	for i, subj := range subjects {
		if err := natsWrite(c, fmt.Sprintf("SUB %s %d\r\n", subj, i+1)); err != nil {
			c.Close()
			return nil, err
		}
	}

	go s.readMessages(ctx, c, ch, channelPrefix)
	return ch, nil
}

func (s *natsStore) readMessages(ctx context.Context, c net.Conn, ch chan Event, prefix string) {
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
		switch {
		case line == "PING":
			_ = natsWrite(c, "PONG\r\n")
			continue
		case strings.HasPrefix(line, "MSG"), strings.HasPrefix(line, "HMSG"):
			// fall through
		default:
			continue
		}

		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}
		channel := parts[1]

		body, _, err := natsReadMsg(line, br)
		if err != nil {
			return
		}

		if !natsChannelMatches(channel, prefix) {
			continue
		}
		decoded, err := hex.DecodeString(strings.TrimSpace(string(body)))
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

// natsChannelMatches reports whether channel should be delivered to a
// subscriber with the given prefix. Both dots and colons are treated as
// separator characters for prefix matching.
func natsChannelMatches(channel, prefix string) bool {
	switch {
	case strings.HasSuffix(prefix, "*"):
		return strings.HasPrefix(channel, strings.TrimSuffix(prefix, "*"))
	case strings.HasSuffix(prefix, ">"):
		return strings.HasPrefix(channel, strings.TrimSuffix(prefix, ">"))
	case strings.HasSuffix(prefix, "."), strings.HasSuffix(prefix, ":"):
		return strings.HasPrefix(channel, prefix)
	default:
		// Exact match or the channel is a child (separated by . or :).
		if channel == prefix {
			return true
		}
		return strings.HasPrefix(channel, prefix+".") || strings.HasPrefix(channel, prefix+":")
	}
}

// Work Queue (JetStream-backed Push / Pull)
//
// Core NATS PUB is fire-and-forget: a message published with no active
// subscriber is silently dropped. The old Pull implementation opened a
// queue-group SUB per call, creating a race with Push.
//
// Push publishes to a JetStream subject covered by natsWorkStream
// (workqueue retention). JetStream retains the message until a pull consumer
// fetches and acks it. Pull creates a durable pull consumer on first call and
// issues MSG.NEXT fetch requests in a loop. The connection is kept alive for
// the duration of a Pull call so the ack is flushed before the socket closes.

// Push enqueues payload on the JetStream work stream subject.
func (s *natsStore) Push(_ context.Context, queue string, payload []byte) error {
	c, err := s.dial()
	if err != nil {
		return err
	}
	defer c.Close()

	encoded := hex.EncodeToString(payload)
	return natsWrite(c, fmt.Sprintf("PUB %s %d\r\n%s\r\n", queue, len(encoded), encoded))
}

// Pull blocks until a work item is available on queue or ctx is cancelled.
// It creates a durable pull consumer on first call and reuses it thereafter.
// Each delivered message is explicitly acked before Pull returns.
func (s *natsStore) Pull(ctx context.Context, queue string) ([]byte, error) {
	c, err := s.dial()
	if err != nil {
		return nil, fmt.Errorf("coord/nats: Pull dial: %w", err)
	}
	defer c.Close()

	cname := consumerName(queue)
	if err := natsEnsurePullConsumer(c, queue, cname); err != nil {
		return nil, fmt.Errorf("coord/nats: Pull consumer: %w", err)
	}

	fetchSubj := fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.%s.%s", natsWorkStream, cname)
	inbox := fmt.Sprintf("_INBOX.pull.%d", time.Now().UnixNano())
	br := bufio.NewReader(c)

	if err := natsWrite(c, fmt.Sprintf("SUB %s %d\r\n", inbox, 1)); err != nil {
		return nil, err
	}

	done := ctx.Done()
	for {
		select {
		case <-done:
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("coord/nats: closed")
		default:
		}

		// Request one message; server waits up to 2 s then sends a 404/408 status.
		fetchReq := `{"batch":1,"expires":2000000000}`
		if err := natsWrite(c, fmt.Sprintf("PUB %s %s %d\r\n%s\r\n",
			fetchSubj, inbox, len(fetchReq), fetchReq)); err != nil {
			return nil, err
		}

		_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	inner:
		for {
			line, err := natsReadLine(br, c)
			if err != nil {
				if isTimeout(err) {
					break inner
				}
				return nil, err
			}
			switch {
			case line == "PING":
				_ = natsWrite(c, "PONG\r\n")
			case strings.HasPrefix(line, "+OK"):
				// skip
			case strings.HasPrefix(line, "MSG"), strings.HasPrefix(line, "HMSG"):
				body, replySubj, err := natsReadMsg(line, br)
				if err != nil {
					return nil, err
				}
				raw := strings.TrimSpace(string(body))
				// Empty body or NATS status line means no messages (404/408).
				if raw == "" {
					break inner
				}
				decoded, err := hex.DecodeString(raw)
				if err != nil {
					return nil, fmt.Errorf("coord/nats: Pull decode: %w", err)
				}
				// Acknowledge before returning so JetStream removes the message.
				// We send PING and wait for PONG to ensure the ACK is flushed to
				// the server before we close the connection — without this the TCP
				// FIN can arrive before the ACK, causing redelivery.
				if replySubj != "" {
					_ = natsWrite(c, fmt.Sprintf("PUB %s 0\r\n\r\nPING\r\n", replySubj))
					_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
					for {
						ackLine, err := natsReadLine(br, c)
						if err != nil {
							break // best effort
						}
						if ackLine == "PONG" {
							break
						}
					}
				}
				return decoded, nil
			}
		}

		select {
		case <-done:
			return nil, ctx.Err()
		case <-s.stopCh:
			return nil, fmt.Errorf("coord/nats: closed")
		default:
		}
	}
}

// natsEnsurePullConsumer creates or re-uses a durable pull consumer on
// natsWorkStream for the given filter subject.
func natsEnsurePullConsumer(c net.Conn, queue, cname string) error {
	config := map[string]any{
		"durable_name":    cname,
		"filter_subject":  queue,
		"ack_policy":      "explicit",
		"deliver_policy":  "all",
		"ack_wait":        int64(30 * time.Second),
		"max_deliver":     5,
		"max_ack_pending": 1,
	}
	payload, _ := json.Marshal(map[string]any{"stream_name": natsWorkStream, "config": config})
	inbox := fmt.Sprintf("_INBOX.cc.%d", time.Now().UnixNano())
	apiSubj := fmt.Sprintf("$JS.API.CONSUMER.CREATE.%s.%s", natsWorkStream, cname)
	resp, err := natsJSRequest(c, apiSubj, inbox, 99, payload)
	if err != nil && !isTimeout(err) {
		return fmt.Errorf("coord/nats: CONSUMER.CREATE: %w", err)
	}
	if len(resp) > 0 {
		var result map[string]any
		if json.Unmarshal(resp, &result) == nil {
			if errMap, ok := result["error"].(map[string]any); ok {
				// err_code 10058 = consumer already exists — acceptable.
				if code, _ := errMap["err_code"].(float64); code != 10058 {
					return fmt.Errorf("coord/nats: CONSUMER.CREATE: %v", errMap)
				}
			}
		}
	}
	return nil
}

// Lifecycle

func (s *natsStore) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stopCh)
	return nil
}

var _ Store = (*natsStore)(nil)
