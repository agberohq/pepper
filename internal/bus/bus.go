// Package bus implements the Pepper message bus abstraction.
//
// Two socket patterns per group (per spec §2.2):
//   - PUSH/PULL  pepper.push.{group}  — DispatchAny (exactly one worker)
//   - PUB/SUB    pepper.pub.{group}   — fan-out modes (all workers)
//
// All response, heartbeat, callback, and control topics use a single
// listener on the router side. Workers push back on named topics.
package bus

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/agberohq/pepper/internal/envelope"
)

// Topic name helpers — all topics follow the pepper.{type}.{qualifier} scheme.

func TopicPush(group string) string       { return "pepper.push." + group }
func TopicPub(group string) string        { return "pepper.pub." + group }
func TopicRes(originID string) string     { return "pepper.res." + originID }
func TopicPipe(topic string) string       { return "pepper.pipe." + topic }
func TopicStream(corrID string) string    { return "pepper.stream." + corrID }
func TopicCb(corrID string) string        { return "pepper.cb." + corrID }
func TopicHB(workerID string) string      { return "pepper.hb." + workerID }
func TopicControl(workerID string) string { return "pepper.control." + workerID }

const TopicBroadcast = "pepper.broadcast"

// Message is a raw envelope received on any socket.
type Message struct {
	Topic string
	Data  []byte
}

// Bus is the central IPC hub. One instance per Pepper router.
type Bus interface {
	// Publish sends data to all subscribers on a topic (PUB/SUB semantics).
	Publish(topic string, data []byte) error

	// Subscribe returns a channel receiving all messages on an exact topic.
	Subscribe(ctx context.Context, topic string) (<-chan Message, error)

	// SubscribePrefix returns a channel receiving messages on any topic
	// that starts with prefix. Used for "pepper.res.*" and "pepper.control.*".
	SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error)

	// PushOne sends data to exactly one subscriber (PUSH/PULL semantics).
	// Blocks until a worker pulls the message or ctx is cancelled.
	PushOne(ctx context.Context, topic string, data []byte) error

	// Addr returns the transport address workers use to connect.
	Addr() string

	// Close shuts down all sockets.
	Close() error
}

// Config holds bus configuration.
type Config struct {
	// URL is the transport URL.
	//   Linux/macOS: tcp://127.0.0.1:0  (OS assigns free port)
	//   Windows:     tcp://127.0.0.1:7731
	URL string

	// SendBuf is the send buffer size in messages.
	SendBuf int

	// RecvBuf is the receive buffer size in messages.
	RecvBuf int
}

func DefaultConfig() Config {
	return Config{URL: defaultURL(), SendBuf: 128, RecvBuf: 128}
}

func defaultURL() string {
	if isWindows() {
		return "tcp://127.0.0.1:7731"
	}
	// Port 0 = OS assigns a free port; actual port read back from Mula.Addr()
	return fmt.Sprintf("tcp://127.0.0.1:0")
}

func isWindows() bool {
	return os.Getenv("GOOS") == "windows" || strings.Contains(os.Getenv("OS"), "Windows")
}

// IsBroadcastTopic reports whether a topic is a PUB/SUB broadcast topic.
func IsBroadcastTopic(topic string) bool {
	return strings.HasPrefix(topic, "pepper.pub.") || topic == TopicBroadcast
}

// IsPushTopic reports whether a topic uses PUSH/PULL delivery.
func IsPushTopic(topic string) bool {
	return strings.HasPrefix(topic, "pepper.push.")
}

// GroupFromPushTopic extracts the group name from pepper.push.{group}.
func GroupFromPushTopic(topic string) string {
	return strings.TrimPrefix(topic, "pepper.push.")
}

// GroupFromPubTopic extracts the group name from pepper.pub.{group}.
func GroupFromPubTopic(topic string) string {
	return strings.TrimPrefix(topic, "pepper.pub.")
}

// WorkerEnvVars returns the environment variables set on worker subprocesses.
// blobDir is passed explicitly — single source of truth from blob package.
func WorkerEnvVars(workerID, busAddr, codec string, groups []string, heartbeatMs, maxConcurrent int, blobDir string) []string {
	return []string{
		"PEPPER_WORKER_ID=" + workerID,
		"PEPPER_BUS_URL=" + busAddr,
		"PEPPER_CODEC=" + codec,
		"PEPPER_GROUPS=" + strings.Join(groups, ","),
		"PEPPER_HEARTBEAT_MS=" + itoa(heartbeatMs),
		"PEPPER_MAX_CONCURRENT=" + itoa(maxConcurrent),
		"PEPPER_BLOB_DIR=" + blobDir,
	}
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}

// Dispatch sends an envelope to the correct socket based on dispatch mode.
// Used by InProcBus tests — Mula handles this internally.
func Dispatch(b Bus, env envelope.Envelope, data []byte) error {
	ctx := context.Background()
	switch env.Dispatch {
	case envelope.DispatchAny:
		return b.PushOne(ctx, TopicPush(env.Group), data)
	default:
		return b.Publish(TopicPub(env.Group), data)
	}
}

func New(transport string, cfg Config) (Bus, error) {
	switch transport {
	case "nanomsg", "nng", "mangos", "":
		return NewNano(cfg)
	case "mula", "tcp":
		return NewMula(cfg)
	case "mock":
		return NewMock(), nil
	default:
		return nil, fmt.Errorf("bus: unknown transport %q", transport)
	}
}
