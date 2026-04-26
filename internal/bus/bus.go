// Package bus provides the Pepper message bus abstraction.
//
// The Bus interface connects the Go router to worker subprocesses.
// Concrete transport implementations live in internal/coord:
//

// Use NewCoordBus to create a Bus from a coord transport, or NewMulaBus for
// a self-contained single-node Bus backed by Mula.
//
// Topic layout:
//
//	pepper.push.{group}  PUSH/PULL — exactly one worker receives
//	pepper.pub.{group}   PUB/SUB  — all workers receive (fan-out)
//	pepper.res.{id}      response routing back to router
//	pepper.broadcast     shutdown / cancel signals
package bus

import (
	"context"
	"fmt"
	"strings"

	"github.com/agberohq/pepper/internal/envelope"
)

// Topic name helpers.
func TopicPush(group string) string       { return "pepper.push." + group }
func TopicPub(group string) string        { return "pepper.pub." + group }
func TopicRes(originID string) string     { return "pepper.res." + originID }
func TopicPipe(topic string) string       { return "pepper.pipe." + topic }
func TopicStream(corrID string) string    { return "pepper.stream." + corrID }
func TopicCb(corrID string) string        { return "pepper.cb." + corrID }
func TopicHB(workerID string) string      { return "pepper.hb." + workerID }
func TopicControl(workerID string) string { return "pepper.control." + workerID }

const TopicBroadcast = "pepper.broadcast"

// Message is a raw message received from the bus.
type Message struct {
	Topic string
	Data  []byte
}

// Bus is the central IPC hub between the router and workers.
type Bus interface {
	// Publish sends data to all subscribers on topic (PUB/SUB).
	Publish(topic string, data []byte) error

	// Subscribe returns a channel receiving all messages on an exact topic.
	Subscribe(ctx context.Context, topic string) (<-chan Message, error)

	// SubscribePrefix returns a channel receiving messages on any topic
	// starting with prefix.
	SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error)

	// PushOne sends data to exactly one subscriber (PUSH/PULL).
	// Blocks until a worker pulls or ctx is cancelled.
	PushOne(ctx context.Context, topic string, data []byte) error

	// Addr returns the transport address workers connect to.
	Addr() string

	// Close shuts down all connections.
	Close() error
}

// Config holds bus configuration.
type Config struct {
	URL     string // transport URL: tcp://host:port, nats://host:4222, redis://host:6379
	SendBuf int
	RecvBuf int
}

func DefaultConfig() Config {
	return Config{URL: "tcp://127.0.0.1:0", SendBuf: 128, RecvBuf: 128}
}

// IsBroadcastTopic reports whether topic is a PUB/SUB broadcast topic.
func IsBroadcastTopic(topic string) bool {
	return strings.HasPrefix(topic, "pepper.pub.") || topic == TopicBroadcast
}

// IsPushTopic reports whether topic uses PUSH/PULL delivery.
func IsPushTopic(topic string) bool { return strings.HasPrefix(topic, "pepper.push.") }

func GroupFromPushTopic(topic string) string { return strings.TrimPrefix(topic, "pepper.push.") }
func GroupFromPubTopic(topic string) string  { return strings.TrimPrefix(topic, "pepper.pub.") }

// WorkerEnvVars returns environment variables injected into worker subprocesses.
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

func itoa(n int) string { return fmt.Sprintf("%d", n) }

// Dispatch routes an envelope to the correct bus pattern.
// Use this for one-off dispatch outside the router; for production use prefer
// router.Router.Dispatch which handles affinity, retries, and deadlines.
func Dispatch(ctx context.Context, b Bus, env envelope.Envelope, data []byte) error {
	switch env.Dispatch {
	case envelope.DispatchAny:
		return b.PushOne(ctx, TopicPush(env.Group), data)
	default:
		return b.Publish(TopicPub(env.Group), data)
	}
}
