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
	"github.com/agberohq/pepper/internal/wire"
)

// Topic name helpers. Every prefix lives in internal/wire; nothing outside this
// block should concatenate a topic by hand.
func TopicPush(group string) string       { return wire.TopicPushPrefix + group }
func TopicPub(group string) string        { return wire.TopicPubPrefix + group }
func TopicRes(originID string) string     { return wire.TopicResPrefix + originID }
func TopicPipe(topic string) string       { return wire.TopicPipePrefix + topic }
func TopicStream(corrID string) string    { return wire.TopicStreamPrefix + corrID }
func TopicCb(corrID string) string        { return wire.TopicCbPrefix + corrID }
func TopicHB(workerID string) string      { return wire.TopicHBPrefix + workerID }
func TopicControl(workerID string) string { return wire.TopicControlPrefix + workerID }

// Subscribe helpers. Use these instead of literal prefixes when subscribing.
func TopicResPrefix() string     { return wire.TopicResPrefix }
func TopicHBPrefix() string      { return wire.TopicHBPrefix }
func TopicControlPrefix() string { return wire.TopicControlPrefix }

// TopicPipePrefix returns the subscription prefix covering every stage topic of
// a pipeline run, including its final topic.
func TopicPipePrefix(pipeline string) string { return wire.TopicPipePrefix + pipeline + "." }

// TopicPipeFinal returns the topic a pipeline's last worker stage forwards to.
func TopicPipeFinal(pipeline string) string {
	return wire.TopicPipePrefix + pipeline + "." + wire.PipeFinalSuffix
}

// TopicBroadcast reaches every worker on the bus.
const TopicBroadcast = wire.TopicBroadcast

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
	return Config{URL: wire.SchemeURL(wire.SchemeTCP, "127.0.0.1:0"), SendBuf: 128, RecvBuf: 128}
}

// IsBroadcastTopic reports whether topic is a PUB/SUB broadcast topic.
func IsBroadcastTopic(topic string) bool {
	return strings.HasPrefix(topic, wire.TopicPubPrefix) || topic == TopicBroadcast
}

// IsPushTopic reports whether topic uses PUSH/PULL delivery.
func IsPushTopic(topic string) bool { return strings.HasPrefix(topic, wire.TopicPushPrefix) }

func GroupFromPushTopic(topic string) string { return strings.TrimPrefix(topic, wire.TopicPushPrefix) }
func GroupFromPubTopic(topic string) string  { return strings.TrimPrefix(topic, wire.TopicPubPrefix) }

// GroupFromResTopic returns the origin id a response topic belongs to.
func GroupFromResTopic(topic string) string { return strings.TrimPrefix(topic, wire.TopicResPrefix) }

// WorkerEnvVars returns environment variables injected into worker subprocesses.
// The variable names live in internal/wire and are read by every bundled runtime.
func WorkerEnvVars(workerID, busAddr, codec string, groups []string, heartbeatMs, maxConcurrent int, blobDir string) []string {
	return []string{
		wire.EnvWorkerID + "=" + workerID,
		wire.EnvBusURL + "=" + busAddr,
		wire.EnvCodec + "=" + codec,
		wire.EnvGroups + "=" + strings.Join(groups, ","),
		wire.EnvHeartbeatMS + "=" + itoa(heartbeatMs),
		wire.EnvMaxConcurrent + "=" + itoa(maxConcurrent),
		wire.EnvBlobDir + "=" + blobDir,
	}
}

// WorkerResourcesVar returns the PEPPER_RESOURCES assignment for a marshalled
// resource config blob.
func WorkerResourcesVar(marshalled []byte) string {
	return wire.EnvResources + "=" + string(marshalled)
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
