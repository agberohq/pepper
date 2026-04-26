// Package coord provides a pluggable coordination store used by the router,
// registry, and session layers.
//
// A Store is a key-value store with pub/sub notifications and a work queue.
// Four backends are supported:
//
//	memory — in-process, zero deps, tests and single-node embedded use
//	mula   — TCP framing protocol, default single-node mode (tcp://host:port)
//	redis  — production, connects to any Redis 6+ server
//	nats   — production, connects to any NATS 2.x server with JetStream
//
// Key namespacing convention:
//
//	pepper:worker:{id}          — worker presence / capability list
//	pepper:cap:{name}           — capability spec
//	pepper:session:{id}:{key}   — session field
//
// Queue namespacing convention (Push/Pull):
//
//	pepper.push.{group}         — work queue for a worker group
//	pepper.push.{workerID}      — per-worker pinned queue
//
// Pub/sub channels mirror the key prefixes so subscribers can react to
// changes without polling:
//
//	pepper:worker:*   — worker join / leave
//	pepper:cap:*      — capability register / deregister
package coord

import "context"

// Event is a notification published on a pub/sub channel.
type Event struct {
	Channel string
	Key     string
	Value   []byte
	Deleted bool
}

// Store is the coordination interface every backend must implement.
// It combines three concerns that all backends must provide:
//
// Key-value store with TTL — used by registry, session, worker presence
// Pub/sub — used for control messages, cap announcements, heartbeats
// Work queue (Push/Pull) — exactly-once delivery of request envelopes
//
//	to worker groups across nodes
type Store interface {
	// — Key-value —

	// Set writes value under key. ttlSeconds ≤ 0 means no expiry.
	Set(ctx context.Context, key string, value []byte, ttlSeconds int64) error

	// Get returns the value for key and true, or nil and false if absent.
	Get(ctx context.Context, key string) ([]byte, bool, error)

	// Delete removes a key.
	Delete(ctx context.Context, key string) error

	// List returns all keys with the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// — Pub/sub —

	// Publish sends payload on channel. All active subscribers receive it.
	Publish(ctx context.Context, channel string, payload []byte) error

	// Subscribe returns a channel receiving events published to any channel
	// whose name starts with prefix. The returned channel is closed when
	// ctx is done.
	Subscribe(ctx context.Context, channelPrefix string) (<-chan Event, error)

	// — Work queue —

	// Push enqueues payload on queue. Exactly one Pull caller receives it.
	// Redis: LPUSH. NATS: publish to a queue-group subject.
	Push(ctx context.Context, queue string, payload []byte) error

	// Pull dequeues the next item from queue, blocking until one is
	// available or ctx is cancelled. Returns ctx.Err() on cancellation.
	Pull(ctx context.Context, queue string) ([]byte, error)

	// — Lifecycle —

	// Close releases all resources held by the backend.
	Close() error
}
