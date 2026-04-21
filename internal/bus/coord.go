package bus

import (
	"context"
	"strings"

	"github.com/agberohq/pepper/internal/coord"
)

// NewCoordBus wraps a coord.Store as a Bus.
//
// Used when WithCoord is set (Redis or NATS). The mapping is:
//
//	Publish      → coord.Publish   (fan-out pub/sub, all subscribers receive)
//	PushOne      → coord.Push      (queue, exactly one subscriber receives)
//	Subscribe    → coord.Subscribe (pub/sub channel)
//	SubscribePrefix → coord.Subscribe with prefix matching
//
// Workers in distributed mode pull from coord queues directly via Pull,
// rather than receiving frames over a Mula TCP socket.
// The Addr() method returns the coord URL so Python workers know to use
// the coord transport instead of Mula framing.
func NewCoordBus(c coord.Store, coordURL string) Bus {
	return &coordBus{c: c, addr: coordURL}
}

type coordBus struct {
	c    coord.Store
	addr string
}

// Addr returns the coord URL (e.g. "redis://..." or "nats://...").
// This is passed to Python workers as PEPPER_BUS_URL so they connect
// to the same coord backend rather than a Mula TCP socket.
func (b *coordBus) Addr() string { return b.addr }

func (b *coordBus) Publish(topic string, data []byte) error {
	return b.c.Publish(context.Background(), topic, data)
}

// PushOne enqueues data to exactly one subscriber via coord.Push.
// For Redis: LPUSH + BRPOP. For NATS: queue-group publish.
// This replaces the previous Publish(topic+".push") fan-out which was incorrect.
func (b *coordBus) PushOne(ctx context.Context, topic string, data []byte) error {
	return b.c.Push(ctx, topic, data)
}

func (b *coordBus) Subscribe(ctx context.Context, topic string) (<-chan Message, error) {
	src, err := b.c.Subscribe(ctx, topic)
	if err != nil {
		return nil, err
	}
	return adaptCoordChan(ctx, src), nil
}

func (b *coordBus) SubscribePrefix(ctx context.Context, prefix string) (<-chan Message, error) {
	src, err := b.c.Subscribe(ctx, prefix)
	if err != nil {
		return nil, err
	}
	return adaptCoordChan(ctx, src), nil
}

func (b *coordBus) Close() error { return b.c.Close() }

func adaptCoordChan(ctx context.Context, src <-chan coord.Event) <-chan Message {
	out := make(chan Message, 64)
	go func() {
		defer close(out)
		for {
			select {
			case ev, ok := <-src:
				if !ok {
					return
				}
				topic := ev.Channel
				if topic == "" {
					topic = ev.Key
				}
				// Strip legacy .push suffix if present (backward compat).
				topic = strings.TrimSuffix(topic, ".push")
				select {
				case out <- Message{Topic: topic, Data: ev.Value}:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}
