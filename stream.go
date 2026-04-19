package pepper

import (
	"context"

	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/pending"
)

// Stream wraps a streaming response channel returned by pp.Stream().
// Iterate chunks via Chunks(ctx); check Err() after the channel closes.
type Stream struct {
	ch  <-chan pending.Response
	c   codec.Codec
	err error
}

// Chunks returns a channel that receives decoded Result chunks.
// The channel is closed when the stream ends (res_end) or ctx is cancelled.
// ctx MUST be passed — without it a stopped consumer leaks the goroutine.
func (s *Stream) Chunks(ctx context.Context) <-chan Result {
	buf := cap(s.ch)
	if buf < 1 {
		buf = 1
	}
	out := make(chan Result, buf)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-s.ch:
				if !ok {
					return
				}
				res := Result{
					payload:  r.Payload,
					codec:    s.c,
					WorkerID: r.WorkerID,
					Err:      r.Err,
				}
				select {
				case out <- res:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Err returns any terminal error from the stream.
// Safe to call after the Chunks channel has been drained.
func (s *Stream) Err() error { return s.err }
