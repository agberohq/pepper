package pepper

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/pending"
)

// BidiStream is a bidirectional streaming handle (§11.2).
type BidiStream struct {
	streamID string
	corrID   string
	group    string
	outCh    <-chan pending.Response
	pp       *Pepper
	codec    interface {
		Marshal(v any) ([]byte, error)
		Unmarshal(data []byte, v any) error
	}
	seqNum uint64
	closed atomic.Bool
}

// Write sends one input chunk to the worker.
func (s *BidiStream) Write(chunk core.In) error {
	if s.closed.Load() {
		return fmt.Errorf("stream closed")
	}
	s.seqNum++
	env := map[string]any{
		"proto_ver": uint8(1),
		"msg_type":  string(envelope.MsgStreamChunk),
		"stream_id": s.streamID,
		"corr_id":   s.corrID,
		"seq":       s.seqNum,
	}
	payload, err := s.codec.Marshal(chunk)
	if err != nil {
		return err
	}
	env["payload"] = payload
	data, err := s.codec.Marshal(env)
	if err != nil {
		return err
	}
	return s.pp.rt.bus.Publish(bus.TopicStream(s.streamID), data)
}

// CloseInput signals that no more input chunks will be sent.
func (s *BidiStream) CloseInput() error {
	if s.closed.Swap(true) {
		return nil
	}
	env := map[string]any{
		"proto_ver": uint8(1),
		"msg_type":  string(envelope.MsgStreamClose),
		"stream_id": s.streamID,
		"corr_id":   s.corrID,
	}
	data, _ := s.codec.Marshal(env)
	return s.pp.rt.bus.Publish(bus.TopicStream(s.streamID), data)
}

// Chunks returns a channel of output results from the worker.
// The channel is closed when the stream ends or ctx is cancelled.
func (s *BidiStream) Chunks(ctx context.Context) <-chan Result {
	out := make(chan Result, 64)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-s.outCh:
				if !ok {
					return
				}
				res := Result{payload: r.Payload, codec: s.pp.codec, WorkerID: r.WorkerID, Err: r.Err}
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
