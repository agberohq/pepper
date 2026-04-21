package pepper

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/pending"
)

// BidiStream is a typed bidirectional streaming handle.
// Obtained via OpenStream[In, Out].
type BidiStream[In any, Out any] struct {
	streamID string
	corrID   string
	outCh    <-chan pending.Response
	pp       *Pepper
	seqNum   uint64
	closed   atomic.Bool
}

// Write sends one input chunk to the worker.
func (s *BidiStream[In, Out]) Write(chunk In) error {
	if s.closed.Load() {
		return fmt.Errorf("stream closed")
	}
	m, err := toWireInput(chunk)
	if err != nil {
		return fmt.Errorf("pepper.BidiStream.Write: encode chunk: %w", err)
	}
	s.seqNum++
	msg := envelope.StreamChunk{
		ProtoVer: envelope.ProtoVer,
		MsgType:  envelope.MsgStreamChunk,
		StreamID: s.streamID,
		CorrID:   s.corrID,
		Seq:      s.seqNum,
	}
	payload, err := s.pp.codec.Marshal(m)
	if err != nil {
		return err
	}
	msg.Payload = payload
	data, err := s.pp.codec.Marshal(msg)
	if err != nil {
		return err
	}
	return s.pp.rt.bus.Publish(bus.TopicStream(s.streamID), data)
}

// CloseInput signals that no more input chunks will be sent.
func (s *BidiStream[In, Out]) CloseInput() error {
	if s.closed.Swap(true) {
		return nil
	}
	msg := envelope.StreamClose{
		ProtoVer: envelope.ProtoVer,
		MsgType:  envelope.MsgStreamClose,
		StreamID: s.streamID,
		CorrID:   s.corrID,
	}
	data, _ := s.pp.codec.Marshal(msg)
	return s.pp.rt.bus.Publish(bus.TopicStream(s.streamID), data)
}

// Chunks returns a channel of decoded output chunks.
// Closed when the stream ends or ctx is cancelled.
// Safe to call on a zero-value Stream — returns a channel that closes immediately.
func (s *BidiStream[In, Out]) Chunks(ctx context.Context) <-chan Out {
	if s.pp == nil || s.outCh == nil {
		ch := make(chan Out)
		close(ch)
		return ch
	}
	return drainResponses[Out](ctx, s.outCh, s.pp.codec)
}

// OpenStream opens a typed bidirectional stream to a capability.
//
//	type AudioChunk struct{ Data []byte `json:"data"` }
//	type TranscriptChunk struct{ Text string `json:"text"` }
//
//	stream, err := pepper.OpenStream[AudioChunk, TranscriptChunk](ctx, pp,
//	    "speech.transcribe", pepper.In{"language": "en"},
//	)
//	stream.Write(AudioChunk{Data: audioBytes})
//	stream.CloseInput()
//	for chunk := range stream.Chunks(ctx) {
//	    fmt.Print(chunk.Text)
//	}
func OpenStream[In any, Out any](ctx context.Context, pp *Pepper, cap string, seed In, opts ...CallOption) (*BidiStream[In, Out], error) {
	wireIn, err := toWireInput(seed)
	if err != nil {
		return nil, fmt.Errorf("pepper.OpenStream: encode seed: %w", err)
	}
	raw, err := pp.openRawStream(ctx, cap, wireIn, opts...)
	if err != nil {
		return nil, err
	}
	return &BidiStream[In, Out]{
		streamID: raw.streamID,
		corrID:   raw.corrID,
		outCh:    raw.outCh,
		pp:       raw.pp,
	}, nil
}

// drainResponses fans a pending.Response channel into a decoded typed channel.
// Used by BidiStream.Chunks and BoundCall.Stream — single implementation.
func drainResponses[T any](ctx context.Context, src <-chan pending.Response, c codec.Codec) <-chan T {
	out := make(chan T, DefaultStreamChanBuffer)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-src:
				if !ok {
					return
				}
				res := Result{payload: r.Payload, codec: c, WorkerID: r.WorkerID, Err: r.Err}
				var v T
				if err := res.Into(&v); err != nil {
					continue
				}
				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Stream is a convenience alias for BidiStream[any, any].
// Use it when you need an untyped streaming handle — for example in
// compile-check tests or adapters that deal with raw map payloads.
// For production use prefer the fully-typed BidiStream[In, Out].
type Stream = BidiStream[any, any]
