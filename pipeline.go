package pepper

// Usage:
//
//	pp.Compose("audio.pipeline",
//	    pepper.Pipe("audio.denoise").WithGroup("gpu"),
//	    pepper.PipeTransform(func(in map[string]any) (map[string]any, error) {
//	        return map[string]any{"text": in["transcript"]}, nil
//	    }),
//	    pepper.Pipe("llm.analyze"),
//	)

import (
	"github.com/agberohq/pepper/internal/compose"
	"github.com/agberohq/pepper/internal/envelope"
)

// Stage is a single step in a pipeline.
// Obtain via Pipe, Transform, PipeReturn, PipeScatter, or PipeBranch.
type Stage = compose.Stage

// Pipe creates a worker-dispatched pipeline stage.
//
//	pepper.Pipe("text.upper")
//	pepper.Pipe("speech.transcribe").WithGroup("asr")
func Pipe(cap string) *compose.PipeStage {
	return compose.Pipe(cap)
}

// Transform creates a router-side transform stage.
// The function runs in the Go process (no worker dispatch) and can reshape
// the payload between stages.
//
//	pepper.PipeTransform(func(in map[string]any) (map[string]any, error) {
//	    transcript, _ := in["text"].(string)
//	    return map[string]any{"prompt": "Analyse: " + transcript}, nil
//	})
func Transform(fn func(map[string]any) (map[string]any, error)) *compose.TransformStage {
	// Wrap the user's simple func into the internal signature that carries
	// the full envelope — users don't need envelope details.
	return compose.Transform(func(_ envelope.Envelope, in map[string]any) (map[string]any, error) {
		return fn(in)
	})
}

// PipeReturn creates a router-side stage that short-circuits the pipeline
// and returns a fixed value.
//
//	pepper.PipeReturn(map[string]any{"status": "ok"})
func PipeReturn(value map[string]any) *compose.ReturnStage {
	return compose.Return(value)
}

// PipeScatter creates a scatter-gather stage that fans out to all workers
// in a group and collects results.
func PipeScatter(cap string) *compose.ScatterStage {
	return compose.Scatter(cap)
}

// EnvelopeInfo carries the subset of envelope fields useful to transform stages.
// Exposed so PipeTransformWithEnv users never need to import internal/envelope.
type EnvelopeInfo struct {
	CorrID    string
	OriginID  string
	Cap       string
	SessionID string
	Meta      map[string]any
}

// PipeTransformWithEnv is like Transform but also receives envelope metadata.
// Use this escape hatch when a transform stage needs the session ID, correlation
// ID, or request meta — for example to prepend a session-scoped prompt prefix.
//
//	pepper.PipeTransformWithEnv(func(env pepper.EnvelopeInfo, in map[string]any) (map[string]any, error) {
//	    prompt, _ := in["prompt"].(string)
//	    return map[string]any{"prompt": "[session:" + env.SessionID + "] " + prompt}, nil
//	})
func PipeTransformWithEnv(fn func(EnvelopeInfo, map[string]any) (map[string]any, error)) *compose.TransformStage {
	return compose.Transform(func(env envelope.Envelope, in map[string]any) (map[string]any, error) {
		info := EnvelopeInfo{
			CorrID:    env.CorrID,
			OriginID:  env.OriginID,
			Cap:       env.Cap,
			SessionID: env.SessionID,
			Meta:      env.Meta,
		}
		return fn(info, in)
	})
}
