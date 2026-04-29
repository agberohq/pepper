package adapter

import (
	"context"
	"fmt"
)

// noopAdapter is a fallback when no adapter is configured
type noopAdapter struct{}

func (a *noopAdapter) BuildRequest(ctx context.Context, in map[string]any) (*Request, error) {
	return &Request{Method: "POST", URL: "/"}, nil
}
func (a *noopAdapter) ParseResponse(ctx context.Context, resp *Response) (map[string]any, error) {
	return map[string]any{"raw": string(resp.Body)}, nil
}
func (a *noopAdapter) HealthCheck(ctx context.Context) error { return nil }
func (a *noopAdapter) Streaming() bool                       { return false }
func (a *noopAdapter) ParseStream(ctx context.Context, resp *Response) (<-chan map[string]any, error) {
	return nil, fmt.Errorf("streaming not supported")
}
