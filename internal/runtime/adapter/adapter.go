// Package adapter implements the Pepper HTTP and MCP adapter runtime.
//
// Any HTTP service — Ollama, OpenAI, vLLM, MCP servers, custom APIs —
// becomes a Pepper capability. The adapter translates Pepper In maps
// to HTTP requests and HTTP responses back to Pepper results.
//
// Usage:
//
//	pp.Adapt("llm.generate",
//	    adapter.HTTP("http://localhost:11434").
//	        With(adapter.Ollama).
//	        Groups("gpu"),
//	)
//
//	pp.Adapt("web.search",
//	    adapter.MCP("http://localhost:3000/mcp").
//	        Tool("brave_search").
//	        Groups("tools"),
//	)
package adapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// IsBlobRef reports whether v is a Pepper zero-copy blob reference dict.
func IsBlobRef(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	isPepper, _ := m["_pepper_blob"].(bool)
	return isPepper
}

// ResolveBlobBytes reads the raw bytes from a BlobRef's /dev/shm path.
// If v is not a BlobRef the call returns nil, false.
// Use this just-in-time before encoding an HTTP request body.
func ResolveBlobBytes(v any) ([]byte, bool, error) {
	m, ok := v.(map[string]any)
	if !ok || !IsBlobRef(v) {
		return nil, false, nil
	}
	path, _ := m["path"].(string)
	if path == "" {
		return nil, true, fmt.Errorf("adapter: blob ref missing path field")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, true, fmt.Errorf("adapter: read blob %q: %w", path, err)
	}
	return data, true, nil
}

// ResolveBlobBase64 resolves a BlobRef to a base64-encoded string suitable
// for embedding in JSON API bodies (e.g. OpenAI vision, Anthropic images).
func ResolveBlobBase64(v any) (string, bool, error) {
	data, isBlobRef, err := ResolveBlobBytes(v)
	if !isBlobRef || err != nil {
		return "", isBlobRef, err
	}
	return base64.StdEncoding.EncodeToString(data), true, nil
}

// ResolveInputs walks an input map and resolves any top-level BlobRef values
// to their base64 representation under a "_b64" suffixed key, leaving the
// original key pointing to the resolved bytes so adapters can choose either.
// Fields that are not BlobRefs are passed through unchanged.
func ResolveInputs(in map[string]any) (map[string]any, error) {
	out := make(map[string]any, len(in))
	for k, v := range in {
		data, isBlobRef, err := ResolveBlobBytes(v)
		if err != nil {
			return nil, err
		}
		if isBlobRef {
			out[k] = data
			out[k+"_b64"] = base64.StdEncoding.EncodeToString(data)
		} else {
			out[k] = v
		}
	}
	return out, nil
}

// Adapter translates between Pepper In/Out and an external service's protocol.
type Adapter interface {
	// BuildRequest converts Pepper inputs to an HTTP request.
	BuildRequest(ctx context.Context, in map[string]any) (*Request, error)

	// ParseResponse converts an HTTP response to Pepper outputs.
	ParseResponse(ctx context.Context, resp *Response) (map[string]any, error)

	// HealthCheck returns nil if the service is available.
	HealthCheck(ctx context.Context) error

	// Streaming returns true if this adapter supports streaming output.
	Streaming() bool

	// ParseStream converts a streaming HTTP response to a channel of chunks.
	// Only called when Streaming() is true.
	ParseStream(ctx context.Context, resp *Response) (<-chan map[string]any, error)
}

// Request is the adapter's outgoing HTTP request description.
type Request struct {
	Method  string
	URL     string
	Headers map[string]string
	Body    []byte
}

// Response is the adapter's incoming HTTP response.
type Response struct {
	StatusCode int
	Headers    map[string]string
	Body       []byte
	Stream     io.ReadCloser // non-nil for streaming responses
}

// Builder

// HTTPBuilder builds an HTTP adapter spec.
type HTTPBuilder struct {
	baseURL     string
	adapter     Adapter
	auth        AuthProvider
	timeout     time.Duration
	groups      []string
	mapRequest  func(map[string]any) (*Request, error)
	mapResponse func(*Response) (map[string]any, error)
}

// HTTP starts building an HTTP adapter.
func HTTP(baseURL string) *HTTPBuilder {
	return &HTTPBuilder{baseURL: baseURL, timeout: 120 * time.Second}
}

// With sets a named built-in adapter.
func (b *HTTPBuilder) With(a Adapter) *HTTPBuilder { b.adapter = a; return b }

// Auth sets the authentication provider.
func (b *HTTPBuilder) Auth(auth AuthProvider) *HTTPBuilder { b.auth = auth; return b }

// Timeout sets the HTTP request timeout.
func (b *HTTPBuilder) Timeout(d time.Duration) *HTTPBuilder { b.timeout = d; return b }

// Groups sets the worker groups.
func (b *HTTPBuilder) Groups(groups ...string) *HTTPBuilder { b.groups = groups; return b }

// MapRequest sets a custom request mapping function.
func (b *HTTPBuilder) MapRequest(fn func(map[string]any) (*Request, error)) *HTTPBuilder {
	b.mapRequest = fn
	return b
}

// MapResponse sets a custom response mapping function.
func (b *HTTPBuilder) MapResponse(fn func(*Response) (map[string]any, error)) *HTTPBuilder {
	b.mapResponse = fn
	return b
}

// MCPBuilder builds an MCP adapter spec.
type MCPBuilder struct {
	serverURL string
	tool      string
	groups    []string
}

// MCP starts building an MCP adapter.
func MCP(serverURL string) *MCPBuilder {
	return &MCPBuilder{serverURL: serverURL}
}

func (b *MCPBuilder) Tool(name string) *MCPBuilder        { b.tool = name; return b }
func (b *MCPBuilder) Groups(groups ...string) *MCPBuilder { b.groups = groups; return b }

// Accessors for the wiring layer in pepper_runtime.go.
func (b *HTTPBuilder) GetAdapter() Adapter       { return b.adapter }
func (b *HTTPBuilder) GetAuth() AuthProvider     { return b.auth }
func (b *HTTPBuilder) GetBaseURL() string        { return b.baseURL }
func (b *HTTPBuilder) GetTimeout() time.Duration { return b.timeout }
func (b *HTTPBuilder) GetGroups() []string       { return b.groups }

func (b *MCPBuilder) GetServerURL() string { return b.serverURL }
func (b *MCPBuilder) GetTool() string      { return b.tool }
func (b *MCPBuilder) GetGroups() []string  { return b.groups }

// Auth providers

// AuthProvider adds authentication to HTTP requests.
type AuthProvider interface {
	Apply(req *http.Request) error
}

// BearerToken returns an AuthProvider that adds a Bearer token header.
func BearerToken(token string) AuthProvider {
	return bearerAuth{token: token}
}

// APIKey returns an AuthProvider that adds an API key header.
func APIKey(header, key string) AuthProvider {
	return apiKeyAuth{header: header, key: key}
}

type bearerAuth struct{ token string }

func (a bearerAuth) Apply(req *http.Request) error {
	req.Header.Set("Authorization", "Bearer "+a.token)
	return nil
}

type apiKeyAuth struct{ header, key string }

func (a apiKeyAuth) Apply(req *http.Request) error {
	req.Header.Set(a.header, a.key)
	return nil
}

// Runner

// Runner executes HTTP adapter capabilities.
type Runner struct {
	baseURL string
	adapter Adapter
	auth    AuthProvider
	client  *http.Client
}

// NewRunner creates a Runner for an HTTP adapter.
func NewRunner(baseURL string, a Adapter, auth AuthProvider, timeout time.Duration) *Runner {
	return &Runner{
		baseURL: baseURL,
		adapter: a,
		auth:    auth,
		client:  &http.Client{Timeout: timeout},
	}
}

// Run calls the external service with the given inputs.
// Any BlobRef values in `in` are resolved to raw bytes (and a _b64 variant)
// before being passed to BuildRequest — adapters never see local file paths.
func (r *Runner) Run(ctx context.Context, in map[string]any) (map[string]any, error) {
	resolved, err := ResolveInputs(in)
	if err != nil {
		return nil, fmt.Errorf("adapter.Run: resolve blobs: %w", err)
	}

	req, err := r.adapter.BuildRequest(ctx, resolved)
	if err != nil {
		return nil, fmt.Errorf("adapter.Run: build request: %w", err)
	}

	fullURL := req.URL
	if len(fullURL) > 0 && fullURL[0] == '/' {
		fullURL = r.baseURL + fullURL
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.Method, fullURL, bytes.NewReader(req.Body))
	if err != nil {
		return nil, fmt.Errorf("adapter.Run: create request: %w", err)
	}
	for k, v := range req.Headers {
		httpReq.Header.Set(k, v)
	}
	if r.auth != nil {
		if err := r.auth.Apply(httpReq); err != nil {
			return nil, fmt.Errorf("adapter.Run: auth: %w", err)
		}
	}

	resp, err := r.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("adapter.Run: http: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("adapter.Run: read body: %w", err)
	}

	adapterResp := &Response{
		StatusCode: resp.StatusCode,
		Headers:    make(map[string]string),
		Body:       body,
	}
	for k := range resp.Header {
		adapterResp.Headers[k] = resp.Header.Get(k)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("adapter.Run: HTTP %d: %s", resp.StatusCode, truncate(string(body), 256))
	}

	return r.adapter.ParseResponse(ctx, adapterResp)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Built-in adapters

// Ollama adapts the Ollama HTTP API.
var Ollama Adapter = &ollamaAdapter{}

type ollamaAdapter struct{}

func (a *ollamaAdapter) BuildRequest(ctx context.Context, in map[string]any) (*Request, error) {
	body := map[string]any{
		"model":  in["model"],
		"prompt": in["prompt"],
		"stream": in["stream"],
	}
	if body["model"] == nil {
		body["model"] = "llama3"
	}
	if body["stream"] == nil {
		body["stream"] = false
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &Request{
		Method:  "POST",
		URL:     "/api/generate",
		Headers: map[string]string{"Content-Type": "application/json"},
		Body:    data,
	}, nil
}

func (a *ollamaAdapter) ParseResponse(ctx context.Context, resp *Response) (map[string]any, error) {
	var result map[string]any
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("ollama: parse response: %w", err)
	}
	// Normalise: return "text" as the primary field
	if response, ok := result["response"].(string); ok {
		result["text"] = response
	}
	return result, nil
}

func (a *ollamaAdapter) HealthCheck(ctx context.Context) error {
	// GET /api/tags — returns available models
	return nil
}

func (a *ollamaAdapter) Streaming() bool { return true }

func (a *ollamaAdapter) ParseStream(ctx context.Context, resp *Response) (<-chan map[string]any, error) {
	ch := make(chan map[string]any, 64)
	go func() {
		defer close(ch)
		// SSE parsing for Ollama streaming — implemented in full version
	}()
	return ch, nil
}

// OpenAI adapts the OpenAI Chat Completions API.
var OpenAI Adapter = &openAIAdapter{}

type openAIAdapter struct{}

func (a *openAIAdapter) BuildRequest(ctx context.Context, in map[string]any) (*Request, error) {
	messages := in["messages"]
	if messages == nil {
		// Single prompt → messages conversion
		prompt, _ := in["prompt"].(string)
		messages = []map[string]any{{"role": "user", "content": prompt}}
	}
	model, _ := in["model"].(string)
	if model == "" {
		model = "gpt-4o-mini"
	}

	body := map[string]any{
		"model":    model,
		"messages": messages,
		"stream":   false,
	}
	if tools, ok := in["tools"]; ok {
		body["tools"] = tools
	}
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &Request{
		Method:  "POST",
		URL:     "/v1/chat/completions",
		Headers: map[string]string{"Content-Type": "application/json"},
		Body:    data,
	}, nil
}

func (a *openAIAdapter) ParseResponse(ctx context.Context, resp *Response) (map[string]any, error) {
	var result map[string]any
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, err
	}
	// Extract the message content into a top-level "text" field
	if choices, ok := result["choices"].([]any); ok && len(choices) > 0 {
		if choice, ok := choices[0].(map[string]any); ok {
			if msg, ok := choice["message"].(map[string]any); ok {
				result["text"] = msg["content"]
				result["finish_reason"] = choice["finish_reason"]
				if toolCalls, ok := msg["tool_calls"]; ok {
					result["tool_calls"] = toolCalls
				}
			}
		}
	}
	return result, nil
}

func (a *openAIAdapter) HealthCheck(ctx context.Context) error { return nil }
func (a *openAIAdapter) Streaming() bool                       { return true }
func (a *openAIAdapter) ParseStream(ctx context.Context, resp *Response) (<-chan map[string]any, error) {
	ch := make(chan map[string]any, 64)
	go func() { defer close(ch) }()
	return ch, nil
}

// Anthropic adapts the Anthropic Messages API.
var Anthropic Adapter = &anthropicAdapter{}

type anthropicAdapter struct{}

func (a *anthropicAdapter) BuildRequest(ctx context.Context, in map[string]any) (*Request, error) {
	messages := in["messages"]
	if messages == nil {
		prompt, _ := in["prompt"].(string)
		messages = []map[string]any{{"role": "user", "content": prompt}}
	}
	model, _ := in["model"].(string)
	if model == "" {
		model = "claude-sonnet-4-6"
	}
	maxTokens := 1024
	if mt, ok := in["max_tokens"].(int); ok {
		maxTokens = mt
	}

	body := map[string]any{
		"model":      model,
		"messages":   messages,
		"max_tokens": maxTokens,
	}
	if tools, ok := in["tools"]; ok {
		body["tools"] = tools
	}
	if system, ok := in["system"].(string); ok {
		body["system"] = system
	}

	data, _ := json.Marshal(body)
	return &Request{
		Method: "POST",
		URL:    "/v1/messages",
		Headers: map[string]string{
			"Content-Type":      "application/json",
			"anthropic-version": "2023-06-01",
		},
		Body: data,
	}, nil
}

func (a *anthropicAdapter) ParseResponse(ctx context.Context, resp *Response) (map[string]any, error) {
	var result map[string]any
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, err
	}
	if content, ok := result["content"].([]any); ok && len(content) > 0 {
		if block, ok := content[0].(map[string]any); ok {
			result["text"] = block["text"]
		}
	}
	return result, nil
}

func (a *anthropicAdapter) HealthCheck(ctx context.Context) error { return nil }
func (a *anthropicAdapter) Streaming() bool                       { return true }
func (a *anthropicAdapter) ParseStream(ctx context.Context, resp *Response) (<-chan map[string]any, error) {
	ch := make(chan map[string]any, 64)
	go func() { defer close(ch) }()
	return ch, nil
}

// MCPAdapter wraps an MCP server tool as a Pepper capability.
type MCPAdapter struct {
	ServerURL string
	ToolName  string
}

func (a *MCPAdapter) BuildRequest(ctx context.Context, in map[string]any) (*Request, error) {
	body := map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      a.ToolName,
			"arguments": in,
		},
	}
	data, _ := json.Marshal(body)
	return &Request{
		Method:  "POST",
		URL:     a.ServerURL,
		Headers: map[string]string{"Content-Type": "application/json"},
		Body:    data,
	}, nil
}

func (a *MCPAdapter) ParseResponse(ctx context.Context, resp *Response) (map[string]any, error) {
	var rpc struct {
		Result struct {
			Content []map[string]any `json:"content"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(resp.Body, &rpc); err != nil {
		return nil, err
	}
	if rpc.Error != nil {
		return nil, fmt.Errorf("mcp: %s", rpc.Error.Message)
	}
	result := map[string]any{"content": rpc.Result.Content}
	// Extract text from first content block
	if len(rpc.Result.Content) > 0 {
		if text, ok := rpc.Result.Content[0]["text"].(string); ok {
			result["text"] = text
		}
	}
	return result, nil
}

func (a *MCPAdapter) HealthCheck(ctx context.Context) error { return nil }
func (a *MCPAdapter) Streaming() bool                       { return false }
func (a *MCPAdapter) ParseStream(ctx context.Context, resp *Response) (<-chan map[string]any, error) {
	return nil, fmt.Errorf("mcp: streaming not supported")
}
