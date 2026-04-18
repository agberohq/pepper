package adapter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// HTTPBuilder / MCPBuilder fluent API

func TestHTTPBuilderDefaults(t *testing.T) {
	b := HTTP("http://localhost:11434")
	if b.baseURL != "http://localhost:11434" {
		t.Errorf("baseURL = %q", b.baseURL)
	}
	if b.timeout != 120*time.Second {
		t.Errorf("timeout = %v, want 120s", b.timeout)
	}
}

func TestHTTPBuilderChaining(t *testing.T) {
	b := HTTP("http://localhost:11434").
		With(Ollama).
		Auth(BearerToken("sk-test")).
		Timeout(30*time.Second).
		Groups("gpu", "llm").
		MapRequest(func(in map[string]any) (*Request, error) { return nil, nil }).
		MapResponse(func(r *Response) (map[string]any, error) { return nil, nil })

	if b.adapter == nil {
		t.Error("adapter should be set")
	}
	if b.auth == nil {
		t.Error("auth should be set")
	}
	if b.timeout != 30*time.Second {
		t.Errorf("timeout = %v, want 30s", b.timeout)
	}
	if len(b.groups) != 2 {
		t.Errorf("groups = %v, want 2", b.groups)
	}
	if b.mapRequest == nil {
		t.Error("mapRequest should be set")
	}
	if b.mapResponse == nil {
		t.Error("mapResponse should be set")
	}
}

func TestMCPBuilderChaining(t *testing.T) {
	b := MCP("http://localhost:3000/mcp").
		Tool("brave_search").
		Groups("tools")

	if b.serverURL != "http://localhost:3000/mcp" {
		t.Errorf("serverURL = %q", b.serverURL)
	}
	if b.tool != "brave_search" {
		t.Errorf("tool = %q, want brave_search", b.tool)
	}
	if len(b.groups) != 1 || b.groups[0] != "tools" {
		t.Errorf("groups = %v", b.groups)
	}
}

// BuildSpec

func TestHTTPBuilderBuildSpec(t *testing.T) {
	spec := HTTP("http://localhost:11434").
		With(Ollama).
		Groups("gpu").
		BuildSpec("llm.generate")

	if spec.Name != "llm.generate" {
		t.Errorf("Name = %q", spec.Name)
	}
	if spec.Source != "http://localhost:11434" {
		t.Errorf("Source = %q", spec.Source)
	}
	if len(spec.Groups) != 1 || spec.Groups[0] != "gpu" {
		t.Errorf("Groups = %v", spec.Groups)
	}
	if spec.AdapterSpec == nil {
		t.Error("AdapterSpec should be set")
	}
}

func TestHTTPBuilderBuildSpecDefaultGroups(t *testing.T) {
	spec := HTTP("http://localhost:8080").BuildSpec("my.cap")
	if len(spec.Groups) != 1 || spec.Groups[0] != "default" {
		t.Errorf("Groups = %v, want [default]", spec.Groups)
	}
}

func TestMCPBuilderBuildSpec(t *testing.T) {
	spec := MCP("http://localhost:3000/mcp").
		Tool("search").
		Groups("tools").
		BuildSpec("web.search")

	if spec.Name != "web.search" {
		t.Errorf("Name = %q", spec.Name)
	}
	if spec.Source != "http://localhost:3000/mcp" {
		t.Errorf("Source = %q", spec.Source)
	}
}

// Auth providers

func TestBearerTokenApply(t *testing.T) {
	auth := BearerToken("sk-abc123")
	req, _ := http.NewRequest("POST", "http://example.com", nil)
	if err := auth.Apply(req); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer sk-abc123" {
		t.Errorf("Authorization = %q, want Bearer sk-abc123", got)
	}
}

func TestAPIKeyApply(t *testing.T) {
	auth := APIKey("X-Api-Key", "my-key")
	req, _ := http.NewRequest("POST", "http://example.com", nil)
	if err := auth.Apply(req); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := req.Header.Get("X-Api-Key"); got != "my-key" {
		t.Errorf("X-Api-Key = %q, want my-key", got)
	}
}

// Runner

func TestRunnerRun(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"response": "hello world", "done": true})
	}))
	defer srv.Close()

	runner := NewRunner(srv.URL, Ollama, nil, 10*time.Second)
	out, err := runner.Run(context.Background(), map[string]any{
		"model":  "llama3",
		"prompt": "say hello",
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out["text"] != "hello world" {
		t.Errorf("text = %v, want hello world", out["text"])
	}
}

func TestRunnerRunHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	runner := NewRunner(srv.URL, Ollama, nil, 10*time.Second)
	_, err := runner.Run(context.Background(), map[string]any{"prompt": "hi"})
	if err == nil {
		t.Error("expected error for HTTP 503")
	}
}

func TestRunnerRunWithAuth(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{
				map[string]any{
					"message":       map[string]any{"content": "response text"},
					"finish_reason": "stop",
				},
			},
		})
	}))
	defer srv.Close()

	runner := NewRunner(srv.URL, OpenAI, BearerToken("sk-test"), 10*time.Second)
	_, err := runner.Run(context.Background(), map[string]any{"prompt": "hi"})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if gotAuth != "Bearer sk-test" {
		t.Errorf("auth header = %q, want Bearer sk-test", gotAuth)
	}
}

// Ollama adapter

func TestOllamaBuildRequest(t *testing.T) {
	req, err := Ollama.BuildRequest(context.Background(), map[string]any{
		"model":  "llama3",
		"prompt": "hello",
	})
	if err != nil {
		t.Fatalf("BuildRequest: %v", err)
	}
	if req.Method != "POST" {
		t.Errorf("Method = %q, want POST", req.Method)
	}
	if req.URL != "/api/generate" {
		t.Errorf("URL = %q, want /api/generate", req.URL)
	}

	var body map[string]any
	json.Unmarshal(req.Body, &body)
	if body["model"] != "llama3" {
		t.Errorf("body model = %v", body["model"])
	}
	if body["prompt"] != "hello" {
		t.Errorf("body prompt = %v", body["prompt"])
	}
}

func TestOllamaBuildRequestDefaultModel(t *testing.T) {
	req, err := Ollama.BuildRequest(context.Background(), map[string]any{"prompt": "hi"})
	if err != nil {
		t.Fatalf("BuildRequest: %v", err)
	}
	var body map[string]any
	json.Unmarshal(req.Body, &body)
	if body["model"] != "llama3" {
		t.Errorf("default model = %v, want llama3", body["model"])
	}
}

func TestOllamaParseResponse(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"response": "world", "done": true})
	out, err := Ollama.ParseResponse(context.Background(), &Response{StatusCode: 200, Body: body})
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if out["text"] != "world" {
		t.Errorf("text = %v, want world", out["text"])
	}
}

func TestOllamaStreaming(t *testing.T) {
	if !Ollama.Streaming() {
		t.Error("Ollama should support streaming")
	}
}

func TestOllamaHealthCheck(t *testing.T) {
	if err := Ollama.HealthCheck(context.Background()); err != nil {
		t.Errorf("HealthCheck: %v", err)
	}
}

// OpenAI adapter

func TestOpenAIBuildRequest(t *testing.T) {
	req, err := OpenAI.BuildRequest(context.Background(), map[string]any{
		"prompt": "hello",
	})
	if err != nil {
		t.Fatalf("BuildRequest: %v", err)
	}
	if req.URL != "/v1/chat/completions" {
		t.Errorf("URL = %q", req.URL)
	}
	var body map[string]any
	json.Unmarshal(req.Body, &body)
	if body["model"] != "gpt-4o-mini" {
		t.Errorf("default model = %v", body["model"])
	}
}

func TestOpenAIBuildRequestWithTools(t *testing.T) {
	tools := []map[string]any{{"type": "function", "function": map[string]any{"name": "search"}}}
	req, err := OpenAI.BuildRequest(context.Background(), map[string]any{
		"prompt": "hi",
		"tools":  tools,
	})
	if err != nil {
		t.Fatalf("BuildRequest: %v", err)
	}
	var body map[string]any
	json.Unmarshal(req.Body, &body)
	if body["tools"] == nil {
		t.Error("tools should be present in request body")
	}
}

func TestOpenAIParseResponse(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"choices": []any{
			map[string]any{
				"message":       map[string]any{"content": "hi there"},
				"finish_reason": "stop",
			},
		},
	})
	out, err := OpenAI.ParseResponse(context.Background(), &Response{StatusCode: 200, Body: body})
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if out["text"] != "hi there" {
		t.Errorf("text = %v, want hi there", out["text"])
	}
	if out["finish_reason"] != "stop" {
		t.Errorf("finish_reason = %v, want stop", out["finish_reason"])
	}
}

// Anthropic adapter

func TestAnthropicBuildRequest(t *testing.T) {
	req, err := Anthropic.BuildRequest(context.Background(), map[string]any{
		"prompt": "hello claude",
		"system": "be concise",
	})
	if err != nil {
		t.Fatalf("BuildRequest: %v", err)
	}
	if req.URL != "/v1/messages" {
		t.Errorf("URL = %q", req.URL)
	}
	if req.Headers["anthropic-version"] == "" {
		t.Error("anthropic-version header missing")
	}
	var body map[string]any
	json.Unmarshal(req.Body, &body)
	if body["system"] != "be concise" {
		t.Errorf("system = %v", body["system"])
	}
	if body["max_tokens"] == nil {
		t.Error("max_tokens should be set")
	}
}

func TestAnthropicParseResponse(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"content": []any{
			map[string]any{"type": "text", "text": "hello!"},
		},
	})
	out, err := Anthropic.ParseResponse(context.Background(), &Response{StatusCode: 200, Body: body})
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if out["text"] != "hello!" {
		t.Errorf("text = %v, want hello!", out["text"])
	}
}

// MCP adapter

func TestMCPAdapterBuildRequest(t *testing.T) {
	a := &MCPAdapter{ServerURL: "http://localhost:3000/mcp", ToolName: "brave_search"}
	req, err := a.BuildRequest(context.Background(), map[string]any{"query": "golang"})
	if err != nil {
		t.Fatalf("BuildRequest: %v", err)
	}
	if req.Method != "POST" {
		t.Errorf("Method = %q", req.Method)
	}
	var body map[string]any
	json.Unmarshal(req.Body, &body)
	if body["method"] != "tools/call" {
		t.Errorf("method = %v", body["method"])
	}
	params, _ := body["params"].(map[string]any)
	if params["name"] != "brave_search" {
		t.Errorf("tool name = %v", params["name"])
	}
}

func TestMCPAdapterParseResponse(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"result": map[string]any{
			"content": []map[string]any{
				{"type": "text", "text": "search result"},
			},
		},
	})
	a := &MCPAdapter{ToolName: "search"}
	out, err := a.ParseResponse(context.Background(), &Response{Body: body})
	if err != nil {
		t.Fatalf("ParseResponse: %v", err)
	}
	if out["text"] != "search result" {
		t.Errorf("text = %v", out["text"])
	}
}

func TestMCPAdapterParseResponseError(t *testing.T) {
	body, _ := json.Marshal(map[string]any{
		"error": map[string]any{"message": "tool not found"},
	})
	a := &MCPAdapter{ToolName: "search"}
	_, err := a.ParseResponse(context.Background(), &Response{Body: body})
	if err == nil {
		t.Error("expected error for RPC error response")
	}
}

func TestMCPAdapterStreaming(t *testing.T) {
	a := &MCPAdapter{}
	if a.Streaming() {
		t.Error("MCP adapter should not support streaming")
	}
}

func TestMCPAdapterParseStreamReturnsError(t *testing.T) {
	a := &MCPAdapter{}
	_, err := a.ParseStream(context.Background(), &Response{})
	if err == nil {
		t.Error("ParseStream should return error for MCP")
	}
}
