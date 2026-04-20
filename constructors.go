package pepper

import (
	"github.com/agberohq/pepper/internal/runtime/adapter"
	"github.com/agberohq/pepper/internal/runtime/cli"
)

// HTTPAdapter starts building an HTTP adapter capability builder.
// Use with pp.Adapt() for the legacy API, or use pepper.HTTP(name, url) with pp.Use().
func HTTPAdapter(baseURL string) *adapter.HTTPBuilder { return adapter.HTTP(baseURL) }

// MCPAdapter starts building an MCP adapter capability builder.
// Use with pp.Adapt() for the legacy API, or use pepper.MCP(name, url) with pp.Use().
func MCPAdapter(serverURL string) *adapter.MCPBuilder { return adapter.MCP(serverURL) }

// Ollama is the built-in adapter for the Ollama HTTP API.
var Ollama adapter.Adapter = adapter.Ollama

// OpenAI is the built-in adapter for the OpenAI Chat Completions API.
var OpenAI adapter.Adapter = adapter.OpenAI

// AnthropicAdapter is the built-in adapter for the Anthropic Messages API.
var AnthropicAdapter adapter.Adapter = adapter.Anthropic

// BearerToken returns an AuthProvider that sets Authorization: Bearer <token>.
func BearerToken(token string) adapter.AuthProvider { return adapter.BearerToken(token) }

// APIKey returns an AuthProvider that sets a named header to the given key.
func APIKey(header, key string) adapter.AuthProvider { return adapter.APIKey(header, key) }

// CMD starts building a CLI tool capability.
// Use with pp.Prepare() for the legacy API, or use pepper.CLI(name, cmd) with pp.Use().
func CMD(command string, args ...string) *cli.Builder { return cli.CMD(command, args...) }

// JSONStdin passes the payload JSON-encoded to the process stdin.
const JSONStdin = cli.JSONStdin

// JSONStdout decodes the process stdout as a JSON result.
const JSONStdout = cli.JSONStdout

// MsgPackStdin passes the payload msgpack-encoded to the process stdin.
const MsgPackStdin = cli.MsgPackStdin

// AdaptMCP registers all tools from an MCP server as a wildcard capability.
func (p *Pepper) AdaptMCP(serverURL string, opts ...CapOption) error {
	// Derive a stable slug from the URL for use as the capability name prefix.
	out := make([]byte, 0, 48)
	for i := 0; i < len(serverURL) && len(out) < 48; i++ {
		c := serverURL[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
			out = append(out, c)
		case c >= 'A' && c <= 'Z':
			out = append(out, c+32)
		default:
			if len(out) > 0 && out[len(out)-1] != '-' {
				out = append(out, '-')
			}
		}
	}
	for len(out) > 0 && out[len(out)-1] == '-' {
		out = out[:len(out)-1]
	}
	return p.Register(MCP("mcp."+string(out), serverURL), opts...)
}
