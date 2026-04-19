package pepper

import (
	"github.com/agberohq/pepper/internal/runtime/adapter"
	"github.com/agberohq/pepper/internal/runtime/cli"
)

// HTTP starts building an HTTP adapter capability.
func HTTP(baseURL string) *adapter.HTTPBuilder { return adapter.HTTP(baseURL) }

// MCP starts building an MCP adapter capability.
func MCP(serverURL string) *adapter.MCPBuilder { return adapter.MCP(serverURL) }

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
func CMD(command string, args ...string) *cli.Builder { return cli.CMD(command, args...) }

// JSONStdin passes the payload JSON-encoded to the process stdin.
const JSONStdin = cli.JSONStdin

// JSONStdout decodes the process stdout as a JSON result.
const JSONStdout = cli.JSONStdout

// MsgPackStdin passes the payload msgpack-encoded to the process stdin.
const MsgPackStdin = cli.MsgPackStdin

// AdaptMCP registers all tools from an MCP server as a wildcard capability.
func (p *Pepper) AdaptMCP(serverURL string, opts ...CapOption) error {
	capName := "mcp." + urlSlug(serverURL)
	return p.Adapt(capName, MCP(serverURL), opts...)
}

func urlSlug(url string) string {
	out := make([]byte, 0, 48)
	for i := 0; i < len(url) && len(out) < 48; i++ {
		c := url[i]
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
	return string(out)
}
