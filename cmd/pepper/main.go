// Command pepper is the Pepper CLI tool.
//
// Usage:
//
//	pepper inspect /tmp/pepper-tap.sock
//	pepper inspect /tmp/pepper-tap.sock --filter cap=face.recognize
//	pepper inspect /tmp/pepper-tap.sock --filter hop=">0"
//	pepper inspect /tmp/pepper-tap.sock --format json
//	pepper inspect /tmp/pepper-tap.sock --filter group=gpu --filter type=res_chunk
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/agberohq/pepper/inspect"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "inspect":
		runInspect(os.Args[2:])
	case "help", "--help", "-h":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "pepper: unknown command %q\n", os.Args[1])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `pepper — Pepper CLI tool

Commands:
  inspect <socket>   Attach to a live Pepper wire tap

inspect flags:
  --filter key=value   Filter events (repeatable)
                       Keys: cap, group, worker, runtime, type, hop
                       Examples: --filter cap=face.recognize
                                 --filter hop=">0"
                                 --filter type=res_chunk
  --format text|json   Output format (default: text)`)
}

func runInspect(args []string) {
	fs := flag.NewFlagSet("inspect", flag.ExitOnError)
	var filters multiFlag
	fs.Var(&filters, "filter", "filter expression (repeatable)")
	format := fs.String("format", "text", "output format: text or json")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if fs.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "pepper inspect: socket path required")
		os.Exit(1)
	}
	sockPath := fs.Arg(0)

	// Build filter from flags
	merged := inspect.Filter{MinHop: -1}
	for _, f := range filters {
		parsed := inspect.ParseFilterArg(f)
		if parsed.Cap != "" {
			merged.Cap = parsed.Cap
		}
		if parsed.Group != "" {
			merged.Group = parsed.Group
		}
		if parsed.Worker != "" {
			merged.Worker = parsed.Worker
		}
		if parsed.Runtime != "" {
			merged.Runtime = parsed.Runtime
		}
		if parsed.MsgType != "" {
			merged.MsgType = parsed.MsgType
		}
		if parsed.MinHop >= 0 {
			merged.MinHop = parsed.MinHop
		}
	}

	// Connect to inspector socket
	conn, err := net.Dial("unix", sockPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "pepper inspect: cannot connect to %q: %v\n", sockPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	// Send filter config
	cfg := struct {
		Filter inspect.Filter `json:"filter"`
		Format string         `json:"format"`
	}{Filter: merged, Format: *format}
	data, _ := json.Marshal(cfg)
	conn.Write(data)

	// Stream events to stdout
	buf := make([]byte, 65536)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "pepper inspect: %v\n", err)
			}
			return
		}
		os.Stdout.Write(buf[:n])
	}
}

// multiFlag is a flag.Value that collects repeated --filter flags.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ", ") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }
