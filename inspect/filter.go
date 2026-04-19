package inspect

import (
	"fmt"
	"strings"
)

// Filter describes which events to forward to a subscriber.
type Filter struct {
	Cap     string // match specific capability
	Group   string // match specific group
	Worker  string // match specific worker
	Runtime string // match specific runtime
	MsgType string // match specific msg_type
	MinHop  int    // minimum hop count (-1 = all)
}

// Matches returns true if the event passes this filter.
func (f Filter) Matches(e Event) bool {
	if f.Cap != "" && e.Cap != f.Cap {
		return false
	}
	if f.Group != "" && e.Group != f.Group {
		return false
	}
	if f.Worker != "" && e.WorkerID != f.Worker {
		return false
	}
	if f.Runtime != "" && e.Runtime != f.Runtime {
		return false
	}
	if f.MsgType != "" && e.Type != f.MsgType {
		return false
	}
	if f.MinHop >= 0 && int(e.Hop) < f.MinHop {
		return false
	}
	return true
}

// ParseFilterArg parses a --filter flag value into a Filter.
// Format: "cap=face.recognize" or "hop=>0" or "group=gpu"
func ParseFilterArg(arg string) Filter {
	f := Filter{MinHop: -1}
	parts := strings.SplitN(arg, "=", 2)
	if len(parts) != 2 {
		return f
	}
	key, val := parts[0], parts[1]
	switch key {
	case "cap":
		f.Cap = val
	case "group":
		f.Group = val
	case "worker":
		f.Worker = val
	case "runtime":
		f.Runtime = val
	case "type":
		f.MsgType = val
	case "hop":
		if strings.HasPrefix(val, ">") {
			fmt.Sscanf(val[1:], "%d", &f.MinHop)
			f.MinHop++ // ">0" means hop >= 1
		}
	}
	return f
}
