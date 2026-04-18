package pending

import (
	"sync/atomic"
)

type Response struct {
	Payload  []byte
	WorkerID string
	Cap      string
	CapVer   string
	Hop      uint8
	Meta     map[string]any
	Err      error
}

type entry struct {
	ch     chan Response
	stream chan Response
	done   atomic.Bool
}
