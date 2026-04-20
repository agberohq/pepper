package pepper

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/pending"
	"github.com/agberohq/pepper/internal/router"
	"github.com/olekukonko/jack"
)

// Vars is the input map for pp.Run() raw snippet execution.
type Vars = map[string]any

type runtimeState struct {
	bus          bus.Bus
	readyWorkers atomic.Int32
	workerStates sync.Map
	loopers      sync.Map
	bgCtx        context.Context
	bgCancel     context.CancelFunc
	router       *router.Router
	blob         *blob.Manager
	reqReaper    *jack.Reaper
	blobReaper   *jack.Reaper
	doctor       *jack.Doctor
}

type workerEntry struct {
	id, busAddr string
	groups      []string
	caps        []string
	ready       bool
	wc          WorkerConfig
}

// WorkerError is returned when a worker process reports a typed error code.
type WorkerError struct{ Code, Message string }

func (e *WorkerError) Error() string { return fmt.Sprintf("worker error [%s]: %s", e.Code, e.Message) }

// rawBidiStream is the untyped handle returned by openRawStream.
// BidiStream[In, Out] wraps it.
type rawBidiStream struct {
	streamID string
	corrID   string
	outCh    <-chan pending.Response
	pp       *Pepper
}
