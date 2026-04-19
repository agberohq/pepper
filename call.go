package pepper

import (
	"time"

	"github.com/agberohq/pepper/internal/core"
	"github.com/agberohq/pepper/internal/envelope"
)

// Call describes a single capability invocation for use with pp.All() and
// the typed pepper.All[O]() free function.
type Call struct {
	Cap  string
	In   core.In
	Opts []CallOption
}

// MakeCall constructs a Call for use with All().
func MakeCall(cap string, in core.In, opts ...CallOption) Call {
	return Call{Cap: cap, In: in, Opts: opts}
}

// CallOption modifies a single invocation.
type CallOption func(*callOpts)

type callOpts struct {
	group      string
	workerID   string
	capVer     string
	dispatch   string // kept as string for flexibility; converted to envelope.Dispatch in buildEnvelope
	quorum     uint8
	deadlineMs int64
	sessionID  string
	maxHops    uint8
	maxCbDepth uint8
	meta       map[string]any // extra envelope Meta entries
}

func defaultCallOpts() callOpts {
	return callOpts{
		dispatch:   string(envelope.DispatchAny),
		maxHops:    10,
		maxCbDepth: 5,
	}
}

// Routing

// WithCallGroup routes the request to a named worker group.
func WithCallGroup(group string) CallOption {
	return func(o *callOpts) { o.group = group }
}

// WithCallWorker pins the request to a specific worker ID.
func WithCallWorker(id string) CallOption {
	return func(o *callOpts) { o.workerID = id }
}

// WithCallDispatch sets the dispatch mode. Accepts envelope.Dispatch values
// or raw strings ("any", "all", "first", "vote", "quorum").
func WithCallDispatch(mode string) CallOption {
	return func(o *callOpts) { o.dispatch = mode }
}

// WithCallQuorum sets the minimum response count for vote/quorum dispatch.
func WithCallQuorum(n uint8) CallOption {
	return func(o *callOpts) { o.quorum = n }
}

// Versioning

// WithCallVersion sets a semver constraint for the target capability.
func WithCallVersion(constraint string) CallOption {
	return func(o *callOpts) { o.capVer = constraint }
}

// Timing

// WithCallDeadline sets an absolute deadline for this request.
func WithCallDeadline(t time.Time) CallOption {
	return func(o *callOpts) { o.deadlineMs = t.UnixMilli() }
}

// WithCallTimeout sets a relative timeout for this request.
func WithCallTimeout(d time.Duration) CallOption {
	return func(o *callOpts) { o.deadlineMs = time.Now().Add(d).UnixMilli() }
}

// Session

// WithCallSession injects a session ID into the request envelope.
func WithCallSession(id string) CallOption {
	return func(o *callOpts) { o.sessionID = id }
}

// Limits

// WithCallMaxHops overrides the pipeline hop limit for this request.
func WithCallMaxHops(n uint8) CallOption {
	return func(o *callOpts) { o.maxHops = n }
}

// WithCallMaxCbDepth overrides the callback nesting limit for this request.
func WithCallMaxCbDepth(n uint8) CallOption {
	return func(o *callOpts) { o.maxCbDepth = n }
}

// WithMeta injects a key/value pair into the envelope Meta for this request.
// Multiple calls accumulate. Reserved keys prefixed with "_" are used internally
// (e.g. "_process_id" for the tracker).
func WithMeta(key string, value any) CallOption {
	return func(o *callOpts) {
		if o.meta == nil {
			o.meta = make(map[string]any)
		}
		o.meta[key] = value
	}
}

// Dispatch mode constants (re-exported for call sites)
// These match envelope.Dispatch values so callers don't need to import internal.

const (
	DispatchAny    = string(envelope.DispatchAny)
	DispatchAll    = string(envelope.DispatchAll)
	DispatchFirst  = string(envelope.DispatchFirst)
	DispatchVote   = string(envelope.DispatchVote)
	DispatchQuorum = string(envelope.DispatchQuorum)
)
