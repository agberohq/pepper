package pepper

import (
	"time"

	"github.com/agberohq/pepper/internal/envelope"
)

// Call describes a single capability invocation for use with pp.All() and
// the typed pepper.All[O]() free function.
type Call struct {
	Cap  string
	In   In
	Opts []CallOption
}

// MakeCall constructs a Call for use with All().
func MakeCall(cap string, in In, opts ...CallOption) Call {
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
}

func defaultCallOpts() callOpts {
	return callOpts{
		dispatch:   string(envelope.DispatchAny),
		maxHops:    10,
		maxCbDepth: 5,
	}
}

// Routing

// WithGroup routes the request to a named worker group.
func WithGroup(group string) CallOption {
	return func(o *callOpts) { o.group = group }
}

// WithWorker pins the request to a specific worker ID.
func WithWorker(id string) CallOption {
	return func(o *callOpts) { o.workerID = id }
}

// WithDispatch sets the dispatch mode. Accepts envelope.Dispatch values
// or raw strings ("any", "all", "first", "vote", "quorum").
func WithDispatch(mode string) CallOption {
	return func(o *callOpts) { o.dispatch = mode }
}

// WithQuorum sets the minimum response count for vote/quorum dispatch.
func WithQuorum(n uint8) CallOption {
	return func(o *callOpts) { o.quorum = n }
}

// Versioning

// WithVersion sets a semver constraint for the target capability.
func WithVersion(constraint string) CallOption {
	return func(o *callOpts) { o.capVer = constraint }
}

// Timing

// WithDeadline sets an absolute deadline for this request.
func WithDeadline(t time.Time) CallOption {
	return func(o *callOpts) { o.deadlineMs = t.UnixMilli() }
}

// WithTimeout sets a relative timeout for this request.
func WithTimeout(d time.Duration) CallOption {
	return func(o *callOpts) { o.deadlineMs = time.Now().Add(d).UnixMilli() }
}

// Session

// WithSession injects a session ID into the request envelope.
func WithSession(id string) CallOption {
	return func(o *callOpts) { o.sessionID = id }
}

// Limits

// WithMaxHops overrides the pipeline hop limit for this request.
func WithMaxHops(n uint8) CallOption {
	return func(o *callOpts) { o.maxHops = n }
}

// WithMaxCbDepth overrides the callback nesting limit for this request.
func WithMaxCbDepth(n uint8) CallOption {
	return func(o *callOpts) { o.maxCbDepth = n }
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
