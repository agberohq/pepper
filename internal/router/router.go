package router

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/pending"
	"github.com/agberohq/pepper/internal/wire"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

type WorkerState struct {
	ID                  string
	Runtime             string
	Groups              []string
	Caps                []string
	Load                atomic.Uint32
	RequestsServed      atomic.Uint64
	UptimeMs            atomic.Int64
	State               atomic.Value // WorkerHealthState
	LastPing            atomic.Int64
	inflight            sync.Map // corrID → *inflightMeta
	consecutiveFailures atomic.Int32
	lastFailureTime     atomic.Int64 // unix nanos
}

type inflightMeta struct {
	originID string
	cap      string
}

type WorkerHealthState int

const (
	WorkerStateUnknown WorkerHealthState = iota
	WorkerStateReady
	WorkerStateDegraded
	WorkerStateDead
)

type Router struct {
	mu               sync.RWMutex
	workers          map[string]*WorkerState
	groups           map[string][]string
	capAffinity      map[string][]string
	originToCorrs    sync.Map // originID → *corrList
	poisonPills      sync.Map // originID → *poisonEntry
	poisonPillTTL    time.Duration
	poisonThreshold  int
	maxRetries       int
	heartbeatTimeout time.Duration
	dlq              dlq.Backend
	bus              bus.Bus
	pending          *pending.Map
	reaper           *jack.Reaper
	codec            codec.Codec
	logger           *ll.Logger
	distributed      bool // when true, dispatch skips local worker lookup
	onDispatch       func(env envelope.Envelope, workerID string)
	// capReadyCh is closed-and-replaced whenever a new cap becomes ready,
	// allowing WaitCapReady to block without polling.
	capReadyMu sync.Mutex
	capReadyCh chan struct{}
	// Circuit breaker settings
	circuitBreakerThreshold int
	circuitBreakerTimeout   time.Duration
	// Auth settings
	authEnabled    bool
	authKeys       map[string]bool   // hex(public key) → valid
	authChallenges map[string][]byte // workerID → pending challenge bytes
	authenticated  map[string]bool   // workerID → authenticated
	// Rate limiting
	rateLimitEnabled bool
	rateLimiters     sync.Map // capName → *jack.RateLimiter
	ratePerCap       float64
	burstPerCap      int
}

type Config struct {
	MaxRetries       int
	PoisonThreshold  int
	PoisonPillTTL    time.Duration
	DLQ              dlq.Backend
	Codec            codec.Codec
	HeartbeatTimeout time.Duration
	// Distributed enables coord-bus dispatch mode. When true, Dispatch pushes
	// directly to the group queue on the bus without requiring a locally
	// registered worker — remote workers on other nodes pull from the shared queue.
	Distributed bool
	// CircuitBreakerThreshold opens the circuit after this many consecutive
	// failures on a single worker. Set to 0 to disable (default: 5).
	CircuitBreakerThreshold int
	// CircuitBreakerTimeout is how long a worker stays open-circuited before
	// allowing a test request (half-open state). Default: 30 seconds.
	CircuitBreakerTimeout time.Duration
	// AuthKeys is the set of authorized worker Ed25519 public keys.
	// When non-empty, workers must authenticate via challenge-response.
	AuthKeys []ed25519.PublicKey
	// RateLimitEnabled enables per-capability rate limiting.
	RateLimitEnabled bool
	// RatePerCap is the sustained requests per second per capability.
	RatePerCap float64
	// BurstPerCap is the max burst size per capability.
	BurstPerCap int
}

func DefaultConfig() Config {
	return Config{
		MaxRetries:              2,
		PoisonThreshold:         2,
		PoisonPillTTL:           time.Hour,
		HeartbeatTimeout:        15 * time.Second,
		CircuitBreakerThreshold: 5,
		CircuitBreakerTimeout:   30 * time.Second,
	}
}

func New(b bus.Bus, p *pending.Map, reaper *jack.Reaper, cfg Config, logger *ll.Logger) *Router {
	hbTimeout := cfg.HeartbeatTimeout
	if hbTimeout == 0 {
		hbTimeout = 15 * time.Second
	}
	authKeys := make(map[string]bool)
	for _, k := range cfg.AuthKeys {
		authKeys[string(k)] = true
	}
	return &Router{
		workers:                 make(map[string]*WorkerState),
		groups:                  make(map[string][]string),
		capAffinity:             make(map[string][]string),
		poisonPillTTL:           cfg.PoisonPillTTL,
		poisonThreshold:         cfg.PoisonThreshold,
		maxRetries:              cfg.MaxRetries,
		heartbeatTimeout:        hbTimeout,
		dlq:                     cfg.DLQ,
		bus:                     b,
		pending:                 p,
		reaper:                  reaper,
		codec:                   cfg.Codec,
		logger:                  logger.Namespace("router"),
		distributed:             cfg.Distributed,
		capReadyCh:              make(chan struct{}),
		circuitBreakerThreshold: cfg.CircuitBreakerThreshold,
		circuitBreakerTimeout:   cfg.CircuitBreakerTimeout,
		authEnabled:             len(cfg.AuthKeys) > 0,
		authKeys:                authKeys,
		authChallenges:          make(map[string][]byte),
		authenticated:           make(map[string]bool),
		rateLimitEnabled:        cfg.RateLimitEnabled,
		ratePerCap:              cfg.RatePerCap,
		burstPerCap:             cfg.BurstPerCap,
	}
}

func (r *Router) RegisterWorker(hello envelope.Hello) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ws := &WorkerState{
		ID:      hello.WorkerID,
		Runtime: hello.Runtime,
		Groups:  hello.Groups,
		Caps:    hello.Caps,
	}
	ws.State.Store(WorkerStateReady)
	ws.LastPing.Store(time.Now().UnixMilli())
	r.workers[hello.WorkerID] = ws
	for _, g := range hello.Groups {
		r.addToGroup(g, hello.WorkerID)
	}
	r.logger.Fields("worker_id", hello.WorkerID, "runtime", hello.Runtime, "groups", hello.Groups).Info("worker registered")

	// If auth is enabled and the worker sent a public key, issue a challenge
	if r.authEnabled && len(hello.PublicKey) > 0 {
		if !r.authKeys[string(hello.PublicKey)] {
			r.logger.Fields("worker_id", hello.WorkerID).Warn("worker public key not authorized")
			return
		}
		challenge := make([]byte, 32)
		_, _ = rand.Read(challenge)
		r.authChallenges[hello.WorkerID] = challenge
		r.authenticated[hello.WorkerID] = false
		msg := envelope.AuthChallenge{
			ProtoVer:  envelope.ProtoVer,
			MsgType:   envelope.MsgAuthChallenge,
			WorkerID:  hello.WorkerID,
			Challenge: challenge,
		}
		data, _ := r.codec.Marshal(msg)
		_ = r.bus.Publish(bus.TopicControl(hello.WorkerID), data)
		r.logger.Fields("worker_id", hello.WorkerID).Info("auth challenge sent")
	} else if r.authEnabled {
		// Auth is enabled but worker didn't send a public key
		r.logger.Fields("worker_id", hello.WorkerID).Warn("auth required but no public key sent")
	}
}

// HandleAuthResponse verifies a worker's auth response and marks it authenticated.
func (r *Router) HandleAuthResponse(resp envelope.AuthResponse) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	challenge, ok := r.authChallenges[resp.WorkerID]
	if !ok {
		return fmt.Errorf("no pending auth challenge for worker %s", resp.WorkerID)
	}
	if !ed25519.Verify(ed25519.PublicKey(resp.PublicKey), challenge, resp.Signature) {
		return fmt.Errorf("auth signature verification failed for worker %s", resp.WorkerID)
	}
	r.authenticated[resp.WorkerID] = true
	delete(r.authChallenges, resp.WorkerID)
	r.logger.Fields("worker_id", resp.WorkerID).Info("worker authenticated")
	return nil
}

// IsAuthenticated returns true if the worker has passed auth (or auth is disabled).
func (r *Router) IsAuthenticated(workerID string) bool {
	if !r.authEnabled {
		return true
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.authenticated[workerID]
}

func (r *Router) MarkCapReady(workerID, cap string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capAffinity[cap] = append(r.capAffinity[cap], workerID)
	r.notifyCapReady()
}

// notifyCapReady wakes all WaitCapReady callers. Must be called with r.mu held.
func (r *Router) notifyCapReady() {
	r.capReadyMu.Lock()
	ch := r.capReadyCh
	r.capReadyCh = make(chan struct{})
	r.capReadyMu.Unlock()
	close(ch)
}

// WaitCapReady blocks until HasCapWorker(cap) returns true or ctx is done.
func (r *Router) WaitCapReady(ctx context.Context, cap string) error {
	for {
		if r.HasCapWorker(cap) {
			return nil
		}
		r.capReadyMu.Lock()
		ch := r.capReadyCh
		r.capReadyMu.Unlock()
		select {
		case <-ch:
			// A new cap became ready — re-check.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (r *Router) MarkWorkerDead(workerID string) {
	r.mu.Lock()
	ws, ok := r.workers[workerID]
	if !ok {
		r.mu.Unlock()
		return
	}
	ws.State.Store(WorkerStateDead)
	r.mu.Unlock()

	var inFlightIDs []string
	ws.inflight.Range(func(corrID, meta any) bool {
		inFlightIDs = append(inFlightIDs, corrID.(string))
		m := meta.(*inflightMeta)
		r.recordCrash(m.originID, workerID, m.cap)
		return true
	})
	for _, id := range inFlightIDs {
		ws.inflight.Delete(id)
	}

	r.pending.CancelByIDs(inFlightIDs, fmt.Errorf("worker %s died", workerID))
	r.logger.Fields("worker_id", workerID, "inflight", len(inFlightIDs)).Warn("worker marked dead")
}

func (r *Router) WorkerAlive(workerID string) bool {
	r.mu.RLock()
	ws, ok := r.workers[workerID]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	last := ws.LastPing.Load()
	if last == 0 {
		return false
	}
	return time.Since(time.UnixMilli(last)) < r.heartbeatTimeout
}

func (r *Router) RemoveWorker(workerID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ws, ok := r.workers[workerID]
	if !ok {
		return
	}
	for _, g := range ws.Groups {
		r.removeFromGroup(g, workerID)
	}
	delete(r.workers, workerID)
}

func (r *Router) UpdateHeartbeat(ping envelope.HbPing) {
	r.mu.Lock()
	ws, ok := r.workers[ping.WorkerID]
	if !ok && r.distributed {
		// Worker came up before this node started — pub/sub does not retain
		// messages, so worker_hello and cap_ready were missed. Reconstruct
		// the full worker state from the heartbeat, which carries both groups
		// and the caps list (populated only after cap_load completes).
		groups := ping.Groups
		if len(groups) == 0 {
			groups = []string{wire.GroupDefault}
		}
		ws = &WorkerState{
			ID:      ping.WorkerID,
			Runtime: ping.Runtime,
			Groups:  groups,
		}
		ws.State.Store(WorkerStateReady)
		ws.LastPing.Store(time.Now().UnixMilli())
		r.workers[ping.WorkerID] = ws
		for _, g := range groups {
			r.addToGroup(g, ping.WorkerID)
		}
		r.logger.Fields("worker_id", ping.WorkerID, "groups", groups, "caps", ping.Caps).Info("worker discovered via heartbeat")
	}
	// Idempotently register cap affinity from the heartbeat caps list.
	// Only processes when caps is non-empty — an empty list means the worker's
	// cap_load hasn't completed yet (first heartbeat race) and should be ignored.
	var newCapRegistered bool
	for _, cap := range ping.Caps {
		found := false
		for _, id := range r.capAffinity[cap] {
			if id == ping.WorkerID {
				found = true
				break
			}
		}
		if !found {
			r.capAffinity[cap] = append(r.capAffinity[cap], ping.WorkerID)
			newCapRegistered = true
		}
	}
	if newCapRegistered {
		r.notifyCapReady()
	}
	r.mu.Unlock()
	if ws == nil {
		return
	}
	ws.Load.Store(uint32(ping.Load))
	ws.LastPing.Store(time.Now().UnixMilli())
	ws.RequestsServed.Store(ping.RequestsServed)
	ws.UptimeMs.Store(ping.UptimeMs)
	if ws.State.Load().(WorkerHealthState) == WorkerStateDead {
		ws.State.Store(WorkerStateReady)
	}
}

func (r *Router) Dispatch(ctx context.Context, env envelope.Envelope) error {
	if r.isPoisoned(env.OriginID) {
		return fmt.Errorf("%w: origin_id %s blacklisted", ErrPoisonPill, env.OriginID)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Rate limiting check per capability
	if r.rateLimitEnabled && env.Cap != "" {
		limiter := r.getOrCreateLimiter(env.Cap)
		if !limiter.Allow(jack.PriorityHigh) {
			return fmt.Errorf("%w: cap=%s", ErrRateLimited, env.Cap)
		}
	}

	// Normalize: empty group means "default" — workers register in "default" group
	// and Python subscribes to pepper.push.default, so an empty group produces a
	// topic mismatch that silently drops the message.
	if env.Group == "" {
		env.Group = wire.GroupDefault
	}

	env.MsgType = envelope.MsgReq
	if env.ReplyTo == "" {
		env.ReplyTo = bus.TopicRes(env.OriginID)
	}

	if env.WorkerID != "" {
		if !r.distributed {
			r.mu.RLock()
			ws, ok := r.workers[env.WorkerID]
			r.mu.RUnlock()
			if !ok || ws.State.Load().(WorkerHealthState) != WorkerStateReady {
				return fmt.Errorf("%w: worker=%s", ErrWorkerDead, env.WorkerID)
			}
		}
		// In distributed mode, the worker lives on a remote node — skip the
		// local liveness check and push directly to the worker's personal queue.
	}

	if env.Dispatch == envelope.DispatchAny && env.WorkerID == "" {
		if r.distributed {
			// In distributed mode, workers can live on remote nodes, so the
			// local workerStates map is not the source of truth. BUT we must
			// still verify at least ONE worker in the cluster has sent
			// cap_ready for this cap — otherwise the router would push the
			// request onto the shared coord queue with nobody draining it,
			// and the caller would hang until ctx deadline with no useful
			// error. cap_ready is fanned out on the shared pepper.control
			// channel, so every node sees every ready worker in the cluster;
			// HasCapWorker answers this without a coord round-trip.
			if !r.HasCapWorker(env.Cap) {
				return fmt.Errorf("%w: cap=%s group=%s (no ready worker in cluster)",
					ErrNoWorkers, env.Cap, env.Group)
			}
		} else {
			env.WorkerID = r.affinityWorker(env.Cap, env.Group)
			if env.WorkerID == "" {
				return fmt.Errorf("%w: group=%s cap=%s", ErrNoWorkers, env.Group, env.Cap)
			}
		}
	}

	data, err := r.codec.Marshal(env)
	if err != nil {
		return fmt.Errorf("router: marshal envelope: %w", err)
	}

	var busErr error
	switch env.Dispatch {
	case envelope.DispatchAny:
		if env.WorkerID != "" {
			// Route directly to the pinned worker's own push topic so it
			// doesn't compete with other workers on the shared group queue.
			busErr = r.bus.PushOne(ctx, bus.TopicPush(env.WorkerID), data)
		} else {
			busErr = r.bus.PushOne(ctx, bus.TopicPush(env.Group), data)
		}
	default:
		busErr = r.bus.Publish(bus.TopicPub(env.Group), data)
	}
	if busErr != nil {
		return fmt.Errorf("router.Dispatch: %w", busErr)
	}

	if env.DeadlineMs > 0 {
		r.reaper.TouchAt(env.CorrID, time.UnixMilli(env.DeadlineMs))
	}
	r.trackInFlight(env)

	if r.onDispatch != nil {
		r.onDispatch(env, env.WorkerID)
	}
	return nil
}

func (r *Router) BroadcastCancel(originID string) {
	msg := envelope.CancelMsg{ProtoVer: 1, MsgType: envelope.MsgCancel, OriginID: originID}
	data, err := r.codec.Marshal(msg)
	if err != nil {
		r.logger.Fields("origin_id", originID, "error", err).Error("cancel: marshal failed")
		return
	}
	if err := r.bus.Publish(bus.TopicBroadcast, data); err != nil {
		r.logger.Fields("origin_id", originID, "error", err).Warn("cancel: broadcast failed")
	}
	r.logger.Fields("origin_id", originID).Debug("cancel broadcast sent")
}

func (r *Router) recordCrash(originID, workerID, capName string) {
	val, _ := r.poisonPills.LoadOrStore(originID, &poisonEntry{})
	entry := val.(*poisonEntry)

	count := entry.crashes.Add(1)
	entry.lastWorker.Store(workerID)

	if int(count) >= r.poisonThreshold && entry.poisoned.CompareAndSwap(false, true) {
		entry.expiresAt.Store(time.Now().Add(r.poisonPillTTL).UnixMilli())
		r.logger.Fields("origin_id", originID, "crashes", count).Error("poison pill declared")
		r.pending.Fail(originID, fmt.Errorf("%w: %s", ErrPoisonPill, originID))
		if r.dlq != nil {
			entry := dlq.Entry{
				OriginID:     originID,
				Cap:          capName,
				CrashCount:   int(count),
				LastWorkerID: workerID,
				DeclaredAt:   time.Now(),
			}
			// A DLQ that cannot persist is worth a loud log: the poison pill is
			// otherwise unrecoverable the moment the worker restarts.
			if err := r.dlq.Write(entry); err != nil {
				r.logger.Fields("origin_id", originID, "error", err).Error("dlq: write poison pill failed")
			}
		}
	}
}

func (r *Router) isPoisoned(originID string) bool {
	val, ok := r.poisonPills.Load(originID)
	if !ok {
		return false
	}
	entry := val.(*poisonEntry)
	if !entry.poisoned.Load() {
		return false
	}
	expires := entry.expiresAt.Load()
	if time.Now().UnixMilli() > expires {
		r.poisonPills.Delete(originID)
		return false
	}
	return true
}

type poisonEntry struct {
	crashes    atomic.Int32
	poisoned   atomic.Bool
	lastWorker atomic.Value
	expiresAt  atomic.Int64
}

func (r *Router) affinityWorker(cap, group string) string {
	r.mu.RLock()
	affinity := r.capAffinity[cap]
	groupWorkers := r.groups[group]
	r.mu.RUnlock()

	if best := r.leastBusy(affinity, group); best != "" {
		return best
	}
	return r.leastBusy(groupWorkers, group)
}

func (r *Router) leastBusy(candidates []string, group string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var best string
	var bestLoad uint32 = 101
	for _, id := range candidates {
		ws, ok := r.workers[id]
		if !ok {
			continue
		}
		if state := ws.State.Load().(WorkerHealthState); state != WorkerStateReady {
			continue
		}
		if r.isCircuitOpen(ws) {
			continue // Skip workers with open circuit breaker
		}
		if group != "" && !containsGroup(ws.Groups, group) {
			continue
		}
		if load := ws.Load.Load(); load < bestLoad {
			bestLoad = load
			best = id
		}
	}
	return best
}

func (r *Router) trackInFlight(env envelope.Envelope) {
	actual, _ := r.originToCorrs.LoadOrStore(env.OriginID, &corrList{})
	actual.(*corrList).add(env.CorrID)

	if env.WorkerID != "" {
		r.mu.RLock()
		ws, ok := r.workers[env.WorkerID]
		r.mu.RUnlock()
		if ok {
			ws.inflight.Store(env.CorrID, &inflightMeta{originID: env.OriginID, cap: env.Cap})
		}
	}
}

type corrList struct {
	mu   sync.Mutex
	list []string
}

func (c *corrList) add(corrID string) {
	c.mu.Lock()
	c.list = append(c.list, corrID)
	c.mu.Unlock()
}

func (c *corrList) remove(corrID string) {
	c.mu.Lock()
	for i, v := range c.list {
		if v == corrID {
			c.list[i] = c.list[len(c.list)-1]
			c.list = c.list[:len(c.list)-1]
			break
		}
	}
	c.mu.Unlock()
}

func (r *Router) ResolveInflight(corrID, originID, workerID string) {
	if v, ok := r.originToCorrs.Load(originID); ok {
		v.(*corrList).remove(corrID)
	}
	if workerID != "" {
		r.mu.RLock()
		ws, ok := r.workers[workerID]
		r.mu.RUnlock()
		if ok {
			ws.inflight.Delete(corrID)
		}
	}
}

// Workers returns a snapshot of all registered workers.
func (r *Router) Workers() []*WorkerState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*WorkerState, 0, len(r.workers))
	for _, ws := range r.workers {
		out = append(out, ws)
	}
	return out
}

// IsCircuitOpen is the exported version of isCircuitOpen for health checks.
func (r *Router) IsCircuitOpen(ws *WorkerState) bool {
	return r.isCircuitOpen(ws)
}

func (r *Router) WorkersInGroup(group string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for _, id := range r.groups[group] {
		ws, ok := r.workers[id]
		if !ok {
			continue
		}
		if ws.State.Load().(WorkerHealthState) != WorkerStateReady {
			continue
		}
		out = append(out, id)
	}
	return out
}

func (r *Router) WorkerCountInGroup(group string) int {
	return len(r.WorkersInGroup(group))
}

// HasCapWorker reports whether at least one live worker has sent cap_ready for cap.
// Used by Pepper.WorkerReady() to let callers poll after Start().
//
// In distributed mode a non-owning node (pure orchestrator) may not have the
// cap-specific worker in r.workers yet when capAffinity is populated (pub/sub
// delivery order is non-deterministic). In that case we accept the affinity
// entry as sufficient proof — the worker is alive and will drain the queue.
func (r *Router) HasCapWorker(cap string) bool {
	r.mu.RLock()
	affinity := r.capAffinity[cap]
	r.mu.RUnlock()
	if len(affinity) == 0 {
		return false
	}
	if r.distributed {
		// Trust the cap_ready affinity entry; the worker may not appear in
		// r.workers yet on a non-owning node.
		return true
	}
	for _, id := range affinity {
		r.mu.RLock()
		ws, ok := r.workers[id]
		r.mu.RUnlock()
		if ok && ws.State.Load().(WorkerHealthState) == WorkerStateReady {
			return true
		}
	}
	return false
}
func (r *Router) addToGroup(group, workerID string) {
	r.groups[group] = append(r.groups[group], workerID)
}

func (r *Router) removeFromGroup(group, workerID string) {
	ids := r.groups[group]
	for i, id := range ids {
		if id == workerID {
			r.groups[group] = append(ids[:i], ids[i+1:]...)
			return
		}
	}
}

// isCircuitOpen returns true if the worker's circuit breaker is open,
// meaning it should be skipped for routing. After the timeout expires,
// the circuit enters half-open state, allowing one test request.
func (r *Router) isCircuitOpen(ws *WorkerState) bool {
	if r.circuitBreakerThreshold <= 0 {
		return false // disabled
	}
	failures := ws.consecutiveFailures.Load()
	if failures < int32(r.circuitBreakerThreshold) {
		return false
	}
	// Circuit is open — check if timeout has passed (half-open state)
	lastFailure := ws.lastFailureTime.Load()
	if lastFailure == 0 {
		return true
	}
	return time.Since(time.Unix(0, lastFailure)) < r.circuitBreakerTimeout
}

// recordWorkerFailure increments the consecutive failure count for circuit breaking.
func (r *Router) recordWorkerFailure(ws *WorkerState) {
	ws.consecutiveFailures.Add(1)
	ws.lastFailureTime.Store(time.Now().UnixNano())
}

// recordWorkerSuccess resets the consecutive failure count on a successful request.
func (r *Router) recordWorkerSuccess(ws *WorkerState) {
	ws.consecutiveFailures.Store(0)
}

// RecordFailure marks a failed request against a worker, feeding the circuit
// breaker. Called by the runtime when an error envelope arrives.
func (r *Router) RecordFailure(workerID string) {
	if r.circuitBreakerThreshold <= 0 || workerID == "" {
		return
	}
	r.mu.RLock()
	ws, ok := r.workers[workerID]
	r.mu.RUnlock()
	if ok {
		r.recordWorkerFailure(ws)
	}
}

// RecordSuccess clears a worker's failure streak.
func (r *Router) RecordSuccess(workerID string) {
	if workerID == "" {
		return
	}
	r.mu.RLock()
	ws, ok := r.workers[workerID]
	r.mu.RUnlock()
	if ok {
		r.recordWorkerSuccess(ws)
	}
}

func containsGroup(groups []string, target string) bool {
	for _, g := range groups {
		if g == target {
			return true
		}
	}
	return false
}

var (
	ErrWorkerDead  = fmt.Errorf("worker dead")
	ErrNoWorkers   = fmt.Errorf("no workers")
	ErrPoisonPill  = fmt.Errorf("poison pill")
	ErrRateLimited = fmt.Errorf("rate limited")
)

// getOrCreateLimiter returns the rate limiter for a capability, creating it lazily.
func (r *Router) getOrCreateLimiter(capName string) *jack.RateLimiter {
	if limiter, ok := r.rateLimiters.Load(capName); ok {
		return limiter.(*jack.RateLimiter)
	}
	// Lazy creation with sync.Map LoadOrStore for thread safety
	burst := r.burstPerCap
	if burst <= 0 {
		burst = 10
	}
	rate := r.ratePerCap
	if rate <= 0 {
		rate = 100
	}
	limiter := jack.NewRateLimiter(rate, burst)
	actual, _ := r.rateLimiters.LoadOrStore(capName, limiter)
	return actual.(*jack.RateLimiter)
}
