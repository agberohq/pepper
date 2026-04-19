package router

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/codec"
	"github.com/agberohq/pepper/internal/dlq"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/pending"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

type WorkerState struct {
	ID             string
	Runtime        string
	Groups         []string
	Caps           []string
	Load           atomic.Uint32
	RequestsServed atomic.Uint64
	UptimeMs       atomic.Int64
	State          atomic.Value // WorkerHealthState
	LastPing       atomic.Int64
	inflight       sync.Map // corrID → *inflightMeta
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
	onDispatch       func(env envelope.Envelope, workerID string)
	onResponse       func(env envelope.Envelope, workerID string, latency time.Duration)
	onError          func(env envelope.Envelope, code envelope.Code)
}

type Config struct {
	MaxRetries       int
	PoisonThreshold  int
	PoisonPillTTL    time.Duration
	DLQ              dlq.Backend
	Codec            codec.Codec
	HeartbeatTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		MaxRetries:       2,
		PoisonThreshold:  2,
		PoisonPillTTL:    time.Hour,
		HeartbeatTimeout: 15 * time.Second,
	}
}

func New(b bus.Bus, p *pending.Map, reaper *jack.Reaper, cfg Config, logger *ll.Logger) *Router {
	hbTimeout := cfg.HeartbeatTimeout
	if hbTimeout == 0 {
		hbTimeout = 15 * time.Second
	}
	return &Router{
		workers:          make(map[string]*WorkerState),
		groups:           make(map[string][]string),
		capAffinity:      make(map[string][]string),
		poisonPillTTL:    cfg.PoisonPillTTL,
		poisonThreshold:  cfg.PoisonThreshold,
		maxRetries:       cfg.MaxRetries,
		heartbeatTimeout: hbTimeout,
		dlq:              cfg.DLQ,
		bus:              b,
		pending:          p,
		reaper:           reaper,
		codec:            cfg.Codec,
		logger:           logger.Namespace("router"),
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
}

func (r *Router) MarkCapReady(workerID, cap string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.capAffinity[cap] = append(r.capAffinity[cap], workerID)
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
	r.mu.RLock()
	ws, ok := r.workers[ping.WorkerID]
	r.mu.RUnlock()
	if !ok {
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

	// Normalize: empty group means "default" — workers register in "default" group
	// and Python subscribes to pepper.push.default, so an empty group produces a
	// topic mismatch that silently drops the message.
	if env.Group == "" {
		env.Group = "default"
	}

	env.MsgType = envelope.MsgReq
	if env.ReplyTo == "" {
		env.ReplyTo = "pepper.res." + env.OriginID
	}

	if env.WorkerID != "" {
		r.mu.RLock()
		ws, ok := r.workers[env.WorkerID]
		r.mu.RUnlock()
		if !ok || ws.State.Load().(WorkerHealthState) != WorkerStateReady {
			return fmt.Errorf("%w: worker=%s", ErrWorkerDead, env.WorkerID)
		}
	}

	if env.Dispatch == envelope.DispatchAny && env.WorkerID == "" {
		env.WorkerID = r.affinityWorker(env.Cap, env.Group)
		if env.WorkerID == "" {
			return fmt.Errorf("%w: group=%s cap=%s", ErrNoWorkers, env.Group, env.Cap)
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
	_ = r.bus.Publish(bus.TopicBroadcast, data)
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
			r.dlq.Write(dlq.Entry{
				OriginID:     originID,
				Cap:          capName,
				CrashCount:   int(count),
				LastWorkerID: workerID,
				DeclaredAt:   time.Now(),
			})
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
func (r *Router) HasCapWorker(cap string) bool {
	r.mu.RLock()
	affinity := r.capAffinity[cap]
	r.mu.RUnlock()
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

func containsGroup(groups []string, target string) bool {
	for _, g := range groups {
		if g == target {
			return true
		}
	}
	return false
}

var (
	ErrWorkerDead = fmt.Errorf("worker dead")
	ErrNoWorkers  = fmt.Errorf("no workers")
	ErrPoisonPill = fmt.Errorf("poison pill")
)
