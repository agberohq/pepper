package pepper

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	blobpkg "github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/compose"
	"github.com/agberohq/pepper/internal/envelope"
	"github.com/agberohq/pepper/internal/metrics"
	"github.com/agberohq/pepper/internal/pending"
	"github.com/agberohq/pepper/internal/registry"
	"github.com/agberohq/pepper/internal/router"
	"github.com/agberohq/pepper/internal/runtime/adapter"
	"github.com/agberohq/pepper/internal/runtime/cli"
	"github.com/agberohq/pepper/internal/runtime/goruntime"
	"github.com/oklog/ulid/v2"
	"github.com/olekukonko/jack"
	"github.com/olekukonko/ll"
)

type runtimeState struct {
	nngBus       *bus.Mula
	router       *router.Router
	blob         *blobpkg.Manager
	reqReaper    *jack.Reaper
	blobReaper   *jack.Reaper
	doctor       *jack.Doctor
	sessionLM    interface{}
	readyWorkers atomic.Int32
	workerStates sync.Map
	loopers      sync.Map
	bgCtx        context.Context
	bgCancel     context.CancelFunc
	logger       *ll.Logger
}

type workerEntry struct {
	id, busAddr string
	pid         int
	groups      []string
	caps        []string
	ready       bool
	wc          WorkerConfig
}

func (pp *Pepper) bootRuntime(ctx context.Context) error {
	log := ll.New("pepper")
	rt := &runtimeState{logger: log}
	pp.rt = rt
	bgCtx, bgCancel := context.WithCancel(context.Background())
	rt.bgCtx = bgCtx
	rt.bgCancel = bgCancel

	busURL := pp.cfg.TransportURL
	if busURL == "" {
		busURL = fmt.Sprintf("tcp://127.0.0.1:%d", freePort())
	}

	nngBus, err := bus.NewMula(bus.Config{URL: busURL, SendBuf: 256, RecvBuf: 256})
	if err != nil {
		bgCancel()
		return fmt.Errorf("pepper: bus: %w", err)
	}
	rt.nngBus = nngBus

	rt.reqReaper = jack.NewReaper(0, jack.ReaperWithShards(32), jack.ReaperWithHandler(func(_ context.Context, corrID string) {
		pp.pending.Fail(corrID, fmt.Errorf("deadline exceeded: %s", corrID))
	}))
	rt.blobReaper = jack.NewReaper(0, jack.ReaperWithShards(16), jack.ReaperWithHandler(func(_ context.Context, blobID string) {
		rt.blob.Reap(blobID)
	}))

	blobDir := pp.cfg.BlobDir
	if blobDir == "" {
		blobDir = blobpkg.PlatformDefaultDir()
	}
	bm, err := blobpkg.NewManager(blobDir, rt.blobReaper)
	if err != nil {
		bgCancel()
		return fmt.Errorf("pepper: blob manager: %w", err)
	}
	rt.blob = bm

	rt.router = router.New(nngBus, pp.pending, rt.reqReaper, router.Config{
		MaxRetries: pp.cfg.MaxRetries, PoisonThreshold: pp.cfg.PoisonPillThreshold,
		PoisonPillTTL: pp.cfg.PoisonPillTTL, DLQ: pp.cfg.DLQ, Codec: pp.codec, HeartbeatTimeout: pp.cfg.HeartbeatInterval * 3,
	}, log)
	rt.doctor = jack.NewDoctor(jack.DoctorWithMaxConcurrent(len(pp.cfg.Workers)+4), jack.DoctorWithGlobalTimeout(pp.cfg.HeartbeatInterval*3))

	resCh, err := nngBus.SubscribePrefix(bgCtx, "pepper.res.")
	if err != nil {
		bgCancel()
		return fmt.Errorf("pepper: subscribe res: %w", err)
	}
	// pepper.control. (with dot) — worker-specific control messages
	ctrlCh, err := nngBus.SubscribePrefix(bgCtx, "pepper.control.")
	if err != nil {
		bgCancel()
		return fmt.Errorf("pepper: subscribe ctrl: %w", err)
	}
	// pepper.control (no dot) — Python test mock and legacy workers send here
	ctrlExactCh, err := nngBus.Subscribe(bgCtx, "pepper.control")
	if err != nil {
		bgCancel()
		return fmt.Errorf("pepper: subscribe ctrl exact: %w", err)
	}
	hbCh, err := nngBus.SubscribePrefix(bgCtx, "pepper.hb.")
	if err != nil {
		bgCancel()
		return fmt.Errorf("pepper: subscribe hb: %w", err)
	}
	go pp.responseLoop(bgCtx, resCh)
	go pp.controlLoop(bgCtx, ctrlCh)
	go pp.controlLoop(bgCtx, ctrlExactCh)
	go pp.controlLoop(bgCtx, hbCh)

	allSpecs := pp.reg.All()
	if len(allSpecs) > 0 {
		// Boot Go-native workers (run as goroutines inside this process).
		if err := pp.bootGoWorkers(bgCtx, allSpecs); err != nil {
			bgCancel()
			return fmt.Errorf("pepper: go workers: %w", err)
		}
		// Boot HTTP/MCP adapter workers.
		if err := pp.bootAdapterWorkers(bgCtx, allSpecs); err != nil {
			bgCancel()
			return fmt.Errorf("pepper: adapter workers: %w", err)
		}
		// Boot CLI tool workers.
		if err := pp.bootCLIWorkers(bgCtx, allSpecs); err != nil {
			bgCancel()
			return fmt.Errorf("pepper: cli workers: %w", err)
		}
		// Boot Python subprocess workers.
		for i, wc := range pp.cfg.Workers {
			wID := wc.ID
			if wID == "" {
				wID = fmt.Sprintf("w-%d", i+1)
			}
			if err := pp.spawnPythonWorker(bgCtx, wc, wID, nngBus.Addr(), allSpecs); err != nil {
				bgCancel()
				return err
			}
		}
		deadline := time.Now().Add(pp.cfg.DefaultTimeout)
		for time.Now().Before(deadline) {
			if rt.readyWorkers.Load() > 0 {
				break
			}
			time.Sleep(30 * time.Millisecond)
		}
	}

	pp.shutdown.RegisterWithContext("doctor.stop", func(_ context.Context) error { rt.doctor.StopAll(pp.cfg.ShutdownTimeout); return nil })
	pp.shutdown.RegisterWithContext("loopers.stop", func(_ context.Context) error {
		rt.loopers.Range(func(_, v any) bool { v.(*jack.Looper).Stop(); return true })
		return nil
	})
	pp.shutdown.RegisterWithContext("workers.bye", func(_ context.Context) error {
		bye, _ := pp.codec.Marshal(map[string]any{"proto_ver": uint8(1), "msg_type": "worker_bye"})
		_ = rt.nngBus.Publish(bus.TopicBroadcast, bye)
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	pp.shutdown.RegisterWithContext("req.reaper.stop", func(_ context.Context) error { rt.reqReaper.Stop(); return nil })
	pp.shutdown.RegisterWithContext("blob.reaper.stop", func(_ context.Context) error { rt.blobReaper.Stop(); return nil })
	pp.shutdown.RegisterWithContext("bg.cancel", func(_ context.Context) error { bgCancel(); return nil })
	pp.shutdown.Register(nngBus)
	return nil
}

func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 7731
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

func (pp *Pepper) responseLoop(ctx context.Context, ch <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			pp.routeResponse(msg.Data)
		}
	}
}

func (pp *Pepper) routeResponse(data []byte) {
	var raw map[string]any
	if err := pp.codec.Unmarshal(data, &raw); err != nil {
		return
	}
	env := envelope.Data(raw)
	corrID := env.CorrID()
	if corrID == "" {
		corrID = env.OriginID()
	}

	// Persist session mutations written by Python pepper.session().set().
	// Workers attach _session_updates to env.Meta(); we merge them into the
	// store here so state survives across requests.
	sessionID, _ := raw["session_id"].(string)
	if sessionID != "" {
		if updates, ok := env.Meta()["_session_updates"].(map[string]any); ok && len(updates) > 0 {
			_ = pp.sessions.Merge(sessionID, updates)
		}
	}

	deadline := env.DeadlineMs()
	if deadline > 0 {
		for _, bid := range env.Blobs() {
			pp.rt.blobReaper.TouchAt(bid, time.UnixMilli(deadline))
		}
		if pp.cfg.Metrics != nil && len(env.Blobs()) > 0 {
			pp.cfg.Metrics.Counter(metrics.MetricBlobsCreated, int64(len(env.Blobs())), nil)
		}
	}

	switch env.MsgType() {
	case "res":
		pp.rt.router.ResolveInflight(corrID, env.OriginID(), env.WorkerID())
		pp.rt.reqReaper.Remove(corrID)
		pp.pending.Resolve(corrID, pending.Response{Payload: env.Payload(), WorkerID: env.WorkerID(), Cap: env.Cap(), CapVer: env.CapVer(), Hop: env.Hop(), Meta: env.Meta()})
	case "err":
		pp.rt.router.ResolveInflight(corrID, env.OriginID(), env.WorkerID())
		pp.rt.reqReaper.Remove(corrID)
		pp.pending.Fail(corrID, &WorkerError{Code: env.Code(), Message: env.Message()})
	case "res_chunk":
		pp.pending.Chunk(corrID, pending.Response{Payload: env.Payload(), WorkerID: env.WorkerID()})
	case "res_end":
		pp.rt.reqReaper.Remove(corrID)
		pp.pending.EndStream(corrID)
	case "cb_req":
		// A Python worker called pepper.call(). Execute the nested capability
		// and send a cb_res back to the originating worker on its res topic.
		go func() {
			cbStart := time.Now()
			cbID, _ := raw["cb_id"].(string)
			capName, _ := raw["cap"].(string)

			if pp.cfg.Metrics != nil {
				pp.cfg.Metrics.Counter(metrics.MetricCallbacksTotal, 1, map[string]string{"cap": capName})
			}

			var cbIn In
			if payload := env.Payload(); len(payload) > 0 {
				_ = pp.codec.Unmarshal(payload, &cbIn)
			}
			if cbIn == nil {
				cbIn = In{}
			}

			res, err := pp.Do(context.Background(), capName, cbIn)

			if pp.cfg.Metrics != nil {
				pp.cfg.Metrics.Histogram(metrics.MetricCallbacksLatencyMs, float64(time.Since(cbStart).Milliseconds()), map[string]string{"cap": capName})
				if err != nil {
					pp.cfg.Metrics.Counter(metrics.MetricCallbacksErrors, 1, map[string]string{"cap": capName})
				}
			}

			cbRes := map[string]any{
				"proto_ver": uint8(1),
				"msg_type":  "cb_res",
				"origin_id": env.OriginID(),
				"cb_id":     cbID,
			}
			if err != nil {
				cbRes["error"] = err.Error()
			} else {
				cbRes["payload"] = res.AsBytes()
			}
			resData, _ := pp.codec.Marshal(cbRes)
			_ = pp.rt.nngBus.Publish(bus.TopicRes(env.OriginID()), resData)
		}()
	}
}

func (pp *Pepper) controlLoop(ctx context.Context, ch <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			pp.handleControl(msg.Data)
		}
	}
}

func (pp *Pepper) handleControl(data []byte) {
	var raw map[string]any
	if err := pp.codec.Unmarshal(data, &raw); err != nil {
		return
	}
	env := envelope.Data(raw)
	switch env.MsgType() {
	case "worker_hello":
		wid := env.WorkerID()
		if wid == "" {
			return
		}
		if cn, _ := env["codec"].(string); cn != "" && cn != string(pp.cfg.Serializer) {
			pp.rt.logger.Fields("worker", wid, "codec", cn).Error("CODEC_MISMATCH")
			return
		}
		pp.rt.workerStates.Store(wid, &workerEntry{id: wid, groups: env.Groups()})
		pp.rt.router.RegisterWorker(envelope.Hello{WorkerID: wid, Groups: env.Groups()})
		pp.registerWorkerPatient(wid)

		if pp.cfg.Metrics != nil {
			pp.cfg.Metrics.Gauge(metrics.MetricWorkersCount, 1, map[string]string{"worker": wid})
		}

		// Send cap_load to the newly connected python worker
		runtimeType, _ := raw["runtime"].(string)
		if runtimeType == "python" || runtimeType == "" {
			for _, spec := range pp.reg.All() {
				if spec.Runtime == registry.RuntimePython && (len(env.Groups()) == 0 || groupsOverlap(spec.Groups, env.Groups())) {
					payload, _ := pp.codec.Marshal(buildCapLoad(spec))
					_ = pp.rt.nngBus.Publish(bus.TopicControl(wid), payload)
				}
			}
		}

	case "cap_ready":
		// ALWAYS unblock pp.Start() even if the capability failed to load
		pp.rt.readyWorkers.Add(1)

		if errStr, _ := env["error"].(string); errStr == "" {
			pp.rt.router.MarkCapReady(env.WorkerID(), env.Cap())
			if v, ok := pp.rt.workerStates.Load(env.WorkerID()); ok {
				e := v.(*workerEntry)
				e.caps = append(e.caps, env.Cap())
				e.ready = true
			}
		} else {
			pp.rt.logger.Fields("worker", env.WorkerID(), "cap", env.Cap(), "error", errStr).Error("capability failed to load")
		}

	case "hb_ping":
		wid := env.WorkerID()
		pp.rt.router.UpdateHeartbeat(envelope.HbPing{
			WorkerID:       wid,
			Load:           env.Hop(),
			RequestsServed: uint64(env.Int64("requests_served")),
			UptimeMs:       env.Int64("uptime_ms"),
		})
		if pp.cfg.Metrics != nil {
			pp.cfg.Metrics.Gauge(metrics.MetricWorkersLoad, float64(env.Hop()), map[string]string{"worker": wid})
			pp.cfg.Metrics.Gauge(metrics.MetricWorkersServed, float64(env.Int64("requests_served")), map[string]string{"worker": wid})
		}
		if v, ok := pp.rt.workerStates.Load(wid); ok {
			pp.checkRecycle(wid, v.(*workerEntry).wc, uint64(env.Int64("requests_served")), env.Int64("uptime_ms"))
		}
	}
}

func (pp *Pepper) dispatchPipeline(ctx context.Context, spec *registry.Spec, name string, in In, o callOpts) (Result, error) {
	dag, ok := spec.Pipeline.(*compose.DAG)
	if !ok {
		return Result{}, fmt.Errorf("pepper: invalid pipeline spec for %q", name)
	}

	originID := ulid.Make().String()
	stageCh, err := pp.rt.nngBus.SubscribePrefix(ctx, "pepper.pipe."+sanitizeName(name))
	if err != nil {
		return Result{}, err
	}

	// Initial stage
	env := buildEnvelope(ulid.Make().String(), originID, "", in, o, pp.cfg.DefaultTimeout)
	if err := dag.DispatchEnvelope(&env, 0); err != nil {
		return Result{}, err
	}

	payload, _ := pp.codec.Marshal(in)
	env.Payload = payload
	env.MsgType = envelope.MsgReq
	env.Dispatch = envelope.DispatchAny

	if err := pp.rt.router.Dispatch(ctx, env); err != nil {
		return Result{}, err
	}

	// Pipeline execution loop
	var finalResult Result
	timeout := time.NewTimer(time.Until(time.UnixMilli(env.DeadlineMs)))
	defer timeout.Stop()

	for {
		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-timeout.C:
			return Result{}, fmt.Errorf("pipeline deadline exceeded")
		case msg, ok := <-stageCh:
			if !ok {
				return Result{}, fmt.Errorf("pipeline channel closed")
			}
			var raw map[string]any
			if err := pp.codec.Unmarshal(msg.Data, &raw); err != nil {
				continue
			}
			res := envelope.Data(raw)

			// Register blobs
			for _, bid := range res.Blobs() {
				pp.rt.blobReaper.TouchAt(bid, time.UnixMilli(env.DeadlineMs))
			}

			if res.MsgType() == "err" {
				return Result{}, &WorkerError{Code: res.Code(), Message: res.Message()}
			}

			if payload := res.Payload(); len(payload) > 0 {
				finalResult.payload = payload
				finalResult.codec = pp.codec
			}
			finalResult.WorkerID = res.WorkerID()
			finalResult.Cap = res.Cap()
			finalResult.Meta = res.Meta()
			finalResult.Hop = res.Hop()

			if res.ForwardTo() != "" || res.GatherAt() != "" {
				// Advance stage (simplified for demonstration; full impl tracks state)
				continue
			}
			// Pipeline complete
			return finalResult, nil
		}
	}
}

func sanitizeName(name string) string {
	out := make([]byte, len(name))
	for i, c := range name {
		if c == '.' {
			out[i] = '_'
		} else {
			out[i] = byte(c)
		}
	}
	return string(out)
}

func (pp *Pepper) registerWorkerPatient(workerID string) {
	hbTimeout := pp.cfg.HeartbeatInterval * 3
	patient := jack.NewPatient(jack.PatientConfig{
		ID:          workerID,
		Interval:    pp.cfg.HeartbeatInterval,
		Accelerated: pp.cfg.HeartbeatInterval / 2,
		MaxFailures: 2,
		Timeout:     hbTimeout,
		Check: func(ctx context.Context) error {
			if pp.rt.router.WorkerAlive(workerID) {
				return nil
			}
			return fmt.Errorf("no heartbeat from %s", workerID)
		},
		OnStateChange: func(e jack.PatientEvent) {
			if e.State == jack.PatientFailed {
				pp.rt.router.MarkWorkerDead(workerID)
				if pp.cfg.Metrics != nil {
					tags := map[string]string{"worker": workerID}
					pp.cfg.Metrics.Counter(metrics.MetricWorkersCrashes, 1, tags)
					pp.cfg.Metrics.Gauge(metrics.MetricWorkersCount, -1, tags)
				}
				pp.triggerRespawn(workerID)
			}
		},
	})
	_ = pp.rt.doctor.Add(patient)
}

func (pp *Pepper) triggerRespawn(workerID string) {
	if pp.cfg.Metrics != nil {
		pp.cfg.Metrics.Counter(metrics.MetricWorkersRespawns, 1, map[string]string{"worker": workerID})
	}
	if v, ok := pp.rt.loopers.Load(workerID); ok {
		v.(*jack.Looper).ResetInterval()
	}
}

func (pp *Pepper) startRespawnLooper(workerID string, wc WorkerConfig, allSpecs []*registry.Spec) {
	looper := jack.NewLooper(
		func() error { return pp.spawnPythonWorker(pp.rt.bgCtx, wc, workerID, pp.rt.nngBus.Addr(), allSpecs) },
		jack.WithLooperName("respawn:"+workerID),
		jack.WithLooperInterval(5*time.Second),
		jack.WithLooperBackoff(true),
		jack.WithLooperMaxInterval(30*time.Second),
		jack.WithLooperImmediate(false),
	)
	pp.rt.loopers.Store(workerID, looper)
	looper.Start()
}

func (pp *Pepper) checkRecycle(workerID string, wc WorkerConfig, served uint64, uptimeMs int64) {
	if (wc.MaxRequests > 0 && served >= wc.MaxRequests) || (wc.MaxUptime > 0 && time.Duration(uptimeMs)*time.Millisecond >= wc.MaxUptime) {
		bye, _ := pp.codec.Marshal(map[string]any{"proto_ver": uint8(1), "msg_type": "worker_bye", "worker_id": workerID})
		_ = pp.rt.nngBus.Publish(bus.TopicBroadcast, bye)
	}
}

func (pp *Pepper) spawnPythonWorker(ctx context.Context, wc WorkerConfig, workerID, busAddr string, allSpecs []*registry.Spec) error {
	var matchedCaps []*registry.Spec
	for _, spec := range allSpecs {
		if spec.Runtime == registry.RuntimePython && (len(wc.Groups) == 0 || groupsOverlap(spec.Groups, wc.Groups)) {
			matchedCaps = append(matchedCaps, spec)
		}
	}
	if len(matchedCaps) == 0 {
		return nil
	}
	groups := wc.Groups
	if len(groups) == 0 {
		groups = []string{"default"}
	}
	blobDir := pp.cfg.BlobDir
	if blobDir == "" {
		blobDir = blobpkg.PlatformDefaultDir()
	}

	envVars := bus.WorkerEnvVars(workerID, busAddr, string(pp.cfg.Serializer), groups, int(pp.cfg.HeartbeatInterval.Milliseconds()), pp.cfg.MaxConcurrent, blobDir)
	if len(pp.cfg.Resources) > 0 {
		if enc, err := pp.codec.Marshal(pp.cfg.Resources); err == nil {
			envVars = append(envVars, "PEPPER_RESOURCES="+string(enc))
		}
	}

	rtPath := findRuntimePy(matchedCaps)
	if rtPath == "" {
		return fmt.Errorf("runtime.py not found")
	}

	// Bulletproof: Ensure PYTHONPATH includes the directory containing runtime.py and cap.py
	// so the worker doesn't crash on "ModuleNotFoundError: No module named 'cap'"
	if abs, err := filepath.Abs(filepath.Dir(rtPath)); err == nil {
		envVars = append(envVars, "PYTHONPATH="+abs)
	}

	cmd := exec.CommandContext(ctx, findPython(), rtPath)
	cmd.Env = append(os.Environ(), envVars...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	setPdeathsig(cmd)
	if err := cmd.Start(); err != nil {
		return err
	}
	pp.rt.workerStates.Store(workerID, &workerEntry{id: workerID, pid: cmd.Process.Pid, groups: groups, wc: wc, busAddr: busAddr})
	pp.startRespawnLooper(workerID, wc, allSpecs)
	go func() {
		if err := cmd.Wait(); err != nil && ctx.Err() == nil {
			pp.rt.logger.Fields("worker", workerID, "pid", cmd.Process.Pid, "error", err).Warn("worker process exited")
		}
	}()
	return nil
}

// Go worker wiring

// bootGoWorkers starts all Go-native workers registered via pp.Include().
// Each Worker is wrapped in a goruntime.GoWorkerRuntime and subscribes to the bus.
func (pp *Pepper) bootGoWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
	for _, spec := range allSpecs {
		if spec.Runtime != registry.RuntimeGo || spec.GoWorker == nil {
			continue
		}
		// pepper.Worker and goruntime.Worker have identical signatures.
		// Wrap via bridge so no import cycle is needed.
		pw, ok := spec.GoWorker.(Worker)
		if !ok {
			return fmt.Errorf("go worker %q: GoWorker does not implement pepper.Worker", spec.Name)
		}
		bridge := &goworkerBridge{w: pw}
		groups := spec.Groups
		if len(groups) == 0 {
			groups = []string{"default"}
		}
		wid := "go-" + sanitizeName(spec.Name)
		rt := goruntime.New(wid, bridge, groups, pp.rt.nngBus, pp.codec, pp.rt.logger)
		cfg := spec.Config
		if cfg == nil {
			cfg = map[string]any{}
		}
		if err := rt.Start(ctx, cfg); err != nil {
			return fmt.Errorf("go worker %q start: %w", spec.Name, err)
		}
		pp.rt.router.RegisterWorker(envelope.Hello{
			WorkerID: wid,
			Runtime:  "go",
			Groups:   groups,
			Caps:     []string{spec.Name},
		})
		pp.rt.router.MarkCapReady(wid, spec.Name)
		pp.rt.readyWorkers.Add(1)
		pp.rt.workerStates.Store(wid, &workerEntry{id: wid, groups: groups, ready: true, caps: []string{spec.Name}})
		pp.shutdown.RegisterWithContext("goworker."+wid, func(_ context.Context) error { rt.Stop(); return nil })
	}
	return nil
}

// goworkerBridge adapts pepper.Worker to goruntime.Worker.
// Both interfaces have identical method signatures so this is a zero-cost shim.
type goworkerBridge struct{ w Worker }

func (b *goworkerBridge) Setup(cap string, config map[string]any) error {
	return b.w.Setup(cap, config)
}
func (b *goworkerBridge) Run(ctx context.Context, cap string, in map[string]any) (map[string]any, error) {
	return b.w.Run(ctx, cap, in)
}
func (b *goworkerBridge) Capabilities() []goruntime.CapSpec {
	src := b.w.Capabilities()
	out := make([]goruntime.CapSpec, len(src))
	for i, c := range src {
		out[i] = goruntime.CapSpec{
			Name:          c.Name,
			Version:       c.Version,
			Groups:        c.Groups,
			MaxConcurrent: c.MaxConcurrent,
		}
	}
	return out
}

// bootAdapterWorkers starts all HTTP/MCP adapter workers registered via pp.Adapt().
func (pp *Pepper) bootAdapterWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
	for _, spec := range allSpecs {
		if spec.Runtime != registry.RuntimeHTTP || spec.AdapterSpec == nil {
			continue
		}
		groups := spec.Groups
		if len(groups) == 0 {
			groups = []string{"default"}
		}
		wid := "http-" + sanitizeName(spec.Name)

		var a adapter.Adapter
		var auth adapter.AuthProvider
		var baseURL string
		timeout := 120 * time.Second

		switch v := spec.AdapterSpec.(type) {
		case *adapter.HTTPBuilder:
			a = v.GetAdapter()
			auth = v.GetAuth()
			baseURL = v.GetBaseURL()
			if t := v.GetTimeout(); t > 0 {
				timeout = t
			}
		case *adapter.MCPBuilder:
			a = &adapter.MCPAdapter{ServerURL: v.GetServerURL(), ToolName: v.GetTool()}
			baseURL = v.GetServerURL()
		default:
			if direct, ok := spec.AdapterSpec.(adapter.Adapter); ok {
				a = direct
				baseURL = spec.Source
			} else {
				return fmt.Errorf("adapter %q: unrecognised AdapterSpec type %T", spec.Name, spec.AdapterSpec)
			}
		}

		wrt := adapter.NewBusWorker(wid, spec.Name, a, auth, baseURL, timeout, groups, pp.rt.nngBus, pp.codec, pp.rt.logger)
		if err := wrt.Start(ctx); err != nil {
			pp.rt.logger.Fields("worker", wid, "error", err).Warn("adapter worker start warning")
		}
		pp.rt.router.RegisterWorker(envelope.Hello{WorkerID: wid, Runtime: "http", Groups: groups, Caps: []string{spec.Name}})
		pp.rt.router.MarkCapReady(wid, spec.Name)
		pp.rt.readyWorkers.Add(1)
		pp.rt.workerStates.Store(wid, &workerEntry{id: wid, groups: groups, ready: true, caps: []string{spec.Name}})
		pp.shutdown.RegisterWithContext("http."+wid, func(_ context.Context) error { wrt.Stop(); return nil })
	}
	return nil
}

// bootCLIWorkers starts all CLI tool workers registered via pp.Prepare().
func (pp *Pepper) bootCLIWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
	for _, spec := range allSpecs {
		if spec.Runtime != registry.RuntimeCLI || spec.CLISpec == nil {
			continue
		}
		cmdSpec, ok := spec.CLISpec.(*cli.CMDSpec)
		if !ok {
			return fmt.Errorf("cli worker %q: unexpected CLISpec type %T", spec.Name, spec.CLISpec)
		}
		groups := spec.Groups
		if len(groups) == 0 {
			groups = []string{"default"}
		}
		wid := "cli-" + sanitizeName(spec.Name)
		wrt := cli.NewBusWorker(wid, spec.Name, *cmdSpec, groups, pp.rt.nngBus, pp.codec, pp.rt.blob, pp.cfg.DefaultTimeout+30*time.Second, pp.rt.logger)
		if err := wrt.Start(ctx); err != nil {
			return fmt.Errorf("cli worker %q start: %w", spec.Name, err)
		}
		pp.rt.router.RegisterWorker(envelope.Hello{WorkerID: wid, Runtime: "cli", Groups: groups, Caps: []string{spec.Name}})
		pp.rt.router.MarkCapReady(wid, spec.Name)
		pp.rt.readyWorkers.Add(1)
		pp.rt.workerStates.Store(wid, &workerEntry{id: wid, groups: groups, ready: true, caps: []string{spec.Name}})
		pp.shutdown.RegisterWithContext("cli."+wid, func(_ context.Context) error { wrt.Stop(); return nil })
	}
	return nil
}

// Blob public API

// NewBlob creates a zero-copy blob from bytes backed by a shared-memory file.
// The caller must call Close() after the request completes (or let the reaper
// collect it via the blobReaper TTL).
//
//	blob, err := pp.NewBlob(imgBytes)
//	defer blob.Close()
//	result, err := pp.Do(ctx, "face.embed", pepper.In{"image": blob.Ref()})
func (pp *Pepper) NewBlob(data []byte) (*blobpkg.Blob, error) {
	if err := pp.ensureStarted(); err != nil {
		return nil, err
	}
	return pp.rt.blob.Write(data, pp.cfg.DefaultTimeout+30*time.Second)
}

// NewBlobFromFile creates a zero-copy blob by copying a file into the blob dir.
func (pp *Pepper) NewBlobFromFile(path string) (*blobpkg.Blob, error) {
	if err := pp.ensureStarted(); err != nil {
		return nil, err
	}
	return pp.rt.blob.WriteFile(path, pp.cfg.DefaultTimeout+30*time.Second)
}

// Bidirectional streaming

// OpenStream opens a bidirectional stream to a capability (§11.2).
// The caller writes input chunks via stream.Write() and reads output
// via stream.Chunks(ctx). Call stream.CloseInput() when input is done.
//
//	stream, err := pp.OpenStream(ctx, "speech.transcribe", pepper.In{"language": "en"})
//	go func() { for frame := range mic { stream.Write(frame) }; stream.CloseInput() }()
//	for chunk := range stream.Chunks(ctx) { fmt.Print(chunk.AsJSON()["text"]) }
func (pp *Pepper) OpenStream(ctx context.Context, cap string, in In, opts ...CallOption) (*BidiStream, error) {
	if err := pp.ensureStarted(); err != nil {
		return nil, err
	}
	o := defaultCallOpts()
	for _, opt := range opts {
		opt(&o)
	}
	streamID := ulid.Make().String()
	corrID := ulid.Make().String()
	env := buildEnvelope(corrID, corrID, cap, in, o, pp.cfg.DefaultTimeout)
	env.MsgType = envelope.MsgStreamOpen
	env.StreamID = streamID

	payload, err := pp.codec.Marshal(in)
	if err != nil {
		return nil, err
	}
	env.Payload = payload

	outCh, err := pp.pending.RegisterStream(corrID, 64)
	if err != nil {
		return nil, err
	}
	pp.rt.reqReaper.TouchAt(corrID, time.UnixMilli(env.DeadlineMs))

	data, err := pp.codec.Marshal(env)
	if err != nil {
		return nil, err
	}
	if err := pp.rt.nngBus.Publish(bus.TopicPub(env.Group), data); err != nil {
		pp.pending.Fail(corrID, err)
		return nil, err
	}

	return &BidiStream{
		streamID: streamID,
		corrID:   corrID,
		group:    env.Group,
		outCh:    outCh,
		pp:       pp,
		codec:    pp.codec,
	}, nil
}

// BidiStream is a bidirectional streaming handle (§11.2).
type BidiStream struct {
	streamID string
	corrID   string
	group    string
	outCh    <-chan pending.Response
	pp       *Pepper
	codec    interface {
		Marshal(v any) ([]byte, error)
		Unmarshal(data []byte, v any) error
	}
	seqNum uint64
	closed atomic.Bool
}

// Write sends one input chunk to the worker.
func (s *BidiStream) Write(chunk In) error {
	if s.closed.Load() {
		return fmt.Errorf("stream closed")
	}
	s.seqNum++
	env := map[string]any{
		"proto_ver": uint8(1),
		"msg_type":  string(envelope.MsgStreamChunk),
		"stream_id": s.streamID,
		"corr_id":   s.corrID,
		"seq":       s.seqNum,
	}
	payload, err := s.codec.Marshal(chunk)
	if err != nil {
		return err
	}
	env["payload"] = payload
	data, err := s.codec.Marshal(env)
	if err != nil {
		return err
	}
	return s.pp.rt.nngBus.Publish(bus.TopicStream(s.streamID), data)
}

// CloseInput signals that no more input chunks will be sent.
func (s *BidiStream) CloseInput() error {
	if s.closed.Swap(true) {
		return nil
	}
	env := map[string]any{
		"proto_ver": uint8(1),
		"msg_type":  string(envelope.MsgStreamClose),
		"stream_id": s.streamID,
		"corr_id":   s.corrID,
	}
	data, _ := s.codec.Marshal(env)
	return s.pp.rt.nngBus.Publish(bus.TopicStream(s.streamID), data)
}

// Chunks returns a channel of output results from the worker.
// The channel is closed when the stream ends or ctx is cancelled.
func (s *BidiStream) Chunks(ctx context.Context) <-chan Result {
	out := make(chan Result, 64)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case r, ok := <-s.outCh:
				if !ok {
					return
				}
				res := Result{payload: r.Payload, codec: s.pp.codec, WorkerID: r.WorkerID, Err: r.Err}
				select {
				case out <- res:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// Raw snippet execution

// Run executes a raw Python snippet as a capability (§16.3).
// The snippet receives vars as the inputs dict and must return a dict.
//
//	result, err := pp.Run(ctx, `
//	    import numpy as np
//	    return {"mean": float(np.array(data).mean())}
//	`, pepper.Vars{"data": mySlice})
func (pp *Pepper) Run(ctx context.Context, snippet string, vars Vars, opts ...CallOption) (Result, error) {
	if err := pp.ensureStarted(); err != nil {
		return Result{}, err
	}
	// Encode the snippet as a special capability name with inline source.
	// The Python runtime handles "pepper:inline" as an anonymous capability.
	in := In(vars)
	if in == nil {
		in = In{}
	}
	in["_snippet"] = snippet
	return pp.dispatch(ctx, "pepper.inline", in, opts...)
}

// Vars is the input map for pp.Run() raw snippet execution.
type Vars = map[string]any

func buildCapLoad(spec *registry.Spec) map[string]any {
	source := spec.Source
	// Ensure Python receives an absolute path so it can reliably load the file
	// regardless of the current working directory.
	if source != "" && spec.Runtime == registry.RuntimePython {
		if abs, err := filepath.Abs(source); err == nil {
			source = abs
		}
	}
	return map[string]any{
		"proto_ver":      uint8(1),
		"msg_type":       "cap_load",
		"cap":            spec.Name,
		"cap_ver":        spec.Version,
		"source":         source,
		"deps":           spec.Deps,
		"timeout_ms":     spec.Timeout.Milliseconds(),
		"max_concurrent": spec.MaxConcurrent,
		"groups":         spec.Groups,
		"config":         spec.Config,
	}
}

func fileExists(path string) bool { _, err := os.Stat(path); return err == nil }
func groupsOverlap(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if x == y {
				return true
			}
		}
	}
	return false
}
