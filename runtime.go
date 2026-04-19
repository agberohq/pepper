package pepper

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	blobpkg "github.com/agberohq/pepper/internal/blob"
	"github.com/agberohq/pepper/internal/bus"
	"github.com/agberohq/pepper/internal/compose"
	"github.com/agberohq/pepper/internal/core"
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
)

// NewBlob creates a zero-copy blob from bytes backed by a shared-memory file.
// The caller must call Close() after the request completes (or let the reaper
// collect it via the blobReaper TTL).
//
//	blob, err := pp.NewBlob(imgBytes)
//	defer blob.Close()
//	result, err := pp.Do(ctx, "face.embed", pepper.In{"image": blob.Ref()})
func (p *Pepper) NewBlob(data []byte) (*blobpkg.Blob, error) {
	if err := p.ensureStarted(); err != nil {
		return nil, err
	}
	return p.rt.blob.Write(data, p.cfg.DefaultTimeout+30*time.Second)
}

// NewBlobFromFile creates a zero-copy blob by copying a file into the blob dir.
func (p *Pepper) NewBlobFromFile(path string) (*blobpkg.Blob, error) {
	if err := p.ensureStarted(); err != nil {
		return nil, err
	}
	return p.rt.blob.WriteFile(path, p.cfg.DefaultTimeout+30*time.Second)
}

func (p *Pepper) bootRuntime(ctx context.Context) error {
	rt := &runtimeState{}
	p.rt = rt
	bgCtx, bgCancel := context.WithCancel(context.Background())
	rt.bgCtx = bgCtx
	rt.bgCancel = bgCancel

	busURL := p.cfg.TransportURL
	if busURL == "" {
		busURL = fmt.Sprintf("tcp://127.0.0.1:%d", freePort())
	}

	nngBus, err := bus.NewMula(bus.Config{URL: busURL, SendBuf: 256, RecvBuf: 256})
	if err != nil {
		bgCancel()
		return fmt.Errorf("pepper: bus: %w", err)
	}
	rt.bus = nngBus

	rt.reqReaper = jack.NewReaper(0, jack.ReaperWithShards(32), jack.ReaperWithHandler(func(_ context.Context, corrID string) {
		p.pending.Fail(corrID, fmt.Errorf("deadline exceeded: %s", corrID))
	}))
	rt.blobReaper = jack.NewReaper(0, jack.ReaperWithShards(16), jack.ReaperWithHandler(func(_ context.Context, blobID string) {
		rt.blob.Reap(blobID)
	}))

	blobDir := p.cfg.BlobDir
	if blobDir == "" {
		blobDir = blobpkg.PlatformDefaultDir()
	}
	bm, err := blobpkg.NewManager(blobDir, rt.blobReaper)
	if err != nil {
		bgCancel()
		return fmt.Errorf("pepper: blob manager: %w", err)
	}
	rt.blob = bm

	rt.router = router.New(nngBus, p.pending, rt.reqReaper, router.Config{
		MaxRetries: p.cfg.MaxRetries, PoisonThreshold: p.cfg.PoisonPillThreshold,
		PoisonPillTTL: p.cfg.PoisonPillTTL, DLQ: p.cfg.DLQ, Codec: p.codec, HeartbeatTimeout: p.cfg.HeartbeatInterval * 3,
	}, p.logger)
	rt.doctor = jack.NewDoctor(jack.DoctorWithMaxConcurrent(len(p.cfg.Workers)+4), jack.DoctorWithGlobalTimeout(p.cfg.HeartbeatInterval*3))

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
	go p.responseLoop(bgCtx, resCh)
	go p.controlLoop(bgCtx, ctrlCh)
	go p.controlLoop(bgCtx, ctrlExactCh)
	go p.controlLoop(bgCtx, hbCh)

	allSpecs := p.reg.All()
	if len(allSpecs) > 0 {
		// Boot Go-native workers (run as goroutines inside this process).
		if err := p.bootGoWorkers(bgCtx, allSpecs); err != nil {
			bgCancel()
			return fmt.Errorf("pepper: go workers: %w", err)
		}
		// Boot HTTP/MCP adapter workers.
		if err := p.bootAdapterWorkers(bgCtx, allSpecs); err != nil {
			bgCancel()
			return fmt.Errorf("pepper: adapter workers: %w", err)
		}
		// Boot CLI tool workers.
		if err := p.bootCLIWorkers(bgCtx, allSpecs); err != nil {
			bgCancel()
			return fmt.Errorf("pepper: cli workers: %w", err)
		}
		// Boot Python subprocess workers.
		for i, wc := range p.cfg.Workers {
			wID := wc.ID
			if wID == "" {
				wID = fmt.Sprintf("w-%d", i+1)
			}
			if err := p.spawnPythonWorker(bgCtx, wc, wID, nngBus.Addr(), allSpecs); err != nil {
				bgCancel()
				return err
			}
		}
		// Wait for at least one worker to become ready.
		// Honour the caller's ctx deadline so test contexts (--timeout 30s)
		// are not outlasted by the DefaultTimeout (also 30 s) wait loop.
		waitDeadline := time.Now().Add(p.cfg.DefaultTimeout)
		if dl, ok := ctx.Deadline(); ok && dl.Before(waitDeadline) {
			waitDeadline = dl
		}
		for time.Now().Before(waitDeadline) {
			if rt.readyWorkers.Load() > 0 {
				break
			}
			select {
			case <-ctx.Done():
				goto doneWaiting
			case <-time.After(30 * time.Millisecond):
			}
		}
	doneWaiting:
		if rt.readyWorkers.Load() == 0 && ctx.Err() != nil {
			bgCancel()
			return fmt.Errorf("pepper: boot: %w", ctx.Err())
		}
	}

	p.shutdown.RegisterWithContext("doctor.stop", func(_ context.Context) error { rt.doctor.StopAll(p.cfg.ShutdownTimeout); return nil })
	p.shutdown.RegisterWithContext("loopers.stop", func(_ context.Context) error {
		rt.loopers.Range(func(_, v any) bool { v.(*jack.Looper).Stop(); return true })
		return nil
	})
	p.shutdown.RegisterWithContext("workers.bye", func(_ context.Context) error {
		bye, _ := p.codec.Marshal(map[string]any{"proto_ver": uint8(1), "msg_type": "worker_bye"})
		_ = rt.bus.Publish(bus.TopicBroadcast, bye)
		time.Sleep(200 * time.Millisecond)
		return nil
	})
	p.shutdown.RegisterWithContext("req.reaper.stop", func(_ context.Context) error { rt.reqReaper.Stop(); return nil })
	p.shutdown.RegisterWithContext("blob.reaper.stop", func(_ context.Context) error { rt.blobReaper.Stop(); return nil })
	p.shutdown.RegisterWithContext("bg.cancel", func(_ context.Context) error { bgCancel(); return nil })
	p.shutdown.Register(nngBus)
	return nil
}

func (p *Pepper) responseLoop(ctx context.Context, ch <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			p.routeResponse(msg.Data)
		}
	}
}

func (p *Pepper) routeResponse(data []byte) {
	var raw map[string]any
	if err := p.codec.Unmarshal(data, &raw); err != nil {
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
			_ = p.sessions.Merge(sessionID, updates)
		}
	}

	deadline := env.DeadlineMs()
	if deadline > 0 {
		for _, bid := range env.Blobs() {
			p.rt.blobReaper.TouchAt(bid, time.UnixMilli(deadline))
		}
		if p.cfg.Metrics != nil && len(env.Blobs()) > 0 {
			p.cfg.Metrics.Counter(metrics.MetricBlobsCreated, int64(len(env.Blobs())), nil)
		}
	}

	switch env.MsgType() {
	case "res":
		p.rt.router.ResolveInflight(corrID, env.OriginID(), env.WorkerID())
		p.rt.reqReaper.Remove(corrID)
		p.pending.Resolve(corrID, pending.Response{Payload: env.Payload(), WorkerID: env.WorkerID(), Cap: env.Cap(), CapVer: env.CapVer(), Hop: env.Hop(), Meta: env.Meta()})
	case "err":
		p.rt.router.ResolveInflight(corrID, env.OriginID(), env.WorkerID())
		p.rt.reqReaper.Remove(corrID)
		p.pending.Fail(corrID, &WorkerError{Code: env.Code(), Message: env.Message()})
	case "res_chunk":
		p.pending.Chunk(corrID, pending.Response{Payload: env.Payload(), WorkerID: env.WorkerID()})
	case "res_end":
		p.rt.reqReaper.Remove(corrID)
		p.pending.EndStream(corrID)
	case "cb_req":
		// A Python worker called pepper.call(). Execute the nested capability
		// and send a cb_res back to the originating worker on its res topic.
		go func() {
			cbStart := time.Now()
			cbID, _ := raw["cb_id"].(string)
			capName, _ := raw["cap"].(string)

			if p.cfg.Metrics != nil {
				p.cfg.Metrics.Counter(metrics.MetricCallbacksTotal, 1, map[string]string{"cap": capName})
			}

			var cbIn core.In
			if payload := env.Payload(); len(payload) > 0 {
				_ = p.codec.Unmarshal(payload, &cbIn)
			}
			if cbIn == nil {
				cbIn = core.In{}
			}

			res, err := p.Do(context.Background(), capName, cbIn)

			if p.cfg.Metrics != nil {
				p.cfg.Metrics.Histogram(metrics.MetricCallbacksLatencyMs, float64(time.Since(cbStart).Milliseconds()), map[string]string{"cap": capName})
				if err != nil {
					p.cfg.Metrics.Counter(metrics.MetricCallbacksErrors, 1, map[string]string{"cap": capName})
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
			resData, _ := p.codec.Marshal(cbRes)
			_ = p.rt.bus.Publish(bus.TopicRes(env.OriginID()), resData)
		}()
	}
}

func (p *Pepper) controlLoop(ctx context.Context, ch <-chan bus.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			p.handleControl(msg.Data)
		}
	}
}

func (p *Pepper) handleControl(data []byte) {
	var raw map[string]any
	if err := p.codec.Unmarshal(data, &raw); err != nil {
		return
	}
	env := envelope.Data(raw)
	switch env.MsgType() {
	case "worker_hello":
		wid := env.WorkerID()
		if wid == "" {
			return
		}
		if cn, _ := env["codec"].(string); cn != "" && cn != string(p.cfg.Serializer) {
			p.cfg.logger.Fields("worker", wid, "codec", cn).Error("CODEC_MISMATCH")
			return
		}
		p.rt.workerStates.Store(wid, &workerEntry{id: wid, groups: env.Groups()})
		p.rt.router.RegisterWorker(envelope.Hello{WorkerID: wid, Groups: env.Groups()})
		p.registerWorkerPatient(wid)

		if p.cfg.Metrics != nil {
			p.cfg.Metrics.Gauge(metrics.MetricWorkersCount, 1, map[string]string{"worker": wid})
		}

		// Send cap_load to the newly connected python worker
		runtimeType, _ := raw["runtime"].(string)
		if runtimeType == "python" || runtimeType == "" {
			for _, spec := range p.reg.All() {
				if spec.Runtime == registry.RuntimePython && (len(env.Groups()) == 0 || len(spec.Groups) == 0 || groupsOverlap(spec.Groups, env.Groups())) {
					payload, _ := p.codec.Marshal(buildCapLoad(spec))
					_ = p.rt.bus.Publish(bus.TopicControl(wid), payload)
				}
			}
		}

	case "cap_ready":
		// ALWAYS unblock pp.Start() even if the capability failed to load
		p.rt.readyWorkers.Add(1)

		if errStr, _ := env["error"].(string); errStr == "" {
			p.rt.router.MarkCapReady(env.WorkerID(), env.Cap())
			if v, ok := p.rt.workerStates.Load(env.WorkerID()); ok {
				e := v.(*workerEntry)
				e.caps = append(e.caps, env.Cap())
				e.ready = true
			}
		} else {
			p.logger.Fields("worker", env.WorkerID(), "cap", env.Cap(), "error", errStr).Error("capability failed to load")
		}

	case "hb_ping":
		wid := env.WorkerID()
		p.rt.router.UpdateHeartbeat(envelope.HbPing{
			WorkerID:       wid,
			Load:           env.Hop(),
			RequestsServed: uint64(env.Int64("requests_served")),
			UptimeMs:       env.Int64("uptime_ms"),
		})
		if p.cfg.Metrics != nil {
			p.cfg.Metrics.Gauge(metrics.MetricWorkersLoad, float64(env.Hop()), map[string]string{"worker": wid})
			p.cfg.Metrics.Gauge(metrics.MetricWorkersServed, float64(env.Int64("requests_served")), map[string]string{"worker": wid})
		}
		if v, ok := p.rt.workerStates.Load(wid); ok {
			p.checkRecycle(wid, v.(*workerEntry).wc, uint64(env.Int64("requests_served")), env.Int64("uptime_ms"))
		}
	}
}

func (p *Pepper) dispatchPipeline(ctx context.Context, spec *registry.Spec, name string, in core.In, o callOpts) (Result, error) {
	dag, ok := spec.Pipeline.(*compose.DAG)
	if !ok {
		return Result{}, fmt.Errorf("pepper: invalid pipeline spec for %q", name)
	}

	originID := ulid.Make().String()
	stageCh, err := p.rt.bus.SubscribePrefix(ctx, "pepper.pipe."+sanitizeName(name))
	if err != nil {
		return Result{}, err
	}

	// Initial stage
	env := buildEnvelope(ulid.Make().String(), originID, "", in, o, p.cfg.DefaultTimeout)
	if err := dag.DispatchEnvelope(&env, 0); err != nil {
		return Result{}, err
	}

	payload, _ := p.codec.Marshal(in)
	env.Payload = payload
	env.MsgType = envelope.MsgReq
	env.Dispatch = envelope.DispatchAny

	if err := p.rt.router.Dispatch(ctx, env); err != nil {
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
			if err := p.codec.Unmarshal(msg.Data, &raw); err != nil {
				continue
			}
			res := envelope.Data(raw)

			// Register blobs
			for _, bid := range res.Blobs() {
				p.rt.blobReaper.TouchAt(bid, time.UnixMilli(env.DeadlineMs))
			}

			if res.MsgType() == "err" {
				return Result{}, &WorkerError{Code: res.Code(), Message: res.Message()}
			}

			if payload := res.Payload(); len(payload) > 0 {
				finalResult.payload = payload
				finalResult.codec = p.codec
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

func (p *Pepper) registerWorkerPatient(workerID string) {
	hbTimeout := p.cfg.HeartbeatInterval * 3
	patient := jack.NewPatient(jack.PatientConfig{
		ID:          workerID,
		Interval:    p.cfg.HeartbeatInterval,
		Accelerated: p.cfg.HeartbeatInterval / 2,
		MaxFailures: 2,
		Timeout:     hbTimeout,
		Check: func(ctx context.Context) error {
			if p.rt.router.WorkerAlive(workerID) {
				return nil
			}
			return fmt.Errorf("no heartbeat from %s", workerID)
		},
		OnStateChange: func(e jack.PatientEvent) {
			if e.State == jack.PatientFailed {
				p.rt.router.MarkWorkerDead(workerID)
				if p.cfg.Metrics != nil {
					tags := map[string]string{"worker": workerID}
					p.cfg.Metrics.Counter(metrics.MetricWorkersCrashes, 1, tags)
					p.cfg.Metrics.Gauge(metrics.MetricWorkersCount, -1, tags)
				}
				p.triggerRespawn(workerID)
			}
		},
	})
	_ = p.rt.doctor.Add(patient)
}

func (p *Pepper) triggerRespawn(workerID string) {
	if p.cfg.Metrics != nil {
		p.cfg.Metrics.Counter(metrics.MetricWorkersRespawns, 1, map[string]string{"worker": workerID})
	}
	if v, ok := p.rt.loopers.Load(workerID); ok {
		v.(*jack.Looper).ResetInterval()
	}
}

func (p *Pepper) startRespawnLooper(workerID string, wc WorkerConfig, allSpecs []*registry.Spec) {
	looper := jack.NewLooper(
		func() error {
			if p.rt.bgCtx.Err() != nil {
				return nil // shutting down — do not respawn
			}
			return p.spawnPythonWorker(p.rt.bgCtx, wc, workerID, p.rt.bus.Addr(), allSpecs)
		},
		jack.WithLooperName("respawn:"+workerID),
		jack.WithLooperInterval(24*time.Hour), // never auto-fires; triggerRespawn calls ResetInterval()
		jack.WithLooperBackoff(false),
		jack.WithLooperMaxInterval(24*time.Hour),
		jack.WithLooperImmediate(false),
	)
	p.rt.loopers.Store(workerID, looper)
	looper.Start()
}

func (p *Pepper) checkRecycle(workerID string, wc WorkerConfig, served uint64, uptimeMs int64) {
	if (wc.MaxRequests > 0 && served >= wc.MaxRequests) || (wc.MaxUptime > 0 && time.Duration(uptimeMs)*time.Millisecond >= wc.MaxUptime) {
		bye, _ := p.codec.Marshal(map[string]any{"proto_ver": uint8(1), "msg_type": "worker_bye", "worker_id": workerID})
		_ = p.rt.bus.Publish(bus.TopicBroadcast, bye)
	}
}

func (p *Pepper) spawnPythonWorker(ctx context.Context, wc WorkerConfig, workerID, busAddr string, allSpecs []*registry.Spec) error {
	var matchedCaps []*registry.Spec
	for _, spec := range allSpecs {
		if spec.Runtime == registry.RuntimePython && (len(wc.Groups) == 0 || len(spec.Groups) == 0 || groupsOverlap(spec.Groups, wc.Groups)) {
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
	blobDir := p.cfg.BlobDir
	if blobDir == "" {
		blobDir = blobpkg.PlatformDefaultDir()
	}

	envVars := bus.WorkerEnvVars(workerID, busAddr, string(p.cfg.Serializer), groups, int(p.cfg.HeartbeatInterval.Milliseconds()), p.cfg.MaxConcurrent, blobDir)
	if len(p.cfg.Resources) > 0 {
		if enc, err := p.codec.Marshal(p.cfg.Resources); err == nil {
			envVars = append(envVars, "PEPPER_RESOURCES="+string(enc))
		}
	}

	rtPath := defaultFinder.Runtime(matchedCaps)
	if rtPath == "" {
		return fmt.Errorf("runtime.py not found")
	}

	// Bulletproof: Ensure PYTHONPATH includes the directory containing runtime.py and cap.py
	// so the worker doesn't crash on "ModuleNotFoundError: No module named 'cap'"
	if abs, err := filepath.Abs(filepath.Dir(rtPath)); err == nil {
		envVars = append(envVars, "PYTHONPATH="+abs)
	}

	cmd := exec.CommandContext(ctx, defaultFinder.Python(), rtPath)
	cmd.Env = append(os.Environ(), envVars...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr // raw process errors only — structured logs go to fd 3
	setPdeathsig(cmd)

	// Open a pipe for Python structured log output (fd 3 in the child).
	// Python writes one JSON line per record; Go re-emits via rt.logger so all
	// logs share one backend (Victoria Logs, stdout, etc.).
	logR, logW, pipeErr := os.Pipe()
	if pipeErr == nil {
		cmd.ExtraFiles = []*os.File{logW}
	}

	if err := cmd.Start(); err != nil {
		if pipeErr == nil {
			logR.Close()
			logW.Close()
		}
		return err
	}
	if pipeErr == nil {
		logW.Close() // parent closes write end; child holds the only reference
		go p.pipeWorkerLogs(logR, workerID)
	}
	p.rt.workerStates.Store(workerID, &workerEntry{id: workerID, pid: cmd.Process.Pid, groups: groups, wc: wc, busAddr: busAddr})
	p.startRespawnLooper(workerID, wc, allSpecs)
	go func() {
		if err := cmd.Wait(); err != nil && ctx.Err() == nil {
			p.logger.Fields("worker", workerID, "pid", cmd.Process.Pid, "error", err).Warn("worker process exited")
		}
	}()
	return nil
}

// bootGoWorkers starts all Go-native workers registered via pp.Include().
// Each Worker is wrapped in a goruntime.GoWorkerRuntime and subscribes to the bus.
func (p *Pepper) bootGoWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
	for _, spec := range allSpecs {
		if spec.Runtime != registry.RuntimeGo || spec.GoWorker == nil {
			continue
		}
		// pepper.Worker and goruntime.Worker have identical signatures.
		// Wrap via bridge so no import cycle is needed.
		pw, ok := spec.GoWorker.(core.Worker)
		if !ok {
			return fmt.Errorf("go worker %q: GoWorker does not implement pepper.Worker", spec.Name)
		}
		bridge := goruntime.NewBridge(pw)
		groups := spec.Groups
		if len(groups) == 0 {
			groups = []string{"default"}
		}
		wid := "go-" + sanitizeName(spec.Name)
		rt := goruntime.New(wid, bridge, groups, p.rt.bus, p.codec, p.logger)
		cfg := spec.Config
		if cfg == nil {
			cfg = map[string]any{}
		}
		if err := rt.Start(ctx, cfg); err != nil {
			return fmt.Errorf("go worker %q start: %w", spec.Name, err)
		}
		p.rt.router.RegisterWorker(envelope.Hello{
			WorkerID: wid,
			Runtime:  "go",
			Groups:   groups,
			Caps:     []string{spec.Name},
		})
		p.rt.router.MarkCapReady(wid, spec.Name)
		p.rt.readyWorkers.Add(1)
		p.rt.workerStates.Store(wid, &workerEntry{id: wid, groups: groups, ready: true, caps: []string{spec.Name}})
		p.shutdown.RegisterWithContext("goworker."+wid, func(_ context.Context) error { rt.Stop(); return nil })
	}
	return nil
}

// bootAdapterWorkers starts all HTTP/MCP adapter workers registered via pp.Adapt().
func (p *Pepper) bootAdapterWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
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

		wrt := adapter.NewBusWorker(wid, spec.Name, a, auth, baseURL, timeout, groups, p.rt.bus, p.codec, p.logger)
		if err := wrt.Start(ctx); err != nil {
			p.logger.Fields("worker", wid, "error", err).Warn("adapter worker start warning")
		}
		p.rt.router.RegisterWorker(envelope.Hello{WorkerID: wid, Runtime: "http", Groups: groups, Caps: []string{spec.Name}})
		p.rt.router.MarkCapReady(wid, spec.Name)
		p.rt.readyWorkers.Add(1)
		p.rt.workerStates.Store(wid, &workerEntry{id: wid, groups: groups, ready: true, caps: []string{spec.Name}})
		p.shutdown.RegisterWithContext("http."+wid, func(_ context.Context) error { wrt.Stop(); return nil })
	}
	return nil
}

// bootCLIWorkers starts all CLI tool workers registered via pp.Prepare().
func (p *Pepper) bootCLIWorkers(ctx context.Context, allSpecs []*registry.Spec) error {
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
		wrt := cli.NewBusWorker(wid, spec.Name, *cmdSpec, groups, p.rt.bus, p.codec, p.rt.blob, p.cfg.DefaultTimeout+30*time.Second, p.logger)
		if err := wrt.Start(ctx); err != nil {
			return fmt.Errorf("cli worker %q start: %w", spec.Name, err)
		}
		p.rt.router.RegisterWorker(envelope.Hello{WorkerID: wid, Runtime: "cli", Groups: groups, Caps: []string{spec.Name}})
		p.rt.router.MarkCapReady(wid, spec.Name)
		p.rt.readyWorkers.Add(1)
		p.rt.workerStates.Store(wid, &workerEntry{id: wid, groups: groups, ready: true, caps: []string{spec.Name}})
		p.shutdown.RegisterWithContext("cli."+wid, func(_ context.Context) error { wrt.Stop(); return nil })
	}
	return nil
}

// OpenStream opens a bidirectional stream to a capability (§11.2).
// The caller writes input chunks via stream.Write() and reads output
// via stream.Chunks(ctx). Call stream.CloseInput() when input is done.
//
//	stream, err := pp.OpenStream(ctx, "speech.transcribe", pepper.In{"language": "en"})
//	go func() { for frame := range mic { stream.Write(frame) }; stream.CloseInput() }()
//	for chunk := range stream.Chunks(ctx) { fmt.Print(chunk.AsJSON()["text"]) }
func (p *Pepper) OpenStream(ctx context.Context, cap string, in core.In, opts ...CallOption) (*BidiStream, error) {
	if err := p.ensureStarted(); err != nil {
		return nil, err
	}
	o := defaultCallOpts()
	for _, opt := range opts {
		opt(&o)
	}
	streamID := ulid.Make().String()
	corrID := ulid.Make().String()
	env := buildEnvelope(corrID, corrID, cap, in, o, p.cfg.DefaultTimeout)
	env.MsgType = envelope.MsgStreamOpen
	env.StreamID = streamID

	payload, err := p.codec.Marshal(in)
	if err != nil {
		return nil, err
	}
	env.Payload = payload

	outCh, err := p.pending.RegisterStream(corrID, 64)
	if err != nil {
		return nil, err
	}
	p.rt.reqReaper.TouchAt(corrID, time.UnixMilli(env.DeadlineMs))

	data, err := p.codec.Marshal(env)
	if err != nil {
		return nil, err
	}
	if err := p.rt.bus.Publish(bus.TopicPub(env.Group), data); err != nil {
		p.pending.Fail(corrID, err)
		return nil, err
	}

	return &BidiStream{
		streamID: streamID,
		corrID:   corrID,
		group:    env.Group,
		outCh:    outCh,
		pp:       p,
		codec:    p.codec,
	}, nil
}

// Run executes a raw Python snippet as a capability (§16.3).
// The snippet receives vars as the inputs dict and must return a dict.
//
//	result, err := pp.Run(ctx, `
//	    import numpy as np
//	    return {"mean": float(np.array(data).mean())}
//	`, pepper.Vars{"data": mySlice})
func (p *Pepper) Run(ctx context.Context, snippet string, vars Vars, opts ...CallOption) (Result, error) {
	if err := p.ensureStarted(); err != nil {
		return Result{}, err
	}
	// Encode the snippet as a special capability name with inline source.
	// The Python runtime handles "pepper:inline" as an anonymous capability.
	in := core.In(vars)
	if in == nil {
		in = core.In{}
	}
	in["_snippet"] = snippet
	return p.dispatch(ctx, "pepper.inline", in, opts...)
}

// pipeWorkerLogs reads structured JSON log lines from Python fd 3 and
// re-emits them through the Go logger so all worker logs share one backend.
func (p *Pepper) pipeWorkerLogs(r *os.File, workerID string) {
	defer r.Close()
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		var entry map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			p.logger.Namespace("python").Fields("worker", workerID).Debug(string(scanner.Bytes()))
			continue
		}
		msg, _ := entry["msg"].(string)
		level, _ := entry["level"].(string)
		// ll.Fields expects alternating key,value pairs — build flat slice
		kv := []any{"worker", workerID}
		for k, v := range entry {
			if k != "msg" && k != "level" && k != "logger" && k != "ts" {
				kv = append(kv, k, v)
			}
		}
		l := p.logger.Namespace("python").Fields(kv...)
		switch level {
		case "DEBUG":
			l.Debug(msg)
		case "WARNING", "WARN":
			l.Warn(msg)
		case "ERROR", "CRITICAL":
			l.Error(msg)
		default:
			l.Info(msg)
		}
	}
}
