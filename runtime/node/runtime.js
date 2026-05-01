#!/usr/bin/env node

//
// Launched by Go as: node runtime.js
//
// Environment variables (same as Python/Julia workers):
//   PEPPER_WORKER_ID, PEPPER_BUS_URL, PEPPER_CODEC, PEPPER_GROUPS,
//   PEPPER_HEARTBEAT_MS, PEPPER_MAX_CONCURRENT, PEPPER_BLOB_DIR,
//   PEPPER_NODE_RUNTIME_DIR
"use strict";

const path    = require("path");
const fs      = require("fs");
const { AsyncLocalStorage } = require("async_hooks");

// Locate our own runtime directory so cap.js / transport.js resolve correctly.
const RUNTIME_DIR = process.env.PEPPER_NODE_RUNTIME_DIR || __dirname;

const { parseFormA, readBlob, initResources, closeResources } = require(path.join(RUNTIME_DIR, "cap.js"));
const { BusTransport } = require(path.join(RUNTIME_DIR, "transport.js"));

// Structured logging (fd 3 → Go log pipe, fallback to stderr)

let _logFd;
try {
  _logFd = fs.createWriteStream(null, { fd: 3 });
} catch (_) {
  _logFd = process.stderr;
}

function _log(level, msg, extra = {}) {
  const entry = { ts: Date.now() / 1000, level, logger: "pepper.runtime", msg, ...extra };
  try { _logFd.write(JSON.stringify(entry) + "\n"); } catch (_) {}
}
const log = {
  info:  (msg, ex) => _log("INFO",  msg, ex),
  warn:  (msg, ex) => _log("WARN",  msg, ex),
  error: (msg, ex) => _log("ERROR", msg, ex),
  debug: (msg, ex) => _log("DEBUG", msg, ex),
};

// Codec

class Codec {
  constructor(name) { this.name = name; }

  marshal(v) {
    if (this.name === "msgpack") {
      try {
        const { encode } = require("@msgpack/msgpack");
        return Buffer.from(encode(v));
      } catch (_) {}
    }
    return Buffer.from(JSON.stringify(v), "utf8");
  }

  unmarshal(data) {
    if (this.name === "msgpack") {
      try {
        const { decode } = require("@msgpack/msgpack");
        return decode(data);
      } catch (_) {}
    }
    return JSON.parse(data.toString("utf8"));
  }
}

// AsyncLocalStorage — per-request context (equivalent of Python contextvars)

const _reqCtx = new AsyncLocalStorage();

function getCtx(key, def) {
  const store = _reqCtx.getStore();
  return store ? (store[key] ?? def) : def;
}

function corrId()       { return getCtx("corrId", ""); }
function originId()     { return getCtx("originId", ""); }
function metaCtx()      { return { ...(getCtx("meta", {})) }; }
function configCtx()    { return { ...(getCtx("config", {})) }; }
function deliveryCount(){ return getCtx("delivery", 0); }
function sessionData()  { return { ...(getCtx("session", {})) }; }

// Cancel tracking

const _cancelledIds = new Set();

function markCancelled(originId) {
  _cancelledIds.add(originId);
  setTimeout(() => _cancelledIds.delete(originId), 300_000);
}

function isCancelled() {
  const oid = getCtx("originId", "");
  return oid ? _cancelledIds.has(oid) : false;
}

// KV store  (process-local, keyed by worker+namespace)

const _kvStores = new Map();

class KVStore {
  constructor(namespace, workerId) {
    this._ns  = namespace;
    this._wid = workerId;
    this._data = new Map();
  }

  _key(k) { return this._wid ? `${this._wid}:${this._ns}:${k}` : `${this._ns}:${k}`; }

  get(key, def = undefined)   { return this._data.has(this._key(key)) ? this._data.get(this._key(key)) : def; }
  set(key, val)               { this._data.set(this._key(key), val); }
  delete(key)                 { this._data.delete(this._key(key)); }
  getOrSet(key, factory) {
    const fk = this._key(key);
    if (!this._data.has(fk)) this._data.set(fk, factory());
    return this._data.get(fk);
  }
}

function getKV(namespace, workerId) {
  const storeKey = workerId ? `${workerId}:${namespace}` : namespace;
  if (!_kvStores.has(storeKey)) _kvStores.set(storeKey, new KVStore(namespace, workerId));
  return _kvStores.get(storeKey);
}

// Session proxy

class SessionProxy {
  constructor(data, store) { this._data = data; this._store = store; }
  get(key, def = undefined) { return this._data[key] ?? def; }
  set(key, val) {
    const meta = this._store.getCtxMeta();
    if (!meta._session_updates) meta._session_updates = {};
    meta._session_updates[key] = val;
    this._data[key] = val;
  }
}

// Pepper public API (injected into capability modules as global `pepper`)

function makePepperAPI(worker) {
  return {
    corrId,
    originId,
    meta:          metaCtx,
    config:        configCtx,
    deliveryCount,
    isCancelled,
    session:       () => new SessionProxy(sessionData(), worker),
    kv:            (ns = "default") => getKV(ns, worker.workerId),
    cacheDir: () => {
      const d = process.env.PEPPER_CACHE_DIR || process.env.HF_HOME ||
        path.join(require("os").homedir(), ".cache", "pepper");
      fs.mkdirSync(d, { recursive: true });
      return d;
    },
    readBlob,
    newBlob: (data, opts = {}) => {
      const blobDir = process.env.PEPPER_BLOB_DIR || "/dev/shm";
      const id   = `blob_${Math.random().toString(36).slice(2)}`;
      const blobPath = path.join(blobDir, `pepper_blob_${id}.dat`);
      fs.writeFileSync(blobPath, data);
      return { _pepper_blob: true, id, path: blobPath, size: data.length,
               dtype: opts.dtype || "", shape: opts.shape || [], format: opts.format || "" };
    },
  };
}

// Global pepper — set once at worker startup so top-level cap code can use it.
let _pepperAPI = null;

// Capability loader

async function loadCapability(capLoad, pepperAPI) {
  const sourcePath = capLoad.source || "";
  if (sourcePath && !fs.existsSync(sourcePath)) {
    throw new Error(`capability source not found: ${sourcePath}`);
  }

  const sourceText = sourcePath ? fs.readFileSync(sourcePath, "utf8") : "";
  const formA = parseFormA(sourceText);

  // Load the module into a fresh require cache entry.
  // Node does not have built-in module isolation, but we can bust the cache.
  if (sourcePath && require.cache[require.resolve(sourcePath)]) {
    delete require.cache[require.resolve(sourcePath)];
  }

  // Inject pepper API as a global before loading the module.
  // Modules access it via the `pepper` global rather than require().
  global.pepper = pepperAPI;

  let capModule = {};
  if (sourcePath) {
    capModule = require(sourcePath);
  }

  const maxConcurrent = capLoad.max_concurrent || 4;
  let _concurrent = 0;

  // Call setup / setup! if present.
  const cfg = capLoad.config || {};
  if (typeof capModule.setup === "function") {
    await capModule.setup(cfg);
  }

  return {
    spec: { ...formA, ...capLoad },
    run:  capModule.run || capModule.default,
    maxConcurrent,
    _concurrent,
  };
}

// Wire helpers

function encodeMsg(topic, data) {
  const tb = Buffer.from(topic, "utf8");
  const header = Buffer.alloc(2);
  header.writeUInt16BE(tb.length, 0);
  return Buffer.concat([header, tb, data]);
}

function replyTopic(env) {
  const t = env.msg_type || "";
  if (["res", "err", "res_chunk", "res_end"].includes(t)) return `pepper.res.${env.origin_id || ""}`;
  if (t === "hb_ping")  return `pepper.hb.${env.worker_id || ""}`;
  if (t === "pipe")     return env.topic || "pepper.pipe.unknown";
  return "pepper.control";
}

const RETRYABLE = new Set([
  "DEADLINE_EXCEEDED", "WORKER_OOM", "NO_WORKERS",
  "WORKER_SHUTTING_DOWN", "DISPATCH_TIMEOUT", "MODEL_OOM", "MODEL_TIMEOUT",
]);

// Worker

class Worker {
  constructor() {
    this.workerId      = process.env.PEPPER_WORKER_ID || "w-node-1";
    this.busUrl        = process.env.PEPPER_BUS_URL   || "127.0.0.1:7731";
    this.groups        = (process.env.PEPPER_GROUPS   || "default").split(",");
    this.heartbeatMs   = parseInt(process.env.PEPPER_HEARTBEAT_MS   || "5000");
    this.maxConcurrent = parseInt(process.env.PEPPER_MAX_CONCURRENT || "8");
    this.codec         = new Codec(process.env.PEPPER_CODEC || "msgpack");
    this.caps          = new Map();
    this.requestsServed= 0;
    this.startedAt     = Date.now();
    this.running       = true;
    this.transport     = null;
    this._sendLock     = Promise.resolve();
    this._msgsReceived = 0;
  }

  // Internal helper accessed by SessionProxy
  getCtxMeta() { return _reqCtx.getStore()?.meta ?? {}; }

  async run() {
    log.info(`pepper worker ${this.workerId} starting, bus=${this.busUrl}`);

    this.transport = new BusTransport(this.busUrl);
    await this.transport.connect();
    log.info("bus connected", { scheme: this.transport.scheme, addr: `${this.transport.host}:${this.transport.port}` });

    const topics = ["pepper.control", "pepper.broadcast", `pepper.control.${this.workerId}`,
                    `pepper.push.${this.workerId}`, ...this.groups.flatMap(g => [`pepper.push.${g}`, `pepper.pub.${g}`])];
    await this.transport.subscribe(topics);

    await new Promise(r => setTimeout(r, 100)); // let subscriptions settle

    this._sendEnvelope(this._makeHello());

    this._heartbeatTimer = setInterval(() => this._heartbeat(), this.heartbeatMs);

    await this._messageLoop();
  }

  async _messageLoop() {
    while (this.running) {
      const data = await this.transport.recvFrame();
      if (!data) break;
      if (data.length < 2) continue;

      const topicLen = data.readUInt16BE(0);
      const payload  = data.slice(2 + topicLen);
      let env;
      try {
        env = payload.length ? this.codec.unmarshal(payload) : {};
      } catch (e) {
        log.warn("decode error: " + e.message);
        continue;
      }

      const t = env.msg_type || "";

      if (t === "cap_load") {
        await this._handleCapLoad(env);
      } else if (t === "req") {
        this._msgsReceived++;
        log.info(`req received #${this._msgsReceived}`, { cap: env.cap, corr_id: env.corr_id, worker: this.workerId, hop: env.hop ?? 0 });
        this._handleRequest(env).catch(e => log.error("request handler crashed: " + e.message));
      } else if (t === "worker_bye") {
        this.running = false;
        break;
      } else if (t === "cancel") {
        if (env.origin_id) markCancelled(env.origin_id);
      }
    }

    clearInterval(this._heartbeatTimer);
    for (const lc of this.caps.values()) {
      if (typeof lc.teardown === "function") { try { await lc.teardown(); } catch (_) {} }
    }
    await closeResources();
    this.transport.close();
  }

  async _handleCapLoad(env) {
    const capName = env.cap || "";
    let errorMsg  = "";
    const t0 = Date.now();
    log.info("cap_load start", { cap: capName, worker: this.workerId });
    try {
      this.caps.set(capName, await loadCapability(env, makePepperAPI(this)));
    } catch (e) {
      errorMsg = e.message;
      log.error("cap_load failed", { cap: capName, worker: this.workerId, error: errorMsg });
    }
    const setupMs = Date.now() - t0;
    if (!errorMsg) log.info("cap_load done", { cap: capName, worker: this.workerId, setup_ms: setupMs });
    this._sendEnvelope({ proto_ver: 1, msg_type: "cap_ready", worker_id: this.workerId,
                         cap: capName, cap_ver: env.cap_ver || "", setup_ms: setupMs, error: errorMsg });
  }

  async _handleRequest(env) {
    const capName  = env.cap        || "";
    const corr     = env.corr_id    || "";
    const origin   = env.origin_id  || "";

    const lc = this.caps.get(capName);
    if (!lc) {
      log.error("cap not found", { cap: capName, corr_id: corr, worker: this.workerId });
      return this._sendEnvelope(this._err(env, "CAP_NOT_FOUND", `capability '${capName}' not loaded`));
    }

    if (_cancelledIds.has(origin)) {
      return this._sendEnvelope(this._err(env, "CANCELLED", "request cancelled"));
    }

    const meta    = { ...(env.meta || {}) };
    const session = meta._session_data || {};

    await _reqCtx.run(
      { corrId: corr, originId: origin, meta, config: lc.spec.config || {}, delivery: env.delivery_count || 0, session },
      async () => {
        const t0 = Date.now();
        log.info("exec start", { cap: capName, corr_id: corr, worker: this.workerId, hop: env.hop ?? 0 });
        const payloadBytes = env.payload ? Buffer.from(env.payload) : Buffer.alloc(0);
        const inputs = payloadBytes.length ? this.codec.unmarshal(payloadBytes) : {};

        let result;
        try {
          lc._concurrent++;
          result = await lc.run(inputs);
          this.requestsServed++;
          lc._concurrent--;
        } catch (e) {
          lc._concurrent--;
          const durMs = Date.now() - t0;
          log.error("exec error", { cap: capName, corr_id: corr, worker: this.workerId, duration_ms: durMs, error: e.message });
          return this._sendEnvelope(this._err(env, "EXEC_ERROR", e.message));
        }

        if (_cancelledIds.has(origin)) {
          return this._sendEnvelope(this._err(env, "CANCELLED", "request cancelled"));
        }

        const durMs = Date.now() - t0;
        log.info("exec done", { cap: capName, corr_id: corr, worker: this.workerId, duration_ms: durMs });

        const forwardTo = env.forward_to || "";
        if (forwardTo) {
          return this._sendEnvelope({ proto_ver: 1, msg_type: "pipe",
            corr_id: corr, origin_id: origin, worker_id: this.workerId,
            cap: capName, hop: (env.hop || 0) + 1, forward_to: forwardTo,
            topic: forwardTo, payload: this.codec.marshal(result ?? {}), meta });
        }

        if (result == null) return;

        this._sendEnvelope({ proto_ver: 1, msg_type: "res",
          corr_id: corr, origin_id: origin, worker_id: this.workerId,
          cap: capName, cap_ver: lc.spec.cap_ver || "",
          payload: this.codec.marshal(result),
          session_id: env.session_id || "", meta });
      }
    );
  }

  _heartbeat() {
    this._sendEnvelope({ proto_ver: 1, msg_type: "hb_ping",
      worker_id: this.workerId, runtime: "node", load: 0,
      groups: this.groups, caps: [...this.caps.keys()],
      requests_served: this.requestsServed,
      uptime_ms: Date.now() - this.startedAt });
  }

  _sendEnvelope(env) {
    if (!env || !Object.keys(env).length) return;
    const topic = replyTopic(env);
    const data  = this.codec.marshal(env);
    const frame = encodeMsg(topic, Buffer.isBuffer(data) ? data : Buffer.from(data));
    this.transport.sendFrame(frame);
  }

  _makeHello() {
    return { proto_ver: 1, msg_type: "worker_hello", worker_id: this.workerId,
             runtime: "node", pid: process.pid, codec: this.codec.name,
             groups: this.groups, caps: [], pipe_subs: [], pipe_pubs: [],
             cb_supported: false, stream_supported: false };
  }

  _err(env, code, message) {
    return { proto_ver: 1, msg_type: "err",
             corr_id: env.corr_id || "", origin_id: env.origin_id || "",
             worker_id: this.workerId, cap: env.cap || "",
             code, message, retryable: RETRYABLE.has(code) };
  }
}

// Main

(async () => {
  const worker = new Worker();
  try {
    await worker.run();
  } catch (e) {
    log.error("worker crashed: " + e.message);
    process.exit(1);
  }
})();
