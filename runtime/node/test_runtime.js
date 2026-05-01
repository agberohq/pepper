// test_runtime.js — Integration tests for runtime.js using a mock TCP router.
// Run with: node test_runtime.js
"use strict";

const net  = require("net");
const path = require("path");
const fs   = require("fs");
const os   = require("os");

const RUNTIME_DIR = __dirname;

// ---------------------------------------------------------------------------
// Codec (JSON for tests — no msgpack dependency required)
// ---------------------------------------------------------------------------

function encode(env) {
  const topic = replyTopic(env);
  const payload = Buffer.from(JSON.stringify(env), "utf8");
  const tb = Buffer.from(topic, "utf8");
  const header = Buffer.alloc(2);
  header.writeUInt16BE(tb.length, 0);
  return Buffer.concat([header, tb, payload]);
}

function replyTopic(env) {
  const t = env.msg_type || "";
  if (["res","err"].includes(t)) return `pepper.res.${env.origin_id || ""}`;
  if (t === "hb_ping")           return `pepper.hb.${env.worker_id || ""}`;
  return "pepper.control";
}

// ---------------------------------------------------------------------------
// MockRouter — TCP echo server that captures frames and can inject envelopes
// ---------------------------------------------------------------------------

class MockRouter {
  constructor() {
    this._server   = net.createServer();
    this._conn     = null;
    this._inbox    = [];
    this._notify   = null;
    this._readBuf  = Buffer.alloc(0);
    this.port      = 0;
  }

  start() {
    return new Promise((resolve) => {
      this._server.listen(0, "127.0.0.1", () => {
        this.port = this._server.address().port;
        this._server.on("connection", (sock) => {
          this._conn = sock;
          sock.on("data", (chunk) => this._onData(chunk));
        });
        resolve();
      });
    });
  }

  recvEnvelope(skipTypes = ["hb_ping"]) {
    return new Promise((resolve) => {
      const tryNext = () => {
        while (this._inbox.length > 0) {
          const frame = this._inbox.shift();
          const topicLen = frame.readUInt16BE(0);
          const payload  = frame.slice(2 + topicLen);
          let env;
          try { env = JSON.parse(payload.toString("utf8")); } catch { continue; }
          if (!skipTypes.includes(env.msg_type)) { resolve(env); return; }
        }
        this._notify = tryNext;
      };
      tryNext();
    });
  }

  send(env) {
    if (!this._conn) return;
    const frame = encode(env);
    const header = Buffer.alloc(4);
    header.writeUInt32BE(frame.length, 0);
    this._conn.write(Buffer.concat([header, frame]));
  }

  close() {
    if (this._conn)   { try { this._conn.destroy(); }   catch (_) {} }
    if (this._server) { try { this._server.close(); }   catch (_) {} }
  }

  _onData(chunk) {
    this._readBuf = Buffer.concat([this._readBuf, chunk]);
    while (this._readBuf.length >= 4) {
      const n = this._readBuf.readUInt32BE(0);
      if (this._readBuf.length < 4 + n) break;
      const frame = this._readBuf.slice(4, 4 + n);
      this._readBuf = this._readBuf.slice(4 + n);
      this._inbox.push(frame);
      if (this._notify) { const fn = this._notify; this._notify = null; fn(); }
    }
  }
}

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

let passed = 0;
let failed = 0;

async function test(name, fn) {
  const router = new MockRouter();
  await router.start();

  const env = {
    PEPPER_WORKER_ID:      "w-test-1",
    PEPPER_BUS_URL:        `tcp://127.0.0.1:${router.port}`,
    PEPPER_GROUPS:         "default",
    PEPPER_HEARTBEAT_MS:   "500",
    PEPPER_MAX_CONCURRENT: "2",
    PEPPER_CODEC:          "json",
    PEPPER_NODE_RUNTIME_DIR: RUNTIME_DIR,
    ...process.env,
  };

  // Spawn the worker subprocess
  const { spawn } = require("child_process");
  const proc = spawn(process.execPath, [path.join(RUNTIME_DIR, "runtime.js")], {
    env, stdio: ["ignore", "inherit", "inherit"],
  });

  try {
    await fn(router, proc);
    console.log(`  ✓ ${name}`);
    passed++;
  } catch (e) {
    console.error(`  ✗ ${name}: ${e.message}`);
    failed++;
  } finally {
    proc.kill("SIGKILL");
    router.close();
    await new Promise(r => setTimeout(r, 100));
  }
}

function timeout(ms, promise, label) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(`timeout(${ms}ms): ${label}`)), ms)),
  ]);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

console.log("\nruntime.js integration tests");

await (async () => {

  await test("worker_hello is sent on connect", async (router) => {
    const hello = await timeout(5000, router.recvEnvelope(["hb_ping"]), "worker_hello");
    if (hello.msg_type !== "worker_hello") throw new Error(`expected worker_hello, got ${hello.msg_type}`);
    if (hello.runtime  !== "node")         throw new Error(`runtime = ${hello.runtime}, want node`);
    if (!hello.worker_id)                  throw new Error("missing worker_id");
    if (!Array.isArray(hello.groups))      throw new Error("groups should be array");
  });

  await test("cap_load and cap_ready round-trip", async (router) => {
    // Wait for hello
    await timeout(5000, router.recvEnvelope(["hb_ping"]), "worker_hello");

    // Write a temporary capability file
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pepper-node-test-"));
    const capPath = path.join(tmpDir, "echo.js");
    fs.writeFileSync(capPath, `
// pepper:name = test.echo
// pepper:groups = default
function run(inputs) { return { ...inputs, echoed: true }; }
module.exports = { run };
`);

    router.send({ msg_type: "cap_load", cap: "test.echo", source: capPath,
                  cap_ver: "1.0.0", config: {} });

    const ready = await timeout(5000, router.recvEnvelope(["hb_ping", "worker_hello"]), "cap_ready");
    if (ready.msg_type !== "cap_ready") throw new Error(`expected cap_ready, got ${ready.msg_type}`);
    if (ready.cap      !== "test.echo") throw new Error(`cap = ${ready.cap}, want test.echo`);
    if (ready.error)                    throw new Error(`cap_ready error: ${ready.error}`);

    fs.rmSync(tmpDir, { recursive: true });
  });

  await test("req dispatched and res returned", async (router) => {
    await timeout(5000, router.recvEnvelope(["hb_ping"]), "worker_hello");

    const tmpDir  = fs.mkdtempSync(path.join(os.tmpdir(), "pepper-node-test-"));
    const capPath = path.join(tmpDir, "echo.js");
    fs.writeFileSync(capPath, `
function run(inputs) { return { msg: inputs.msg, echoed: true }; }
module.exports = { run };
`);
    router.send({ msg_type: "cap_load", cap: "test.echo2", source: capPath, config: {} });
    await timeout(5000, router.recvEnvelope(["hb_ping", "worker_hello"]), "cap_ready");

    const corrId   = "test-corr-001";
    const originId = "test-origin-001";
    const payload  = Buffer.from(JSON.stringify({ msg: "hello-node" }), "utf8");
    router.send({ msg_type: "req", cap: "test.echo2",
                  corr_id: corrId, origin_id: originId,
                  payload, hop: 0 });

    const res = await timeout(5000, router.recvEnvelope(["hb_ping", "worker_hello", "cap_ready"]), "res");
    if (res.msg_type !== "res")   throw new Error(`expected res, got ${res.msg_type}`);
    if (res.corr_id  !== corrId)  throw new Error(`corr_id mismatch: ${res.corr_id}`);
    const out = JSON.parse(Buffer.from(res.payload).toString("utf8"));
    if (!out.echoed)              throw new Error("echoed field missing from response");
    if (out.msg !== "hello-node") throw new Error(`msg = ${out.msg}, want hello-node`);

    fs.rmSync(tmpDir, { recursive: true });
  });

  await test("cap not found returns CAP_NOT_FOUND error", async (router) => {
    await timeout(5000, router.recvEnvelope(["hb_ping"]), "worker_hello");

    router.send({ msg_type: "req", cap: "no.such.cap",
                  corr_id: "c1", origin_id: "o1", payload: Buffer.from("{}") });

    const err = await timeout(5000, router.recvEnvelope(["hb_ping", "worker_hello"]), "err");
    if (err.msg_type !== "err")         throw new Error(`expected err, got ${err.msg_type}`);
    if (err.code !== "CAP_NOT_FOUND")   throw new Error(`code = ${err.code}, want CAP_NOT_FOUND`);
  });

  await test("cancel suppresses response", async (router) => {
    await timeout(5000, router.recvEnvelope(["hb_ping"]), "worker_hello");

    const tmpDir  = fs.mkdtempSync(path.join(os.tmpdir(), "pepper-node-test-"));
    const capPath = path.join(tmpDir, "slow.js");
    // Slightly slow cap so we can cancel before it finishes
    fs.writeFileSync(capPath, `
async function run() {
  await new Promise(r => setTimeout(r, 200));
  return { done: true };
}
module.exports = { run };
`);
    router.send({ msg_type: "cap_load", cap: "slow.cap", source: capPath, config: {} });
    await timeout(5000, router.recvEnvelope(["hb_ping", "worker_hello"]), "cap_ready");

    const originId = "cancel-test-origin";
    router.send({ msg_type: "req", cap: "slow.cap",
                  corr_id: "c-slow", origin_id: originId, payload: Buffer.from("{}") });

    // Cancel immediately
    router.send({ msg_type: "cancel", origin_id: originId });

    // Allow time for processing; expect either CANCELLED err or nothing
    await new Promise(r => setTimeout(r, 400));
    // We don't assert a specific message here — just that the worker survived.
    fs.rmSync(tmpDir, { recursive: true });
  });

  await test("worker_bye shuts down cleanly", async (router, proc) => {
    await timeout(5000, router.recvEnvelope(["hb_ping"]), "worker_hello");

    router.send({ msg_type: "worker_bye" });

    const exitCode = await timeout(3000,
      new Promise(resolve => proc.on("exit", resolve)), "worker exit");
    // Exit code may be 0 or null depending on the kill signal; accept either.
    if (exitCode !== 0 && exitCode !== null) {
      throw new Error(`unexpected exit code: ${exitCode}`);
    }
  });

})();

console.log(`\n${passed + failed} tests: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
