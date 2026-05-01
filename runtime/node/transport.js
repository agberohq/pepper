
//
// Three backends matching transport.py exactly:
//   mula  — raw TCP framing (4-byte big-endian length prefix)
//   redis — ioredis (BRPOP for push queues, subscribe for control)
//   nats  — nats.ws / nats.js (JetStream push consumers)
//
// Only Mula is required for single-node deployments.
"use strict";

const net = require("net");
const { EventEmitter } = require("events");

// Wire helpers

/** Decode (topic, payload) from a Worker._send_envelope frame. */
function unwrapFrame(data) {
  if (data.length < 2) return { topic: "", payload: data };
  const topicLen = data.readUInt16BE(0);
  if (topicLen === 0) return { topic: "", payload: data.slice(2) };
  if (data.length < 2 + topicLen) return { topic: "", payload: data };
  return {
    topic:   data.slice(2, 2 + topicLen).toString("utf8"),
    payload: data.slice(2 + topicLen),
  };
}

/** Wrap payload with zero-length topic prefix for message_loop compatibility. */
function wrapPayload(payload) {
  const prefix = Buffer.alloc(2); // 0x00 0x00
  return Buffer.concat([prefix, payload]);
}

// Mula transport  (raw TCP, 4-byte big-endian framing)

class MulaTransport extends EventEmitter {
  constructor(host, port) {
    super();
    this.host = host;
    this.port = port;
    this._socket = null;
    this._inbox  = [];        // received frames waiting to be read
    this._notify = null;      // resolve fn for the next recvFrame() promise
    this._readBuf = Buffer.alloc(0);
  }

  connect(retries = 10) {
    return new Promise((resolve, reject) => {
      let attempt = 0;
      const tryConnect = () => {
        const socket = new net.Socket();
        socket.connect(this.port, this.host, () => {
          this._socket = socket;
          socket.on("data", (chunk) => this._onData(chunk));
          socket.on("error", (e) => {
            if (attempt < retries) {
              attempt++;
              const delay = Math.min(100 * Math.pow(2, attempt - 1), 4000);
              setTimeout(tryConnect, delay);
            } else {
              reject(new Error(`mula: could not connect to ${this.host}:${this.port}: ${e.message}`));
            }
          });
          socket.on("close", () => this.emit("close"));
          resolve();
        });
        socket.once("error", (e) => {
          if (!this._socket) {
            // connection failed before 'connect' fired
            attempt++;
            if (attempt < retries) {
              const delay = Math.min(100 * Math.pow(2, attempt - 1), 4000);
              setTimeout(tryConnect, delay);
            } else {
              reject(new Error(`mula: connect failed after ${retries} attempts: ${e.message}`));
            }
          }
        });
      };
      tryConnect();
    });
  }

  subscribe(topics) {
    // Send pipe-separated topic list as the hello frame (Mula handshake).
    this.sendFrame(Buffer.from(topics.join("|"), "utf8"));
  }

  sendFrame(data) {
    if (!this._socket) return;
    const header = Buffer.alloc(4);
    header.writeUInt32BE(data.length, 0);
    try {
      this._socket.write(Buffer.concat([header, data]));
    } catch (e) {
      console.error("[pepper/transport] mula send failed:", e.message);
    }
  }

  /** @returns {Promise<Buffer|null>} */
  recvFrame() {
    if (this._inbox.length > 0) return Promise.resolve(this._inbox.shift());
    return new Promise((resolve) => { this._notify = resolve; });
  }

  close() {
    if (this._socket) { try { this._socket.destroy(); } catch (_) {} this._socket = null; }
    if (this._notify) { this._notify(null); this._notify = null; }
  }

  _onData(chunk) {
    this._readBuf = Buffer.concat([this._readBuf, chunk]);
    while (this._readBuf.length >= 4) {
      const n = this._readBuf.readUInt32BE(0);
      if (this._readBuf.length < 4 + n) break;
      const frame = this._readBuf.slice(4, 4 + n);
      this._readBuf = this._readBuf.slice(4 + n);
      if (this._notify) {
        const fn = this._notify;
        this._notify = null;
        fn(frame);
      } else {
        this._inbox.push(frame);
      }
    }
  }
}

// Redis transport

class RedisTransport extends EventEmitter {
  constructor(host, port) {
    super();
    this.host   = host;
    this.port   = port;
    this._client      = null;
    this._brpopClient = null;
    this._inbox       = [];
    this._notify      = null;
    this._queues      = [];
    this._stop        = false;
  }

  async connect(retries = 10) {
    let Redis;
    try { Redis = require("ioredis"); }
    catch { throw new Error("redis transport requires ioredis: npm install ioredis"); }

    for (let i = 0; i < retries; i++) {
      try {
        this._client      = new Redis({ host: this.host, port: this.port });
        this._brpopClient = new Redis({ host: this.host, port: this.port });
        await this._client.ping();
        await this._brpopClient.ping();
        return;
      } catch (e) {
        await new Promise(r => setTimeout(r, Math.min(100 * Math.pow(2, i), 4000)));
      }
    }
    throw new Error(`redis: could not connect to ${this.host}:${this.port}`);
  }

  subscribe(topics) {
    this._queues     = topics.filter(t => t.startsWith("pepper.push."));
    const subTopics  = topics.filter(t => !t.startsWith("pepper.push."));
    if (subTopics.length > 0) {
      this._client.subscribe(...subTopics);
      this._client.on("message", (_ch, msg) => this._enqueue(Buffer.from(msg)));
    }
    if (this._queues.length > 0) this._brpopLoop();
  }

  async _brpopLoop() {
    while (!this._stop) {
      try {
        const result = await this._brpopClient.brpop(...this._queues, 2);
        if (result) this._enqueue(wrapPayload(Buffer.from(result[1])));
      } catch (e) {
        if (!this._stop) await new Promise(r => setTimeout(r, 500));
      }
    }
  }

  sendFrame(data) {
    const { topic, payload } = unwrapFrame(data);
    if (!topic || !this._client) return;
    this._client.publish(topic, payload).catch(e =>
      console.error("[pepper/transport] redis publish failed:", e.message));
  }

  recvFrame() {
    if (this._inbox.length > 0) return Promise.resolve(this._inbox.shift());
    return new Promise((resolve) => { this._notify = resolve; });
  }

  close() {
    this._stop = true;
    if (this._client)      { try { this._client.disconnect(); }      catch (_) {} }
    if (this._brpopClient) { try { this._brpopClient.disconnect(); } catch (_) {} }
    if (this._notify) { this._notify(null); this._notify = null; }
  }

  _enqueue(buf) {
    if (this._notify) {
      const fn = this._notify;
      this._notify = null;
      fn(buf);
    } else {
      this._inbox.push(buf);
    }
  }
}

// NATS transport

class NATSTransport extends EventEmitter {
  constructor(host, port) {
    super();
    this.url    = `nats://${host}:${port}`;
    this._nc    = null;
    this._js    = null;
    this._inbox = [];
    this._notify = null;
    this._stop  = false;
  }

  async connect(retries = 10) {
    let nats;
    try { nats = require("nats"); }
    catch { throw new Error("nats transport requires nats: npm install nats"); }

    for (let i = 0; i < retries; i++) {
      try {
        this._nc = await nats.connect({ servers: this.url });
        this._js = this._nc.jetstream();
        return;
      } catch (e) {
        await new Promise(r => setTimeout(r, Math.min(100 * Math.pow(2, i), 4000)));
      }
    }
    throw new Error(`nats: could not connect to ${this.url}`);
  }

  async subscribe(topics) {
    const { StringCodec } = require("nats");
    const sc = StringCodec();
    const pushTopics = topics.filter(t => t.startsWith("pepper.push."));
    const coreTopics = topics.filter(t => !t.startsWith("pepper.push."));

    for (const subject of coreTopics) {
      const sub = this._nc.subscribe(subject);
      (async () => {
        for await (const msg of sub) {
          this._enqueue(wrapPayload(Buffer.from(msg.data)));
        }
      })();
    }

    for (const subject of pushTopics) {
      const queue = "c-" + subject.replace(/\./g, "_");
      try {
        const consumer = await this._js.subscribe(subject, {
          queue, stream: "pepper-work", mack: true,
        });
        (async () => {
          for await (const msg of consumer) {
            this._enqueue(wrapPayload(Buffer.from(msg.data)));
            msg.ack();
          }
        })();
      } catch (e) {
        console.error(`[pepper/transport] nats js subscribe failed [${subject}]:`, e.message);
      }
    }
  }

  sendFrame(data) {
    const { topic, payload } = unwrapFrame(data);
    if (!topic || !this._nc) return;
    try { this._nc.publish(topic, payload); }
    catch (e) { console.error("[pepper/transport] nats publish failed:", e.message); }
  }

  recvFrame() {
    if (this._inbox.length > 0) return Promise.resolve(this._inbox.shift());
    return new Promise((resolve) => { this._notify = resolve; });
  }

  async close() {
    this._stop = true;
    try { await this._nc?.close(); } catch (_) {}
    if (this._notify) { this._notify(null); this._notify = null; }
  }

  _enqueue(buf) {
    if (this._notify) {
      const fn = this._notify;
      this._notify = null;
      fn(buf);
    } else {
      this._inbox.push(buf);
    }
  }
}

// BusTransport — public façade

function parseUrl(url) {
  if (!url.includes("://")) {
    const [host, portStr] = url.split(":");
    return { scheme: "mula", host: host || "127.0.0.1", port: parseInt(portStr) || 7731 };
  }
  const [scheme, rest] = url.split("://");
  const [host, portStr] = rest.split(":");
  const defaults = { redis: 6379, nats: 4222, mula: 7731, tcp: 7731 };
  return { scheme: scheme.toLowerCase(), host: host || "127.0.0.1", port: parseInt(portStr) || defaults[scheme] || 7731 };
}

class BusTransport {
  constructor(url) {
    const { scheme, host, port } = parseUrl(url);
    this.scheme = scheme;
    this.host   = host;
    this.port   = port;
    this._impl  = scheme === "redis" ? new RedisTransport(host, port)
                : scheme === "nats"  ? new NATSTransport(host, port)
                : scheme === "mula" || scheme === "tcp" ? new MulaTransport(host, port)
                : (() => { throw new Error(`unsupported bus scheme: ${scheme}`); })();
  }

  connect(retries)        { return this._impl.connect(retries); }
  subscribe(topics)       { return this._impl.subscribe(topics); }
  sendFrame(data)         { return this._impl.sendFrame(data); }
  recvFrame()             { return this._impl.recvFrame(); }
  close()                 { return this._impl.close(); }
}

module.exports = { BusTransport, unwrapFrame, wrapPayload, parseUrl };
