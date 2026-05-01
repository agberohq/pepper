
//
// Covers:
//   - parseFormA()    Form A header parser  (spec §10.1)
//   - readBlob()      Zero-copy blob access
//   - Resource registry
"use strict";

// Form A header parser

const HEADER_RE = /^\/\/\s*pepper:(\w+)\s*=\s*(.+)$/;

/**
 * Parse Form A magic comment header from a JavaScript source string.
 * Reads contiguous // pepper:key = value lines from the top of the file.
 * Stops at the first non-comment, non-blank line.
 *
 * @param {string} source
 * @returns {Object.<string,string>}
 */
function parseFormA(source) {
  const meta = {};
  for (const line of source.split("\n")) {
    const stripped = line.trim();
    if (!stripped) continue;
    if (stripped.startsWith("#!")) continue; // shebang
    const m = HEADER_RE.exec(stripped);
    if (m) {
      meta[m[1]] = m[2].trim();
    } else if (stripped.startsWith("//")) {
      continue; // plain comment — keep scanning
    } else {
      break; // code started — stop
    }
  }
  return meta;
}

// Resource registry

const _resources = new Map();
const _resourceFactories = new Map();

/**
 * Register a resource factory. The factory receives the worker config and
 * returns the resource instance. Called once per worker at startup.
 *
 * @param {string} name
 * @param {function(Object): any} factory
 */
function registerResource(name, factory) {
  _resourceFactories.set(name, factory);
}

/**
 * Return the shared resource registered under name, or undefined.
 * @param {string} name
 */
function getResource(name) {
  return _resources.get(name);
}

/**
 * Initialise all registered resources. Called once before capability setup.
 * @param {Object} config
 */
async function initResources(config) {
  for (const [name, factory] of _resourceFactories) {
    try {
      _resources.set(name, await factory(config));
    } catch (e) {
      console.error(`[pepper] resource init failed [name=${name}]:`, e.message);
    }
  }
}

/**
 * Tear down all resources. Called on worker shutdown.
 */
async function closeResources() {
  for (const [name, res] of _resources) {
    if (res && typeof res.teardown === "function") {
      try { await res.teardown(); } catch (_) {}
    }
  }
  _resources.clear();
}

// Blob access  (spec §12.4)

class BlobRef {
  constructor(ref) {
    this._ref    = ref;
    this.path    = ref.path    ?? "";
    this.size    = ref.size    ?? 0;
    this.dtype   = ref.dtype   ?? "";
    this.shape   = ref.shape   ?? [];
    this.format  = ref.format  ?? "";
    this.id      = ref.id      ?? "";
  }

  /** Read the blob as a Buffer (one copy). */
  asBytes() {
    return require("fs").readFileSync(this.path);
  }

  /**
   * Return the blob data as a typed array when dtype/shape are known.
   * Falls back to a plain Buffer otherwise.
   */
  asTypedArray() {
    const buf = this.asBytes();
    const TypedArrayMap = {
      float32: Float32Array, float64: Float64Array,
      int8:    Int8Array,    int16:   Int16Array,
      int32:   Int32Array,  int64:   BigInt64Array,
      uint8:   Uint8Array,  uint16:  Uint16Array,
      uint32:  Uint32Array, uint64:  BigUint64Array,
    };
    const Ctor = TypedArrayMap[this.dtype];
    return Ctor
      ? new Ctor(buf.buffer, buf.byteOffset, buf.byteLength / Ctor.BYTES_PER_ELEMENT)
      : buf;
  }
}

/**
 * Convert a blob reference dict (with _pepper_blob: true) to a BlobRef.
 *
 * @param {Object} ref
 * @returns {BlobRef}
 */
function readBlob(ref) {
  if (!ref || !ref._pepper_blob) {
    throw new Error("readBlob: argument is not a pepper blob reference");
  }
  return new BlobRef(ref);
}

module.exports = { parseFormA, registerResource, getResource, initResources, closeResources, readBlob, BlobRef };
