// test_cap.js — Unit tests for cap.js
// Run with: node test_cap.js
// Or with a test runner: npx jest test_cap.js
"use strict";

const assert = require("assert");
const path   = require("path");
const fs     = require("fs");
const os     = require("os");

const { parseFormA, readBlob, BlobRef,
        registerResource, getResource, initResources, closeResources } = require("./cap.js");

let passed = 0;
let failed = 0;

function test(name, fn) {
  try {
    fn();
    console.log(`  ✓ ${name}`);
    passed++;
  } catch (e) {
    console.error(`  ✗ ${name}: ${e.message}`);
    failed++;
  }
}

async function testAsync(name, fn) {
  try {
    await fn();
    console.log(`  ✓ ${name}`);
    passed++;
  } catch (e) {
    console.error(`  ✗ ${name}: ${e.message}`);
    failed++;
  }
}

// ---------------------------------------------------------------------------
// parseFormA
// ---------------------------------------------------------------------------

console.log("\nparseFormA");

test("parses contiguous header", () => {
  const src = `// pepper:name = tools.search
// pepper:version = 1.0.0
// pepper:groups = default,gpu

function setup(config) {}
`;
  const meta = parseFormA(src);
  assert.strictEqual(meta.name,    "tools.search");
  assert.strictEqual(meta.version, "1.0.0");
  assert.strictEqual(meta.groups,  "default,gpu");
});

test("stops at first non-comment line", () => {
  const src = `// pepper:name = tools.search
// pepper:version = 1.0.0

function run(inputs) {
  // pepper:deps = something-else
}
`;
  const meta = parseFormA(src);
  assert.strictEqual(meta.name, "tools.search");
  assert.ok(!meta.deps, "should not parse deps inside function body");
});

test("ignores shebang line", () => {
  const src = `#!/usr/bin/env node
// pepper:name = x
const x = 1;
`;
  const meta = parseFormA(src);
  assert.strictEqual(meta.name, "x");
});

test("returns empty object for empty source", () => {
  assert.deepStrictEqual(parseFormA(""), {});
});

test("handles blank lines in header", () => {
  const src = `// pepper:name = a

// pepper:version = 2.0.0
const x = 1;
`;
  const meta = parseFormA(src);
  assert.strictEqual(meta.name,    "a");
  assert.strictEqual(meta.version, "2.0.0");
});

// ---------------------------------------------------------------------------
// readBlob
// ---------------------------------------------------------------------------

console.log("\nreadBlob");

test("throws on non-blob dict", () => {
  assert.throws(() => readBlob({ foo: "bar" }), /not a pepper blob/);
});

test("throws on null", () => {
  assert.throws(() => readBlob(null), /not a pepper blob/);
});

test("constructs BlobRef from valid reference", () => {
  const ref = {
    _pepper_blob: true,
    id:     "blob_abc123",
    path:   "/dev/shm/pepper_blob_abc123.dat",
    size:   1024,
    dtype:  "float32",
    shape:  [256, 4],
    format: "raw",
  };
  const b = readBlob(ref);
  assert.ok(b instanceof BlobRef);
  assert.strictEqual(b.id,    "blob_abc123");
  assert.strictEqual(b.size,  1024);
  assert.strictEqual(b.dtype, "float32");
  assert.deepStrictEqual(b.shape, [256, 4]);
});

test("asBytes reads file contents", () => {
  const tmp = path.join(os.tmpdir(), `pepper_test_blob_${Date.now()}.dat`);
  const data = Buffer.from("hello pepper");
  fs.writeFileSync(tmp, data);
  try {
    const b = readBlob({ _pepper_blob: true, path: tmp, size: data.length });
    const bytes = b.asBytes();
    assert.ok(bytes.equals(data), "bytes should match written data");
  } finally {
    fs.unlinkSync(tmp);
  }
});

test("asTypedArray returns Float32Array when dtype=float32", () => {
  const tmp = path.join(os.tmpdir(), `pepper_test_f32_${Date.now()}.dat`);
  const arr = new Float32Array([1.0, 2.0, 3.0, 4.0]);
  fs.writeFileSync(tmp, Buffer.from(arr.buffer));
  try {
    const b = readBlob({ _pepper_blob: true, path: tmp, size: arr.byteLength, dtype: "float32", shape: [4] });
    const out = b.asTypedArray();
    assert.ok(out instanceof Float32Array);
    assert.strictEqual(out.length, 4);
    assert.ok(Math.abs(out[0] - 1.0) < 1e-6);
  } finally {
    fs.unlinkSync(tmp);
  }
});

// ---------------------------------------------------------------------------
// Resource registry
// ---------------------------------------------------------------------------

console.log("\nresource registry");

await (async () => {
  await testAsync("initResources calls factories", async () => {
    registerResource("test_conn", (cfg) => ({ url: cfg.url, connected: true }));
    await initResources({ url: "redis://localhost:6379" });
    const res = getResource("test_conn");
    assert.ok(res, "resource should be available");
    assert.strictEqual(res.url, "redis://localhost:6379");
    assert.ok(res.connected);
    await closeResources();
    assert.strictEqual(getResource("test_conn"), undefined, "resource should be gone after close");
  });

  await testAsync("teardown is called on close", async () => {
    let tornDown = false;
    registerResource("td_res", () => ({
      teardown: async () => { tornDown = true; },
    }));
    await initResources({});
    await closeResources();
    assert.ok(tornDown, "teardown should have been called");
  });
})();

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------

console.log(`\n${passed + failed} tests: ${passed} passed, ${failed} failed`);
process.exit(failed > 0 ? 1 : 0);
