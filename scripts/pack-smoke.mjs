import assert from "node:assert/strict";
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

const cjs = require("wflow-core");
const cjsTesting = require("wflow-core/testing");
const cjsCodec = require("wflow-core/codec");
assert.equal(typeof cjs.createWorkflowWorker, "function", "CJS createWorkflowWorker");
assert.equal(typeof cjs.resolveWorkflowsPath, "function", "CJS resolveWorkflowsPath");
assert.equal(typeof cjs.AdapterError, "function", "CJS AdapterError");
assert.equal(typeof cjs.isWorkflowNotFound, "function", "CJS isWorkflowNotFound");
assert.equal(cjs.ERROR_CODES.WAIT_NOT_ACTIVE, "WAIT_NOT_ACTIVE", "CJS ERROR_CODES");
assert.equal(typeof cjsTesting.createTestEngine, "function", "CJS createTestEngine");
assert.equal(typeof cjsTesting.createMemoryAdapters, "function", "CJS createMemoryAdapters");
assert.equal(typeof cjsCodec.createAesGcmPayloadCodec, "function", "CJS createAesGcmPayloadCodec");

const cjsWorkflows = cjs.resolveWorkflowsPath();
assert.ok(existsSync(cjsWorkflows), `CJS default workflows path missing: ${cjsWorkflows}`);
assert.ok(existsSync(require.resolve("wflow-core/workflows")), "CJS ./workflows subpath missing");

const esm = await import("wflow-core");
const esmTesting = await import("wflow-core/testing");
const esmCodec = await import("wflow-core/codec");
assert.equal(typeof esm.createWorkflowWorker, "function", "ESM createWorkflowWorker");
assert.equal(typeof esm.resolveWorkflowsPath, "function", "ESM resolveWorkflowsPath");
assert.equal(typeof esm.AdapterError, "function", "ESM AdapterError");
assert.equal(typeof esm.isWorkflowNotFound, "function", "ESM isWorkflowNotFound");
assert.equal(esm.ERROR_CODES.WAIT_NOT_ACTIVE, "WAIT_NOT_ACTIVE", "ESM ERROR_CODES");
assert.equal(typeof esmTesting.createTestEngine, "function", "ESM createTestEngine");
assert.equal(typeof esmTesting.createMemoryAdapters, "function", "ESM createMemoryAdapters");
assert.equal(typeof esmCodec.createAesGcmPayloadCodec, "function", "ESM createAesGcmPayloadCodec");

const esmWorkflows = esm.resolveWorkflowsPath();
assert.ok(existsSync(esmWorkflows), `ESM default workflows path missing: ${esmWorkflows}`);

// The workflow sandbox must never pull the codec or Node's crypto module.
const textEncoder = new TextEncoder();
for (const [format, bundle] of [["cjs", cjsWorkflows], ["esm", esmWorkflows]]) {
  const source = readFileSync(bundle, "utf8");
  assert.ok(!source.includes("node:crypto"), `${format} workflows bundle must not reference node:crypto`);
  assert.ok(!source.includes("createAesGcmPayloadCodec"), `${format} workflows bundle must not bundle the codec`);
}

// Exercise the built codec across the package boundary: json/plain in, encrypted on the wire, restored on decode.
const codec = cjsCodec.createAesGcmPayloadCodec({ key: new Uint8Array(32).fill(7) });
const original = { metadata: { encoding: textEncoder.encode("json/plain") }, data: textEncoder.encode('{"secret":true}') };
const [encoded] = await codec.encode([original]);
assert.equal(Buffer.from(encoded.metadata.encoding).toString(), cjsCodec.ENCRYPTED_ENCODING, "codec marks encrypted payloads");
assert.notEqual(Buffer.from(encoded.data).toString(), '{"secret":true}', "codec encrypts data");
const [decoded] = await codec.decode([encoded]);
assert.equal(Buffer.from(decoded.data).toString(), '{"secret":true}', "codec round trip");
assert.equal(Buffer.from(decoded.metadata.encoding).toString(), "json/plain", "codec restores the original encoding");

console.log(`pack smoke ok: cjs=${cjsWorkflows} esm=${esmWorkflows}`);
