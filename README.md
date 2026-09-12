# `wflow-core`

Temporal-backed, TypeScript-native workflow heart for HRMS and other domains. The SDK keeps the reusable parts of wflow (versioned definitions, human tasks, assignment rules, condition routing, gateways, waiting, child processes, actions, timeout and audit events) in a deterministic workflow. Business rules stay in adapters, so the engine has no dependency on an HRMS module or database.

## Install and run

```ts
import { Client, Connection } from "@temporalio/client";
import { createWorkflowClient, createWorkflowWorker } from "wflow-core";

const adapters = {
  definitions: { async get(tenantId, ref) { return definitionRepository.get(tenantId, ref); } },
  async authorize(request) { return auth.can(request); },
  async resolveAssignees(request) { return people.resolve(request); },
  actions: new Map([["personnel.create", { async execute(context, input) { return personnel.create(context, input); } }]]),
  async publishEvent(event) { await eventStore.appendIfAbsent(event.eventId, event); },
};
const connection = await Connection.connect({ address: "127.0.0.1:7233" });
const temporal = new Client({ connection, namespace: "default" });
const engine = createWorkflowClient({ client: temporal, taskQueue: "wflow-core", adapters });
const started = await engine.start({ tenantId: "acme", instanceId: "leave-42", businessKey: "leave-42", initiatorId: "u-7", definition, data: {} });
await started.result();
```

Run a worker in a separate process:

```ts
import { NativeConnection } from "@temporalio/worker";
const workerConnection = await NativeConnection.connect({ address: "127.0.0.1:7233" });
await createWorkflowWorker({ connection: workerConnection, namespace: "default", taskQueue: "wflow-core", adapters }).then((worker) => worker.run());
```

Run the self-contained demo without installing a Temporal server:

```bash
pnpm demo
```

The demo uses Temporal's time-skipping test service, creates a leave approval task, submits an approval command, executes a balance-sync action, and prints the durable event sequence. It is useful for checking the SDK wiring before connecting a real Temporal cluster.

With a local Temporal Server running on `127.0.0.1:7233` (for example through OrbStack), run the real-server demo:

```bash
pnpm demo:real
```

The Temporal Web UI is available at `http://127.0.0.1:8233` when the server is started with the Temporal development image.

`adapters.publishEvent` must deduplicate by `eventId`; this is the projection boundary for notifications, task indexes and audit logs. Adapter inputs and outputs are schema checked and bounded. Do not put secrets or sensitive form values in event details.

## Testing without a Temporal server

`wflow-core/testing` ships a time-skipping harness so consumers can exercise definitions and host adapters without a running server. The main entry does not import `@temporalio/testing`; that package is an optional peer dependency and is only needed when this subpath is imported.

```ts
import { createMemoryAdapters, createTestEngine } from "wflow-core/testing";

const memory = createMemoryAdapters({ users: { "department-manager": ["manager-001"] } });
const test = await createTestEngine({ adapters: memory.adapters });
try {
  await test.run(async () => {
    const started = await test.engine.start({ tenantId: "acme", instanceId: "leave-1", businessKey: "leave-1", initiatorId: "u-7", definition, data: {} });
    // Drive commands through test.engine and assert on memory.events.
    await started.result();
  });
} finally {
  await test.close();
}
```

`createMemoryAdapters` provides an in-memory definition store, allow-by-default authorization, a users-map `resolveAssignees`, an actions map, a custom node-handler map (`nodeHandlers`), and an event log that deduplicates by `eventId`. Every part is overridable. `createTestEngine` accepts explicit adapters, a caller-owned `TestWorkflowEnvironment`, a task queue and extra `workerOptions`; it defaults to `TestWorkflowEnvironment.createTimeSkipping()` with `maxCachedWorkflows: 0` and exposes `{ environment, engine, worker, taskQueue, run, close }`. Set `startWorker: false` when only the environment and client are needed.

## Definition model

Definitions are immutable `{ schemaVersion: 1, id, version, name, nodes }` documents. Nodes include `approval`, `task`, `decision`, `action`, `cc`, `delay`, `wait`, `exclusive`, `inclusive`, `parallel`, `loop`, `forEach`, `eventGateway`, `child` and `custom`. Human nodes support `all`, `any`, `sequential`, `percentage` and `candidate` modes, transfer, ordered additional approvals, assignment resolvers, field permissions and timeout outcomes. Conditions can inspect workflow data with `and`, `or`, `not`, existence, emptiness and typed comparisons.

```ts
// Collection loop (OA 明细逐条审批): items must resolve to a JSON array; exceeding maxIterations
// fails with FOREACH_LIMIT_REACHED instead of truncating. sequential is the default and keeps the
// last itemKey/indexKey; parallel runs chunks of `concurrency` (default 4) against local copies.
const each = {
  id: "lines", type: "forEach", items: { path: ["expenseLines"] }, itemKey: "line", indexKey: "lineNo",
  maxIterations: 200, completionCondition: { op: "exists", value: { path: ["lineDone"] } },
  nodes: [{ id: "line-review", type: "task", mode: "all", assignees: { type: "users", userIds: ["finance"] } }],
};

// Event-based gateway: the first event branch to receive wins; otherwise the earliest timer branch
// wins. Losing waits are cancelled (later event commands fail with WAIT_NOT_ACTIVE). Only a winning
// event branch stores its payload in `resultKey`; timer branches store nothing.
const race = {
  id: "signed", type: "eventGateway", branches: [
    { event: "contract.signed", resultKey: "signature", nodes: [/* ... */] },
    { timeoutMs: 86_400_000, nodes: [/* ... */] },
  ],
};
```

Use `parseDefinition` when publishing definitions. It rejects duplicate IDs, recursive or oversized structures, unsafe object keys, invalid field writes, and conflicting writes in concurrent branches. A child definition is loaded by `(tenantId, id, version)` and is therefore pinned for replay.

## Host-defined node types (`custom`)

A `custom` node plugs host logic into a definition without forking the SDK. `kind` selects a handler registered on `Adapters.nodeHandlers`, `config` is bounded JSON frozen with the definition, and the node's `resultKey` participates in the same write/conflict validation as any other leaf node.

```ts
import { defineNode, type Adapters } from "wflow-core";
import { z } from "zod";

const adapters: Adapters = {
  // ...definitions, authorize, resolveAssignees, actions, publishEvent
  nodeHandlers: new Map([
    ["fraud.check", defineNode({
      input: z.object({ threshold: z.number() }),
      output: z.object({ score: z.number(), flagged: z.boolean() }),
      async execute(context, config, data) {
        // context: { tenantId, instanceId, businessKey, initiatorId, definition, executionId, nodeId }
        return { output: await fraud.score(context.businessKey, config.threshold, data) };
      },
    })],
  ]),
};

const definition = parseDefinition({
  schemaVersion: 1, id: "leave", version: 1, name: "Leave",
  nodes: [
    { id: "verify", type: "custom", kind: "fraud.check", config: { threshold: 0.8 }, resultKey: "fraud" },
    { id: "review", type: "approval", mode: "all", assignees: { type: "users", userIds: ["manager"] } },
  ],
});
```

The handler always runs in the `executeNode` activity, never in the workflow sandbox: the workflow only dispatches the recorded `context`/`config`/`data` and consumes the recorded outcome, so host I/O, randomness and clock access stay deterministic-safe. `defineNode` mirrors `defineAction` and validates `config` against `input` and `outcome.output` against `output` at the activity boundary; both are bounded by the same JSON limits as every other adapter payload.

Execution semantics:

- `output` is stored under the node's `resultKey` when one is declared; without a `resultKey` the output is discarded, like other nodes without a result key. A handler that returns no `output` stores `null` under the `resultKey`.
- `jumpTo` must name a **top-level** node of the current definition. It is validated in the workflow, emits `workflow.nodeCompleted` with `action: "custom_jump"` and `target`, then rebases the run at the target exactly like a `router` jump (scope cancellation; node-id based, never positional). An unknown target fails the instance with `INVALID_JUMP_TARGET`; when `jumpTo` is present, `output` is ignored.
- an unregistered `kind` (or an adapter without `nodeHandlers`) fails the activity non-retryably with `UNKNOWN_NODE_KIND`; a handler throwing `AdapterError` keeps its safe code and retry intent at the boundary and never leaks the host message.
- node events (`enter`/`leave` listeners) and step accounting apply like any other leaf node, and custom nodes can appear inside gateways, loops and `forEach` like any leaf.

## Commands and read access

`WorkflowEngineClient.command` accepts `approve`, `reject`, `complete`, `claim`, `transfer`, `addAssignee`, `event`, `cancel`, `suspend`, `resume`, `migrate`, `returnTo`, `withdraw`, `reassign` and `override`. Every command has `requestId`, `tenantId`, `instanceId` and `actorId`; retries with the same payload return the original receipt. Different payloads cannot reuse a command ID. The client checks authorization, and the workflow checks it again before mutating state. `snapshot` also checks authorization before returning workflow data.

The host MUST restrict `suspend`, `resume`, `migrate`, `reassign` and `override` to authorized operators. Administrative commands preserve the actual operator and, for override, the original assignee in audit events. `returnTo` and `withdraw` currently require a single active top-level human node. `resubmit` optionally defines a task for the initiator. This is not arbitrary graph rollback.

`mapAssignees` optionally applies host delegation rules after assignment resolution. `workflow.childStarted` identifies child instances for host projections before the child starts. Projection delivery blocks workflow progression until committed; use an atomic transaction for deduplication and task/notification writes.

Temporal provides durable timers, signal/update serialization, retries and replay. Keep adapters in activities: workflow code must stay deterministic and must never import Prisma, fetch, random, `eval` or `Function`; use Temporal's workflow time and UUID helpers when a workflow needs time or IDs.

Workflow data and history are stored by Temporal. Secure the namespace and data converter appropriately. Do not expose raw Temporal credentials or unrestricted Query/Update access to browsers. Deployment versioning and history replay checks are required when changing interpreter code; immutable business definitions alone do not guarantee replay compatibility.

The wflow importer covers a subset of the original designer semantics and rejects identified unsupported configurations with explicit error codes. The standalone SDK works with its own generic definition format and host adapters independently of wflow; inspect the importer tests under `tests/wflow.test.ts` for the exact validation scope, and see [ROADMAP.md](./ROADMAP.md) for forward-looking scope decisions and versioning/replay guidance.

## Client capabilities

`engine.start` accepts per-call Temporal options; omitted fields keep the SDK defaults (`FAIL` conflict policy, `REJECT_DUPLICATE` reuse policy and the configured task queue):

```ts
const started = await engine.start(input, {
  taskQueue: "wflow-core-high-priority",
  memo: { source: "leave-portal" },
  searchAttributes: { Tenant: ["acme"] },
  workflowExecutionTimeout: "30 days",
  workflowRunTimeout: "1 day",
  workflowTaskTimeout: "10 seconds",
  workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING,
  workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
});
```

`engine.list`, `engine.count` and `engine.describe` wrap Temporal visibility/describe calls with a stable, JSON-serializable shape. They authorize first: `list` and `count` call `authorize({ operation: "list" })`, so hosts MUST implement that operation (the reference server keeps it admin-only). `describe` uses the existing `read` check. The visibility `query` is passed through unchanged, so scope it to the tenant (for example through a tenant search attribute) or enforce tenancy in `authorize`.

```ts
const query = 'ExecutionStatus = "Running"';
const { count } = await engine.count({ tenantId: "acme", actorId: "u-7", query });
for await (const workflow of await engine.list({ tenantId: "acme", actorId: "u-7", query, pageSize: 100 })) {
  console.log(workflow.workflowId, workflow.status, workflow.startTime, workflow.searchAttributes);
}
const description = await engine.describe({ tenantId: "acme", actorId: "u-7", instanceId: "leave-42" });
console.log(description.isRunning, description.pendingActivityCount, description.historyLength);
```

`engine.waitEvent` signals the active `wait` node whose `event` matches, without reading the snapshot and passing a waitId by hand. It throws `WAIT_NOT_ACTIVE` (a `WorkflowValidationError`) when no matching wait is open, so callers can retry:

```ts
await engine.waitEvent({ tenantId: "acme", actorId: "u-7", instanceId: "leave-42", event: "contract.signed", data: { by: "u-9" } });
```

`engine.signal` delivers an external event through the `workflowEventV1` Temporal signal (payload `{ event, data? }`; `event` is 1-128 characters, `data` is bounded JSON). It authorizes `authorize({ operation: "signal" })` before sending. An active `wait` or `eventGateway` branch whose `event` matches receives it; otherwise the workflow buffers it. `engine.startOrSignal(input, { event, data }, options?)` authorizes `start` and then `signal`, and calls Temporal `signalWithStart`: it starts the instance or delivers the signal to a running one. Its `existing` is always `false` because Temporal does not report which branch happened; use `startOrGet` when that distinction matters. The reference server host keeps `signal` admin-only.

```ts
await engine.signal({ tenantId: "acme", actorId: "u-7", instanceId: "leave-42", event: "contract.signed", data: { by: "u-9" } });
const started = await engine.startOrSignal(input, { event: "contract.signed", data: { by: "u-9" } });
```

Signal buffering is deterministic and bounded:

- with no matching active wait, the signal is appended to a FIFO buffer of at most 100 entries; the oldest entry is dropped once the bound is exceeded;
- a `wait` node consumes the first buffered event whose name matches (non-matching events stay buffered, in arrival order); an `eventGateway` consumes buffered events in arrival order, each resolving the earliest still-unreceived matching branch, so arrival order wins the race, not branch order;
- a signal delivered before its wait exists is consumed exactly once when that wait is reached; buffered events survive a safe-boundary `continueAsNew` and are dropped once the run is terminal; malformed payloads are ignored instead of failing the workflow task;
- signal receipts surface from the node flow (the signal handler never calls an activity): `workflow.waitCreated` is followed by `workflow.eventReceived` with details `{ waitId, event, source: "signal" }`. Signals carry no identity, so reach for `waitEvent` / the `event` command when `actorId`/`requestId` auditing is required (`operation: "command"`).

`engine.command` accepts an optional `updateTimeout` forwarded in the Temporal update options. Use `isWorkflowNotFound(error)` to treat a missing execution as a not-found result instead of an infrastructure error.

## Typed errors

`ERROR_CODES` is a frozen map of every `WorkflowValidationError` code and `ApplicationFailure` type the SDK emits, with a matching `ErrorCode` union. Host adapters can throw `AdapterError` to surface a safe code and an explicit retry intent; the activity boundary maps it to an `ApplicationFailure` that keeps the code but never the host message or stack:

```ts
import { AdapterError, ERROR_CODES, isWorkflowNotFound, WorkflowValidationError } from "wflow-core";

async publishEvent(event) {
  try { await eventStore.append(event); }
  catch (error) { throw new AdapterError("PROJECTION_UNAVAILABLE", { retryable: true, cause: error }); }
}
```

## Observability hook

`Adapters.telemetry.onActivity` receives `{ activity, phase: "start" | "end" | "error", context?, error?, durationMs? }` around every engine activity (`authorize`, `resolveAssignees`, `executeAction`, `executeNode`, `integrate`, `syncBusinessData`, `loadDefinition`, `publishEvent`). It runs in the activity/worker runtime, never in the workflow sandbox, and a throwing hook is swallowed so observability can never fail an engine activity.

```ts
const adapters: Adapters = {
  // ...definitions, authorize, resolveAssignees, actions, publishEvent
  telemetry: { onActivity: ({ activity, phase, durationMs, error }) => metrics.observe(activity, phase, durationMs, error) },
};
```

For deeper metrics and tracing use Temporal Runtime metrics (`Runtime.install` with the Prometheus/OpenTelemetry exporters) and the client/worker interceptor chains instead of expanding this hook.

## Activity and event delivery tuning

`definition.settings.activity` and `definition.settings.eventDelivery` tune activity retries deterministically (the definition is pinned in the workflow input). Defaults match the historical proxy options: 30s start-to-close, 5 attempts, 1s initial interval. Action nodes keep their own `retry` config precedence over `activity`. An absent `eventDelivery.maximumAttempts` keeps today's blocking behaviour (retry forever); once set and exhausted, the workflow fails with `EVENT_DELIVERY_FAILED` instead of stalling.

```ts
const definition = parseDefinition({
  schemaVersion: 1, id: "leave", version: 1, name: "Leave",
  settings: {
    activity: { startToCloseSeconds: 60, maximumAttempts: 3 },
    eventDelivery: { maximumAttempts: 5, initialIntervalMs: 500, maximumIntervalMs: 30_000 },
  },
  nodes: [
    { id: "review", type: "approval", mode: "all", assignees: { type: "users", userIds: ["manager"] } },
  ],
});
```

## Payload encryption (`wflow-core/codec`)

Sensitive business data can be encrypted end to end with the AES-256-GCM payload codec. The codec ships on its own subpath and is the only SDK module that imports `node:crypto`, so the workflow bundle (`dist/workflows.js`) stays codec-free:

```ts
import { Client, Connection } from "@temporalio/client";
import { NativeConnection } from "@temporalio/worker";
import { createAesGcmPayloadCodec } from "wflow-core/codec";
import { createWorkflowClient, createWorkflowWorker } from "wflow-core";

// 32 raw bytes: base64 from env/KMS. `{ keys, activeKeyId }` enables rotation.
const codec = createAesGcmPayloadCodec({ key: process.env.WORKFLOW_PII_KEY! });
const dataConverter = { payloadCodecs: [codec] };

const connection = await Connection.connect({ address: "127.0.0.1:7233" });
const temporal = new Client({ connection, namespace: "default", dataConverter });
const engine = createWorkflowClient({ client: temporal, taskQueue: "wflow-core", adapters });

const workerConnection = await NativeConnection.connect({ address: "127.0.0.1:7233" });
await createWorkflowWorker({ connection: workerConnection, namespace: "default", taskQueue: "wflow-core", adapters, dataConverter }).then((worker) => worker.run());
```

Wire the **same** `dataConverter` into every client and worker that produces or consumes payloads, otherwise they cannot decode each other's data. Behavior:

- every payload except already-encrypted (`binary/encrypted`) and data-less payloads is encrypted; `metadata` gains `encryption-key-id`, `encoding: "binary/encrypted"` and `plain-encoding` (the original encoding, restored on decode; absent falls back to `binary/plain`);
- `data` is `nonce(12) || ciphertext || authTag(16)` with a fresh random nonce per payload. `{ key }` registers the key under the id `default`; `{ keys, activeKeyId }` encrypts with the active key and decrypts with any registered key, so rotation only needs the old key to stay in the map;
- tampered ciphertext (auth tag mismatch), a missing/unknown key id, malformed data or a non-32-byte key throw a clear `ValueError` — never silently return plaintext;
- configuring no codec changes nothing: `createAesGcmPayloadCodec` is opt-in.

Search attributes are not payloads and are **never** processed by the codec (they are stored unencrypted in the visibility store — keep PII out of them). TypeScript `memo` values are routed through the data converter, so this codec encrypts them as well; keep codec-independent memo values non-sensitive. To read encrypted history in the Temporal Web UI or CLI, run a Codec Server that reuses the same codec.

## Long-lived instances: continue-as-new and idempotent start

`definition.settings.continueAsNewAfterEvents` (integer, 100–50,000) turns a long approval flow into a chain of Temporal runs before its history grows unbounded. When the current run's `workflowInfo().historyLength` reaches the threshold at a **safe top-level boundary**, the engine calls Temporal `continueAsNew` with the pinned definition, the mutated business `data` and a bounded `resume` payload:

```ts
const definition = parseDefinition({
  schemaVersion: 1, id: "leave", version: 1, name: "Leave",
  settings: { continueAsNewAfterEvents: 5_000 },
  nodes: [/* ... */],
});
```

Safe boundaries are intentionally strict. A continue is only considered between top-level nodes, and only when all of the following hold:

- no active human task or event wait is in flight;
- the instance is not suspended and not cancelling;
- no `returnTo`/`withdraw`/reject-jump fallback is pending (`returnTarget`/`returnOrigin`/`skipTo`);
- every update/signal handler has finished;
- all `publishEvent` deliveries queued so far have completed.

Nested containers (`exclusive`/`inclusive`/`parallel`/`loop`/`forEach`/`eventGateway`/`child`) always finish their current container before a boundary is reached, so the engine never interrupts a partially executed gateway or loop. `continueAsNew` is the last action of the run: no event is emitted after it, and the new run does not re-publish `workflow.started`, re-authorize the start or re-run the `create`-side `formSync`/startup listeners.

Carried state is deliberately bounded and replay-driven:

| Field | Purpose |
| --- | --- |
| `offset` | index of the next top-level node in the pinned definition |
| `sequence` | keeps host projection ordering strictly increasing across runs |
| `steps` / `commandBytes` | cumulative steps make `maxSteps` a TOTAL budget across the run chain (so `executionId` values stay unique); `commandBytes` keeps the 1MB fingerprint-byte cap cumulative. `maxCommands` stays a per-run count limit |
| `agreedUsers` | `deduplication: ONCE` auto-agree keeps working after the resume |
| `completedHumans` | `returnTo`/`withdraw` targets stay addressable |
| `completedTasks` | last 10 handled tasks; the newest handled node is always kept so `withdraw` can still restore its other assignees' decisions |
| `receipts` | last 200 resolved `requestId -> {fingerprint, receipt}` entries; the same `requestId` retried after a continue returns the original receipt instead of re-applying |
| `pendingEvents` | buffered external signals (at most 100, oldest dropped) travel to the next run so a wait reached after the continue still consumes them |

Tradeoffs: only the newest task restore snapshots travel, so withdrawing an already-non-withdrawable older node after a resume may restore it without its previous assignee decisions; receipts are capped at 200, so a very old `requestId` evicted from the carry can be re-applied in a later run; buffered signals are capped at 100, so a burst beyond that drops the earliest deliveries. If the assembled resume input would exceed the 512KB input bound, the workflow fails with `RESUME_STATE_TOO_LARGE` instead of silently truncating. Leaving `continueAsNewAfterEvents` unset keeps the single-run behavior exactly as before.

`engine.startOrGet(input, options?)` is the idempotent start for request-replay scenarios:

```ts
const started = await engine.startOrGet(input);
// started.existing === false on the first call, true when an instance already exists
const result = await started.result();
```

It first attempts the normal `start`. On `WorkflowExecutionAlreadyStartedError` it authorizes `read`, reads the snapshot and returns `{ workflowId, result(), existing: true }` only when `tenantId`, `businessKey`, `initiatorId` and the definition reference `{id, version}` all match the request. Any mismatch throws `WorkflowValidationError("INSTANCE_ALREADY_EXISTS")`; a denied read keeps today's `FORBIDDEN`. `WorkflowIdConflictPolicy.USE_EXISTING` is intentionally not used because it would silently accept mismatched inputs. `engine.start` is unchanged apart from adding `existing: false`.

## Migrating running instances to a new definition version

Definitions are immutable and pinned per instance, so a running instance keeps executing the version it was started with even after a new one is published. To roll a *running* instance onto a newer version, use the ops runbook **pause → migrate → resume**:

```ts
await engine.command({ type: "suspend", requestId: "ops-suspend", tenantId: "acme", instanceId: "leave-42", actorId: "u-admin" });
await engine.migrate({ tenantId: "acme", actorId: "u-admin", instanceId: "leave-42", requestId: "ops-migrate-v2", toVersion: 2, comment: { ticket: "OPS-17" } });
await engine.command({ type: "resume", requestId: "ops-resume", tenantId: "acme", instanceId: "leave-42", actorId: "u-admin" });
```

`engine.migrate` is a command like any other: it authorizes `operation: "command"` (hosts SHOULD restrict it to operators; the reference server keeps it admin-only), is idempotent per `requestId`, and returns `{ requestId, eventId }`. The target must already be published: the engine loads `{ id: <current id>, version: toVersion }` through `activities.loadDefinition`, and the host definition store must retain immutable published versions for the lifetime of instances.

Preconditions (a failed update throws a typed `WorkflowValidationError` and changes nothing):

| Code | Meaning |
| --- | --- |
| `MIGRATION_REQUIRES_SUSPENDED` | the instance must be suspended first |
| `MIGRATION_REQUIRES_IDLE` | no active human task and no active event wait; suspend does not clear active tasks |
| `MIGRATION_REQUIRES_TOP_LEVEL` | the run must be parked at a top-level node, never inside a gateway/loop/forEach/child |
| `MIGRATION_INVALID_TARGET` | target id must equal the current definition id and `toVersion` must differ from the current version |
| `MIGRATION_TARGET_MISSING` | the tracked resume node no longer exists in the target graph (the instance fails with this code) |

Mechanics and guarantees:

- the update handler validates, loads the target definition (an activity result recorded in history, so replay is deterministic), emits `workflow.migrateRequested` with `{ actorId, requestId, fromVersion, toVersion }` and cancels the run scope. The main loop then swaps the definition and emits `workflow.migrated` with `{ fromVersion, toVersion, fromNodeId, toNodeId }` **before continuing**, so the audit projection is never lost;
- the run rebases on the **tracked next top-level node id**: the same node id must exist in the target top-level `nodes`, and execution resumes there. The rebase is node-id based, never positional, so removing or renaming the resume node fails with `MIGRATION_TARGET_MISSING` instead of silently jumping to a different position;
- carried state is pruned to the target graph: `completedHumans`/`completedTasks` (and pending withdraw restore snapshots) entries whose `nodeId` no longer exists are dropped, `agreedUsers` survives (so `deduplication: ONCE` keeps working), and `returnTarget`/`returnOrigin`/`skipTo` plus any pending `resubmit` are reset because the old fallback path may be gone;
- `snapshot().definition` returns the new `{ id, version }` once the swap happened;
- a later `continueAsNew` travels with the migrated definition in `input.definition` and the rebased index in `resume.offset`, so the next Temporal run continues in the new graph;
- the current run keeps the `settings`/`limits` it started with for determinism; a `continueAsNew` after migration adopts the target version's settings in the next run.

`workflow.migrateRequested` and `workflow.migrated` form the host's migration audit trail. Never edit a published definition in place — publish a new version and migrate explicitly.

## Publishing and consumption

The package ships a dual build:

- `dist/*.js` (`main` and the `require` condition) is CommonJS and keeps the layout the existing host consumes.
- `dist/*.mjs` (the `import` condition) is ES modules, paired with `dist/*.d.mts` declarations.
- Both builds carry type declarations (`dist/*.d.ts` for CJS, `dist/*.d.mts` for ESM). `main` and `types` stay on the CJS build so legacy resolvers keep working.

Subpath exports are `.`, `./client`, `./worker`, `./workflows`, `./protocol`, `./testing`, `./codec` and `./package.json`; each maps `import`/`require` to the matching file. Deep imports into `dist/` are not part of the API. The workflow bundle entry is `dist/workflows.js`; `resolveWorkflowsPath()` returns its absolute path and is the default for `createWorkflowWorker` unless a `workflowsPath` override is passed.

`@temporalio/testing` is an optional peer dependency and only required for `wflow-core/testing`. The other `@temporalio/*` packages are regular dependencies, so hosts should run a Temporal worker version that matches them (`1.23.x` at the time of writing) to keep client/worker protocol and payload compatibility. Runtime dependencies (`@temporalio/*`, `zod`, `acorn`) are externalized rather than bundled in both builds, so there is a single copy in the host's `node_modules`.

`sideEffects: false` is safe: the workflow bundle entry is loaded by absolute path by the Temporal worker bundler rather than imported for its side effects, and every exported module only defines functions, classes and schemas.

