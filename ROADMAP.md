# wflow-core Roadmap & Scope Decisions

> What the engine can do today, what it deliberately does not do, what comes next, and how versions and replay are managed. Usage guide: [README.md](./README.md).

---

## 1. Current capabilities

### 1.1 Definition DSL

- JSON definitions (`schemaVersion: 1`) strictly validated by `parseDefinition`: duplicate ids, recursive/oversized structures, unsafe object keys, conflicting writes in concurrent branches, field-write validation.
- Human nodes: `approval` / `task` (all / any / sequential / percentage / candidate, transfer, before/after additions, timeout, deduplication, signature, field permissions, assignment resolvers).
- Automation and structural nodes: `action`, `cc`, `delay`, `delayUntil`, `wait`, `trigger` (EL / JS / HTTP / SIGNAL), `router`, `decision`, `terminate`, `child` (sync/async, variable and business-key inheritance, explicit mappings, status sync).
- Containers: `exclusive` / `inclusive` / `parallel` / `loop` / `forEach` (sequential or bounded-concurrent item iteration, `completionCondition`, explicit failure past the upper bound) / `eventGateway` (event vs. timer race, losing waits cancelled).
- Condition AST: and / or / not, exists / empty, eq / ne / gt / gte / lt / lte / in / contains, between / before / after / timeBetween, initiator dimensions, `eval` (SpEL translator / JS sandbox with a deterministic execution budget).
- Host-defined nodes: `custom` + `defineNode` / `Adapters.nodeHandlers`. Handlers always run inside the `executeNode` activity and support `resultKey` and `jumpTo`.

### 1.2 Lifecycle and history governance

- Long history via `continueAsNewAfterEvents`: triggered only at a safe **top-level** boundary; the resume state carries `offset` / `sequence` / `steps` / `commandBytes` / `agreedUsers` / `completedHumans` / `completedTasks` / `receipts` / `pendingEvents` within bounded limits. Nested containers are never interrupted.
- Idempotent start `startOrGet` (throws `INSTANCE_ALREADY_EXISTS` on identity or definition mismatch) and `startOrSignal` (delivers a signal as part of the start).
- External signals: `signal` / `waitEvent`, FIFO buffering (100 entries), `eventGateway` races by arrival order, buffered signals survive `continueAsNew`, dropped after a terminal state.
- Running-instance migration: pause -> `migrate` -> resume, rebased by top-level node id, target version must be published, audited with `workflow.migrateRequested` / `workflow.migrated`.
- Command idempotency: `requestId` fingerprint + receipt, same id with a different payload rejected, receipts carried across `continueAsNew` (200 entries).

### 1.3 Observability, security, testing and packaging

- Observability: `Adapters.telemetry.onActivity` covers the eight engine activities with start / end / error (including `durationMs` and an optional `context`). Hook errors are swallowed and never affect a workflow.
- Security: `wflow-core/codec` provides `createAesGcmPayloadCodec` (AES-256-GCM, key rotation, separate subpath so `node:crypto` never enters a workflow bundle); an `ERROR_CODES` catalog plus the `AdapterError` safety boundary.
- Testing: `wflow-core/testing` provides in-memory adapters, an `eventId`-deduplicated event log and a time-skipping `TestEngine` without an external Temporal server.
- Packaging: tsup dual build (CJS + ESM, each with type declarations); subpath exports `.` / `./client` / `./worker` / `./workflows` / `./protocol` / `./testing` / `./codec`; `test:pack` smoke-checks entry points, workflow-bundle purity and a cross-package encryption round trip.

---

## 2. Non-goals

| # | Non-goal | Decision and rationale |
|---|---|---|
| 1 | **BPMN 2.0 XML import/export** | The JSON DSL is the single canonical format; there is no BPMN consumer. If it ever becomes a requirement, an importer would need at least: a supported subset plus a rejection list (XSD / DI / Flowable semantics mapping), explicit degradation for boundary events and compensation, and round-trip regression tests. Generating XML alone has no payoff. |
| 2 | **Compensation / transaction subprocesses** | With Temporal, compensation is host-implemented as a saga (explicit compensating activities + idempotency keys + retry conventions). A BPMN transaction boundary would make the engine own a compensation stack, isolation/rollback semantics and retry policy, overlapping Temporal's retry model. |
| 3 | **Embedded subprocess scope + scoped terminate** | Only a top-level `terminate` and the standalone `child` subprocess exist today. Scoped termination needs a scope stack, variable visibility, history compatibility and replay semantics, and would break the "containers always finish" safe boundary used by continue-as-new / migration. Express it with gateway branches or `child` instead. |
| 4 | **Boundary events beyond human timeout / wait / gateway timers** | The three existing timer semantics cover the decided cases: node `timeout` (approve / reject / notify), `wait.timeoutMs` (fail / continue) and `eventGateway.timeoutMs`. Error, message and escalation boundary events need a new event-dispatch and scope model, so they are not introduced. |
| 5 | **Message correlation beyond wait / event / signal** | No correlation-id routing, topic subscription or message-broker integration. Explicit waits and races are expressed with `wait` / `eventGateway` + `signal` / `waitEvent`; broadcast and cross-instance delivery remain the host's `Adapters.integrate` responsibility. |

---

## 3. Upcoming work (by value)

1. **Metrics and deeper OpenTelemetry integration**: beyond the `telemetry` hook, provide wiring examples for Temporal Runtime metrics (Prometheus / OpenTelemetry exporters) and client/worker interceptors; propagate activity tracing context through Temporal headers instead of turning the hook into a generic telemetry bus.
2. **Multi-tenant namespace / task-queue routing**: the SDK currently assumes the caller has chosen a namespace and task queue. Provide client/worker factory helpers and configuration conventions for one-namespace-per-tenant setups to cut host boilerplate.
3. **Data retention and archiving**: Temporal retention is host-configured; add projection archiving/cleanup hooks and export guidance on the SDK side rather than implementing business retention inside the engine.
4. **Work calendars / holidays**: `delayUntil`, approval timeouts and other deadlines currently use natural durations. Inject a host calendar (working days, holidays, shifts) to compute due dates, reminders and timeouts.
5. **Richer form field types**: `Field` currently covers string / number / boolean / array / object (length, range, pattern). Extend with enum, date / datetime, duration and reference validation, staying consistent with a host form subsystem.
6. **Weighted / quorum approvals**: current modes are all / any / sequential / percentage / candidate. Weighted votes and quorum require extending the Task counting model and defining consistent semantics with additions, transfer, timeout and withdrawal.

---

## 4. Versioning, replay and migration

### 4.1 Worker deployment versions

- Use Temporal's Worker Versioning: validate a new version on a separate task queue or with low traffic, then gradually switch `current`; to roll back, shift traffic back to the old deployment instead of rolling back event history.
- Govern business definitions and engine code separately: a definition's `{ id, version }` is pinned in the workflow input, and only `engine.migrate` intentionally moves running instances to a new version.

### 4.2 Replay regression

- `pnpm test:integration` executes and replays scenarios on a `TestWorkflowEnvironment`, covering continue-as-new, signal buffering, eventGateway races, forEach, custom nodes and migration.
- After changing interpreter code (`workflow.ts`, `nodes/*`, `resume.ts`, ...) always run the integration suite; before a production change, also replay real histories offline with Temporal's replay support to confirm there is no non-determinism.
- When adding definition capabilities, also add "old runs still replay" cases instead of only testing the new feature.

### 4.3 `patched` policy for interpreter changes

- For interpreter behavior changes that cannot be solved by publishing a new definition version, use `patched('code-change-id')` to keep both the old and new paths, then `deprecatePatch` once all old runs have finished.
- Definition changes always publish a new version and explicitly migrate running instances with `engine.migrate`; never edit a published definition in place.

### 4.4 Migration runbook

Preconditions, node-id-based rebase rules, carried-state pruning and audit events for pause -> migrate -> resume are documented in the README section "Migrating running instances to a new definition version".
