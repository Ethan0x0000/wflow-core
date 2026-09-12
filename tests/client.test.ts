import { WorkflowExecutionAlreadyStartedError, WorkflowIdConflictPolicy, WorkflowIdReusePolicy, type Client } from "@temporalio/client";
import { describe, expect, it } from "vitest";
import { WorkflowEngineClient } from "../src/client.js";
import { parseDefinition } from "../src/definition.js";
import { WorkflowValidationError } from "../src/errors.js";
import { parseWorkflowId, WORKFLOW_EVENT_SIGNAL, workflowId } from "../src/protocol.js";
import type { Command, Snapshot } from "../src/schema.js";

type Recorded = {
  start: Record<string, unknown>[];
  signalWithStarts: Record<string, unknown>[];
  signals: { name: string; payload: unknown }[];
  list: { query?: string; pageSize?: number }[];
  counts: string[];
  updates: Record<string, unknown>[];
  authorize: { tenantId: string; actorId: string; operation: string; instanceId?: string }[];
};

function harness(options: { allow?: boolean; waits?: Snapshot["waits"]; conflict?: boolean; read?: boolean; deny?: string[]; snapshot?: Partial<Snapshot> } = {}) {
  const recorded: Recorded = { start: [], signalWithStarts: [], signals: [], list: [], counts: [], updates: [], authorize: [] };
  const snapshot: Snapshot = {
    tenantId: "tenant", instanceId: "instance", businessKey: "instance", definition: { id: "definition", version: 1 },
    initiatorId: "employee", status: "running", tasks: [], waits: options.waits ?? [], data: {}, suspended: false, returnTargets: [],
    ...options.snapshot,
  };
  const item = {
    workflowId: workflowId("tenant", "instance"), runId: "run-1", type: "genericWorkflowV1", taskQueue: "queue",
    status: { code: 1, name: "RUNNING" }, historyLength: 12,
    startTime: new Date("2026-01-01T00:00:00.000Z"), closeTime: undefined,
    memo: { note: "hello" }, searchAttributes: { Custom: "value" },
  };
  const handle = {
    async query() { return snapshot; },
    async result() { return { status: "completed" as const, data: {} }; },
    async signal(name: string, payload: unknown) { recorded.signals.push({ name, payload }); },
    async executeUpdate(_name: string, updateOptions: Record<string, unknown>) { recorded.updates.push(updateOptions); return { requestId: "request-1", eventId: "event-1" }; },
    async describe() {
      return {
        workflowId: workflowId("tenant", "instance"), runId: "run-1", taskQueue: "queue", status: { code: 1, name: "RUNNING" },
        historyLength: 12, startTime: new Date("2026-01-01T00:00:00.000Z"), closeTime: undefined, memo: { note: "hello" }, searchAttributes: {},
        raw: { pendingActivities: [{ activityId: "activity-1", activityType: { name: "executeAction" }, state: 1, attempt: 2 }] },
      };
    },
  };
  const client = {
    workflow: {
      async start(_type: string, startOptions: Record<string, unknown>) {
        recorded.start.push(startOptions);
        if (options.conflict) throw new WorkflowExecutionAlreadyStartedError("already started", String(startOptions.workflowId), "genericWorkflowV1");
        return { workflowId: String(startOptions.workflowId), result: async () => ({ status: "completed", data: {} }) };
      },
      async signalWithStart(_type: string, startOptions: Record<string, unknown>) {
        recorded.signalWithStarts.push(startOptions);
        return { workflowId: String(startOptions.workflowId), result: async () => ({ status: "completed", data: {} }) };
      },
      getHandle() { return handle; },
      list(listOptions: { query?: string; pageSize?: number }) {
        recorded.list.push(listOptions);
        return { [Symbol.asyncIterator]: async function* () { yield item; } };
      },
      async count(query: string) { recorded.counts.push(query); return { count: 2, groups: [] }; },
    },
  } as unknown as Client;
  const adapters = {
    async authorize(request: Recorded["authorize"][number]) {
      recorded.authorize.push(request);
      if (options.read === false && request.operation === "read") return false;
      if (options.deny?.includes(request.operation)) return false;
      return options.allow ?? true;
    },
  };
  return { recorded, engine: new WorkflowEngineClient({ client, taskQueue: "queue", adapters }) };
}

const definition = parseDefinition({ schemaVersion: 1, id: "definition", version: 1, name: "Definition", nodes: [{ id: "wait", type: "wait", event: "go" }] });
const input = { tenantId: "tenant", instanceId: "instance", businessKey: "instance", initiatorId: "employee", definition, data: {} };

describe("workflow engine client start", () => {
  it("forwards host start options and keeps FAIL/REJECT_DUPLICATE defaults", async () => {
    const { recorded, engine } = harness();
    await engine.start(input, {
      taskQueue: "other-queue", memo: { source: "test" }, searchAttributes: { Custom: ["x"] },
      workflowExecutionTimeout: "1 hour", workflowRunTimeout: 60_000, workflowTaskTimeout: "5 seconds",
      workflowIdConflictPolicy: WorkflowIdConflictPolicy.USE_EXISTING, workflowIdReusePolicy: WorkflowIdReusePolicy.ALLOW_DUPLICATE,
    });
    expect(recorded.start[0]).toMatchObject({
      taskQueue: "other-queue", memo: { source: "test" }, searchAttributes: { Custom: ["x"] },
      workflowExecutionTimeout: "1 hour", workflowRunTimeout: 60_000, workflowTaskTimeout: "5 seconds",
      workflowIdConflictPolicy: "USE_EXISTING", workflowIdReusePolicy: "ALLOW_DUPLICATE",
      args: [expect.objectContaining({ instanceId: "instance" })],
    });
    await engine.start(input);
    expect(recorded.start[1]).toMatchObject({ taskQueue: "queue", workflowIdConflictPolicy: "FAIL", workflowIdReusePolicy: "REJECT_DUPLICATE" });
    expect(recorded.start[1]).not.toHaveProperty("memo");
    expect(recorded.start[1]).not.toHaveProperty("searchAttributes");
  });
});

describe("workflow engine client startOrGet", () => {
  it("returns existing:false for a fresh start and existing:true for a matching instance", async () => {
    const fresh = await harness().engine.startOrGet(input);
    expect(fresh).toMatchObject({ workflowId: "wf:6:tenant:instance", existing: false });

    const { recorded, engine } = harness({ conflict: true });
    const existing = await engine.startOrGet(input);
    expect(existing).toMatchObject({ workflowId: "wf:6:tenant:instance", existing: true });
    expect(recorded.authorize.at(-1)).toMatchObject({ operation: "read", actorId: "employee", instanceId: "instance" });
    expect(await existing.result()).toEqual({ status: "completed", data: {} });
  });

  it("rejects mismatched existing instances with INSTANCE_ALREADY_EXISTS", async () => {
    await expect(harness({ conflict: true, snapshot: { businessKey: "other" } }).engine.startOrGet(input))
      .rejects.toThrowError(new WorkflowValidationError("INSTANCE_ALREADY_EXISTS"));
    await expect(harness({ conflict: true, snapshot: { definition: { id: "definition", version: 2 } } }).engine.startOrGet(input))
      .rejects.toThrowError(new WorkflowValidationError("INSTANCE_ALREADY_EXISTS"));
    await expect(harness({ conflict: true, snapshot: { initiatorId: "other" } }).engine.startOrGet(input))
      .rejects.toThrowError(new WorkflowValidationError("INSTANCE_ALREADY_EXISTS"));
  });

  it("keeps the read authorization gate before returning an existing instance", async () => {
    await expect(harness({ conflict: true, read: false }).engine.startOrGet(input))
      .rejects.toThrowError(new WorkflowValidationError("FORBIDDEN"));
  });
});

describe("workflow engine client visibility", () => {
  it("authorizes list before mapping visibility results to a stable shape", async () => {
    const { recorded, engine } = harness();
    const workflows = await engine.list({ tenantId: "tenant", actorId: "employee", query: 'ExecutionStatus = "Running"', pageSize: 25 });
    expect(recorded.authorize[0]).toMatchObject({ tenantId: "tenant", actorId: "employee", operation: "list" });
    expect(recorded.list[0]).toEqual({ query: 'ExecutionStatus = "Running"', pageSize: 25 });
    const items = [];
    for await (const workflow of workflows) items.push(workflow);
    expect(items).toEqual([{
      workflowId: "wf:6:tenant:instance", instanceId: "instance", runId: "run-1", status: "RUNNING",
      startTime: "2026-01-01T00:00:00.000Z", closeTime: null, taskQueue: "queue", historyLength: 12,
      memo: { note: "hello" }, searchAttributes: { Custom: "value" },
    }]);
  });

  it("does not touch visibility when list is not authorized", async () => {
    const { recorded, engine } = harness({ allow: false });
    await expect(engine.list({ tenantId: "tenant", actorId: "employee" })).rejects.toThrowError(new WorkflowValidationError("FORBIDDEN"));
    await expect(engine.count({ tenantId: "tenant", actorId: "employee" })).rejects.toThrowError(new WorkflowValidationError("FORBIDDEN"));
    expect(recorded.list).toEqual([]);
    expect(recorded.counts).toEqual([]);
  });

  it("counts through visibility after authorizing list", async () => {
    const { recorded, engine } = harness();
    await expect(engine.count({ tenantId: "tenant", actorId: "employee", query: 'ExecutionStatus = "Running"' })).resolves.toEqual({ count: 2 });
    await engine.count({ tenantId: "tenant", actorId: "employee" });
    expect(recorded.counts).toEqual(['ExecutionStatus = "Running"', ""]);
    expect(recorded.authorize[1]).toMatchObject({ operation: "list" });
  });

  it("describes with an authorize read check and a stable shape", async () => {
    const { recorded, engine } = harness();
    const description = await engine.describe({ tenantId: "tenant", actorId: "employee", instanceId: "instance" });
    expect(recorded.authorize[0]).toMatchObject({ operation: "read", instanceId: "instance" });
    expect(description).toEqual({
      workflowId: "wf:6:tenant:instance", instanceId: "instance", runId: "run-1", status: "RUNNING", isRunning: true,
      historyLength: 12, startTime: "2026-01-01T00:00:00.000Z", closeTime: null, taskQueue: "queue",
      pendingActivityCount: 1, pendingActivities: [{ activityId: "activity-1", type: "executeAction", state: 1, attempt: 2 }],
    });
  });
});

describe("workflow engine client commands", () => {
  it("finds the active wait and reuses the event command", async () => {
    const { recorded, engine } = harness({ waits: [{ id: "wait-n1", nodeId: "wait", event: "go" }] });
    const receipt = await engine.waitEvent({ tenantId: "tenant", actorId: "employee", instanceId: "instance", event: "go", data: { ok: true } });
    expect(receipt).toEqual({ requestId: "request-1", eventId: "event-1" });
    const sent = (recorded.updates[0]!.args as [{ requestId: string; waitId: string; event: string; data: unknown }])[0]!;
    expect(sent.waitId).toBe("wait-n1");
    expect(sent.event).toBe("go");
    expect(sent.data).toEqual({ ok: true });
    expect(sent.requestId.length).toBeGreaterThan(0);
  });

  it("rejects with WAIT_NOT_ACTIVE when no wait matches", async () => {
    const { recorded, engine } = harness();
    await expect(engine.waitEvent({ tenantId: "tenant", actorId: "employee", instanceId: "instance", event: "missing" }))
      .rejects.toThrowError(new WorkflowValidationError("WAIT_NOT_ACTIVE"));
    expect(recorded.updates).toEqual([]);
  });

  it("forwards a migrate command through the authorized command path", async () => {
    const { recorded, engine } = harness();
    const receipt = await engine.migrate({ tenantId: "tenant", actorId: "employee", instanceId: "instance", requestId: "migrate-1", toVersion: 3, comment: { text: "upgrade" } });
    expect(receipt).toEqual({ requestId: "request-1", eventId: "event-1" });
    expect(recorded.authorize[0]).toMatchObject({ operation: "command", tenantId: "tenant", actorId: "employee" });
    expect((recorded.authorize[0] as { command?: Command }).command).toMatchObject({ type: "migrate", requestId: "migrate-1", toVersion: 3 });
    const sent = (recorded.updates[0]!.args as [Command])[0]!;
    expect(sent).toMatchObject({ type: "migrate", tenantId: "tenant", instanceId: "instance", actorId: "employee", requestId: "migrate-1", toVersion: 3, comment: { text: "upgrade" } });
    await expect(engine.migrate({ tenantId: "tenant", actorId: "employee", instanceId: "instance", requestId: "migrate-2", toVersion: 0 })).rejects.toThrow();
    expect(recorded.updates).toHaveLength(1);
  });

  it("forwards the command update timeout when provided", async () => {
    const { recorded, engine } = harness();
    const command = { type: "cancel" as const, requestId: "request-1", tenantId: "tenant", instanceId: "instance", actorId: "employee" };
    await engine.command(command, { updateTimeout: "30 seconds" });
    await engine.command({ ...command, requestId: "request-2" });
    expect(recorded.updates[0]).toMatchObject({ updateTimeout: "30 seconds" });
    expect(recorded.updates[1]).not.toHaveProperty("updateTimeout");
  });
});

describe("workflow engine client signals", () => {
  it("authorizes operation signal before sending the Temporal signal", async () => {
    const { recorded, engine } = harness();
    await engine.signal({ tenantId: "tenant", actorId: "employee", instanceId: "instance", event: "go", data: { ok: true } });
    expect(recorded.authorize[0]).toMatchObject({ tenantId: "tenant", actorId: "employee", operation: "signal", instanceId: "instance" });
    expect(recorded.signals).toEqual([{ name: WORKFLOW_EVENT_SIGNAL, payload: { event: "go", data: { ok: true } } }]);
  });

  it("omits absent data and keeps the authorization gate before signaling", async () => {
    const { recorded, engine } = harness();
    await engine.signal({ tenantId: "tenant", actorId: "employee", instanceId: "instance", event: "go" });
    expect(recorded.signals[0]!.payload).toEqual({ event: "go" });
    const denied = harness({ deny: ["signal"] });
    await expect(denied.engine.signal({ tenantId: "tenant", actorId: "employee", instanceId: "instance", event: "go" }))
      .rejects.toThrowError(new WorkflowValidationError("FORBIDDEN"));
    expect(denied.recorded.signals).toEqual([]);
  });

  it("validates the event name and JSON payload", async () => {
    const { recorded, engine } = harness();
    await expect(engine.signal({ tenantId: "tenant", actorId: "employee", instanceId: "instance", event: "" })).rejects.toThrow();
    await expect(engine.signal({ tenantId: "tenant", actorId: "employee", instanceId: "instance", event: "x".repeat(129) })).rejects.toThrow();
    await expect(engine.signal({ tenantId: "tenant", actorId: "employee", instanceId: "instance", event: "go", extra: true } as never)).rejects.toThrow();
    expect(recorded.signals).toEqual([]);
  });
});

describe("workflow engine client startOrSignal", () => {
  it("authorizes start then signal and forwards the signal with start options", async () => {
    const { recorded, engine } = harness();
    const started = await engine.startOrSignal(input, { event: "go", data: { ok: true } }, { memo: { source: "test" } });
    expect(started).toMatchObject({ workflowId: "wf:6:tenant:instance", existing: false });
    expect(recorded.authorize.map((entry) => entry.operation)).toEqual(["start", "signal"]);
    expect(recorded.signalWithStarts[0]).toMatchObject({
      workflowId: "wf:6:tenant:instance", taskQueue: "queue", memo: { source: "test" },
      signal: WORKFLOW_EVENT_SIGNAL, signalArgs: [{ event: "go", data: { ok: true } }],
      workflowIdConflictPolicy: "USE_EXISTING", workflowIdReusePolicy: "REJECT_DUPLICATE",
      args: [expect.objectContaining({ instanceId: "instance" })],
    });
    expect(await started.result()).toEqual({ status: "completed", data: {} });
  });

  it("does not touch Temporal when start or signal is denied", async () => {
    const startDenied = harness({ deny: ["start"] });
    await expect(startDenied.engine.startOrSignal(input, { event: "go" }))
      .rejects.toThrowError(new WorkflowValidationError("FORBIDDEN"));
    expect(startDenied.recorded.signalWithStarts).toEqual([]);
    const signalDenied = harness({ deny: ["signal"] });
    await expect(signalDenied.engine.startOrSignal(input, { event: "go" }))
      .rejects.toThrowError(new WorkflowValidationError("FORBIDDEN"));
    expect(signalDenied.recorded.signalWithStarts).toEqual([]);
  });

  it("validates the start input and signal payload before authorizing", async () => {
    const { recorded, engine } = harness();
    await expect(engine.startOrSignal({ ...input, businessKey: "" }, { event: "go" })).rejects.toThrow();
    await expect(engine.startOrSignal(input, { event: "go", extra: true } as never)).rejects.toThrow();
    expect(recorded.authorize).toEqual([]);
    expect(recorded.signalWithStarts).toEqual([]);
  });
});

describe("workflow id helpers", () => {
  it("round-trips tenant-scoped ids used by list mapping", () => {
    expect(parseWorkflowId(workflowId("tenant:with:colons", "instance"))).toEqual({ tenantId: "tenant:with:colons", instanceId: "instance" });
  });
});
