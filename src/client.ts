import { Client, WorkflowExecutionAlreadyStartedError, WorkflowIdConflictPolicy, WorkflowIdReusePolicy, WorkflowNotFoundError, type WorkflowExecutionInfo } from "@temporalio/client";
import type { Duration, SearchAttributes } from "@temporalio/common";
import { createHash, randomUUID } from "node:crypto";
import { z } from "zod";
import { assertJsonSize, canonical, parseDefinition, validateFields } from "./definition.js";
import { WorkflowValidationError } from "./errors.js";
import type { Adapters } from "./ports.js";
import { COMMAND_UPDATE, SNAPSHOT_QUERY, WORKFLOW_EVENT_SIGNAL, WORKFLOW_TYPE, parseWorkflowId, workflowId } from "./protocol.js";
import { commandSchema, dataSchema, eventSignalSchema, idSchema, inputSchema, jsonSchema, type Command, type Data, type EventSignal, type Identity, type Json, type Snapshot, type WorkflowInput, type WorkflowResult } from "./schema.js";

export type WorkflowClientOptions = { client: Client; taskQueue: string; adapters: Pick<Adapters, "authorize"> };

/** Host options forwarded per start call; omitted fields keep the SDK defaults. */
export type StartOptions = {
  taskQueue?: string;
  searchAttributes?: SearchAttributes;
  memo?: Record<string, unknown>;
  workflowExecutionTimeout?: Duration;
  workflowRunTimeout?: Duration;
  workflowTaskTimeout?: Duration;
  workflowIdConflictPolicy?: WorkflowIdConflictPolicy;
  workflowIdReusePolicy?: WorkflowIdReusePolicy;
};

/** Options for {@link WorkflowEngineClient.command}; `updateTimeout` is forwarded to Temporal's update options. */
export type CommandOptions = { updateTimeout?: Duration };

/** Start outcome; `existing` is true only when {@link WorkflowEngineClient.startOrGet} matched a running instance. */
export type StartResult = { workflowId: string; result: () => Promise<WorkflowResult>; existing: boolean };

/** A stable, JSON-serializable projection of a Temporal visibility entry. */
export type WorkflowListItem = {
  workflowId: string;
  instanceId: string | null;
  runId: string;
  status: string;
  startTime: string;
  closeTime: string | null;
  taskQueue: string;
  historyLength: number;
  memo: Record<string, unknown>;
  searchAttributes: Record<string, unknown>;
};

/** `list` returns an async iterable: `for await (const workflow of await engine.list({ ... }))`. */
export type WorkflowList = AsyncIterable<WorkflowListItem>;

export type WorkflowListOptions = { tenantId: string; actorId: string; query?: string; pageSize?: number };
export type WorkflowCountOptions = { tenantId: string; actorId: string; query?: string };
export type WorkflowCount = { count: number };

/** A stable, JSON-serializable description of one execution. */
export type WorkflowDescription = {
  workflowId: string;
  instanceId: string;
  runId: string;
  status: string;
  isRunning: boolean;
  historyLength: number;
  startTime: string;
  closeTime: string | null;
  taskQueue: string;
  pendingActivityCount: number;
  pendingActivities: { activityId: string; type: string; state: number; attempt: number }[];
};

export type WaitEventInput = {
  tenantId: string;
  actorId: string;
  instanceId: string;
  event: string;
  data?: Json;
  requestId?: string;
};

/** Input for {@link WorkflowEngineClient.signal}; `event` is bounded to 1-128 characters. */
export type SignalInput = Identity & { instanceId: string; event: string; data?: Json };

/** Input for {@link WorkflowEngineClient.migrate}; `toVersion` is the published target version. */
export type MigrateInput = Identity & { instanceId: string; requestId: string; toVersion: number; comment?: Data };

const listOptionsSchema = z.strictObject({
  tenantId: idSchema, actorId: idSchema, query: z.string().max(10_000).optional(), pageSize: z.number().int().min(1).max(1000).optional(),
});
const countOptionsSchema = z.strictObject({ tenantId: idSchema, actorId: idSchema, query: z.string().max(10_000).optional() });
const waitEventSchema = z.strictObject({
  tenantId: idSchema, actorId: idSchema, instanceId: idSchema, event: idSchema, data: jsonSchema.optional(), requestId: idSchema.optional(),
});
const signalInputSchema = z.strictObject({ tenantId: idSchema, actorId: idSchema, instanceId: idSchema, ...eventSignalSchema.shape });
const migrateInputSchema = z.strictObject({
  tenantId: idSchema, actorId: idSchema, instanceId: idSchema, requestId: idSchema, toVersion: z.number().int().min(1), comment: dataSchema.optional(),
});

/** Converts Temporal values (Dates, arrays, plain objects) into a JSON-serializable shape. */
function plain(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();
  if (Array.isArray(value)) return value.map(plain);
  if (value !== null && typeof value === "object") return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, plain(child)]));
  return value;
}
function plainRecord(value: unknown): Record<string, unknown> {
  const record = plain(value);
  return record !== null && typeof record === "object" && !Array.isArray(record) ? record as Record<string, unknown> : {};
}

function toWorkflowListItem(info: WorkflowExecutionInfo): WorkflowListItem {
  return {
    workflowId: info.workflowId,
    instanceId: parseWorkflowId(info.workflowId)?.instanceId ?? null,
    runId: info.runId,
    status: info.status.name,
    startTime: info.startTime.toISOString(),
    closeTime: info.closeTime ? info.closeTime.toISOString() : null,
    taskQueue: info.taskQueue,
    historyLength: info.historyLength,
    memo: plainRecord(info.memo),
    searchAttributes: plainRecord(info.searchAttributes),
  };
}

export class WorkflowEngineClient {
  constructor(private readonly options: WorkflowClientOptions) {}

  async start(input: WorkflowInput, options: StartOptions = {}): Promise<StartResult> {
    assertJsonSize(input);
    const parsed = inputSchema.parse({ ...input, definition: parseDefinition(input.definition) });
    validateFields(parsed.definition.inputFields ?? [], parsed.data, false);
    const id = workflowId(parsed.tenantId, parsed.instanceId);
    const allowed = await this.options.adapters.authorize({
      tenantId: parsed.tenantId, actorId: parsed.initiatorId, operation: "start", instanceId: parsed.instanceId,
      businessKey: parsed.businessKey, definition: { id: parsed.definition.id, version: parsed.definition.version },
    });
    if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
    const handle = await this.options.client.workflow.start(WORKFLOW_TYPE, {
      taskQueue: options.taskQueue ?? this.options.taskQueue, workflowId: id,
      args: [parsed],
      // FAIL + REJECT_DUPLICATE stay the defaults so existing start semantics do not change.
      workflowIdConflictPolicy: options.workflowIdConflictPolicy ?? WorkflowIdConflictPolicy.FAIL,
      workflowIdReusePolicy: options.workflowIdReusePolicy ?? WorkflowIdReusePolicy.REJECT_DUPLICATE,
      ...(options.searchAttributes === undefined ? {} : { searchAttributes: options.searchAttributes }),
      ...(options.memo === undefined ? {} : { memo: options.memo }),
      ...(options.workflowExecutionTimeout === undefined ? {} : { workflowExecutionTimeout: options.workflowExecutionTimeout }),
      ...(options.workflowRunTimeout === undefined ? {} : { workflowRunTimeout: options.workflowRunTimeout }),
      ...(options.workflowTaskTimeout === undefined ? {} : { workflowTaskTimeout: options.workflowTaskTimeout }),
    });
    return { workflowId: id, result: () => handle.result(), existing: false };
  }

  /**
   * Idempotent start: a matching existing instance is returned with `existing: true` instead of
   * failing the duplicate. The read authorization runs before the snapshot lookup, and the
   * snapshot must match tenantId, businessKey, initiatorId and the pinned definition reference;
   * anything else throws INSTANCE_ALREADY_EXISTS. `WorkflowIdConflictPolicy.USE_EXISTING` is
   * deliberately not used because it would silently accept mismatched inputs.
   */
  async startOrGet(input: WorkflowInput, options: StartOptions = {}): Promise<StartResult> {
    try {
      return await this.start(input, options);
    } catch (error) {
      if (!isWorkflowAlreadyStarted(error)) throw error;
    }
    assertJsonSize(input);
    const parsed = inputSchema.parse({ ...input, definition: parseDefinition(input.definition) });
    validateFields(parsed.definition.inputFields ?? [], parsed.data, false);
    const id = workflowId(parsed.tenantId, parsed.instanceId);
    const allowed = await this.options.adapters.authorize({
      tenantId: parsed.tenantId, actorId: parsed.initiatorId, operation: "read", instanceId: parsed.instanceId,
      businessKey: parsed.businessKey, definition: { id: parsed.definition.id, version: parsed.definition.version },
    });
    if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
    const handle = this.options.client.workflow.getHandle(id);
    const snapshot = await handle.query<Snapshot>(SNAPSHOT_QUERY);
    if (snapshot.tenantId !== parsed.tenantId || snapshot.businessKey !== parsed.businessKey || snapshot.initiatorId !== parsed.initiatorId
      || snapshot.definition.id !== parsed.definition.id || snapshot.definition.version !== parsed.definition.version) {
      throw new WorkflowValidationError("INSTANCE_ALREADY_EXISTS");
    }
    return { workflowId: id, result: () => handle.result(), existing: true };
  }

  /**
   * Starts the instance, or delivers the external event to an already running one, through
   * Temporal's `signalWithStart`. Both `start` and `signal` are authorized before the call.
   * `existing` is always false: Temporal does not report whether the signal reached an existing
   * run, so use `startOrGet` when that distinction matters.
   */
  async startOrSignal(input: WorkflowInput, signal: EventSignal, options: StartOptions = {}): Promise<StartResult> {
    assertJsonSize(input);
    assertJsonSize(signal);
    const parsed = inputSchema.parse({ ...input, definition: parseDefinition(input.definition) });
    validateFields(parsed.definition.inputFields ?? [], parsed.data, false);
    const payload = eventSignalSchema.parse(signal);
    const id = workflowId(parsed.tenantId, parsed.instanceId);
    const scope = { tenantId: parsed.tenantId, actorId: parsed.initiatorId, instanceId: parsed.instanceId,
      businessKey: parsed.businessKey, definition: { id: parsed.definition.id, version: parsed.definition.version } };
    const startAllowed = await this.options.adapters.authorize({ ...scope, operation: "start" });
    if (!startAllowed) throw new WorkflowValidationError("FORBIDDEN");
    const signalAllowed = await this.options.adapters.authorize({ ...scope, operation: "signal" });
    if (!signalAllowed) throw new WorkflowValidationError("FORBIDDEN");
    const handle = await this.options.client.workflow.signalWithStart(WORKFLOW_TYPE, {
      taskQueue: options.taskQueue ?? this.options.taskQueue, workflowId: id,
      args: [parsed], signal: WORKFLOW_EVENT_SIGNAL, signalArgs: [payload],
      // USE_EXISTING is the point of signalWithStart: deliver to the running run instead of failing.
      workflowIdConflictPolicy: options.workflowIdConflictPolicy ?? WorkflowIdConflictPolicy.USE_EXISTING,
      workflowIdReusePolicy: options.workflowIdReusePolicy ?? WorkflowIdReusePolicy.REJECT_DUPLICATE,
      ...(options.searchAttributes === undefined ? {} : { searchAttributes: options.searchAttributes }),
      ...(options.memo === undefined ? {} : { memo: options.memo }),
      ...(options.workflowExecutionTimeout === undefined ? {} : { workflowExecutionTimeout: options.workflowExecutionTimeout }),
      ...(options.workflowRunTimeout === undefined ? {} : { workflowRunTimeout: options.workflowRunTimeout }),
      ...(options.workflowTaskTimeout === undefined ? {} : { workflowTaskTimeout: options.workflowTaskTimeout }),
    });
    return { workflowId: id, result: () => handle.result(), existing: false };
  }

  /**
   * Lists workflow executions through Temporal visibility. The host authorization runs first with
   * operation "list"; instanceId is not part of the request. The query string is passed through
   * unchanged, so hosts must scope it to their tenant (or authorize "list" accordingly).
   */
  async list(options: WorkflowListOptions): Promise<WorkflowList> {
    const parsed = listOptionsSchema.parse(options);
    const allowed = await this.options.adapters.authorize({ tenantId: parsed.tenantId, actorId: parsed.actorId, operation: "list" });
    if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
    const source = this.options.client.workflow.list({
      ...(parsed.query === undefined ? {} : { query: parsed.query }),
      ...(parsed.pageSize === undefined ? {} : { pageSize: parsed.pageSize }),
    });
    return (async function* () { for await (const info of source) yield toWorkflowListItem(info); })();
  }

  /** Counts workflow executions matching a visibility query; authorize "list" runs first. */
  async count(options: WorkflowCountOptions): Promise<WorkflowCount> {
    const parsed = countOptionsSchema.parse(options);
    const allowed = await this.options.adapters.authorize({ tenantId: parsed.tenantId, actorId: parsed.actorId, operation: "list" });
    if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
    const result = await this.options.client.workflow.count(parsed.query ?? "");
    return { count: result.count };
  }

  /** Describes one execution; authorize "read" runs before the Temporal describe call. */
  async describe(identity: Identity & { instanceId: string }): Promise<WorkflowDescription> {
    const parsed = z.strictObject({ tenantId: idSchema, actorId: idSchema, instanceId: idSchema }).parse(identity) as Identity & { instanceId: string };
    const allowed = await this.options.adapters.authorize({ tenantId: parsed.tenantId, actorId: parsed.actorId, operation: "read", instanceId: parsed.instanceId });
    if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
    const description = await this.options.client.workflow.getHandle(workflowId(parsed.tenantId, parsed.instanceId)).describe();
    const pending = description.raw.pendingActivities ?? [];
    return {
      workflowId: description.workflowId,
      instanceId: parsed.instanceId,
      runId: description.runId,
      status: description.status.name,
      isRunning: description.status.name === "RUNNING",
      historyLength: description.historyLength,
      startTime: description.startTime.toISOString(),
      closeTime: description.closeTime ? description.closeTime.toISOString() : null,
      taskQueue: description.taskQueue,
      pendingActivityCount: pending.length,
      pendingActivities: pending.map((activity) => ({
        activityId: activity.activityId ?? "", type: activity.activityType?.name ?? "",
        state: activity.state ?? 0, attempt: activity.attempt ?? 0,
      })),
    };
  }

  /** Command passthrough; `updateTimeout` is forwarded in Temporal's update options. */
  async command(command: Command, options: CommandOptions = {}): Promise<{ requestId: string; eventId: string }> {
    assertJsonSize(command);
    const parsed = commandSchema.parse(command);
    const allowed = await this.options.adapters.authorize({
      tenantId: parsed.tenantId, actorId: parsed.actorId, operation: "command", instanceId: this.instanceIdFromCommand(parsed), command: parsed,
    });
    if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
    const handle = this.options.client.workflow.getHandle(workflowId(parsed.tenantId, this.instanceIdFromCommand(parsed)));
    const digest = createHash('sha256').update(canonical(parsed)).digest('hex');
    const updateOptions: { args: [Command]; updateId: string; updateTimeout?: Duration } = { args: [parsed], updateId: `${parsed.requestId}:${digest}` };
    if (options.updateTimeout !== undefined) updateOptions.updateTimeout = options.updateTimeout;
    return handle.executeUpdate<{ requestId: string; eventId: string }, [Command]>(COMMAND_UPDATE, updateOptions);
  }

  /**
   * Migrates a running instance to a published definition version. The host must publish the
   * target version first; the engine loads it through `activities.loadDefinition` and only accepts
   * the command while the instance is suspended and idle (see the runbook in README).
   */
  async migrate(input: MigrateInput): Promise<{ requestId: string; eventId: string }> {
    const parsed = migrateInputSchema.parse(input);
    return this.command({
      type: "migrate", requestId: parsed.requestId, tenantId: parsed.tenantId, instanceId: parsed.instanceId,
      actorId: parsed.actorId, toVersion: parsed.toVersion, ...(parsed.comment === undefined ? {} : { comment: parsed.comment }),
    });
  }

  async snapshot(identity: Identity & { instanceId: string }): Promise<Snapshot> {
    const parsed = z.strictObject({ tenantId: idSchema, actorId: idSchema, instanceId: idSchema }).parse(identity) as Identity & { instanceId: string };
    const handle = this.options.client.workflow.getHandle(workflowId(parsed.tenantId, parsed.instanceId));
    const snapshot = await handle.query<Snapshot>(SNAPSHOT_QUERY);
    const allowed = await this.options.adapters.authorize({ tenantId: parsed.tenantId, actorId: parsed.actorId, operation: "read", instanceId: parsed.instanceId, businessKey: snapshot.businessKey, definition: snapshot.definition });
    if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
    return snapshot;
  }

  /**
   * Signals the active `wait` node whose `event` matches, hiding the snapshot lookup and the
   * waitId handshake. Throws WAIT_NOT_ACTIVE when no wait matches, so callers can retry later.
   */
  async waitEvent(input: WaitEventInput): Promise<{ requestId: string; eventId: string }> {
    const parsed = waitEventSchema.parse(input);
    const snapshot = await this.snapshot({ tenantId: parsed.tenantId, actorId: parsed.actorId, instanceId: parsed.instanceId });
    const wait = snapshot.waits.find((entry) => entry.event === parsed.event);
    if (!wait) throw new WorkflowValidationError("WAIT_NOT_ACTIVE");
    return this.command({
      type: "event", requestId: parsed.requestId ?? randomUUID(), tenantId: parsed.tenantId, instanceId: parsed.instanceId,
      actorId: parsed.actorId, waitId: wait.id, event: parsed.event, data: parsed.data ?? null,
    });
  }

  /**
   * Delivers an external event through the `WORKFLOW_EVENT_SIGNAL` Temporal signal. An active wait
   * whose `event` matches receives it, otherwise the workflow buffers it until a wait matches (up
   * to 100 oldest-dropped). `signal` is authorized first; hosts may restrict it more than commands.
   */
  async signal(input: SignalInput): Promise<void> {
    assertJsonSize(input);
    const parsed = signalInputSchema.parse(input);
    const allowed = await this.options.adapters.authorize({ tenantId: parsed.tenantId, actorId: parsed.actorId, operation: "signal", instanceId: parsed.instanceId });
    if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
    const handle = this.options.client.workflow.getHandle(workflowId(parsed.tenantId, parsed.instanceId));
    await handle.signal(WORKFLOW_EVENT_SIGNAL, parsed.data === undefined ? { event: parsed.event } : { event: parsed.event, data: parsed.data });
  }

  private instanceIdFromCommand(command: Command): string {
    return command.instanceId;
  }
}

/** True for the Temporal error raised when an execution does not exist (or its id was never used). */
export function isWorkflowNotFound(error: unknown): error is WorkflowNotFoundError {
  return error instanceof WorkflowNotFoundError || (error instanceof Error && error.name === "WorkflowNotFoundError");
}

/** True for the Temporal conflict raised when the workflow id is already running or reused. */
export function isWorkflowAlreadyStarted(error: unknown): error is WorkflowExecutionAlreadyStartedError {
  return error instanceof WorkflowExecutionAlreadyStartedError
    || (error instanceof Error && error.name === "WorkflowExecutionAlreadyStartedError");
}

export function createWorkflowClient(options: WorkflowClientOptions): WorkflowEngineClient { return new WorkflowEngineClient(options); }
