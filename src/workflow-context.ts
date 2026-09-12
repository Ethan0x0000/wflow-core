import type { CancellationScope } from "@temporalio/workflow";
import { assertJsonSize } from "./definition.js";
import { eventSignalSchema, jsonSchema, type CommandReceipt, type Data, type Definition, type HumanNode, type InitiatorContext, type Json, type Listener, type Node, type Settings, type Status, type SyncEvent, type Task, type Wait, type WorkflowEvent, type WorkflowInput, type WorkflowResult } from "./schema.js";
import type { InstanceContext } from "./ports.js";
import { activityTuning } from "./settings.js";
import { activities } from "./workflow-activities.js";

class Rejected extends Error {}
// AUTO_PASS/AUTO_REFUSE nodes terminate the whole instance, matching the Java AutoPassBehavior/AutoRefuseBehavior.
class AutoTerminated extends Error { constructor(public readonly outcome: "approve" | "reject") { super(`AUTO_${outcome.toUpperCase()}`); } }

export type TaskEntry = { task: Task; node: HumanNode; data: Data };
// receivedOrder records the deterministic update-processing order for event-gateway races;
// `source` marks signal-driven receipts so the node flow can emit workflow.eventReceived itself
// (signal handlers must stay free of activity calls).
export type WaitEntry = { wait: Wait; received: boolean; value?: Json; receivedOrder?: number; source?: "signal" };
// Signals with no matching active wait are buffered in arrival order; the bound drops the oldest.
export type PendingEvent = { event: string; data: Json };
export const PENDING_EVENT_LIMIT = 100;
export type HumanCompletion = { nodeId: string; name: string; actorIds: string[] };
export type CommandEntry = { fingerprint: string; promise: Promise<CommandReceipt> };
// A validated migrate command parks the run in the main loop (never swaps mid-node); the target
// definition travels through the update and is applied at the next cancellation boundary.
export type MigrationRequest = { definition: Definition; requestId: string };
// Resolved command receipts survive continueAsNew so a retried requestId returns the original
// event instead of re-applying the command in a later run.
export type ReceiptEntry = { fingerprint: string; receipt: CommandReceipt };

export interface WorkflowContext {
  input: WorkflowInput;
  context: InstanceContext;
  data: Data;
  initiator: InitiatorContext;
  info: { runId: string };
  settings: Settings;
  limits: { maxSteps: number; maxCommands: number };
  status: Status;
  cancelling: boolean;
  suspended: boolean;
  sequence: number;
  steps: number;
  commandBytes: number;
  waitReceipts: number;
  eventTail: Promise<void>;
  returnTarget: string | undefined;
  returnOrigin: string | undefined;
  skipTo: number | undefined;
  // Definition migration: the next top-level node (rebase anchor) plus the container depth that
  // keeps migration away from partially executed nested containers. `migrateTo` is set by the
  // update handler and consumed by the main loop after the scope cancellation.
  nextNodeId: string | undefined;
  depth: number;
  migrateTo: MigrationRequest | undefined;
  scope: CancellationScope;
  activeTasks: Map<string, TaskEntry>;
  activeWaits: Map<string, WaitEntry>;
  pendingEvents: PendingEvent[];
  commands: Map<string, CommandEntry>;
  receipts: Map<string, ReceiptEntry>;
  completedTasks: Map<string, Task>;
  restoredTasks: Map<string, Task>;
  completedHumans: Map<string, HumanCompletion>;
  agreedUsers: Set<string>;
  nodeReasons: Map<string, string>;
  workflow: (raw: WorkflowInput) => Promise<WorkflowResult>;
  emit(eventType: WorkflowEvent["eventType"], details?: Data): Promise<string>;
  fireListeners(events: Record<string, Listener[]> | undefined, source: "node" | "process", nodeId: string | undefined, eventName: string, executionId: string, variables: Data): Promise<void>;
  syncBusiness(event: SyncEvent, variables: Data): Promise<void>;
  // `top` marks the top-level list; nested containers call runNodes without it so continueAsNew is
  // only ever considered between top-level nodes.
  runNodes(nodes: Node[], variables: Data, top?: boolean): Promise<void>;
}

const nodeEvents = (node: Node): Record<string, Listener[]> | undefined => ("events" in node ? node.events : undefined);

async function runIntegration(ctx: WorkflowContext, source: "node" | "process" | "trigger", event: string, executionId: string, nodeId: string | undefined, type: "HTTP" | "SIGNAL", config: Data, variables: Data): Promise<Json> {
  return activities.integrate.executeWithOptions(activityTuning(ctx.settings), [{ context: ctx.context, executionId, ...(nodeId ? { nodeId } : {}), source, event, type, config, data: jsonSchema.parse(variables) as Data }]);
}

function put(variables: Data, key: string | undefined, value: Json): void {
  if (key === undefined) return;
  assertJsonSize({ ...variables, [key]: value });
  variables[key] = value;
}

// Java fixed mapping: "#name" reads a variable, JSON objects/arrays parse, everything else stays a literal string.
function literal(source: string): Json {
  const text = source.trim();
  if (text.startsWith("{") || text.startsWith("[")) { try { return jsonSchema.parse(JSON.parse(text)); } catch { return source; } }
  return source;
}

// Java NODE_USERS preset: parser results are cached per node and always take precedence.
function presetFor(variables: Data, nodeId: string): string[] | undefined {
  const presets = variables._nodeUsers;
  if (presets === null || typeof presets !== "object" || Array.isArray(presets)) return undefined;
  const stored = (presets as Record<string, Json>)[nodeId];
  return Array.isArray(stored) ? stored.map(String) : undefined;
}
function cachePreset(variables: Data, nodeId: string, users: string[]): void {
  if (!users.length) return;
  const presets = variables._nodeUsers;
  const current = presets !== null && typeof presets === "object" && !Array.isArray(presets) ? presets as Record<string, Json> : {};
  current[nodeId] = [...users];
  assertJsonSize({ ...variables, _nodeUsers: current });
  variables._nodeUsers = current;
}

export { AutoTerminated, Rejected, cachePreset, literal, nodeEvents, presetFor, put, runIntegration };

/** The slice of context a signal handler needs; the handler stays synchronous and activity-free. */
export type EventSignalState = Pick<WorkflowContext, "activeWaits" | "waitReceipts" | "pendingEvents" | "status">;

/**
 * Applies one WORKFLOW_EVENT_SIGNAL payload. The first unreceived active wait whose event matches
 * (registration order) wins the deterministic race; otherwise the signal is buffered in arrival
 * order. Suspended instances still use active waits / the buffer (resume consumes), terminal runs
 * drop signals. Malformed payloads are ignored instead of failing the workflow task.
 */
export function receiveEventSignal(ctx: EventSignalState, raw: unknown): void {
  const parsed = eventSignalSchema.safeParse(raw);
  if (!parsed.success || ctx.status !== "running") return;
  const active = [...ctx.activeWaits.values()].find((entry) => !entry.received && entry.wait.event === parsed.data.event);
  if (active) {
    active.received = true;
    active.value = parsed.data.data ?? null;
    active.source = "signal";
    active.receivedOrder = ++ctx.waitReceipts;
    return;
  }
  ctx.pendingEvents.push({ event: parsed.data.event, data: parsed.data.data ?? null });
  while (ctx.pendingEvents.length > PENDING_EVENT_LIMIT) ctx.pendingEvents.shift();
}

/** Consumes the first buffered signal matching `event` (FIFO); non-matching events stay buffered. */
export function takePendingEvent(ctx: Pick<WorkflowContext, "pendingEvents">, event: string): PendingEvent | undefined {
  const index = ctx.pendingEvents.findIndex((pending) => pending.event === event);
  return index < 0 ? undefined : ctx.pendingEvents.splice(index, 1)[0];
}
