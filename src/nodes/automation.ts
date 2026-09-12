import { ActivityFailure, ApplicationFailure, condition, sleep } from "@temporalio/workflow";
import { executeScript, mapValues, resolveValue } from "../definition.js";
import { WorkflowValidationError } from "../errors.js";
import type { NodeOutcome } from "../ports.js";
import { dataSchema, jsonSchema, type Data, type Node, type Value } from "../schema.js";
import { actionTuning, activityTuning } from "../settings.js";
import { activities } from "../workflow-activities.js";
import { cachePreset, presetFor, put, runIntegration, takePendingEvent, type WaitEntry, type WorkflowContext } from "../workflow-context.js";

type TriggerNode = Extract<Node, { type: "trigger" }>;
type ActionNode = Extract<Node, { type: "action" }>;
type CcNode = Extract<Node, { type: "cc" }>;
type DelayNode = Extract<Node, { type: "delay" }>;
type DelayUntilNode = Extract<Node, { type: "delayUntil" }>;
type WaitNode = Extract<Node, { type: "wait" }>;
type EventGatewayNode = Extract<Node, { type: "eventGateway" }>;
type CustomNode = Extract<Node, { type: "custom" }>;

async function runTrigger(ctx: WorkflowContext, node: TriggerNode, executionId: string, variables: Data): Promise<void> {
  if (node.action === "EL" || node.action === "JS") put(variables, node.resultKey, executeScript(node.action, node.script!, variables));
  else if (node.action === "NONE") put(variables, node.resultKey, null);
  else if (node.action === "HTTP") put(variables, node.resultKey, await runIntegration(ctx, "trigger", "http", executionId, node.id, "HTTP", dataSchema.parse(node.http ?? {}), variables));
  else {
    const signal = node.signal!;
    const name = signal.name.replace(/\$\{(.+?)\}/g, (_match, key) => {
      const found = resolveValue({ path: String(key).trim().split(".") } as Value, variables);
      return found === undefined ? "" : String(found);
    });
    put(variables, node.resultKey, await runIntegration(ctx, "trigger", "signal", executionId, node.id, "SIGNAL", dataSchema.parse({ ...signal, name }), variables));
  }
}

async function runAction(ctx: WorkflowContext, node: ActionNode, executionId: string, variables: Data): Promise<void> {
  put(variables, node.resultKey, await activities.executeAction.executeWithOptions(actionTuning(ctx.settings, node.retry), [{
    context: ctx.context, executionId, idempotencyKey: `${ctx.info.runId}:${executionId}`,
    action: node.action, input: mapValues(node.input, variables),
  }]));
}

/**
 * Host-defined node: the handler itself runs in an activity, so the workflow only executes recorded
 * inputs and outputs. `output` is stored under resultKey (null when the handler returned none; a
 * missing resultKey discards it). A `jumpTo` must name a top-level node and rebases the run exactly
 * like a router target; it wins over `output`, which is then ignored.
 */
async function runCustom(ctx: WorkflowContext, node: CustomNode, executionId: string, variables: Data): Promise<void> {
  let outcome: NodeOutcome;
  try {
    outcome = await activities.executeNode.executeWithOptions(activityTuning(ctx.settings), [{
      context: ctx.context, executionId, nodeId: node.id, kind: node.kind, config: node.config ?? {}, data: variables,
    }]);
  } catch (error) {
    // Surface the activity boundary's safe code (UNKNOWN_NODE_KIND, AdapterError codes) as the
    // instance failure code; the boundary never forwards host messages.
    if (error instanceof ActivityFailure && error.cause instanceof ApplicationFailure && error.cause.type) {
      throw new WorkflowValidationError(error.cause.type);
    }
    throw error;
  }
  if (outcome.jumpTo !== undefined) {
    if (!ctx.input.definition.nodes.some((candidate) => candidate.id === outcome.jumpTo)) throw new WorkflowValidationError("INVALID_JUMP_TARGET");
    ctx.returnTarget = outcome.jumpTo;
    await ctx.emit("workflow.nodeCompleted", { nodeId: node.id, executionId, action: "custom_jump", target: outcome.jumpTo });
    ctx.scope.cancel();
    return;
  }
  put(variables, node.resultKey, outcome.output ?? null);
}

async function runCc(ctx: WorkflowContext, node: CcNode, executionId: string, variables: Data): Promise<void> {
  const preset = presetFor(variables, node.id);
  const recipients = preset ?? (await activities.resolveAssignees.executeWithOptions(activityTuning(ctx.settings), [{ context: ctx.context, executionId, nodeId: node.id, assignment: node.recipients, data: variables }])).users;
  if (!preset) cachePreset(variables, node.id, recipients);
  await ctx.emit("workflow.cc", { nodeId: node.id, executionId, recipients });
}

async function runDelay(node: DelayNode): Promise<void> {
  await sleep(node.durationMs);
}

/**
 * Pure target-time calculation for delayUntil: the node offset wins, then the host definition
 * setting, then the historical +08:00 (480 minutes) fallback.
 */
export function delayUntilTarget(node: DelayUntilNode, defaultOffsetMinutes?: number, now: number = Date.now()): number {
  if (node.at !== undefined) return Date.parse(node.at);
  const offset = (node.tzOffsetMinutes ?? defaultOffsetMinutes ?? 480) * 60_000;
  const shifted = new Date(now + offset);
  const [hours = 0, minutes = 0, seconds = 0] = node.timeOfDay!.split(":").map(Number);
  return Date.UTC(shifted.getUTCFullYear(), shifted.getUTCMonth(), shifted.getUTCDate(), hours, minutes, seconds) - offset;
}

async function runDelayUntil(ctx: WorkflowContext, node: DelayUntilNode): Promise<void> {
  await sleep(Math.max(1, delayUntilTarget(node, ctx.settings.tzOffsetMinutes) - Date.now()));
}

async function runWait(ctx: WorkflowContext, node: WaitNode, executionId: string, variables: Data): Promise<void> {
  const waiting: WaitEntry = { wait: { id: `wait-${executionId}`, nodeId: node.id, event: node.event }, received: false };
  // A signal buffered before this node was reached completes the wait immediately. waitCreated and
  // eventReceived still fire so host projections see the same wait lifecycle as the command path.
  const pending = takePendingEvent(ctx, node.event);
  if (pending) {
    await ctx.emit("workflow.waitCreated", { wait: jsonSchema.parse(waiting.wait) });
    await ctx.emit("workflow.eventReceived", { waitId: waiting.wait.id, event: node.event, source: "signal" });
    put(variables, node.resultKey, pending.data);
    return;
  }
  const deadline = node.timeoutMs === undefined ? undefined : Date.now() + node.timeoutMs;
  ctx.activeWaits.set(waiting.wait.id, waiting);
  try {
    await ctx.emit("workflow.waitCreated", { wait: jsonSchema.parse(waiting.wait) });
    const received = deadline === undefined ? (await condition(() => waiting.received), true)
      : await condition(() => waiting.received, Math.max(1, deadline - Date.now()));
    await condition(() => !ctx.suspended);
    if (!received && node.onTimeout !== "continue") throw new WorkflowValidationError("WAIT_TIMEOUT");
    if (received) {
      // Signal receipts surface from the node flow (never the handler) to keep it activity-free.
      if (waiting.source === "signal") await ctx.emit("workflow.eventReceived", { waitId: waiting.wait.id, event: node.event, source: "signal" });
      put(variables, node.resultKey, waiting.value ?? null);
    }
  } finally { ctx.activeWaits.delete(waiting.wait.id); }
}

/**
 * BPMN event-based gateway: registers one wait per event branch and one deadline per timer branch.
 * The earliest received event wins; with no receipt, the earliest timer deadline wins (ties fall
 * back to branch order). All losing waits are removed, so a late event command fails WAIT_NOT_ACTIVE.
 */
async function runEventGateway(ctx: WorkflowContext, node: EventGatewayNode, executionId: string, variables: Data): Promise<void> {
  const entries = node.branches.map((branch, index) => {
    const waiting: WaitEntry | undefined = branch.event === undefined ? undefined
      : { wait: { id: `wait-${executionId}-${index}`, nodeId: node.id, event: branch.event }, received: false };
    return { branch, index, waiting, deadline: branch.timeoutMs === undefined ? undefined : Date.now() + branch.timeoutMs };
  });
  // Signals buffered before the gateway are consumed in arrival order, each resolving the earliest
  // still-unreceived branch whose event matches: the first receipt wins the race exactly as if the
  // signals had arrived live. Non-matching buffered events stay buffered for later waits.
  for (const pending of [...ctx.pendingEvents]) {
    const entry = entries.find((candidate) => candidate.waiting !== undefined && !candidate.waiting.received && candidate.branch.event === pending.event);
    if (!entry?.waiting) continue;
    ctx.pendingEvents.splice(ctx.pendingEvents.indexOf(pending), 1);
    entry.waiting.received = true;
    entry.waiting.value = pending.data;
    entry.waiting.source = "signal";
    entry.waiting.receivedOrder = ++ctx.waitReceipts;
  }
  for (const entry of entries) if (entry.waiting) ctx.activeWaits.set(entry.waiting.wait.id, entry.waiting);
  try {
    for (const entry of entries) if (entry.waiting) await ctx.emit("workflow.waitCreated", { wait: jsonSchema.parse(entry.waiting.wait) });
    const deadline = entries.reduce<number | undefined>((earliest, entry) => entry.deadline !== undefined && (earliest === undefined || entry.deadline < earliest) ? entry.deadline : earliest, undefined);
    const anyReceived = (): boolean => entries.some((entry) => entry.waiting?.received);
    const received = deadline === undefined ? (await condition(anyReceived), true)
      : await condition(anyReceived, Math.max(1, deadline - Date.now()));
    await condition(() => !ctx.suspended);
    const winner = received
      ? entries.filter((entry) => entry.waiting?.received).sort((a, b) => (a.waiting!.receivedOrder ?? 0) - (b.waiting!.receivedOrder ?? 0) || a.index - b.index)[0]!
      : entries.filter((entry) => entry.deadline === deadline).sort((a, b) => a.index - b.index)[0]!;
    // Signal receipts surface from the node flow (never the handler) to keep it activity-free.
    if (winner.waiting?.source === "signal") await ctx.emit("workflow.eventReceived", { waitId: winner.waiting.wait.id, event: winner.waiting.wait.event, source: "signal" });
    // Only the winning event branch stores its payload; timer branches store nothing.
    if (winner.waiting && winner.branch.resultKey) put(variables, winner.branch.resultKey, winner.waiting.value ?? null);
    await ctx.runNodes(winner.branch.nodes, variables);
  } finally {
    for (const entry of entries) if (entry.waiting) ctx.activeWaits.delete(entry.waiting.wait.id);
  }
}

export { runAction, runCc, runCustom, runDelay, runDelayUntil, runEventGateway, runTrigger, runWait };
