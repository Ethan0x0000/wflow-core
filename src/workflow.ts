import {
  allHandlersFinished, ApplicationFailure, CancellationScope, condition, ContinueAsNew, continueAsNew, defineQuery, defineSignal, defineUpdate,
  isCancellation, setHandler, workflowInfo,
} from "@temporalio/workflow";
import { z } from "zod";
import { assertJsonSize, canonical, evaluate, executeScript, parseDefinition, validateFields, WorkflowValidationError } from "./definition.js";
import { assertMigratable, migrationOffset, pruneMigrationState } from "./migration.js";
import type { InstanceContext } from "./ports.js";
import { COMMAND_UPDATE, SNAPSHOT_QUERY, WORKFLOW_EVENT_SIGNAL, workflowId } from "./protocol.js";
import { commandSchema, dataSchema, inputSchema, jsonSchema, type Command, type CommandReceipt, type Data, type Definition, type EventSignal, type Json, type Listener, type Node, type Settings, type Snapshot, type SyncEvent, type Task, type WorkflowEvent, type WorkflowInput, type WorkflowResult } from "./schema.js";
import { assertResumePayload, carryResumeState, rememberReceipt, resumeInitialState } from "./resume.js";
import { activityTuning, eventDeliveryTuning } from "./settings.js";
import { actionableUsers, applyTaskCommand } from "./tasks.js";
import { runAction, runCc, runCustom, runDelay, runDelayUntil, runEventGateway, runTrigger, runWait } from "./nodes/automation.js";
import { runExclusive, runForEach, runInclusive, runLoop, runParallel } from "./nodes/gateway.js";
import { runHuman } from "./nodes/human.js";
import { runChild } from "./nodes/subproc.js";
import { activities, events } from "./workflow-activities.js";
import { AutoTerminated, nodeEvents, receiveEventSignal, Rejected, runIntegration, type WorkflowContext } from "./workflow-context.js";

const commandUpdate = defineUpdate<CommandReceipt, [Command]>(COMMAND_UPDATE);
const snapshotQuery = defineQuery<Snapshot>(SNAPSHOT_QUERY);
const eventSignal = defineSignal<[EventSignal]>(WORKFLOW_EVENT_SIGNAL);

// A bounded event delivery policy turns exhausted projection retries into a deterministic failure
// instead of leaving the workflow waiting on the host forever.
function deliveryFailure(error: unknown, settings: Settings): unknown {
  if (error instanceof WorkflowValidationError && error.code === "EVENT_DELIVERY_FAILED") return error;
  if (settings.eventDelivery?.maximumAttempts === undefined || isCancellation(error)) return error;
  return new WorkflowValidationError("EVENT_DELIVERY_FAILED");
}

const isDeliveryFailure = (error: unknown): boolean => error instanceof WorkflowValidationError && error.code === "EVENT_DELIVERY_FAILED";

export async function genericWorkflowV1(raw: WorkflowInput): Promise<WorkflowResult> {
  let input: WorkflowInput;
  try {
    assertJsonSize(raw);
    input = inputSchema.parse(raw);
    input.definition = parseDefinition(input.definition);
    // Resume runs carry data already mutated by earlier nodes; host field validation applies to
    // the original start only.
    if (!input.resume) validateFields(input.definition.inputFields ?? [], input.data, false);
  } catch {
    throw ApplicationFailure.nonRetryable("Invalid workflow input", "INVALID_WORKFLOW_INPUT");
  }
  const info = workflowInfo();
  if (info.workflowId !== workflowId(input.tenantId, input.instanceId)) {
    throw ApplicationFailure.nonRetryable("Workflow identity mismatch", "IDENTITY_MISMATCH");
  }
  const context: InstanceContext = {
    tenantId: input.tenantId, instanceId: input.instanceId, businessKey: input.businessKey,
    initiatorId: input.initiatorId, definition: { id: input.definition.id, version: input.definition.version },
  };
  const data = input.data;

  function emit(eventType: WorkflowEvent["eventType"], details: Data = {}): Promise<string> {
    const event: WorkflowEvent = {
      eventId: `${info.runId}:${++ctx.sequence}`, eventType, version: 1, occurredAt: new Date().toISOString(),
      tenantId: context.tenantId, instanceId: context.instanceId, businessKey: context.businessKey,
      definition: context.definition, sequence: ctx.sequence, details: jsonSchema.parse(details) as Data,
    };
    const delivered = ctx.eventTail.then(() => CancellationScope.nonCancellable(() => events.publishEvent.executeWithOptions(eventDeliveryTuning(ctx.settings), [event])));
    ctx.eventTail = delivered.then(() => undefined, (error: unknown) => { throw deliveryFailure(error, ctx.settings); });
    return ctx.eventTail.then(() => event.eventId);
  }

  // Node events (enter/leave/created/complete/calcComplete) and process events (startup/pass/reject/revoked).
  async function fireListeners(eventsByType: Record<string, Listener[]> | undefined, source: "node" | "process", nodeId: string | undefined, eventName: string, executionId: string, variables: Data): Promise<void> {
    for (const listener of eventsByType?.[eventName] ?? []) {
      if (listener.type === "HTTP") {
        await runIntegration(ctx, source, eventName, executionId, nodeId, "HTTP", dataSchema.parse(listener.config ?? {}), variables);
        continue;
      }
      const script = listener.action ?? (typeof listener.config?.script === "string" ? listener.config.script : undefined);
      if (script) executeScript(listener.type, script, variables);
    }
  }

  // ProcSetting.formSync: create/update/pass/reject/revoke/delete business data synchronisation.
  async function syncBusiness(event: SyncEvent, variables: Data): Promise<void> {
    const rule = ctx.settings.formSync;
    if (!rule?.enable || !rule.events.includes(event)) return;
    await activities.syncBusinessData({ context, event, rule, data: jsonSchema.parse(variables) as Data });
  }

  // Only ever called from the top-level list: nested containers (gateways/loops/forEach/child)
  // finish before the parent boundary runs again. All outstanding work must be settled and the
  // event tail delivered before the run can continue.
  async function continueAtBoundary(offset: number): Promise<void> {
    const threshold = settings.continueAsNewAfterEvents;
    const hist = workflowInfo().historyLength;
    if (threshold === undefined || offset <= 0 || hist < threshold) return;
    if (ctx.activeTasks.size > 0 || ctx.activeWaits.size > 0 || ctx.suspended || ctx.cancelling) return;
    if (ctx.returnTarget !== undefined || ctx.returnOrigin !== undefined || ctx.skipTo !== undefined) return;
    if (!allHandlersFinished()) return;
    const next: WorkflowInput = { ...input, data, resume: carryResumeState({
      offset, sequence: ctx.sequence, steps: ctx.steps, commandBytes: ctx.commandBytes,
      agreedUsers: ctx.agreedUsers, completedHumans: ctx.completedHumans, completedTasks: ctx.completedTasks, receipts: ctx.receipts,
      pendingEvents: ctx.pendingEvents,
    }) };
    assertResumePayload(next);
    await ctx.eventTail;
    await continueAsNew<typeof genericWorkflowV1>(next);
  }

  async function runNodes(nodes: Node[], variables: Data, top = false): Promise<void> {
    // Nested containers make the run non-rebasable: a migration must never re-enter a partially
    // executed gateway/loop/forEach/child, so the depth guards the preconditions.
    if (!top) ctx.depth += 1;
    try {
      for (const node of nodes) {
        if (top) {
          // Rebase anchor: the node a migration resumes at if the run is cancelled while parked here.
          ctx.nextNodeId = node.id;
          await continueAtBoundary(input.definition.nodes.indexOf(node));
        }
        await condition(() => !ctx.suspended);
        if (++ctx.steps > ctx.limits.maxSteps) throw new WorkflowValidationError("STEP_LIMIT_REACHED");
        const executionId = `n${ctx.steps}`;
        await ctx.emit("workflow.nodeEntered", { nodeId: node.id, executionId });
        await ctx.fireListeners(nodeEvents(node), "node", node.id, "enter", executionId, variables);
        switch (node.type) {
          case 'decision': if (node.outcome === 'reject') throw new Rejected(); break;
          case 'terminate': {
            await ctx.emit("workflow.nodeCompleted", { nodeId: node.id, executionId, action: node.outcome === "approve" ? "auto_pass" : "auto_refuse" });
            await ctx.fireListeners(nodeEvents(node), "node", node.id, "leave", executionId, variables);
            throw new AutoTerminated(node.outcome);
          }
          case "delayUntil": await runDelayUntil(ctx, node); break;
          case "trigger": await runTrigger(ctx, node, executionId, variables); break;
          case "router": {
            const matches = node.when ? evaluate(node.when, variables, ctx.initiator) : true;
            if (matches) {
              ctx.returnTarget = node.targetNodeId;
              await ctx.emit("workflow.nodeCompleted", { nodeId: node.id, executionId, action: "router_jump", target: node.targetNodeId });
              ctx.scope.cancel();
              return;
            }
            break;
          }
          case "approval": case "task": await runHuman(ctx, node, executionId, variables); break;
          case "action": await runAction(ctx, node, executionId, variables); break;
          case "custom": await runCustom(ctx, node, executionId, variables); break;
          case "cc": await runCc(ctx, node, executionId, variables); break;
          case "delay": await runDelay(node); break;
          case "wait": await runWait(ctx, node, executionId, variables); break;
          case "exclusive": await runExclusive(ctx, node, variables); break;
          case "inclusive": await runInclusive(ctx, node, variables); break;
          case "parallel": await runParallel(ctx, node.branches, variables); break;
          case "loop": await runLoop(ctx, node, variables); break;
          case "forEach": await runForEach(ctx, node, variables); break;
          case "eventGateway": await runEventGateway(ctx, node, executionId, variables); break;
          case "child": await runChild(ctx, node, executionId, variables); break;
        }
        await condition(() => !ctx.suspended);
        await ctx.emit("workflow.nodeCompleted", { nodeId: node.id, executionId, ...(ctx.nodeReasons.has(executionId) ? { reason: ctx.nodeReasons.get(executionId) } : {}) });
        await ctx.fireListeners(nodeEvents(node), "node", node.id, "leave", executionId, variables);
      }
    } finally {
      if (!top) ctx.depth -= 1;
    }
  }

  const limits = input.definition.limits ?? { maxSteps: 1000, maxCommands: 2000 };
  const settings: Settings = input.definition.settings ?? {};
  const initial = resumeInitialState(input.resume);
  const ctx: WorkflowContext = {
    input, context, data, info, settings, limits,
    initiator: { initiatorId: input.initiatorId, ...(input.initiator ?? {}) },
    status: "running", cancelling: false, suspended: false,
    sequence: initial.sequence, steps: initial.steps, commandBytes: initial.commandBytes,
    waitReceipts: 0,
    eventTail: Promise.resolve(),
    returnTarget: undefined, returnOrigin: undefined, skipTo: undefined,
    nextNodeId: undefined, depth: 0, migrateTo: undefined,
    scope: new CancellationScope(),
    activeTasks: new Map(), activeWaits: new Map(), pendingEvents: initial.pendingEvents, commands: new Map(), receipts: initial.receipts,
    completedTasks: initial.completedTasks, restoredTasks: new Map(), completedHumans: initial.completedHumans,
    agreedUsers: initial.agreedUsers, nodeReasons: new Map(),
    workflow: genericWorkflowV1,
    emit, fireListeners, syncBusiness, runNodes,
  };
  // A resume run restores the resubmit marker from carried state; only the initial start seeds it.
  if (!input.resume && input.definition.resubmit) ctx.completedHumans.set(input.definition.resubmit.id, { nodeId: input.definition.resubmit.id, name: input.definition.resubmit.name ?? input.definition.resubmit.id, actorIds: [input.initiatorId] });

  // Mirrors the returnTo rebase: the definition is swapped only after the cancelled scope fully
  // unwound, then the run restarts at the tracked top-level node in the target graph. The new
  // definition travels in `input.definition`, so a later continueAsNew carries it to the next run.
  function applyMigration(target: Definition): number {
    const offset = migrationOffset(target, ctx.nextNodeId);
    pruneMigrationState(target, ctx);
    input.definition = target;
    ctx.input.definition = target;
    context.definition = { id: target.id, version: target.version };
    ctx.returnTarget = undefined;
    ctx.returnOrigin = undefined;
    ctx.skipTo = undefined;
    return offset;
  }

  setHandler(snapshotQuery, () => ({
    ...context, status: ctx.status, suspended: ctx.suspended,
    returnTargets: [...ctx.completedHumans.values()],
    tasks: [...ctx.activeTasks.values()].map(({ task }) => task),
    waits: [...ctx.activeWaits.values()].map(({ wait }) => wait), data,
  }));

  // Signal handlers mutate in-memory state only: no activity calls, no emissions, no throws.
  setHandler(eventSignal, (payload) => receiveEventSignal(ctx, payload));

  setHandler(commandUpdate, async (rawCommand) => {
    try {
      assertJsonSize(rawCommand);
      const command = commandSchema.parse(rawCommand);
      if (command.tenantId !== context.tenantId) throw new WorkflowValidationError("TENANT_MISMATCH");
      const fingerprint = canonical(command);
      const existing = ctx.commands.get(command.requestId);
      if (existing) {
        if (existing.fingerprint !== fingerprint) throw new WorkflowValidationError("REQUEST_ID_CONFLICT");
        return await existing.promise;
      }
      // Receipts carried through continueAsNew replay the original outcome instead of re-applying.
      const carried = ctx.receipts.get(command.requestId);
      if (carried) {
        if (carried.fingerprint !== fingerprint) throw new WorkflowValidationError("REQUEST_ID_CONFLICT");
        return carried.receipt;
      }
      if (ctx.commands.size >= limits.maxCommands || ctx.commandBytes + fingerprint.length > 1_000_000) throw new WorkflowValidationError("COMMAND_LIMIT_REACHED");
      const promise = (async (): Promise<CommandReceipt> => {
        const allowed = await activities.authorize.executeWithOptions(activityTuning(settings), [{
          tenantId: command.tenantId, actorId: command.actorId, operation: "command",
          instanceId: context.instanceId, businessKey: context.businessKey, definition: context.definition, command,
        }]);
        if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
        // Authorization yields; validate live ownership and terminal state only after it returns.
        if (ctx.status !== "running" || ctx.cancelling) throw new WorkflowValidationError("INSTANCE_CLOSED");
        if (ctx.returnTarget) throw new WorkflowValidationError('INSTANCE_RETURNING');
        if (command.instanceId !== context.instanceId) throw new WorkflowValidationError("INSTANCE_MISMATCH");
        if (ctx.suspended && !["resume", "cancel", "reassign", "migrate"].includes(command.type)) throw new WorkflowValidationError("INSTANCE_SUSPENDED");
        let eventId: string;
        if (command.type === 'returnTo' || command.type === 'withdraw') {
          const target = ctx.completedHumans.get(command.nodeId);
          // The resubmit (initiator) node is virtual: it is seeded into completedHumans when the run starts
          // and re-run from offset 0 (see the resubmit branch below), so it never appears in definition.nodes.
          // Its position is therefore "before the first node" rather than missing.
          const resubmitTarget = input.definition.resubmit?.id === command.nodeId;
          const targetIndex = resubmitTarget ? -1 : input.definition.nodes.findIndex((n) => n.id === command.nodeId);
          if (!target || (targetIndex < 0 && !resubmitTarget)) throw new WorkflowValidationError('INVALID_RETURN_TARGET');
          if (command.type === 'returnTo') {
            const active = ctx.activeTasks.get(command.taskId);
            const currentIndex = input.definition.nodes.findIndex((n) => n.id === active?.node.id);
            if (!active || ctx.activeTasks.size !== 1 || currentIndex < 0 || targetIndex >= currentIndex) throw new WorkflowValidationError('INVALID_RETURN_TARGET');
            if (!active.node.allowReturn || !actionableUsers(active.task).includes(command.actorId)) throw new WorkflowValidationError('RETURN_FORBIDDEN');
            ctx.returnOrigin = active.node.id;
          } else {
            // Java WithdrawCmd: only the newest handled node can be withdrawn, and its next node must be unprocessed.
            const active = [...ctx.activeTasks.values()][0];
            const lastCompleted = [...ctx.completedHumans.values()].at(-1);
            const handled = [...ctx.activeTasks.values()].some((entry) => entry.task.approved.length || (entry.task.additions ?? []).some((addition) => addition.completed));
            if (!active || lastCompleted?.nodeId !== command.nodeId || !target.actorIds.includes(command.actorId) || handled) throw new WorkflowValidationError('WITHDRAW_FORBIDDEN');
            // Restore the previous task keeping the other assignees' decisions (Java resets only the handler's record).
            const previous = ctx.completedTasks.get(command.nodeId);
            if (previous) {
              const { deadline: _deadline, ...rest } = previous;
              ctx.restoredTasks.set(command.nodeId, jsonSchema.parse({ ...rest,
                status: "pending", approved: previous.approved.filter((user) => user !== command.actorId),
                ...(previous.additions ? { additions: previous.additions.map((addition) => addition.userId === command.actorId || addition.ownerId === command.actorId ? { ...addition, completed: false } : addition) } : {}),
                signature: null, createdAt: new Date().toISOString() }) as Task);
            }
            ctx.returnOrigin = undefined;
          }
          ctx.returnTarget = command.nodeId;
          eventId = await ctx.emit('workflow.returnRequested', { targetNodeId: command.nodeId, actorId: command.actorId, requestId: command.requestId, action: command.type });
          ctx.scope.cancel();
        } else if (command.type === "suspend" || command.type === "resume") {
          ctx.suspended = command.type === "suspend";
          eventId = await ctx.emit(ctx.suspended ? "workflow.suspended" : "workflow.resumed", { actorId: command.actorId, requestId: command.requestId });
        } else if (command.type === "migrate") {
          assertMigratable({ suspended: ctx.suspended, activeTasks: ctx.activeTasks.size, activeWaits: ctx.activeWaits.size,
            depth: ctx.depth, currentVersion: context.definition.version, toVersion: command.toVersion });
          if (ctx.migrateTo) throw new WorkflowValidationError("INVALID_COMMAND");
          // The target definition is loaded through the host store (published before migrating); the
          // activity result is recorded in history, so replay is deterministic.
          const definition = await activities.loadDefinition.executeWithOptions(activityTuning(settings), [{
            tenantId: context.tenantId, reference: { id: context.definition.id, version: command.toVersion },
          }]);
          // The load yields: re-check live state before parking the run for the swap.
          assertMigratable({ suspended: ctx.suspended, activeTasks: ctx.activeTasks.size, activeWaits: ctx.activeWaits.size,
            depth: ctx.depth, currentVersion: context.definition.version, toVersion: definition.version });
          if (definition.id !== context.definition.id) throw new WorkflowValidationError("MIGRATION_INVALID_TARGET");
          ctx.migrateTo = { definition, requestId: command.requestId };
          try {
            eventId = await ctx.emit("workflow.migrateRequested", {
              actorId: command.actorId, requestId: command.requestId, fromVersion: context.definition.version, toVersion: definition.version,
            });
          } catch (error) {
            // A failed projection must not leave a pending swap for a later unrelated cancellation.
            ctx.migrateTo = undefined;
            throw error;
          }
          ctx.scope.cancel();
        } else if (command.type === "cancel") {
          ctx.cancelling = true;
          ctx.scope.cancel();
          eventId = await ctx.emit("workflow.cancelRequested", { actorId: command.actorId, requestId: command.requestId });
        } else if (command.type === "event") {
          const waiting = ctx.activeWaits.get(command.waitId);
          if (!waiting || waiting.received || waiting.wait.event !== command.event) throw new WorkflowValidationError("WAIT_NOT_ACTIVE");
          waiting.value = command.data;
          waiting.received = true;
          // Deterministic race order for eventGateway winner selection.
          waiting.receivedOrder = ++ctx.waitReceipts;
          eventId = await ctx.emit("workflow.eventReceived", { waitId: command.waitId, actorId: command.actorId, requestId: command.requestId });
        } else {
          if (!("taskId" in command)) throw new WorkflowValidationError("INVALID_COMMAND");
          const active = ctx.activeTasks.get(command.taskId);
          if (!active) throw new WorkflowValidationError("TASK_NOT_ACTIVE");
          if ((command.type === "approve" || command.type === "complete" || command.type === "reject" || command.type === "override") && command.otherNodeUsers) {
            const current = active.data._nodeUsers;
            const presets: Record<string, Json> = current !== null && typeof current === "object" && !Array.isArray(current) ? current as Record<string, Json> : {};
            for (const [nodeId, users] of Object.entries(command.otherNodeUsers)) presets[nodeId] = users as Json;
            assertJsonSize({ ...active.data, _nodeUsers: presets });
            active.data._nodeUsers = presets;
          }
          applyTaskCommand(active.task, active.node, command, input.initiatorId, active.data);
          eventId = await ctx.emit("workflow.taskChanged", {
            task: jsonSchema.parse(active.task), action: command.type, actorId: command.actorId, requestId: command.requestId,
            comment: command.comment ?? {},
            ...(command.type === 'override' ? { originalAssignee: command.userId, overriddenAction: command.action } : {}),
          });
          if (command.type === "approve" || command.type === "complete" || command.type === "override") await ctx.syncBusiness("update", active.data);
        }
        const receipt = { requestId: command.requestId, eventId };
        rememberReceipt(ctx.receipts, command.requestId, { fingerprint, receipt });
        return receipt;
      })();
      ctx.commands.set(command.requestId, { fingerprint, promise });
      ctx.commandBytes += fingerprint.length;
      try { return await promise; }
      catch (error) { ctx.commands.delete(command.requestId); ctx.commandBytes -= fingerprint.length; throw error; }
    } catch (error) {
      if (error instanceof WorkflowValidationError) throw ApplicationFailure.nonRetryable(error.code, error.code);
      if (error instanceof z.ZodError) throw ApplicationFailure.nonRetryable("Invalid command", "INVALID_COMMAND");
      throw error;
    }
  });

  try {
    // Resume runs must not re-authorize the start, re-emit workflow.started or re-apply the
    // create-side formSync/listeners.
    let offset = input.resume?.offset ?? 0;
    if (!input.resume) {
      const allowed = await activities.authorize.executeWithOptions(activityTuning(settings), [{ tenantId: context.tenantId, actorId: input.initiatorId, operation: "start",
        instanceId: context.instanceId, businessKey: context.businessKey, definition: context.definition }]);
      if (!allowed) throw new WorkflowValidationError("FORBIDDEN");
      await ctx.emit("workflow.started", { initiatorId: input.initiatorId });
      await ctx.syncBusiness("create", data);
      await ctx.fireListeners(input.definition.events, "process", undefined, "startup", "start", data);
    }
    let resubmit = false;
    for (;;) {
      ctx.scope = new CancellationScope();
      try {
        await ctx.scope.run(async () => {
          if (resubmit && input.definition.resubmit) await runNodes([input.definition.resubmit], data);
          // returnSkip jumps straight back to the node that requested the fallback once the target completes.
          await runNodes(input.definition.nodes.slice(offset, ctx.skipTo === undefined ? undefined : offset + 1), data, true);
          await condition(() => !ctx.suspended);
        });
        if (ctx.skipTo !== undefined) { offset = ctx.skipTo; ctx.skipTo = undefined; continue; }
        break;
      } catch (error) {
        if (isCancellation(error) && ctx.migrateTo && !ctx.cancelling) {
          const migration = ctx.migrateTo;
          ctx.migrateTo = undefined;
          const fromVersion = input.definition.version;
          offset = applyMigration(migration.definition);
          // A pending resubmit/fallback belonged to the old graph; it is reset, not carried over.
          resubmit = false;
          // Emitted from the main loop (not the update handler) so the projection is never lost.
          await ctx.emit("workflow.migrated", { fromVersion, toVersion: input.definition.version,
            fromNodeId: input.definition.nodes[offset]!.id, toNodeId: input.definition.nodes[offset]!.id });
          continue;
        }
        if (!isCancellation(error) || !ctx.returnTarget || ctx.cancelling) throw error;
        resubmit = ctx.returnTarget === input.definition.resubmit?.id;
        offset = resubmit ? 0 : input.definition.nodes.findIndex((node) => node.id === ctx.returnTarget);
        if (settings.returnSkip && ctx.returnOrigin) ctx.skipTo = input.definition.nodes.findIndex((node) => node.id === ctx.returnOrigin);
        for (const [id] of ctx.completedHumans) {
          if (id !== input.definition.resubmit?.id && input.definition.nodes.findIndex((node) => node.id === id) >= offset) ctx.completedHumans.delete(id);
        }
        ctx.returnTarget = undefined;
        ctx.returnOrigin = undefined;
      }
    }
    ctx.status = "completed";
  } catch (error) {
    // continueAsNew aborts the run by throwing; it must reach the SDK runtime untouched.
    if (error instanceof ContinueAsNew) throw error;
    ctx.scope.cancel();
    ctx.status = error instanceof Rejected ? "rejected"
      : error instanceof AutoTerminated ? (error.outcome === "approve" ? "completed" : "rejected")
      : isCancellation(error) ? "cancelled" : "failed";
    if (ctx.status === "failed") {
      await CancellationScope.nonCancellable(async () => {
        await condition(allHandlersFinished);
        try {
          await ctx.emit("workflow.failed", { code: error instanceof WorkflowValidationError ? error.code : "EXECUTION_FAILED" });
        } catch (emitError) {
          // The projection is the failing dependency: keep the original failure code instead of masking it.
          if (!isDeliveryFailure(emitError)) throw emitError;
        }
      });
      throw ApplicationFailure.nonRetryable("Workflow execution failed", error instanceof WorkflowValidationError ? error.code : "EXECUTION_FAILED");
    }
  }
  try {
    await CancellationScope.nonCancellable(async () => {
      await condition(allHandlersFinished);
      await ctx.emit(ctx.status === "completed" ? "workflow.completed" : ctx.status === "rejected" ? "workflow.rejected" : "workflow.cancelled");
      await ctx.syncBusiness(ctx.status === "completed" ? "pass" : ctx.status === "rejected" ? "reject" : "revoke", data);
      await ctx.fireListeners(input.definition.events, "process", undefined, ctx.status === "completed" ? "pass" : ctx.status === "rejected" ? "reject" : "revoked", "process", data);
    });
  } catch (error) {
    if (isDeliveryFailure(error)) throw ApplicationFailure.nonRetryable("Workflow execution failed", "EVENT_DELIVERY_FAILED");
    throw error;
  }
  return { status: ctx.status as WorkflowResult["status"], data };
}
