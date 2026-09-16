import { Client } from "@temporalio/client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { afterEach, describe, expect, it } from "vitest";
import { createActivities } from "../src/activities.js";
import type { ActionHandler, IntegrationHandler, NodeContext, NodeHandler, SyncHandler } from "../src/ports.js";
import { AdapterError } from "../src/errors.js";
import { WorkflowEngineClient } from "../src/client.js";
import { createMemoryAdapters, createTestEngine } from "../src/testing.js";
import { parseDefinition } from "../src/definition.js";
import { importWflowDefinition } from "../src/wflow.js";
import { resolveWorkflowsPath } from "../src/worker.js";
import { workflowId } from "../src/protocol.js";
import type { Data, Definition, Node, Snapshot, WorkflowEvent, WorkflowInput } from "../src/schema.js";
import { COMMAND_UPDATE, SNAPSHOT_QUERY, WORKFLOW_EVENT_SIGNAL, WORKFLOW_TYPE } from "../src/protocol.js";

describe("Temporal workflow runtime", () => {
  let environment: TestWorkflowEnvironment | undefined;
  afterEach(async () => { await environment?.teardown(); environment = undefined; });

  async function runDefinition(definition: Definition, scenario: (engine: WorkflowEngineClient, handle: Awaited<ReturnType<WorkflowEngineClient['start']>>, history: WorkflowEvent[]) => Promise<void>, actions: ReadonlyMap<string, ActionHandler> = new Map(), child?: Definition, integrate?: IntegrationHandler, sync?: SyncHandler, data: Data = {}, initiator?: WorkflowInput["initiator"], nodeHandlers?: ReadonlyMap<string, NodeHandler>) {
    const memory = createMemoryAdapters({ definitions: { async get() { return child ?? definition; } }, actions, ...(integrate ? { integrate } : {}), ...(sync ? { syncBusinessData: sync } : {}), ...(nodeHandlers ? { nodeHandlers } : {}) });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: 'scenarios' });
    environment = test.environment;
    await test.run(async () => {
      const handle = await test.engine.start({ tenantId: 'tenant', instanceId: 'scenario-1', businessKey: 'scenario-1', initiatorId: 'employee', definition, data, ...(initiator ? { initiator } : {}) });
      await scenario(test.engine, handle, memory.events);
    });
  }
  async function runScenario(nodes: Node[], scenario: (engine: WorkflowEngineClient, handle: Awaited<ReturnType<WorkflowEngineClient['start']>>, history: WorkflowEvent[]) => Promise<void>, actions: ReadonlyMap<string, ActionHandler> = new Map(), child?: Definition, settings?: Definition['settings'], integrate?: IntegrationHandler, sync?: SyncHandler, data: Data = {}, initiator?: WorkflowInput["initiator"], nodeHandlers?: ReadonlyMap<string, NodeHandler>) {
    const definition = parseDefinition({ schemaVersion: 1, id: 'scenario', version: 1, name: 'Scenario', nodes, ...(settings ? { settings } : {}) });
    await runDefinition(definition, scenario, actions, child, integrate, sync, data, initiator, nodeHandlers);
  }
  const human = (id: string): Node => ({ id, type: 'approval', assignees: { type: 'users', userIds: [id] }, mode: 'all', selfApproval: 'allow', rejectRule: { type: 'END' } });
  async function waitFor<T>(value: () => T | undefined): Promise<T> {
    await expect.poll(value).toBeTruthy();
    return value()!;
  }
  async function taskIds(engine: WorkflowEngineClient, count: number) {
    let snapshot: Snapshot | undefined;
    await expect.poll(async () => { snapshot = await engine.snapshot({ tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee' }); return snapshot.tasks.length; }).toBe(count);
    return snapshot!.tasks;
  }
  // Parks a run inside a top-level action activity: the workflow is idle (no task/wait) and can be
  // suspended while the first call is held, then migrated once the result is released. Actions are
  // not tracked in activeTasks, and the post-activity suspended check is a cleanly cancellable await.
  function holdingAction() {
    const resolvers: (() => void)[] = [];
    let calls = 0;
    const handler: ActionHandler = { execute: async () => {
      calls += 1;
      if (calls === 1) await new Promise<void>((resolve) => resolvers.push(resolve));
      return "held";
    } };
    return { handler, calls: () => calls, release: () => { for (const resolve of resolvers.splice(0)) resolve(); } };
  }

  it('honours NODE_USERS presets over rule parsing and caches resolved assignees', async () => {
    // Preset wins over the declared assignee rule.
    await runScenario([human('first')], async (engine) => {
      const task = (await taskIds(engine, 1))[0]!;
      expect(task.assignees).toEqual(['two']);
      const snapshot = await engine.snapshot({ tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee' });
      expect((snapshot.data._nodeUsers as Record<string, string[]>).first).toEqual(['two']);
    }, new Map(), undefined, undefined, undefined, undefined, { _nodeUsers: { first: ['two'] } });
  }, 30_000);

  it('writes parsed assignees back into NODE_USERS and drops them when reloadUser is set', async () => {
    await runScenario([human('first'), human('second')], async (engine, handle) => {
      const task = (await taskIds(engine, 1))[0]!;
      let snapshot = await engine.snapshot({ tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee' });
      expect((snapshot.data._nodeUsers as Record<string, string[]>).first).toEqual(['first']);
      await engine.command({ type: 'approve', requestId: 'first', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'first' });
      await expect.poll(async () => { snapshot = await engine.snapshot({ tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee' }); return snapshot.tasks[0]?.nodeId; }).toBe('second');
      expect((snapshot.data._nodeUsers as Record<string, string[]>).first).toBeUndefined();
      expect((snapshot.data._nodeUsers as Record<string, string[]>).second).toEqual(['second']);
      await engine.command({ type: 'approve', requestId: 'second', tenantId: 'tenant', instanceId: 'scenario-1', taskId: snapshot.tasks[0]!.id, actorId: 'second' });
      expect((await handle.result()).status).toBe('completed');
    }, new Map(), undefined, { reloadUser: true });
  }, 30_000);

  it('waits for every parallel approval before the continuation', async () => {
    await runScenario([{ id: 'parallel', type: 'parallel', branches: [[human('first')], [human('second')]] }, human('join')], async (engine, handle, events) => {
      const tasks = await taskIds(engine, 2);
      await engine.command({ type: 'approve', requestId: 'first', tenantId: 'tenant', instanceId: 'scenario-1', taskId: tasks.find((t) => t.nodeId === 'first')!.id, actorId: 'first' });
      expect((await taskIds(engine, 1))[0]!.nodeId).toBe('second');
      expect(events.some((e) => e.eventType === 'workflow.nodeEntered' && e.details.nodeId === 'join')).toBe(false);
      await engine.command({ type: 'approve', requestId: 'second', tenantId: 'tenant', instanceId: 'scenario-1', taskId: tasks.find((t) => t.nodeId === 'second')!.id, actorId: 'second' });
      const joined = (await taskIds(engine, 1))[0]!;
      expect(joined.nodeId).toBe('join');
      await engine.command({ type: 'approve', requestId: 'join', tenantId: 'tenant', instanceId: 'scenario-1', taskId: joined.id, actorId: 'join' });
      expect((await handle.result()).status).toBe('completed');
    });
  }, 30_000);

  it('rejects reuse of a command id with a different payload', async () => {
    await runScenario([{ id: 'review', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['one', 'two'] } }], async (engine, handle) => {
      const task = (await taskIds(engine, 1))[0]!;
      const command = { type: 'approve' as const, requestId: 'same-id', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'one' };
      const receipt = await engine.command(command);
      expect(await engine.command(command)).toEqual(receipt);
      await expect(engine.command({ ...command, actorId: 'two' })).rejects.toThrow();
      expect((await taskIds(engine, 1))[0]!.approved).toEqual(['one']);
      await engine.command({ ...command, requestId: 'different-id', actorId: 'two' });
      expect((await handle.result()).status).toBe('completed');
    });
  }, 30_000);

  it('cancels active parallel siblings on rejection', async () => {
    await runScenario([{ id: 'parallel', type: 'parallel', branches: [[human('first')], [human('second')]] }], async (engine, handle, events) => {
      const tasks = await taskIds(engine, 2);
      await engine.command({ type: 'reject', requestId: 'reject', tenantId: 'tenant', instanceId: 'scenario-1', taskId: tasks[0]!.id, actorId: tasks[0]!.assignees[0]! });
      expect((await handle.result()).status).toBe('rejected');
      expect(events.some((e) => e.eventType === 'workflow.taskChanged' && e.details.action === 'cancel')).toBe(true);
    });
  }, 30_000);

  it.each(['exclusive', 'inclusive'] as const)('%s routes only matching branches and merges outputs', async (type) => {
    const yes = { op: 'eq' as const, left: { value: true }, right: { value: true } };
    const action = (id: string): Node => ({ id, type: 'action', action: 'echo', input: { value: { value: id } }, resultKey: id });
    await runScenario([{ id: 'route', type, branches: [{ when: yes, nodes: [action('one')] }, { when: yes, nodes: [action('two')] }], otherwise: [action('otherwise')] }, action('joined')], async (_engine, handle) => {
      expect((await handle.result()).data).toEqual(type === 'exclusive' ? { one: 'one', joined: 'joined' } : { one: 'one', two: 'two', joined: 'joined' });
    }, new Map<string, ActionHandler>([['echo', { execute: async (_context, input) => input.value! }]]));
  }, 30_000);

  it('retries an activity with a stable idempotency key and runs a versioned child', async () => {
    const keys: string[] = [];
    const child = parseDefinition({ schemaVersion: 1, id: 'child', version: 2, name: 'Child', nodes: [{ id: 'child-action', type: 'action', action: 'echo', input: { value: { path: ['input'] } }, resultKey: 'output' }] });
    await runScenario([{ id: 'retry', type: 'action', action: 'flaky', input: {}, resultKey: 'retry', retry: { startToCloseMs: 5000, maximumAttempts: 3, initialIntervalMs: 1 } }, { id: 'child', type: 'child', definition: { id: 'child', version: 2 }, input: { input: { path: ['retry'] } }, resultKey: 'child' }], async (_engine, handle) => {
      expect((await handle.result()).data).toEqual({ retry: 9, child: { input: 9, output: 9 } });
      expect(keys).toHaveLength(3); expect(new Set(keys).size).toBe(1);
    }, new Map<string, ActionHandler>([['flaky', { execute: async (context) => { keys.push(context.idempotencyKey); if (keys.length < 3) throw new Error('Transient failure'); return 9; } }], ['echo', { execute: async (_context, input) => input.value! }]]), child);
  }, 30_000);

  it('passes inherited and mapped data to a synchronous child and syncs values back', async () => {
    const child = parseDefinition({ schemaVersion: 1, id: 'child', version: 2, name: 'Child',
      inputFields: [{ key: 'order', type: 'string' }, { key: 'auto', type: 'string' }],
      nodes: [{ id: 'child-approval', type: 'approval', mode: 'all', fields: [{ key: 'order', type: 'string' }], assignees: { type: 'users', userIds: ['worker'] }, rejectRule: { type: 'END' } }] });
    const actions = new Map<string, ActionHandler>([['seed', { execute: async () => 'initial' }], ['seed2', { execute: async () => 'yes' }], ['done', { execute: async () => 'ok' }]]);
    await runScenario([
      { id: 'seed', type: 'action', action: 'seed', input: {}, resultKey: 'order' },
      { id: 'seed2', type: 'action', action: 'seed2', input: {}, resultKey: 'auto' },
      { id: 'sub', type: 'child', definition: { id: 'child', version: 2 }, input: {}, inheritVariables: true, formAutoMapping: true,
        initiator: { type: 'fixed', userId: 'u-admin' }, resultKey: 'subResult',
        mappings: [{ source: 'order', target: 'order', isVar: true, sync: true, fixed: false }] },
      { id: 'done', type: 'action', action: 'done', input: {}, resultKey: 'final' },
    ], async (engine, handle, events) => {
      const started = await waitFor(() => events.find((event) => event.eventType === 'workflow.childStarted' && event.instanceId === 'scenario-1'));
      const childId = String(started.details.childInstanceId);
      const childStarted = await waitFor(() => events.find((event) => event.instanceId === childId && event.eventType === 'workflow.started'));
      expect(childStarted.details.initiatorId).toBe('u-admin');
      let snapshot: Snapshot | undefined;
      await expect.poll(async () => { snapshot = await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: childId }); return { data: snapshot.data, tasks: snapshot.tasks.length }; }).toMatchObject({ data: { order: 'initial', auto: 'yes' }, tasks: 1 });
      await engine.command({ type: 'approve', requestId: 'child-approve', tenantId: 'tenant', instanceId: childId, taskId: snapshot!.tasks[0]!.id, actorId: 'worker', data: { order: 'changed' } });
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data).toMatchObject({ order: 'changed', auto: 'yes', final: 'ok' });
    }, actions, child);
  }, 30_000);

  it('continues after a rejected child unless statusSync propagates the rejection', async () => {
    const child = parseDefinition({ schemaVersion: 1, id: 'child', version: 2, name: 'Child',
      nodes: [{ id: 'child-approval', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['worker'] }, rejectRule: { type: 'END' } }] });
    await runScenario([
      { id: 'sub', type: 'child', definition: { id: 'child', version: 2 }, input: {} },
      { id: 'done', type: 'action', action: 'done', input: {}, resultKey: 'final' },
    ], async (engine, handle, events) => {
      const started = await waitFor(() => events.find((event) => event.eventType === 'workflow.childStarted'));
      const childId = String(started.details.childInstanceId);
      let snapshot: Snapshot | undefined;
      await expect.poll(async () => { snapshot = await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: childId }); return snapshot.tasks.length; }).toBe(1);
      await engine.command({ type: 'reject', requestId: 'child-reject', tenantId: 'tenant', instanceId: childId, taskId: snapshot!.tasks[0]!.id, actorId: 'worker' });
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.final).toBe('ok');
    }, new Map<string, ActionHandler>([['done', { execute: async () => 'ok' }]]), child);
  }, 30_000);

  it('propagates a rejected child with statusSync', async () => {
    const child = parseDefinition({ schemaVersion: 1, id: 'child', version: 2, name: 'Child',
      nodes: [{ id: 'child-approval', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['worker'] }, rejectRule: { type: 'END' } }] });
    await runScenario([{ id: 'sub', type: 'child', definition: { id: 'child', version: 2 }, input: {}, statusSync: true }], async (engine, handle, events) => {
      const started = await waitFor(() => events.find((event) => event.eventType === 'workflow.childStarted'));
      const childId = String(started.details.childInstanceId);
      let snapshot: Snapshot | undefined;
      await expect.poll(async () => { snapshot = await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: childId }); return snapshot.tasks.length; }).toBe(1);
      await engine.command({ type: 'reject', requestId: 'child-reject', tenantId: 'tenant', instanceId: childId, taskId: snapshot!.tasks[0]!.id, actorId: 'worker' });
      expect((await handle.result()).status).toBe('rejected');
    }, new Map(), child);
  }, 30_000);

  it('starts an async child, continues the parent and leaves the child running independently', async () => {
    const child = parseDefinition({ schemaVersion: 1, id: 'child', version: 2, name: 'Child',
      nodes: [{ id: 'child-approval', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['worker'] }, rejectRule: { type: 'END' } }] });
    await runScenario([
      { id: 'sub', type: 'child', definition: { id: 'child', version: 2 }, input: {}, async: true, resultKey: 'subResult' },
      { id: 'after', type: 'task', mode: 'all', assignees: { type: 'users', userIds: ['me'] } },
    ], async (engine, handle, events) => {
      const tasks = await taskIds(engine, 1);
      expect(tasks[0]!.nodeId).toBe('after');
      const started = events.find((event) => event.eventType === 'workflow.childStarted' && event.instanceId === 'scenario-1')!;
      expect(started.details.async).toBe(true);
      const childId = String(started.details.childInstanceId);
      await expect.poll(() => events.some((event) => event.instanceId === childId && event.eventType === 'workflow.started')).toBe(true);
      // The parent keeps its own thread of control: the child task can finish before the parent task does.
      let childSnapshot: Snapshot | undefined;
      await expect.poll(async () => { childSnapshot = await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: childId }); return childSnapshot.tasks.length; }).toBe(1);
      await engine.command({ type: 'approve', requestId: 'child-approve', tenantId: 'tenant', instanceId: childId, taskId: childSnapshot!.tasks[0]!.id, actorId: 'worker' });
      await expect.poll(() => events.some((event) => event.instanceId === childId && event.eventType === 'workflow.completed')).toBe(true);
      await engine.command({ type: 'complete', requestId: 'after', tenantId: 'tenant', instanceId: 'scenario-1', taskId: tasks[0]!.id, actorId: 'me' });
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.subResult).toMatchObject({ instanceId: childId, async: true });
    }, new Map(), child);
  }, 30_000);

  it('withdraws the newest handled node and keeps the other assignees decisions', async () => {
    await runScenario([
      { id: 'first', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['one', 'two'] }, rejectRule: { type: 'END' } },
      { id: 'second', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['three'] }, rejectRule: { type: 'END' } },
    ], async (engine, handle, events) => {
      let tasks = await taskIds(engine, 1);
      await engine.command({ type: 'approve', requestId: 'one', tenantId: 'tenant', instanceId: 'scenario-1', taskId: tasks[0]!.id, actorId: 'one' });
      await engine.command({ type: 'approve', requestId: 'two', tenantId: 'tenant', instanceId: 'scenario-1', taskId: tasks[0]!.id, actorId: 'two' });
      await expect.poll(async () => (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks[0]?.nodeId).toBe('second');
      tasks = (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks;
      await engine.command({ type: 'withdraw', requestId: 'withdraw', tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'one', nodeId: 'first' });
      let restored: Snapshot | undefined;
      await expect.poll(async () => { restored = await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' }); return restored.tasks[0]?.nodeId; }).toBe('first');
      expect(restored!.tasks[0]!.approved).toEqual(['two']);
      await engine.command({ type: 'approve', requestId: 'reopen', tenantId: 'tenant', instanceId: 'scenario-1', taskId: restored!.tasks[0]!.id, actorId: 'one' });
      await expect.poll(async () => (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks[0]?.nodeId).toBe('second');
      tasks = (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks;
      await engine.command({ type: 'approve', requestId: 'three', tenantId: 'tenant', instanceId: 'scenario-1', taskId: tasks[0]!.id, actorId: 'three' });
      expect((await handle.result()).status).toBe('completed');
      expect(events.some((event) => event.eventType === 'workflow.returnRequested' && event.details.action === 'withdraw')).toBe(true);
    });
  }, 30_000);

  it('skips intermediate nodes on fallback when returnSkip is set', async () => {
    await runScenario([
      { id: 'first', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['one'] }, rejectRule: { type: 'END' } },
      { id: 'middle', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['two'] }, rejectRule: { type: 'END' } },
      { id: 'last', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['two'] }, rejectRule: { type: 'END' } },
      { id: 'end', type: 'approval', mode: 'all', allowReturn: true, assignees: { type: 'users', userIds: ['two'] }, rejectRule: { type: 'END' } },
    ], async (engine, handle, events) => {
      const approve = async (requestId: string, actorId: string, nodeId: string) => {
        let task: Snapshot['tasks'][number] | undefined;
        await expect.poll(async () => { task = (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks.find((candidate) => candidate.nodeId === nodeId); return Boolean(task); }).toBe(true);
        await engine.command({ type: 'approve', requestId, tenantId: 'tenant', instanceId: 'scenario-1', taskId: task!.id, actorId });
        return task!;
      };
      await approve('first', 'one', 'first');
      await approve('middle', 'two', 'middle');
      await approve('last', 'two', 'last');
      let end: Snapshot['tasks'][number] | undefined;
      await expect.poll(async () => { end = (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks.find((candidate) => candidate.nodeId === 'end'); return Boolean(end); }).toBe(true);
      await engine.command({ type: 'returnTo', requestId: 'return', tenantId: 'tenant', instanceId: 'scenario-1', taskId: end!.id, actorId: 'two', nodeId: 'first' });
      await approve('first-again', 'one', 'first');
      // returnSkip jumps straight back to the fallback origin, so middle and last never run twice.
      await expect.poll(async () => (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks[0]?.nodeId).toBe('end');
      const runs = (nodeId: string) => events.filter((event) => event.eventType === 'workflow.taskCreated' && (event.details.task as { nodeId: string }).nodeId === nodeId).length;
      expect(runs('middle')).toBe(1);
      expect(runs('last')).toBe(1);
      const final = (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks[0]!;
      await engine.command({ type: 'approve', requestId: 'end-again', tenantId: 'tenant', instanceId: 'scenario-1', taskId: final.id, actorId: 'two' });
      expect((await handle.result()).status).toBe('completed');
    }, new Map(), undefined, { returnSkip: true });
  }, 30_000);

  it.each(['returnTo', 'withdraw'] as const)('%s can target the initiator through the virtual resubmit node', async (type) => {
    // importWflowDefinition derives the resubmit node from the Start node: it is seeded into
    // completedHumans at start-up but never appears in definition.nodes, so it needs its own target rule.
    const definition = importWflowDefinition({ id: 'scenario', name: 'Scenario', nodes: [
      { id: 'start', type: 'Start' },
      { id: 'review', type: 'Approval', props: { ruleType: 'ASSIGN_USER', assignUser: ['manager'],
        taskMode: { type: 'OR' }, operationPerms: [{ action: 'fallback', enable: true }] } },
    ] });
    expect(definition.resubmit?.id).toBe('start');
    expect(definition.nodes.some((node) => node.id === 'start')).toBe(false);
    await runDefinition(definition, async (engine, handle, events) => {
      const review = (await taskIds(engine, 1))[0]!;
      if (type === 'returnTo') await engine.command({ type: 'returnTo', requestId: 'fallback', tenantId: 'tenant', instanceId: 'scenario-1', taskId: review.id, actorId: 'manager', nodeId: 'start' });
      else await engine.command({ type: 'withdraw', requestId: 'withdraw', tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee', nodeId: 'start' });
      // The initiator receives a fresh resubmit task, then the approval node runs again with a new task.
      let resubmit: Snapshot['tasks'][number] | undefined;
      await expect.poll(async () => { resubmit = (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks[0]; return resubmit?.nodeId; }).toBe('start');
      await engine.command({ type: 'complete', requestId: 'resubmit', tenantId: 'tenant', instanceId: 'scenario-1', taskId: resubmit!.id, actorId: 'employee' });
      let again: Snapshot['tasks'][number] | undefined;
      await expect.poll(async () => { again = (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).tasks[0]; return again?.nodeId; }).toBe('review');
      expect(again!.id).not.toBe(review.id);
      await engine.command({ type: 'approve', requestId: 'approve', tenantId: 'tenant', instanceId: 'scenario-1', taskId: again!.id, actorId: 'manager' });
      expect((await handle.result()).status).toBe('completed');
      expect(events.some((event) => event.eventType === 'workflow.returnRequested')).toBe(true);
    });
  }, 30_000);

  it.each(['approve', 'reject'] as const)('uses durable approval timeout: %s', async (outcome) => {
    await runScenario([{ id: 'timeout', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager'] }, rejectRule: { type: 'END' }, timeout: { afterMs: 86_400_000, outcome } }], async (_engine, handle, events) => {
      expect((await handle.result()).status).toBe(outcome === 'approve' ? 'completed' : 'rejected');
      expect(events.some((e) => e.details.action === 'timeout')).toBe(true);
    });
  }, 30_000);

  it('reminds on timeout without closing the task', async () => {
    await runScenario([{ id: 'remind', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager'] }, timeout: { afterMs: 3_600_000, outcome: 'notify', repeat: 1 } }], async (engine, handle, events) => {
      // Virtual time only advances while awaiting a workflow result or through an explicit sleep.
      await environment!.sleep(3_600_000);
      await expect.poll(() => events.filter((e) => e.details.action === 'timeout').length).toBe(1);
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'approve', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      expect((await handle.result()).status).toBe('completed');
    });
  }, 30_000);

  it('continues after a NEXT rejection and terminates on END', async () => {
    await runScenario([
      { id: 'review', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager'] }, rejectRule: { type: 'NEXT' } },
      human('after'),
    ], async (engine, handle) => {
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'reject', requestId: 'reject-next', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const next = (await taskIds(engine, 1))[0]!;
      expect(next.nodeId).toBe('after');
      await engine.command({ type: 'approve', requestId: 'after', tenantId: 'tenant', instanceId: 'scenario-1', taskId: next.id, actorId: 'after' });
      expect((await handle.result()).status).toBe('completed');
    });
  }, 30_000);

  it.each([['approve', 'completed'], ['reject', 'rejected']] as const)('terminates the instance on an automatic %s node', async (outcome, status) => {
    await runScenario([{ id: 'auto', type: 'terminate', outcome }, human('unreachable')], async (_engine, handle, events) => {
      expect((await handle.result()).status).toBe(status);
      expect(events.some((e) => e.eventType === 'workflow.nodeCompleted' && e.details.action === `auto_${outcome === 'approve' ? 'pass' : 'refuse'}`)).toBe(true);
    });
  }, 30_000);

  it('executes trigger scripts and stores their result', async () => {
    await runScenario([{ id: 'compute', type: 'trigger', action: 'JS', script: 'return 42;', resultKey: 'answer' }], async (_engine, handle) => {
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.answer).toBe(42);
    });
  }, 30_000);

  it('routes by the first-class initiator context on the workflow input', async () => {
    const role = { op: 'initiator' as const, dimension: 'role' as const, compare: 'has' as const, values: ['hr'] };
    await runScenario([
      { id: 'route', type: 'exclusive', branches: [{ when: role, nodes: [{ id: 'hr', type: 'trigger', action: 'JS', script: 'return "hr";', resultKey: 'branch' }] }], otherwise: [{ id: 'other', type: 'trigger', action: 'JS', script: 'return "other";', resultKey: 'branch' }] },
    ], async (_engine, handle) => {
      expect((await handle.result()).data.branch).toBe('hr');
    }, new Map(), undefined, undefined, undefined, undefined, {}, { roles: ['hr'] });
  }, 30_000);

  it('fails fast when a trigger script exceeds the execution budget', async () => {
    await runScenario([{ id: 'spin', type: 'trigger', action: 'JS', script: 'while (true) {}', resultKey: 'x' }], async (_engine, handle) => {
      const failure = await handle.result().then(() => undefined, (error: unknown) => error) as { cause?: { type?: string } } | undefined;
      expect(failure?.cause?.type).toBe('SCRIPT_BUDGET_EXCEEDED');
    });
  }, 30_000);

  it('runs HTTP node event listeners through the host integration adapter', async () => {
    const calls: string[] = [];
    await runScenario([{ id: 'review', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager'] }, rejectRule: { type: 'NEXT' }, events: { created: [{ type: 'HTTP', config: { url: 'https://example.test/hook' } }] } }],
      async (engine, handle) => {
        await taskIds(engine, 1);
        await expect.poll(() => calls.length).toBe(1);
        expect(calls[0]).toBe('created');
        const task = (await taskIds(engine, 1))[0]!;
        await engine.command({ type: 'approve', requestId: 'approve', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
        expect((await handle.result()).status).toBe('completed');
      }, new Map(), undefined, undefined, async (request) => { calls.push(request.event); return null; });
  }, 30_000);

  it('runs formSync business data rules on create, update and pass', async () => {
    const calls: { event: string; data: Record<string, unknown> }[] = [];
    await runScenario([{ id: 'review', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager'] }, rejectRule: { type: 'END' }, fields: [{ key: 'reason', type: 'string' }] }], async (engine, handle) => {
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'sync-approve', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager', data: { reason: 'leave' } });
      expect((await handle.result()).status).toBe('completed');
      expect(calls.map((call) => call.event)).toEqual(['create', 'update', 'pass']);
      expect(calls.at(-1)!.data).toMatchObject({ reason: 'leave' });
    }, new Map(), undefined, { formSync: { enable: true, events: ['create', 'update', 'pass'], type: 'API', apiUrl: 'https://example.test/sync' } }, undefined,
      async (request) => { calls.push({ event: request.event, data: request.data }); });
  }, 30_000);

  it('auto-approves users that already agreed when deduplication is ONCE', async () => {
    await runScenario([
      { id: 'first', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager'] }, rejectRule: { type: 'NEXT' } },
      { id: 'second', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager', 'director'] }, rejectRule: { type: 'NEXT' } },
    ], async (engine, handle, events) => {
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'first', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const next = (await taskIds(engine, 1))[0]!;
      expect(next.nodeId).toBe('second');
      await expect.poll(async () => (await engine.snapshot({ tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee' })).tasks.find((t) => t.nodeId === 'second')?.approved).toContain('manager');
      await expect.poll(() => events.some((e) => e.details.action === 'auto_agree')).toBe(true);
      await engine.command({ type: 'approve', requestId: 'second', tenantId: 'tenant', instanceId: 'scenario-1', taskId: next.id, actorId: 'director' });
      expect((await handle.result()).status).toBe('completed');
    }, new Map(), undefined, { deduplication: { type: 'ONCE', skip: false } });
  }, 30_000);

  it('receives a scoped event and cancellation removes pending tasks', async () => {
    await runScenario([{ id: 'event', type: 'wait', event: 'confirmed', resultKey: 'event' }, human('manager')], async (engine, handle) => {
      let snapshot: Snapshot | undefined;
      await expect.poll(async () => { snapshot = await engine.snapshot({ tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee' }); return snapshot.waits.length; }).toBe(1);
      await engine.command({ type: 'event', requestId: 'signal', tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee', waitId: snapshot!.waits[0]!.id, event: 'confirmed', data: { received: true } });
      await taskIds(engine, 1);
      await engine.command({ type: 'cancel', requestId: 'cancel', tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee' });
      expect(await handle.result()).toEqual({ status: 'cancelled', data: { event: { received: true }, _nodeUsers: { manager: ['manager'] } } });
    });
  }, 30_000);

  it('forwards start options and keeps duplicate starts rejected by default', async () => {
    const definition = parseDefinition({ schemaVersion: 1, id: 'options', version: 1, name: 'Options', nodes: [{ id: 'wait', type: 'wait', event: 'go', resultKey: 'go' }] });
    const memory = createMemoryAdapters({ definitions: definition });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: 'options' });
    environment = test.environment;
    await test.run(async () => {
      const input = { tenantId: 'tenant', instanceId: 'options-1', businessKey: 'options-1', initiatorId: 'employee', definition, data: {} };
      const started = await test.engine.start(input, { memo: { source: 'sdk-test' }, workflowExecutionTimeout: '1 hour', workflowRunTimeout: '30 minutes', workflowTaskTimeout: '10 seconds' });
      expect(started.workflowId).toBe(workflowId('tenant', 'options-1'));
      const description = await test.environment.client.workflow.getHandle(started.workflowId).describe();
      expect(description.memo).toMatchObject({ source: 'sdk-test' });
      await expect(test.engine.start(input)).rejects.toThrow();
    });
  }, 30_000);

  it('describes running instances through the client describe shape', async () => {
    await runScenario([human('manager')], async (engine, handle) => {
      // The time-skipping test server implements DescribeWorkflowExecution but not the visibility
      // RPCs (list/count), which stay covered by tests/client.test.ts against a fake Client.
      const id = workflowId('tenant', 'scenario-1');
      const description = await engine.describe({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' });
      expect(description).toMatchObject({ workflowId: id, instanceId: 'scenario-1', taskQueue: 'scenarios', status: 'RUNNING', isRunning: true, pendingActivityCount: 0 });
      expect(description.historyLength).toBeGreaterThan(0);
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'approve-visibility', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      expect((await handle.result()).status).toBe('completed');
    });
  }, 30_000);

  it('delivers wait events through the ergonomic waitEvent helper', async () => {
    await runScenario([{ id: 'event', type: 'wait', event: 'confirmed', resultKey: 'event' }, human('manager')], async (engine, handle) => {
      await expect.poll(async () => (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).waits.length).toBe(1);
      await expect(engine.waitEvent({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1', event: 'other' })).rejects.toThrowError('WAIT_NOT_ACTIVE');
      await engine.waitEvent({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1', event: 'confirmed', data: { received: true } });
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'approve-wait-event', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const result = await handle.result();
      expect(result.data.event).toEqual({ received: true });
    });
  }, 30_000);

  it('signals a running wait and emits one signal receipt from the node flow', async () => {
    await runScenario([{ id: 'event', type: 'wait', event: 'confirmed', resultKey: 'event' }, human('manager')], async (engine, handle, events) => {
      await expect.poll(async () => (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).waits.length).toBe(1);
      await environment!.client.workflow.getHandle(workflowId('tenant', 'scenario-1'))
        .signal(WORKFLOW_EVENT_SIGNAL, { event: 'confirmed', data: { live: true } });
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'approve-signal', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const result = await handle.result();
      expect(result.data.event).toEqual({ live: true });
      const receipts = events.filter((event) => event.eventType === 'workflow.eventReceived' && event.details.source === 'signal');
      expect(receipts).toHaveLength(1);
      expect(receipts[0]!.details.waitId).toBe('wait-n1');
    });
  }, 30_000);

  it('buffers a signal sent before the wait node exists and completes the wait later', async () => {
    await runScenario([human('manager'), { id: 'event', type: 'wait', event: 'confirmed', resultKey: 'event' }], async (engine, handle, events) => {
      await taskIds(engine, 1);
      await environment!.client.workflow.getHandle(workflowId('tenant', 'scenario-1'))
        .signal(WORKFLOW_EVENT_SIGNAL, { event: 'confirmed', data: { early: true } });
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'approve-early', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const result = await handle.result();
      expect(result.data.event).toEqual({ early: true });
      expect(events.filter((event) => event.eventType === 'workflow.waitCreated')).toHaveLength(1);
      expect(events.filter((event) => event.eventType === 'workflow.eventReceived' && event.details.source === 'signal')).toHaveLength(1);
    });
  }, 30_000);

  it('consumes buffered signals FIFO across successive waits', async () => {
    await runScenario([
      human('manager'),
      { id: 'first', type: 'wait', event: 'go', resultKey: 'first' },
      { id: 'second', type: 'wait', event: 'go', resultKey: 'second' },
    ], async (engine, handle) => {
      await taskIds(engine, 1);
      const signaling = environment!.client.workflow.getHandle(workflowId('tenant', 'scenario-1'));
      await signaling.signal(WORKFLOW_EVENT_SIGNAL, { event: 'go', data: 'one' });
      await signaling.signal(WORKFLOW_EVENT_SIGNAL, { event: 'go', data: 'two' });
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'approve-fifo', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const result = await handle.result();
      expect(result.data.first).toBe('one');
      expect(result.data.second).toBe('two');
    });
  }, 30_000);

  it('keeps non-matching buffered signals for a later wait', async () => {
    await runScenario([
      human('manager'),
      { id: 'first', type: 'wait', event: 'alpha', resultKey: 'alpha' },
      { id: 'second', type: 'wait', event: 'beta', resultKey: 'beta' },
    ], async (engine, handle) => {
      await taskIds(engine, 1);
      const signaling = environment!.client.workflow.getHandle(workflowId('tenant', 'scenario-1'));
      await signaling.signal(WORKFLOW_EVENT_SIGNAL, { event: 'beta', data: 'b' });
      await signaling.signal(WORKFLOW_EVENT_SIGNAL, { event: 'alpha', data: 'a' });
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'approve-reorder', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const result = await handle.result();
      expect(result.data).toMatchObject({ alpha: 'a', beta: 'b' });
    });
  }, 30_000);

  it('buffers signals while suspended and consumes them after resume', async () => {
    await runScenario([human('manager'), { id: 'event', type: 'wait', event: 'go', resultKey: 'event' }], async (engine, handle) => {
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'suspend', requestId: 'suspend', tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'manager' });
      await environment!.client.workflow.getHandle(workflowId('tenant', 'scenario-1'))
        .signal(WORKFLOW_EVENT_SIGNAL, { event: 'go', data: { after: 'resume' } });
      await engine.command({ type: 'resume', requestId: 'resume', tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'manager' });
      await engine.command({ type: 'approve', requestId: 'approve-after-resume', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const result = await handle.result();
      expect(result.data.event).toEqual({ after: 'resume' });
    });
  }, 30_000);

  it('lets the earliest buffered signal win an eventGateway across branch order', async () => {
    await runScenario([
      human('manager'),
      { id: 'race', type: 'eventGateway', branches: [
        { event: 'alpha', resultKey: 'outcome', nodes: [{ id: 'alphaBranch', type: 'trigger', action: 'JS', script: 'return "alpha";', resultKey: 'branch' }] },
        { event: 'beta', resultKey: 'outcome', nodes: [{ id: 'betaBranch', type: 'trigger', action: 'JS', script: 'return "beta";', resultKey: 'branch' }] },
      ] },
    ], async (engine, handle, events) => {
      await taskIds(engine, 1);
      const signaling = environment!.client.workflow.getHandle(workflowId('tenant', 'scenario-1'));
      // beta arrives first even though alpha is the first branch: arrival order must win the race.
      await signaling.signal(WORKFLOW_EVENT_SIGNAL, { event: 'beta', data: 'B' });
      await signaling.signal(WORKFLOW_EVENT_SIGNAL, { event: 'alpha', data: 'A' });
      const task = (await taskIds(engine, 1))[0]!;
      await engine.command({ type: 'approve', requestId: 'approve-race', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' });
      const result = await handle.result();
      expect(result.data.outcome).toBe('B');
      expect(result.data.branch).toBe('beta');
      expect(events.some((event) => event.eventType === 'workflow.nodeEntered' && event.details.nodeId === 'betaBranch')).toBe(true);
      expect(events.some((event) => event.eventType === 'workflow.nodeEntered' && event.details.nodeId === 'alphaBranch')).toBe(false);
    });
  }, 30_000);

  it('carries a buffered signal across continueAsNew', async () => {
    const nodes: Node[] = [
      ...Array.from({ length: 50 }, (_value, index) => ({ id: `f${index}`, type: 'trigger' as const, action: 'NONE' as const })),
      { id: 'event', type: 'wait', event: 'go', resultKey: 'event' },
    ];
    await runScenario(nodes, async (_engine, handle, events) => {
      await environment!.client.workflow.getHandle(workflowId('tenant', 'scenario-1'))
        .signal(WORKFLOW_EVENT_SIGNAL, { event: 'go', data: { carried: true } });
      const result = await handle.result();
      expect(result.data.event).toEqual({ carried: true });
      expect(new Set(events.map((event) => event.eventId.split(':')[0])).size).toBeGreaterThan(1);
    }, new Map(), undefined, { continueAsNewAfterEvents: 100 });
  }, 30_000);

  it('delivers an external signal to a running wait through engine.signal', async () => {
    await runScenario([{ id: 'event', type: 'wait', event: 'go', resultKey: 'event' }], async (engine, handle) => {
      await expect.poll(async () => (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).waits.length).toBe(1);
      await engine.signal({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1', event: 'go', data: { by: 'client' } });
      const result = await handle.result();
      expect(result.data.event).toEqual({ by: 'client' });
    });
  }, 30_000);

  it('startOrSignal starts a fresh instance and delivers the first signal to the first wait', async () => {
    const definition = parseDefinition({ schemaVersion: 1, id: 'signal-start', version: 1, name: 'SignalStart',
      nodes: [{ id: 'event', type: 'wait', event: 'go', resultKey: 'event' }, { id: 'after', type: 'trigger', action: 'NONE' }] });
    const memory = createMemoryAdapters({ definitions: definition });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: 'signal-start' });
    environment = test.environment;
    await test.run(async () => {
      const input = { tenantId: 'tenant', instanceId: 'signal-1', businessKey: 'signal-1', initiatorId: 'employee', definition, data: {} };
      const started = await test.engine.startOrSignal(input, { event: 'go', data: { started: true } });
      expect(started.existing).toBe(false);
      const result = await started.result();
      expect(result.status).toBe('completed');
      expect(result.data.event).toEqual({ started: true });
      expect(memory.events.filter((event) => event.eventType === 'workflow.started')).toHaveLength(1);
    });
  }, 30_000);

  it('startOrSignal signals a running instance without starting a second run', async () => {
    const definition = parseDefinition({ schemaVersion: 1, id: 'signal-existing', version: 1, name: 'SignalExisting',
      nodes: [{ id: 'event', type: 'wait', event: 'go', resultKey: 'event' }] });
    const memory = createMemoryAdapters({ definitions: definition });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: 'signal-existing' });
    environment = test.environment;
    await test.run(async () => {
      const input = { tenantId: 'tenant', instanceId: 'signal-2', businessKey: 'signal-2', initiatorId: 'employee', definition, data: {} };
      const first = await test.engine.start(input);
      expect(first.existing).toBe(false);
      await expect.poll(() => memory.events.filter((event) => event.eventType === 'workflow.started').length).toBe(1);
      const second = await test.engine.startOrSignal(input, { event: 'go', data: { existing: true } });
      const result = await second.result();
      expect(result.data.event).toEqual({ existing: true });
      expect(memory.events.filter((event) => event.eventType === 'workflow.started')).toHaveLength(1);
    });
  }, 30_000);

  it('iterates a forEach over resolved items and keeps per-item results', async () => {
    await runScenario([
      { id: 'each', type: 'forEach', items: { path: ['numbers'] }, itemKey: 'item', indexKey: 'position', maxIterations: 10,
        nodes: [{ id: 'collect', type: 'trigger', action: 'JS', script: 'ctx.collected = (ctx.collected ?? []).concat([ctx.item]); return ctx.item;' }] },
    ], async (_engine, handle) => {
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.collected).toEqual([10, 20, 30]);
      // Sequential iterations share the parent variables, so the last item/index remain visible.
      expect(result.data.item).toBe(30);
      expect(result.data.position).toBe(2);
    }, new Map(), undefined, undefined, undefined, undefined, { numbers: [10, 20, 30] });
  }, 30_000);

  it('stops a sequential forEach when its completionCondition holds', async () => {
    await runScenario([
      { id: 'each', type: 'forEach', items: { value: [1, 2, 3, 4] }, itemKey: 'item', maxIterations: 10,
        completionCondition: { op: 'gte', left: { path: ['sum'] }, right: { value: 3 } },
        nodes: [{ id: 'add', type: 'trigger', action: 'JS', script: 'ctx.sum = (ctx.sum ?? 0) + ctx.item; return ctx.sum;' }] },
    ], async (_engine, handle) => {
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.sum).toBe(3);
      expect(result.data.item).toBe(2);
    });
  }, 30_000);

  it('fails a forEach whose items do not resolve to an array', async () => {
    await runScenario([
      { id: 'each', type: 'forEach', items: { path: ['missing'] }, itemKey: 'item', maxIterations: 10,
        nodes: [{ id: 'inner', type: 'delay', durationMs: 1 }] },
    ], async (_engine, handle) => {
      const failure = await handle.result().then(() => undefined, (error: unknown) => error) as { cause?: { type?: string } } | undefined;
      expect(failure?.cause?.type).toBe('FOREACH_REQUIRES_ARRAY');
    });
  }, 30_000);

  it('runs a parallel forEach in bounded chunks and merges non-iteration keys', async () => {
    await runScenario([
      { id: 'each', type: 'forEach', items: { value: [1, 2, 3, 4] }, itemKey: 'item', indexKey: 'position',
        parallel: true, concurrency: 2, maxIterations: 10,
        nodes: [{ id: 'store', type: 'trigger', action: 'JS', script: "ctx['out' + ctx.item] = ctx.item * 2; return null;" }] },
    ], async (_engine, handle) => {
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data).toMatchObject({ out1: 2, out2: 4, out3: 6, out4: 8 });
      // itemKey/indexKey are iteration-local in parallel mode and never reach the parent variables.
      expect(result.data.item).toBeUndefined();
      expect(result.data.position).toBeUndefined();
    });
  }, 30_000);

  it('runs a host-defined custom node and stores its output under resultKey', async () => {
    const seen: NodeContext[] = [];
    const handlers = new Map<string, NodeHandler>([['host.verify', { execute: async (context, config, data) => { seen.push(context); return { output: { verified: true, config, data } }; } }]]);
    await runScenario([
      { id: 'custom', type: 'custom', kind: 'host.verify', config: { amount: 7 }, resultKey: 'verification' },
      { id: 'after', type: 'trigger', action: 'NONE' },
    ], async (_engine, handle) => {
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.verification).toEqual({ verified: true, config: { amount: 7 }, data: {} });
      expect(seen[0]).toMatchObject({ tenantId: 'tenant', instanceId: 'scenario-1', businessKey: 'scenario-1', initiatorId: 'employee', definition: { id: 'scenario', version: 1 }, executionId: 'n1', nodeId: 'custom' });
    }, new Map(), undefined, undefined, undefined, undefined, {}, undefined, handlers);
  }, 30_000);

  it('fails a custom node whose kind is not registered with UNKNOWN_NODE_KIND', async () => {
    await runScenario([{ id: 'custom', type: 'custom', kind: 'host.missing' }], async (_engine, handle) => {
      const failure = await handle.result().then(() => undefined, (error: unknown) => error) as { cause?: { type?: string; nonRetryable?: boolean } } | undefined;
      expect(failure?.cause?.type).toBe('UNKNOWN_NODE_KIND');
      expect(failure?.cause?.nonRetryable).toBe(true);
    });
  }, 30_000);

  it('jumps from a custom node to a top-level target and skips intermediate nodes', async () => {
    const handlers = new Map<string, NodeHandler>([['host.route', { execute: async () => ({ jumpTo: 'target' }) }]]);
    await runScenario([
      { id: 'custom', type: 'custom', kind: 'host.route' },
      { id: 'skipped', type: 'trigger', action: 'JS', script: 'ctx.visited = (ctx.visited ?? []).concat(["skipped"]); return null;' },
      { id: 'target', type: 'trigger', action: 'JS', script: 'ctx.visited = (ctx.visited ?? []).concat(["target"]); return null;' },
    ], async (_engine, handle, events) => {
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.visited).toEqual(['target']);
      expect(events.some((event) => event.eventType === 'workflow.nodeEntered' && event.details.nodeId === 'skipped')).toBe(false);
      expect(events.some((event) => event.eventType === 'workflow.nodeCompleted' && event.details.action === 'custom_jump' && event.details.target === 'target')).toBe(true);
    }, new Map(), undefined, undefined, undefined, undefined, {}, undefined, handlers);
  }, 30_000);

  it('fails a custom node that jumps to an unknown node with INVALID_JUMP_TARGET', async () => {
    const handlers = new Map<string, NodeHandler>([['host.route', { execute: async () => ({ jumpTo: 'missing' }) }]]);
    await runScenario([{ id: 'custom', type: 'custom', kind: 'host.route' }], async (_engine, handle) => {
      const failure = await handle.result().then(() => undefined, (error: unknown) => error) as { cause?: { type?: string } } | undefined;
      expect(failure?.cause?.type).toBe('INVALID_JUMP_TARGET');
    }, new Map(), undefined, undefined, undefined, undefined, {}, undefined, handlers);
  }, 30_000);

  it('fails a custom node handler AdapterError with its safe code and no message leak', async () => {
    const handlers = new Map<string, NodeHandler>([['host.fail', { execute: async () => { throw new AdapterError('CUSTOM_FAILED', { retryable: false, cause: new Error('secret host detail') }); } }]]);
    await runScenario([{ id: 'custom', type: 'custom', kind: 'host.fail' }], async (_engine, handle) => {
      const failure = await handle.result().then(() => undefined, (error: unknown) => error) as { message?: string; cause?: { type?: string; nonRetryable?: boolean; message?: string } } | undefined;
      expect(failure?.cause?.type).toBe('CUSTOM_FAILED');
      expect(failure?.cause?.nonRetryable).toBe(true);
      expect(`${failure?.message ?? ''} ${failure?.cause?.message ?? ''}`).not.toContain('secret host detail');
    }, new Map(), undefined, undefined, undefined, undefined, {}, undefined, handlers);
  }, 30_000);

  it('runs a custom node inside a sequential forEach and keeps the last output', async () => {
    const calls: string[] = [];
    const handlers = new Map<string, NodeHandler>([['host.record', { execute: async (_context, _config, data) => { calls.push(String(data.item)); return { output: Number(data.item) * 2 }; } }]]);
    await runScenario([
      { id: 'each', type: 'forEach', items: { value: [1, 2, 3] }, itemKey: 'item', maxIterations: 10,
        nodes: [{ id: 'record', type: 'custom', kind: 'host.record', resultKey: 'last' }] },
    ], async (_engine, handle) => {
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(calls).toEqual(['1', '2', '3']);
      expect(result.data.last).toBe(6);
    }, new Map(), undefined, undefined, undefined, undefined, {}, undefined, handlers);
  }, 30_000);

  it('runs the first event branch of an eventGateway and stores its payload', async () => {
    await runScenario([
      { id: 'race', type: 'eventGateway', branches: [
        { event: 'approved', resultKey: 'outcome', nodes: [{ id: 'approveBranch', type: 'trigger', action: 'JS', script: 'return "approved";', resultKey: 'branch' }] },
        { event: 'rejected', resultKey: 'outcome', nodes: [{ id: 'rejectBranch', type: 'trigger', action: 'JS', script: 'return "rejected";', resultKey: 'branch' }] },
        { timeoutMs: 86_400_000, nodes: [{ id: 'timeoutBranch', type: 'trigger', action: 'JS', script: 'return "timeout";', resultKey: 'branch' }] },
      ] },
    ], async (engine, handle, events) => {
      let waits: Snapshot['waits'] = [];
      await expect.poll(async () => { waits = (await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' })).waits; return waits.length; }).toBe(2);
      expect(waits.map((wait) => wait.event)).toEqual(['approved', 'rejected']);
      await engine.waitEvent({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1', event: 'approved', data: { by: 'manager' } });
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.outcome).toEqual({ by: 'manager' });
      expect(result.data.branch).toBe('approved');
      expect(events.some((event) => event.eventType === 'workflow.nodeEntered' && event.details.nodeId === 'approveBranch')).toBe(true);
      expect(events.some((event) => event.eventType === 'workflow.nodeEntered' && (event.details.nodeId === 'rejectBranch' || event.details.nodeId === 'timeoutBranch'))).toBe(false);
    });
  }, 30_000);

  it('lets the earliest timer branch win an eventGateway race and cancels losing waits', async () => {
    await runScenario([
      { id: 'race', type: 'eventGateway', branches: [
        { event: 'never', resultKey: 'outcome', nodes: [{ id: 'eventBranch', type: 'trigger', action: 'JS', script: 'return "event";', resultKey: 'branch' }] },
        { timeoutMs: 1000, nodes: [{ id: 'timeoutBranch', type: 'trigger', action: 'JS', script: 'return "timeout";', resultKey: 'branch' }] },
      ] },
      human('after'),
    ], async (engine, handle, events) => {
      let waitId = '';
      await expect.poll(async () => {
        const snapshot = await engine.snapshot({ tenantId: 'tenant', actorId: 'employee', instanceId: 'scenario-1' });
        waitId = snapshot.waits[0]?.id ?? '';
        return snapshot.waits.length;
      }).toBe(1);
      await environment!.sleep(1000);
      const task = (await taskIds(engine, 1))[0]!;
      expect(task.nodeId).toBe('after');
      const late = await engine.command({ type: 'event', requestId: 'late-event', tenantId: 'tenant', instanceId: 'scenario-1', actorId: 'employee', waitId, event: 'never', data: {} })
        .then(() => undefined, (error: unknown) => error) as { cause?: { type?: string } } | undefined;
      expect(late?.cause?.type).toBe('WAIT_NOT_ACTIVE');
      await engine.command({ type: 'approve', requestId: 'after', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'after' });
      const result = await handle.result();
      expect(result.status).toBe('completed');
      expect(result.data.branch).toBe('timeout');
      // The losing event branch stores nothing; the timer winner writes no result key.
      expect(result.data.outcome).toBeUndefined();
      expect(events.some((event) => event.eventType === 'workflow.nodeEntered' && event.details.nodeId === 'timeoutBranch')).toBe(true);
      expect(events.some((event) => event.eventType === 'workflow.nodeEntered' && event.details.nodeId === 'eventBranch')).toBe(false);
    });
  }, 30_000);

  it('accepts a per-command update timeout', async () => {
    await runScenario([human('manager')], async (engine, handle) => {
      const task = (await taskIds(engine, 1))[0]!;
      const receipt = await engine.command({ type: 'approve', requestId: 'timeout-command', tenantId: 'tenant', instanceId: 'scenario-1', taskId: task.id, actorId: 'manager' }, { updateTimeout: '1 minute' });
      expect(receipt.requestId).toBe('timeout-command');
      expect((await handle.result()).status).toBe('completed');
    });
  }, 30_000);

  it('fails with EVENT_DELIVERY_FAILED when bounded event delivery retries are exhausted', async () => {
    const definition = parseDefinition({ schemaVersion: 1, id: 'delivery', version: 1, name: 'Delivery',
      nodes: [{ id: 'done', type: 'trigger', action: 'NONE' }], settings: { eventDelivery: { maximumAttempts: 1 } } });
    const memory = createMemoryAdapters({ definitions: definition, publishEvent: async () => { throw new Error('projection unavailable'); } });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: 'delivery' });
    environment = test.environment;
    const failure = await test.run(async () => {
      const started = await test.engine.start({ tenantId: 'tenant', instanceId: 'delivery-1', businessKey: 'delivery-1', initiatorId: 'employee', definition, data: {} });
      try { await started.result(); return undefined; }
      catch (error) { return error as { cause?: { type?: string } }; }
    });
    expect(failure?.cause?.type).toBe('EVENT_DELIVERY_FAILED');
  }, 30_000);

  it("continues as new across many top-level nodes and keeps one ordered event stream", async () => {
    const nodes: Node[] = Array.from({ length: 50 }, (_value, index) => ({ id: `f${index}`, type: "trigger", action: "NONE" }));
    await runScenario(nodes, async (engine, handle, events) => {
      const result = await handle.result();
      expect(result.status).toBe("completed");
      const runs = new Set(events.map((event) => event.eventId.split(":")[0]));
      expect(runs.size).toBeGreaterThan(1);
      // The carried sequence keeps host projections ordered across the whole run chain.
      const sequences = events.map((event) => event.sequence);
      for (let index = 1; index < sequences.length; index += 1) expect(sequences[index]).toBeGreaterThan(sequences[index - 1]!);
      // Startup is not replayed on resume runs.
      expect(events.filter((event) => event.eventType === "workflow.started")).toHaveLength(1);
      expect(events.filter((event) => event.eventType === "workflow.nodeEntered")).toHaveLength(50);
      expect(events.filter((event) => event.eventType === "workflow.completed")).toHaveLength(1);
      const snapshot = await engine.snapshot({ tenantId: "tenant", instanceId: "scenario-1", actorId: "employee" });
      expect(snapshot.status).toBe("completed");
      expect(snapshot.tasks).toEqual([]);
    }, new Map(), undefined, { continueAsNewAfterEvents: 100 });
  }, 30_000);

  it("returns the carried receipt for a requestId retried after continueAsNew", async () => {
    const nodes: Node[] = [
      { id: "review", type: "approval", mode: "all", assignees: { type: "users", userIds: ["review"] }, rejectRule: { type: "END" } },
      ...Array.from({ length: 30 }, (_value, index) => ({ id: `f${index}`, type: "trigger" as const, action: "NONE" as const })),
      { id: "hold", type: "wait", event: "release" },
    ];
    await runScenario(nodes, async (engine, _handle, events) => {
      const task = (await taskIds(engine, 1))[0]!;
      const command = { type: "approve" as const, requestId: "approve-once", tenantId: "tenant", instanceId: "scenario-1", taskId: task.id, actorId: "review" };
      const receipt = await engine.command(command);
      const runCount = (): number => new Set(events.map((event) => event.eventId.split(":")[0])).size;
      await expect.poll(runCount, { timeout: 30_000, interval: 250 }).toBeGreaterThan(1);
      // Wait for the hold node through the projection: a snapshot query issued while the run chain
      // is still continueAsNew-ing can stall inside the time-skipping test server.
      await expect.poll(() => events.some((event) => event.eventType === "workflow.waitCreated"
        && (event.details.wait as { event?: string } | undefined)?.event === "release"), { timeout: 30_000, interval: 250 }).toBe(true);
      expect((await engine.snapshot({ tenantId: "tenant", instanceId: "scenario-1", actorId: "employee" })).waits.map((wait) => wait.event)).toEqual(["release"]);
      // The same requestId must replay the original receipt instead of re-applying the approval.
      expect(await engine.command(command)).toEqual(receipt);
      expect(events.filter((event) => event.eventType === "workflow.taskChanged" && event.details.action === "approve")).toHaveLength(1);
      await engine.waitEvent({ tenantId: "tenant", actorId: "employee", instanceId: "scenario-1", event: "release" });
      // handle.result() is not used here: with a continueAsNew chain the time-skipping test server
      // can strand GetWorkflowExecutionHistory's waitNewEvent page follow (empty pages, no close
      // event), so completion is asserted from the projected event stream plus the final snapshot.
      await expect.poll(() => events.some((event) => event.eventType === "workflow.completed"), { timeout: 30_000, interval: 100 }).toBe(true);
      const sequences = events.map((event) => event.sequence);
      for (let index = 1; index < sequences.length; index += 1) expect(sequences[index]).toBeGreaterThan(sequences[index - 1]!);
      const completed = await engine.snapshot({ tenantId: "tenant", instanceId: "scenario-1", actorId: "employee" });
      expect(completed.status).toBe("completed");
      expect(completed.waits).toEqual([]);
    }, new Map(), undefined, { continueAsNewAfterEvents: 100 });
  }, 90_000);

  it("enforces maxSteps as a total budget across continueAsNew runs", async () => {
    const nodes: Node[] = Array.from({ length: 60 }, (_value, index) => ({ id: `f${index}`, type: "trigger", action: "NONE" }));
    const definition = parseDefinition({ schemaVersion: 1, id: "steps", version: 1, name: "Steps", nodes, settings: { continueAsNewAfterEvents: 100 }, limits: { maxSteps: 30, maxCommands: 2000 } });
    const memory = createMemoryAdapters({ definitions: definition });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: "steps" });
    environment = test.environment;
    const failure = await test.run(async () => {
      const started = await test.engine.start({ tenantId: "tenant", instanceId: "steps-1", businessKey: "steps-1", initiatorId: "employee", definition, data: {} });
      return started.result().then(() => undefined, (error: unknown) => error as { cause?: { type?: string } });
    });
    expect(failure?.cause?.type).toBe("STEP_LIMIT_REACHED");
    expect(new Set(memory.events.map((event) => event.eventId.split(":")[0])).size).toBeGreaterThan(1);
    expect(memory.events.filter((event) => event.eventType === "workflow.nodeEntered").length).toBeLessThanOrEqual(30);
  }, 30_000);

  it("startOrGet reuses a matching instance and rejects mismatched business identity", async () => {
    const nodes: Node[] = [{ id: "hold", type: "wait", event: "go" }];
    const definition = parseDefinition({ schemaVersion: 1, id: "idem", version: 1, name: "Idem", nodes });
    const v2 = parseDefinition({ schemaVersion: 1, id: "idem", version: 2, name: "Idem", nodes });
    const memory = createMemoryAdapters({ definitions: definition });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: "idempotent" });
    environment = test.environment;
    await test.run(async () => {
      const input = { tenantId: "tenant", instanceId: "idem-1", businessKey: "idem-1", initiatorId: "employee", definition, data: {} };
      const first = await test.engine.startOrGet(input);
      expect(first.existing).toBe(false);
      const second = await test.engine.startOrGet(input);
      expect(second.existing).toBe(true);
      expect(second.workflowId).toBe(first.workflowId);
      const started = (): number => memory.events.filter((event) => event.eventType === "workflow.started").length;
      await expect.poll(started, { timeout: 10_000 }).toBe(1);
      await expect(test.engine.startOrGet({ ...input, businessKey: "other" })).rejects.toThrow("INSTANCE_ALREADY_EXISTS");
      await expect(test.engine.startOrGet({ ...input, definition: v2 })).rejects.toThrow("INSTANCE_ALREADY_EXISTS");
      await expect(test.engine.startOrGet({ ...input, data: { changed: true } })).resolves.toMatchObject({ existing: true });
      // None of the duplicate paths may emit a second start projection.
      expect(started()).toBe(1);
    });
  }, 30_000);

  it("migrates a suspended idle instance and runs the target version's replacement node", async () => {
    const hold = holdingAction();
    const task = (id: string, userId: string): Node => ({ id, type: "task", mode: "all", assignees: { type: "users", userIds: [userId] } });
    const v1 = parseDefinition({ schemaVersion: 1, id: "migration", version: 1, name: "Migration", nodes: [
      { id: "hold", type: "action", action: "hold", input: {}, resultKey: "held" },
      task("legacy", "legacy"),
    ] });
    const v2 = parseDefinition({ schemaVersion: 1, id: "migration", version: 2, name: "Migration", nodes: [
      { id: "hold", type: "action", action: "hold", input: {}, resultKey: "held" },
      task("modern", "modern"),
    ] });
    const memory = createMemoryAdapters({ definitions: [v1, v2], actions: new Map([["hold", hold.handler]]) });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: "migration" });
    environment = test.environment;
    try {
      await test.run(async () => {
        const handle = await test.engine.start({ tenantId: "tenant", instanceId: "migration-1", businessKey: "migration-1", initiatorId: "employee", definition: v1, data: {} });
        await expect.poll(hold.calls).toBe(1);
        await test.engine.command({ type: "suspend", requestId: "suspend", tenantId: "tenant", instanceId: "migration-1", actorId: "admin" });
        hold.release();
        const receipt = await test.engine.migrate({ tenantId: "tenant", actorId: "admin", instanceId: "migration-1", requestId: "migrate-1", toVersion: 2, comment: { text: "v2 rollout" } });
        expect(receipt.requestId).toBe("migrate-1");
        await expect.poll(() => memory.events.some((event) => event.eventType === "workflow.migrated"), { timeout: 10_000 }).toBe(true);
        const requested = memory.events.find((event) => event.eventType === "workflow.migrateRequested")!;
        expect(requested.details).toMatchObject({ actorId: "admin", requestId: "migrate-1", fromVersion: 1, toVersion: 2 });
        expect(requested.definition).toEqual({ id: "migration", version: 1 });
        const migrated = memory.events.find((event) => event.eventType === "workflow.migrated")!;
        expect(migrated.details).toMatchObject({ fromVersion: 1, toVersion: 2, fromNodeId: "hold", toNodeId: "hold" });
        expect(migrated.definition).toEqual({ id: "migration", version: 2 });
        await test.engine.command({ type: "resume", requestId: "resume", tenantId: "tenant", instanceId: "migration-1", actorId: "admin" });
        let snapshot: Snapshot | undefined;
        await expect.poll(async () => { snapshot = await test.engine.snapshot({ tenantId: "tenant", instanceId: "migration-1", actorId: "employee" }); return snapshot.tasks[0]?.nodeId; }).toBe("modern");
        expect(snapshot!.definition).toEqual({ id: "migration", version: 2 });
        const modern = snapshot!.tasks[0]!;
        await test.engine.command({ type: "complete", requestId: "complete-modern", tenantId: "tenant", instanceId: "migration-1", taskId: modern.id, actorId: "modern" });
        expect((await handle.result()).status).toBe("completed");
        expect(memory.events.some((event) => event.eventType === "workflow.taskCreated" && (event.details.task as { nodeId: string }).nodeId === "legacy")).toBe(false);
      });
    } finally { hold.release(); }
  }, 30_000);

  it("rejects migration while the instance is not suspended", async () => {
    const hold = holdingAction();
    const v1 = parseDefinition({ schemaVersion: 1, id: "migration", version: 1, name: "Migration", nodes: [
      { id: "hold", type: "action", action: "hold", input: {}, resultKey: "held" },
      human("legacy"),
    ] });
    const v2 = parseDefinition({ schemaVersion: 1, id: "migration", version: 2, name: "Migration", nodes: [
      { id: "hold", type: "action", action: "hold", input: {}, resultKey: "held" },
      human("modern"),
    ] });
    const memory = createMemoryAdapters({ definitions: [v1, v2], actions: new Map([["hold", hold.handler]]) });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: "migration" });
    environment = test.environment;
    await test.run(async () => {
      // The held activity must resolve before the worker drains, otherwise shutdown waits forever.
      try {
        await test.engine.start({ tenantId: "tenant", instanceId: "migration-1", businessKey: "migration-1", initiatorId: "employee", definition: v1, data: {} });
        await expect.poll(hold.calls).toBe(1);
        const failure = await test.engine.migrate({ tenantId: "tenant", actorId: "admin", instanceId: "migration-1", requestId: "migrate-1", toVersion: 2 })
          .then(() => undefined, (error: unknown) => error) as { cause?: { type?: string } } | undefined;
        expect(failure?.cause?.type).toBe("MIGRATION_REQUIRES_SUSPENDED");
        expect(memory.events.some((event) => event.eventType === "workflow.migrateRequested")).toBe(false);
      } finally { hold.release(); }
    });
  }, 30_000);

  it("rejects migration while a task is active even when suspended", async () => {
    const v1 = parseDefinition({ schemaVersion: 1, id: "migration", version: 1, name: "Migration", nodes: [human("review"), human("legacy")] });
    const v2 = parseDefinition({ schemaVersion: 1, id: "migration", version: 2, name: "Migration", nodes: [human("review"), human("modern")] });
    const memory = createMemoryAdapters({ definitions: [v1, v2] });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: "migration" });
    environment = test.environment;
    await test.run(async () => {
      await test.engine.start({ tenantId: "tenant", instanceId: "migration-1", businessKey: "migration-1", initiatorId: "employee", definition: v1, data: {} });
      await expect.poll(async () => (await test.engine.snapshot({ tenantId: "tenant", instanceId: "migration-1", actorId: "employee" })).tasks.length).toBe(1);
      await test.engine.command({ type: "suspend", requestId: "suspend", tenantId: "tenant", instanceId: "migration-1", actorId: "admin" });
      const failure = await test.engine.migrate({ tenantId: "tenant", actorId: "admin", instanceId: "migration-1", requestId: "migrate-1", toVersion: 2 })
        .then(() => undefined, (error: unknown) => error) as { cause?: { type?: string } } | undefined;
      expect(failure?.cause?.type).toBe("MIGRATION_REQUIRES_IDLE");
      expect(memory.events.some((event) => event.eventType === "workflow.migrateRequested")).toBe(false);
    });
  }, 30_000);

  it("rejects a same-version target and keeps unknown target versions as load errors", async () => {
    const hold = holdingAction();
    const v1 = parseDefinition({ schemaVersion: 1, id: "migration", version: 1, name: "Migration", nodes: [
      { id: "hold", type: "action", action: "hold", input: {}, resultKey: "held" },
      human("legacy"),
    ] });
    const memory = createMemoryAdapters({ definitions: [v1], actions: new Map([["hold", hold.handler]]) });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: "migration" });
    environment = test.environment;
    await test.run(async () => {
      try {
        await test.engine.start({ tenantId: "tenant", instanceId: "migration-1", businessKey: "migration-1", initiatorId: "employee", definition: v1, data: {} });
        await expect.poll(hold.calls).toBe(1);
        await test.engine.command({ type: "suspend", requestId: "suspend", tenantId: "tenant", instanceId: "migration-1", actorId: "admin" });
        const same = await test.engine.migrate({ tenantId: "tenant", actorId: "admin", instanceId: "migration-1", requestId: "migrate-same", toVersion: 1 })
          .then(() => undefined, (error: unknown) => error) as { cause?: { type?: string } } | undefined;
        expect(same?.cause?.type).toBe("MIGRATION_INVALID_TARGET");
        // A target the definition store cannot resolve fails inside the load activity instead.
        await expect(test.engine.migrate({ tenantId: "tenant", actorId: "admin", instanceId: "migration-1", requestId: "migrate-missing", toVersion: 9 })).rejects.toThrow();
        expect(memory.events.some((event) => event.eventType === "workflow.migrateRequested")).toBe(false);
      } finally { hold.release(); }
    });
  }, 30_000);

  it("carries the migrated definition and offset through continueAsNew", async () => {
    const hold = holdingAction();
    const fillers: Node[] = Array.from({ length: 50 }, (_value, index) => ({ id: `f${index}`, type: "trigger", action: "NONE" }));
    const v1 = parseDefinition({ schemaVersion: 1, id: "migration", version: 1, name: "Migration", settings: { continueAsNewAfterEvents: 100 }, nodes: [
      { id: "hold", type: "action", action: "hold", input: {}, resultKey: "held" }, ...fillers, human("legacy"),
    ] });
    const v2 = parseDefinition({ schemaVersion: 1, id: "migration", version: 2, name: "Migration", settings: { continueAsNewAfterEvents: 100 }, nodes: [
      { id: "hold", type: "action", action: "hold", input: {}, resultKey: "held" }, ...fillers, human("modern"),
    ] });
    const memory = createMemoryAdapters({ definitions: [v1, v2], actions: new Map([["hold", hold.handler]]) });
    const test = await createTestEngine({ adapters: memory.adapters, taskQueue: "migration" });
    environment = test.environment;
    try {
      await test.run(async () => {
        await test.engine.start({ tenantId: "tenant", instanceId: "migration-1", businessKey: "migration-1", initiatorId: "employee", definition: v1, data: {} });
        await expect.poll(hold.calls).toBe(1);
        await test.engine.command({ type: "suspend", requestId: "suspend", tenantId: "tenant", instanceId: "migration-1", actorId: "admin" });
        hold.release();
        await test.engine.migrate({ tenantId: "tenant", actorId: "admin", instanceId: "migration-1", requestId: "migrate-1", toVersion: 2 });
        await expect.poll(() => memory.events.some((event) => event.eventType === "workflow.migrated"), { timeout: 30_000 }).toBe(true);
        await test.engine.command({ type: "resume", requestId: "resume", tenantId: "tenant", instanceId: "migration-1", actorId: "admin" });
        const created = (): WorkflowEvent | undefined => memory.events.find((event) => event.eventType === "workflow.taskCreated" && (event.details.task as { nodeId: string }).nodeId === "modern");
        await expect.poll(created, { timeout: 30_000 }).toBeTruthy();
        // The resumed chain runs the migrated graph: multiple Temporal runs, v2 pinned, no legacy node.
        expect(new Set(memory.events.map((event) => event.eventId.split(":")[0])).size).toBeGreaterThan(1);
        expect(created()!.definition).toEqual({ id: "migration", version: 2 });
        expect(memory.events.some((event) => event.eventType === "workflow.taskCreated" && (event.details.task as { nodeId: string }).nodeId === "legacy")).toBe(false);
        const task = (created()!.details.task as { id: string }).id;
        await test.engine.command({ type: "approve", requestId: "approve-modern", tenantId: "tenant", instanceId: "migration-1", taskId: task, actorId: "modern" });
        await expect.poll(() => memory.events.some((event) => event.eventType === "workflow.completed" && event.definition.version === 2), { timeout: 30_000 }).toBe(true);
      });
    } finally { hold.release(); }
  }, 90_000);

  it("persists a human task across replay and handles an idempotent approval", async () => {
    environment = await TestWorkflowEnvironment.createTimeSkipping();
    const definition = parseDefinition({
      schemaVersion: 1, id: "leave", version: 1, name: "Leave", nodes: [
        { id: "manager", type: "approval", assignees: { type: "users", userIds: ["manager"] }, mode: "all" },
        { id: "wait", type: "wait", event: "handoff", timeoutMs: 86_400_000, onTimeout: "continue", resultKey: "handoff" },
      ],
    });
    const memory = createMemoryAdapters({ definitions: definition });
    const taskQueue = "sdk-test";
    const options = { connection: environment.nativeConnection, namespace: environment.namespace,
      taskQueue, maxCachedWorkflows: 0, workflowsPath: resolveWorkflowsPath(), activities: createActivities(memory.adapters) };
    const worker = await Worker.create(options);
    const workflowClient = new WorkflowEngineClient({ client: environment.client as Client, taskQueue, adapters: memory.adapters });
    const input = { tenantId: "tenant", instanceId: "leave-1", businessKey: "leave-1", initiatorId: "employee", definition, data: {} };
    const started = await worker.runUntil(async () => {
      const started = await workflowClient.start(input);
      let snapshot = await environment!.client.workflow.getHandle(workflowId("tenant", "leave-1")).query<Snapshot>(SNAPSHOT_QUERY);
      for (let i = 0; i < 20 && !snapshot.tasks.length; i++) snapshot = await environment!.client.workflow.getHandle(workflowId("tenant", "leave-1")).query<Snapshot>(SNAPSHOT_QUERY);
      const task = snapshot.tasks[0];
      if (!task) throw new Error("task was not created");
      return { started, taskId: task.id };
    });
    expect(worker.getState()).toBe('STOPPED');
    const restartedWorker = await Worker.create(options);
    const result = await restartedWorker.runUntil(async () => {
      const snapshot = await workflowClient.snapshot({ tenantId: 'tenant', actorId: 'manager', instanceId: 'leave-1' });
      expect(snapshot.tasks[0]?.id).toBe(started.taskId);
      const command = { type: "approve" as const, requestId: "approve-1", tenantId: "tenant", instanceId: "leave-1", actorId: "manager", taskId: started.taskId };
      await workflowClient.command(command);
      await workflowClient.command(command);
      return started.started.result();
    });
    expect(result.status).toBe("completed");
    expect(memory.events.some((event) => event.eventType === "workflow.completed")).toBe(true);
    const history = await environment.client.workflow.getHandle(workflowId('tenant', 'leave-1')).fetchHistory();
    await Worker.runReplayHistory({ workflowsPath: options.workflowsPath }, history, workflowId('tenant', 'leave-1'));
  }, 30_000);
});
