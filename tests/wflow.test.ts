import { describe, it, expect } from 'vitest';
import { importWflowDefinition, importWflowCondition } from '../src/wflow';
import { evaluate, executeScript, translateSpel, validateFields } from '../src/definition';
import { actionableUsers, applyTaskCommand } from '../src/tasks';
import type { HumanNode, Task } from '../src/schema';

const approval = (id: string, mode = 'AND') => ({ id, type: 'Approval', props: { ruleType: 'ASSIGN_USER', assignUser: ['manager'], taskMode: { type: mode, percentage: 67 } } });
describe('wflow editor compatibility', () => {
  it.each([['AUTO_PASS', 'approve'], ['AUTO_REFUSE', 'reject']])('imports automatic terminal node %s', (mode, outcome) => {
    expect(importWflowDefinition({ id: 'automatic', name: 'Automatic', nodes: [{ id: 'decision', type: 'Approval', props: { mode } }] }).nodes).toEqual([{ id: 'decision', type: 'terminate', outcome }]);
  });
  it.each([['FIXED', 'delay'], ['DATETIME', 'delayUntil'], ['TODAY', 'delayUntil']] as const)('imports waiting mode %s', (mode, type) => {
    const props = mode === 'FIXED' ? { type: mode, timeout: 2, timeUnit: 'H' } : mode === 'DATETIME' ? { type: mode, dateTime: '2030-01-02 03:04:05' } : { type: mode, time: '08:30:00' };
    const node = importWflowDefinition({ id: 'test', name: 'Test', nodes: [{ id: 'wait', type: 'Waiting', props }] }).nodes[0]!;
    expect(node.type).toBe(type);
    if (mode === 'DATETIME' && node.type === 'delayUntil') expect(node.at).toBe('2030-01-02T03:04:05+08:00');
    if (mode === 'TODAY' && node.type === 'delayUntil') expect(node.timeOfDay).toBe('08:30:00');
  });
  it('imports Trigger nodes for EL/JS/HTTP and signals', () => {
    const nodes = importWflowDefinition({ id: 'trigger-test', name: 'Trigger', nodes: [
      { id: 'el', type: 'Trigger', props: { type: 'EL', el: 'ctx.amount' } },
      { id: 'js', type: 'Trigger', props: { type: 'JS', jsCode: 'return ctx.amount * 2;' } },
      { id: 'http', type: 'Trigger', props: { type: 'HTTP', http: { url: 'https://example.test', method: 'POST' } } },
      { id: 'signal', type: 'Trigger', props: { type: 'SIGNAL', signal: { name: 'notify-${instId}', scope: 'INSTANCE', instId: 'other' } } },
    ] }).nodes;
    expect(nodes[0]).toMatchObject({ type: 'trigger', action: 'EL', script: 'ctx.amount' });
    expect(nodes[1]).toMatchObject({ type: 'trigger', action: 'JS' });
    expect(nodes[2]).toMatchObject({ type: 'trigger', action: 'HTTP' });
    expect(nodes[3]).toMatchObject({ type: 'trigger', action: 'SIGNAL', signal: { scope: 'INSTANCE', instId: 'other' } });
  });
  it('imports Subproc options including async, mappings and initiator overrides', () => {
    const node = importWflowDefinition({ id: 'parent', name: 'Parent', nodes: [{ id: 'sub', type: 'Subproc', props: {
      code: 'child-flow', defineId: 'child-flow:3', version: 3, isBindVer: true,
      isAsync: true, isSyncAllVar: true, isSyncBizKey: true, formAutoMapping: true, statusSync: true,
      initiatorType: 'FIXED', fixedUser: { id: 'u-admin' },
      contextMap: [
        { source: 'amount', target: 'cost', isVar: true },
        { source: '#orderId', target: 'orderId', isFixed: true, isVar: true, sync: true },
        { source: '{"nested":1}', target: 'payload', isFixed: true },
      ],
    } }] }).nodes[0]!;
    expect(node).toMatchObject({ type: 'child', definition: { id: 'child-flow:3', version: 3 }, async: true,
      inheritVariables: true, inheritBusinessKey: true, formAutoMapping: true, statusSync: true,
      initiator: { type: 'fixed', userId: 'u-admin' },
      mappings: [
        { source: 'amount', target: 'cost', isVar: true, sync: false, fixed: false },
        { source: '#orderId', target: 'orderId', isVar: true, sync: true, fixed: true },
        { source: '{"nested":1}', target: 'payload', isVar: false, sync: false, fixed: true },
      ] });
  });
  it('leaves unbound subprocess references at version 0 so the host resolves the active version', () => {
    const node = importWflowDefinition({ id: 'parent', name: 'Parent', nodes: [{ id: 'sub', type: 'Subproc', props: { code: 'child-flow' } }] }).nodes[0]!;
    expect(node).toMatchObject({ type: 'child', definition: { id: 'child-flow', version: 0 } });
  });
  it('defaults a missing reject rule to NEXT like the Java reference', () => {
    const node = importWflowDefinition({ id: 'reject', name: 'Reject', nodes: [approval('a')] }).nodes[0]!;
    expect(node).toMatchObject({ rejectRule: { type: 'NEXT' } });
  });
  it('maps ROOT_SELF and the original timeout refusal enum', () => {
    expect(importWflowDefinition({ id: 'self', name: 'Self', nodes: [{ id: 'review', type: 'Approval', props: { ruleType: 'ROOT_SELF', timeout: { enable: true, type: 'TO_REFUSE', time: 1, timeUnit: 'D' } } }] }).nodes[0]).toMatchObject({ assignees: { type: 'initiator' }, timeout: { afterMs: 86_400_000, outcome: 'reject' } });
  });
  it('matches wflow empty and strict numeric range conditions', () => {
    const empty = importWflowCondition({ groups: [{ conditions: [{ group: 'FORM', symbol: 'value', compare: 'EM' }] }] });
    for (const value of [null, '', []]) expect(evaluate(empty, { value })).toBe(true);
    expect(evaluate(empty, { value: 0 })).toBe(false);
    const between = importWflowCondition({ groups: [{ conditions: [{ group: 'FORM', symbol: 'value', compare: 'BT', compareVal: [1, 3] }] }] });
    expect(evaluate(between, { value: 1 })).toBe(false); expect(evaluate(between, { value: 2 })).toBe(true);
  });
  it('accepts empty optional fields and rejects empty mandatory fields', () => {
    for (const value of [null, '', []]) expect(() => validateFields([{ key: 'value', type: Array.isArray(value) ? 'array' : 'string', required: true }], { value }, true)).toThrow('REQUIRED_FIELD');
    expect(() => validateFields([{ key: 'value', type: 'string' }], { value: null }, true)).not.toThrow();
  });
  it('enforces field regex patterns', () => {
    const fields = [{ key: 'code', type: 'string' as const, pattern: '^WF\\d+$' }];
    expect(() => validateFields(fields, { code: 'WF123' }, true)).not.toThrow();
    expect(() => validateFields(fields, { code: 'bad' }, true)).toThrow('INVALID_FIELD_PATTERN');
  });
  it('translates SpEL expressions to the JS sandbox semantics', () => {
    expect(translateSpel("#amount > 100 and #name != null")).toBe('ctx.amount > 100 && ctx.name != null');
    // `not` now always wraps its operand, which fixes precedence over comparisons.
    expect(translateSpel("#name.length() > 3 or not #active")).toBe('ctx.name.length > 3 || !(ctx.active)');
    expect(translateSpel("#name.equals('x')")).toBe("ctx.name === ('x')");
    expect(() => translateSpel('T(java.lang.Math).max(1,2)')).toThrow('SCRIPT_EXECUTION_FAILED');
    expect(executeScript('EL', "#amount > 100 and #name.length() > 1", { amount: 200, name: 'ab' })).toBe(true);
    expect(executeScript('EL', "#roles.contains('admin')", { roles: ['admin'] })).toBe(true);
  });
  it.each([['AND', 'all'], ['OR', 'any'], ['NEXT', 'sequential'], ['CUSTOM', 'percentage']])('maps %s to %s', (old, mode) => {
    const definition = importWflowDefinition({ id: 'test', name: 'Test', nodes: [approval('approve', old)] });
    expect(definition.nodes[0]).toMatchObject({ mode });
  });
  it('imports nested gateways including the default branch and continuation', () => {
    const definition = importWflowDefinition({ id: 'test', name: 'Test', nodes: [
      { id: 'root', type: 'Start', childId: 'fork' },
      { id: 'fork', type: 'Gateway', childId: 'join', props: { type: 'Exclusive', branch: [
        { id: 'condition', props: { logic: true, groups: [{ logic: true, conditions: [{ group: 'FORM', symbol: 'days', compare: 'GT', compareVal: [3] }] }] } },
        { id: 'default', props: {} },
      ] }, branch: [[approval('long')], [approval('short')]] },
      { id: 'join', type: 'Join', childId: 'after' }, approval('after'),
    ] });
    const gateway = definition.nodes[0]!;
    expect(gateway.type).toBe('exclusive');
    if (gateway.type !== 'exclusive') throw new Error('Expected gateway');
    expect(evaluate(gateway.branches[0]!.when, { days: 4 })).toBe(true);
    expect(evaluate(gateway.branches[0]!.when, { days: 2 })).toBe(false);
    expect(gateway.otherwise[0]?.id).toBe('short');
    expect(definition.nodes[1]?.id).toBe('after');
  });
  it('rejects dangling links and duplicate identifiers instead of losing nodes', () => {
    expect(() => importWflowDefinition({ id: 'test', name: 'Test', nodes: [{ ...approval('a'), childId: 'missing' }] })).toThrow('DANGLING_WFLOW_LINK');
    expect(() => importWflowDefinition({ id: 'test', name: 'Test', nodes: [approval('a'), approval('a')] })).toThrow('DUPLICATE_NODE_ID');
  });
  it('maps fixed waiting to a durable timer', () => {
    expect(importWflowDefinition({ id: 'test', name: 'Test', nodes: [{ id: 'wait', type: 'Waiting', props: { type: 'FIXED', timeout: 2, timeUnit: 'H' } }] }).nodes).toEqual([{ id: 'wait', type: 'delay', durationMs: 7_200_000 }]);
  });
  it('imports Router node and initiator conditions', () => {
    const routerDef = importWflowDefinition({
      id: 'router-test',
      name: 'Router Test',
      nodes: [
        { id: 'start', type: 'Start', childId: 'r1' },
        { id: 'r1', type: 'Router', props: { hasCondition: true, groups: [{ conditions: [{ group: 'INITIATOR', valueType: 'dept', compare: 'IN', compareVal: [{ id: '1486186' }] }] }], target: { id: 'target_node' } }, childId: 'target_node' },
        approval('target_node'),
      ],
    });
    expect(routerDef.nodes[0]).toMatchObject({ id: 'r1', type: 'router', targetNodeId: 'target_node' });
    const cond = (routerDef.nodes[0] as { when: import('../src/schema').Condition }).when;
    expect(evaluate(cond, { _initiatorDeptLevels: ['1486186'] })).toBe(true);
    expect(evaluate(cond, { _initiatorDeptLevels: ['9999999'] })).toBe(false);
  });
  it('evaluates time, before, after and timeBetween conditions', () => {
    const before = importWflowCondition({ groups: [{ conditions: [{ group: 'FORM', symbol: 'time', compare: 'BF', compareVal: ['12:00:00'] }] }] });
    expect(evaluate(before, { time: '09:00:00' })).toBe(true);
    expect(evaluate(before, { time: '14:00:00' })).toBe(false);

    const betweenTime = importWflowCondition({ groups: [{ conditions: [{ group: 'FORM', symbol: 'time', compare: 'CT', compareVal: ['09:00:00', '18:00:00'] }] }] });
    expect(evaluate(betweenTime, { time: '12:00:00' })).toBe(true);
    expect(evaluate(betweenTime, { time: '20:00:00' })).toBe(false);
  });
  it('enforces needSign requirement on task approval', () => {
    const node: HumanNode = { id: 'a', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager'] }, needSign: true };
    const task: Task = { id: 'task', nodeId: 'a', type: 'approval', mode: 'all', assignees: ['manager'], candidates: [], approved: [], fields: [], status: 'pending', createdAt: new Date(0).toISOString() };
    const base = { tenantId: 't', instanceId: 'i', taskId: 'task', requestId: 'r', actorId: 'manager' };
    expect(() => applyTaskCommand(task, node, { ...base, type: 'approve' }, 'employee', {})).toThrow('SIGNATURE_REQUIRED');
    expect(() => applyTaskCommand(task, node, { ...base, type: 'approve', signature: 'data:image/png;base64,123' }, 'employee', {})).not.toThrow();
    expect(task.status).toBe('approved');
    expect(task.signature).toBe('data:image/png;base64,123');
  });
  it('carries the admin override signature onto the task like FlowManagerServiceImpl', () => {
    const node: HumanNode = { id: 'a', type: 'approval', mode: 'all', assignees: { type: 'users', userIds: ['manager'] }, needSign: true };
    const task: Task = { id: 'task', nodeId: 'a', type: 'approval', mode: 'all', assignees: ['manager'], candidates: [], approved: [], fields: [], status: 'pending', createdAt: new Date(0).toISOString() };
    const base = { tenantId: 't', instanceId: 'i', taskId: 'task', requestId: 'r' };
    applyTaskCommand(task, node, { ...base, type: 'override', actorId: 'admin', userId: 'manager', action: 'approve', signature: 'data:image/png;base64,456' }, 'employee', {});
    expect(task.status).toBe('approved');
    expect(task.signature).toBe('data:image/png;base64,456');
  });
});
describe('additional approval ordering', () => {
  const build = (mode: 'any' | 'sequential') => {
    const node: HumanNode = { id: 'a', type: 'approval', mode, assignees: { type: 'users', userIds: ['manager'] }, allowAddAssignees: true };
    const task: Task = { id: 'task', nodeId: 'a', type: 'approval', mode, assignees: ['manager'], candidates: [], approved: [], fields: [], status: 'pending', createdAt: new Date(0).toISOString() };
    return { node, task, base: { tenantId: 't', instanceId: 'i', taskId: 'task', requestId: 'r' } };
  };
  it('adds an after-signature to a parallel node and waits for it', () => {
    const { node, task, base } = build('any');
    applyTaskCommand(task, node, { ...base, type: 'addAssignee', actorId: 'manager', userId: 'hr', position: 'after' }, 'employee', {});
    expect(actionableUsers(task)).toEqual(['manager']);
    applyTaskCommand(task, node, { ...base, type: 'approve', actorId: 'manager' }, 'employee', {});
    expect(task.status).toBe('pending');
    expect(actionableUsers(task)).toEqual(['hr']);
    applyTaskCommand(task, node, { ...base, type: 'approve', actorId: 'hr' }, 'employee', {});
    expect(task.status).toBe('approved');
  });
  it('forbids before-signature on parallel nodes like the Java AddMultiInstanceCmd', () => {
    const { node, task, base } = build('any');
    expect(() => applyTaskCommand(task, node, { ...base, type: 'addAssignee', actorId: 'manager', userId: 'hr', position: 'before' }, 'employee', {})).toThrow('BEFORE_ADD_FORBIDDEN');
  });
  it('reorders a sequential node with a before-signature', () => {
    const { node, task, base } = build('sequential');
    applyTaskCommand(task, node, { ...base, type: 'addAssignee', actorId: 'manager', userId: 'hr', position: 'before' }, 'employee', {});
    expect(actionableUsers(task)).toEqual(['hr']);
    applyTaskCommand(task, node, { ...base, type: 'approve', actorId: 'hr' }, 'employee', {});
    expect(actionableUsers(task)).toEqual(['manager']);
    applyTaskCommand(task, node, { ...base, type: 'approve', actorId: 'manager' }, 'employee', {});
    expect(task.status).toBe('approved');
  });
});
