import { describe, expect, it } from "vitest";
import { evaluate, parseDefinition, WorkflowValidationError } from "../src/definition.js";
import { authorizationSchema } from "../src/ports.js";
import { applyTaskCommand } from "../src/tasks.js";
import { importWflowDefinition } from "../src/wflow.js";
import type { Condition, HumanNode, Task } from "../src/schema.js";

const human: HumanNode = {
  id: "manager", type: "approval", assignees: { type: "users", userIds: ["u1", "u2"] }, mode: "all",
  fields: [{ key: "comment", type: "string" }], allowTransfer: true, allowAddAssignees: true,
};
function task(): Task {
  return { id: "task-1", nodeId: "manager", type: "approval", mode: "all", assignees: ["u1", "u2"], candidates: [], approved: [], status: "pending", createdAt: new Date(0).toISOString(), fields: human.fields ?? [] };
}

describe("workflow definition", () => {
  it("evaluates typed conditions and rejects concurrent writes", () => {
    expect(evaluate({ op: "and", conditions: [
      { op: "eq", left: { path: ["amount"] }, right: { value: 10 } },
      { op: "exists", value: { path: ["employee"] } },
    ] }, { amount: 10, employee: "u1" })).toBe(true);
    expect(() => parseDefinition({ schemaVersion: 1, id: "x", version: 1, name: "x", nodes: [{
      id: "p", type: "parallel", branches: [
        [{ id: "a", type: "action", action: "a", input: {}, resultKey: "same" }],
        [{ id: "b", type: "action", action: "b", input: {}, resultKey: "same" }],
      ],
    }] })).toThrowError(new WorkflowValidationError("CONCURRENT_VARIABLE_WRITE"));
  });

  it("rejects unsafe fields and duplicate node identifiers", () => {
    expect(() => parseDefinition({ schemaVersion: 1, id: "x", version: 1, name: "x", nodes: [
      { id: "x", type: "delay", durationMs: 1 }, { id: "x", type: "delay", durationMs: 1 },
    ] })).toThrow("DUPLICATE_NODE_ID");
    expect(() => parseDefinition({ schemaVersion: 1, id: "x", version: 1, name: "x", nodes: [
      { id: "a", type: "task", assignees: { type: "users", userIds: ["u"] }, mode: "all", fields: [{ key: "__proto__", type: "string" }] },
    ] })).toThrow();
  });

  it("bounds activity and event delivery tuning settings", () => {
    const definition = (settings: unknown) => parseDefinition({ schemaVersion: 1, id: "x", version: 1, name: "x", nodes: [{ id: "a", type: "trigger", action: "NONE" }], settings });
    expect(definition({ activity: { startToCloseSeconds: 5, maximumAttempts: 2, initialIntervalMs: 100 }, eventDelivery: { maximumAttempts: 3, maximumIntervalMs: 1000 } }).settings)
      .toMatchObject({ activity: { startToCloseSeconds: 5, maximumAttempts: 2, initialIntervalMs: 100 }, eventDelivery: { maximumAttempts: 3, maximumIntervalMs: 1000 } });
    expect(definition({ eventDelivery: {} }).settings).toEqual({ eventDelivery: {} });
    expect(() => definition({ activity: { maximumAttempts: 0 } })).toThrow();
    expect(() => definition({ eventDelivery: { startToCloseSeconds: 0 } })).toThrow();
    expect(() => definition({ activity: { unknown: 1 } })).toThrow();
  });

  it("bounds the continueAsNew history threshold", () => {
    const definition = (continueAsNewAfterEvents: unknown) => parseDefinition({
      schemaVersion: 1, id: "x", version: 1, name: "x", nodes: [{ id: "a", type: "trigger", action: "NONE" }],
      settings: { continueAsNewAfterEvents },
    });
    expect(definition(100).settings).toEqual({ continueAsNewAfterEvents: 100 });
    expect(definition(50_000).settings).toEqual({ continueAsNewAfterEvents: 50_000 });
    expect(() => definition(99)).toThrow();
    expect(() => definition(50_001)).toThrow();
    expect(() => definition(100.5)).toThrow();
  });

  it("imports the common wflow linked-node shape without executing legacy scripts", () => {
    const definition = importWflowDefinition({ id: "legacy-leave", version: 3, name: "Legacy leave", nodes: [
      { id: "start", type: "Start", childId: "approve" },
      { id: "approve", type: "Approval", childId: "finish", props: { assignUser: ["manager"], taskMode: { type: "AND" } } },
      { id: "finish", type: "Join" },
    ] });
    expect(definition.id).toBe("legacy-leave");
    expect(definition.nodes[0]?.type).toBe("approval");
    expect(importWflowDefinition({ id: "legacy", name: "Legacy", nodes: [{ id: "script", type: "Trigger", props: { type: "JS", jsCode: "return 1" } }] }).nodes[0]).toMatchObject({ type: "trigger", action: "JS" });
  });
});

describe("forEach and eventGateway definitions", () => {
  const definitionWith = (node: unknown) => ({ schemaVersion: 1, id: "x", version: 1, name: "x", nodes: [node] });
  const forEach = (overrides: Record<string, unknown> = {}) => ({
    id: "each", type: "forEach", items: { path: ["items"] }, itemKey: "item",
    maxIterations: 100, nodes: [{ id: "step", type: "delay", durationMs: 1 }], ...overrides,
  });

  it("accepts a bounded forEach and rejects invalid option combinations", () => {
    expect(parseDefinition(definitionWith(forEach({ indexKey: "position", completionCondition: { op: "exists", value: { path: ["item"] } } }))).nodes).toHaveLength(1);
    expect(parseDefinition(definitionWith(forEach({ parallel: true, concurrency: 32 }))).nodes).toHaveLength(1);
    expect(() => parseDefinition(definitionWith(forEach({ itemKey: undefined })))).toThrow();
    expect(() => parseDefinition(definitionWith(forEach({ concurrency: 4 })))).toThrow();
    expect(() => parseDefinition(definitionWith(forEach({ parallel: true, concurrency: 0 })))).toThrow();
    expect(() => parseDefinition(definitionWith(forEach({ parallel: true, concurrency: 33 })))).toThrow();
    expect(() => parseDefinition(definitionWith(forEach({ parallel: true, completionCondition: { op: "exists", value: { path: ["item"] } } })))).toThrow();
    expect(() => parseDefinition(definitionWith(forEach({ indexKey: "item" })))).toThrow();
    expect(() => parseDefinition(definitionWith(forEach({ maxIterations: 0 })))).toThrow();
    expect(() => parseDefinition(definitionWith(forEach({ maxIterations: 1001 })))).toThrow();
    expect(() => parseDefinition(definitionWith(forEach({ nodes: [] })))).toThrow();
  });

  it("treats forEach iteration and nested writes as parent writes", () => {
    expect(() => parseDefinition(definitionWith({ id: "p", type: "parallel", branches: [
      [forEach({ itemKey: "shared" })],
      [{ id: "writer", type: "action", action: "a", input: {}, resultKey: "shared" }],
    ] }))).toThrowError(new WorkflowValidationError("CONCURRENT_VARIABLE_WRITE"));
    expect(() => parseDefinition(definitionWith(forEach({ nodes: [
      { id: "step", type: "delay", durationMs: 1 }, { id: "step", type: "delay", durationMs: 1 },
    ] })))).toThrow("DUPLICATE_NODE_ID");
  });

  it("accepts eventGateway races and rejects malformed branches", () => {
    const gateway = (branches: unknown[]) => ({ id: "race", type: "eventGateway", branches });
    const event = { event: "approved", resultKey: "outcome", nodes: [{ id: "winner", type: "delay", durationMs: 1 }] };
    const timer = { timeoutMs: 1000, nodes: [{ id: "timeout", type: "delay", durationMs: 1 }] };
    expect(parseDefinition(definitionWith(gateway([event, timer]))).nodes).toHaveLength(1);
    expect(() => parseDefinition(definitionWith(gateway([])))).toThrow();
    expect(() => parseDefinition(definitionWith(gateway([{ ...event, timeoutMs: 1000 }])))).toThrow();
    expect(() => parseDefinition(definitionWith(gateway([{ nodes: event.nodes }])))).toThrow();
    expect(() => parseDefinition(definitionWith(gateway([{ ...timer, resultKey: "x" }])))).toThrow();
    expect(() => parseDefinition(definitionWith(gateway([{ ...event, timeoutMs: 0 }])))).toThrow();
    expect(() => parseDefinition(definitionWith(gateway([event, { ...event, nodes: [{ id: "winner", type: "delay", durationMs: 1 }] }])))).toThrow("DUPLICATE_NODE_ID");
  });
});

describe("custom node definitions", () => {
  const definition = (node: unknown) => ({ schemaVersion: 1, id: "x", version: 1, name: "x", nodes: [node] });

  it("accepts a bounded custom node and rejects invalid kinds and unknown fields", () => {
    expect(parseDefinition(definition({ id: "custom", type: "custom", kind: "host.verify", config: { attempts: 2 }, resultKey: "verification" })).nodes).toHaveLength(1);
    expect(parseDefinition(definition({ id: "custom", type: "custom", kind: "k", events: { enter: [{ type: "JS", action: "return 1;" }] } })).nodes).toHaveLength(1);
    expect(() => parseDefinition(definition({ id: "custom", type: "custom", kind: "" }))).toThrow();
    expect(() => parseDefinition(definition({ id: "custom", type: "custom", kind: "x".repeat(129) }))).toThrow();
    expect(() => parseDefinition(definition({ id: "custom", type: "custom", kind: "k", unexpected: true }))).toThrow();
    expect(() => parseDefinition(definition({ id: "custom", type: "custom", kind: "k", events: { enter: [{ type: "JS", action: "process.exit(1)" }] } }))).toThrow("SCRIPT_REJECTED");
  });

  it("counts a custom resultKey as a write and validates nested node ids", () => {
    expect(() => parseDefinition(definition({ id: "parallel", type: "parallel", branches: [
      [{ id: "one", type: "custom", kind: "k", resultKey: "shared" }],
      [{ id: "two", type: "custom", kind: "k", resultKey: "shared" }],
    ] }))).toThrowError(new WorkflowValidationError("CONCURRENT_VARIABLE_WRITE"));
    expect(() => parseDefinition(definition({ id: "each", type: "forEach", items: { value: [1] }, itemKey: "item", maxIterations: 2,
      nodes: [{ id: "step", type: "custom", kind: "k" }, { id: "step", type: "custom", kind: "k" }] }))).toThrow("DUPLICATE_NODE_ID");
  });
});

describe("authorization requests", () => {
  it("allows list without an instanceId and still requires it for read, command and signal", () => {
    expect(authorizationSchema.parse({ tenantId: "t", actorId: "u", operation: "list" })).toMatchObject({ operation: "list" });
    expect(authorizationSchema.parse({ tenantId: "t", actorId: "u", operation: "read", instanceId: "i" })).toMatchObject({ operation: "read", instanceId: "i" });
    expect(authorizationSchema.parse({ tenantId: "t", actorId: "u", operation: "signal", instanceId: "i" })).toMatchObject({ operation: "signal", instanceId: "i" });
    expect(() => authorizationSchema.parse({ tenantId: "t", actorId: "u", operation: "read" })).toThrow();
    expect(() => authorizationSchema.parse({ tenantId: "t", actorId: "u", operation: "command" })).toThrow();
    expect(() => authorizationSchema.parse({ tenantId: "t", actorId: "u", operation: "signal" })).toThrow();
  });
});

describe("initiator conditions", () => {
  const user: Condition = { op: "initiator", dimension: "user", compare: "in", values: ["u1"] };
  const dept: Condition = { op: "initiator", dimension: "dept", compare: "in", values: ["d1"] };
  const role: Condition = { op: "initiator", dimension: "role", compare: "has", values: ["hr"] };

  it("reads the first-class initiator context", () => {
    expect(evaluate(user, {}, { initiatorId: "u1" })).toBe(true);
    expect(evaluate(user, {}, { initiatorId: "u2" })).toBe(false);
    expect(evaluate(dept, {}, { initiatorId: "u1", deptLevels: ["d2", "d1"] })).toBe(true);
    expect(evaluate(dept, {}, { initiatorId: "u1", deptId: "d9" })).toBe(false);
    expect(evaluate(role, {}, { initiatorId: "u1", roles: ["hr"] })).toBe(true);
    expect(evaluate(role, {}, { initiatorId: "u1", roles: ["finance"] })).toBe(false);
  });

  it("falls back to the legacy _initiator data keys", () => {
    expect(evaluate(user, { _initiatorId: "u1" })).toBe(true);
    expect(evaluate(dept, { _initiatorDeptLevels: ["d1"] })).toBe(true);
    expect(evaluate(role, { _initiatorRoles: [{ id: "hr" }] })).toBe(true);
    expect(evaluate(role, { _initiatorRoles: ["hr"] })).toBe(true);
  });

  it("prefers the first-class context and propagates through and/or/not", () => {
    expect(evaluate(role, { _initiatorRoles: ["hr"] }, { initiatorId: "u1", roles: ["finance"] })).toBe(false);
    expect(evaluate({ op: "not", condition: role }, {}, { initiatorId: "u1", roles: ["hr"] })).toBe(false);
    expect(evaluate({ op: "and", conditions: [user, role] }, {}, { initiatorId: "u1", roles: ["hr"] })).toBe(true);
    expect(evaluate({ op: "or", conditions: [role, user] }, {}, { initiatorId: "u9" })).toBe(false);
  });
});

describe("human tasks", () => {
  it("supports quorum voting and keeps duplicate votes idempotent at the command boundary", () => {
    const node = { ...human, mode: "percentage", percentage: 50 } as HumanNode;
    const current = task(); current.mode = "percentage";
    applyTaskCommand(current, node, { type: "approve", requestId: "r1", tenantId: "t", instanceId: "i", actorId: "u1", taskId: current.id }, "owner", {});
    expect(current.status).toBe("approved");
    expect(() => applyTaskCommand(current, node, { type: "approve", requestId: "r2", tenantId: "t", instanceId: "i", actorId: "u1", taskId: current.id }, "owner", {})).toThrow("TASK_CLOSED");
  });

  it("requires claim before a candidate can act", () => {
    const node = { ...human, mode: "candidate" } as HumanNode;
    const current = task(); current.mode = "candidate"; current.assignees = []; current.candidates = ["u1"];
    expect(() => applyTaskCommand(current, node, { type: "approve", requestId: "r1", tenantId: "t", instanceId: "i", actorId: "u1", taskId: current.id }, "owner", {})).toThrow("NOT_TASK_ASSIGNEE");
    applyTaskCommand(current, node, { type: "claim", requestId: "r2", tenantId: "t", instanceId: "i", actorId: "u1", taskId: current.id }, "owner", {});
    applyTaskCommand(current, node, { type: "approve", requestId: "r3", tenantId: "t", instanceId: "i", actorId: "u1", taskId: current.id }, "owner", {});
    expect(current.status).toBe("approved");
  });
});
