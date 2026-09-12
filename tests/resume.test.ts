import { describe, expect, it } from "vitest";
import { WorkflowValidationError } from "../src/errors.js";
import { assertResumePayload, carryResumeState, rememberReceipt, RESUME_LIMITS, resumeInitialState, type ResumeInputs } from "../src/resume.js";
import { inputSchema, resumeSchema, type Task } from "../src/schema.js";
import type { HumanCompletion } from "../src/workflow-context.js";

const task = (nodeId: string): Task => ({
  id: `task-${nodeId}`, nodeId, type: "approval", mode: "all",
  assignees: ["u1"], candidates: [], approved: [], status: "approved",
  createdAt: "2026-01-01T00:00:00.000Z", fields: [],
});

function inputs(overrides: Partial<ResumeInputs> = {}): ResumeInputs {
  return {
    offset: 3, sequence: 7, steps: 4, commandBytes: 120,
    agreedUsers: new Set(["u1"]),
    completedHumans: new Map<string, HumanCompletion>([["first", { nodeId: "first", name: "first", actorIds: ["u1"] }]]),
    completedTasks: new Map<string, Task>([["first", task("first")]]),
    receipts: new Map([["r1", { fingerprint: "fp-1", receipt: { requestId: "r1", eventId: "run:1" } }]]),
    pendingEvents: [{ event: "go", data: { ready: true } }],
    ...overrides,
  };
}

describe("continueAsNew resume schema", () => {
  it("accepts a bounded resume state and rejects malformed shapes", () => {
    const carry = carryResumeState(inputs());
    expect(resumeSchema.parse(carry)).toMatchObject({ offset: 3, sequence: 7, steps: 4, commandBytes: 120 });
    expect(() => resumeSchema.parse({ ...carry, offset: -1 })).toThrow();
    expect(() => resumeSchema.parse({ ...carry, steps: 1.5 })).toThrow();
    expect(() => resumeSchema.parse({ ...carry, extra: true })).toThrow();
    expect(() => resumeSchema.parse({ ...carry, receipts: Array.from({ length: 201 }, (_value, index) => ({ requestId: `r${index}`, fingerprint: "fp", receipt: { requestId: `r${index}`, eventId: "e" } })) })).toThrow();
  });

  it("validates resume through the workflow input schema", () => {
    const definition = { schemaVersion: 1, id: "d", version: 1, name: "D", nodes: [{ id: "n", type: "trigger", action: "NONE" }] };
    const parsed = inputSchema.parse({ tenantId: "t", instanceId: "i", businessKey: "b", initiatorId: "u", definition, data: {}, resume: carryResumeState(inputs()) });
    expect(parsed.resume?.offset).toBe(3);
    expect(() => inputSchema.parse({ tenantId: "t", instanceId: "i", businessKey: "b", initiatorId: "u", definition, data: {}, resume: { ...carryResumeState(inputs()), completedTasks: "bad" } })).toThrow();
  });
});

describe("carried state helpers", () => {
  it("keeps only the newest receipts and completed tasks", () => {
    const receipts = new Map(Array.from({ length: RESUME_LIMITS.receipts + 5 }, (_value, index) => [`r${index}`, { fingerprint: `fp${index}`, receipt: { requestId: `r${index}`, eventId: `e${index}` } }]));
    const completedTasks = new Map(Array.from({ length: RESUME_LIMITS.completedTasks + 5 }, (_value, index) => [`n${index}`, task(`n${index}`)]));
    const carry = carryResumeState(inputs({ receipts, completedTasks }));
    expect(carry.receipts).toHaveLength(RESUME_LIMITS.receipts);
    expect(carry.receipts[0]!.requestId).toBe("r5");
    expect(carry.receipts.at(-1)!.requestId).toBe(`r${RESUME_LIMITS.receipts + 4}`);
    expect(carry.completedTasks).toHaveLength(RESUME_LIMITS.completedTasks);
  });

  it("always carries the newest handled node even when map order is stale", () => {
    const completedTasks = new Map(Array.from({ length: RESUME_LIMITS.completedTasks + 2 }, (_value, index) => [`n${index}`, task(`n${index}`)]));
    // "rerun" completed after n11 but its map entry keeps the original (older) position.
    completedTasks.set("rerun", task("rerun"));
    const completedHumans = new Map<string, HumanCompletion>([...completedTasks.keys()].map((nodeId) => [nodeId, { nodeId, name: nodeId, actorIds: [] }]));
    completedHumans.delete("rerun");
    completedHumans.set("rerun", { nodeId: "rerun", name: "rerun", actorIds: [] });
    const carry = carryResumeState(inputs({ completedTasks, completedHumans }));
    expect(carry.completedTasks.some((entry) => entry.nodeId === "rerun")).toBe(true);
  });

  it("round-trips carried state into per-run maps", () => {
    const carry = carryResumeState(inputs());
    const initial = resumeInitialState(carry);
    expect(initial.sequence).toBe(7);
    expect(initial.steps).toBe(4);
    expect(initial.commandBytes).toBe(120);
    expect([...initial.agreedUsers]).toEqual(["u1"]);
    expect(initial.completedHumans.get("first")).toEqual({ nodeId: "first", name: "first", actorIds: ["u1"] });
    expect(initial.completedTasks.get("first")?.nodeId).toBe("first");
    expect(initial.receipts.get("r1")?.receipt.eventId).toBe("run:1");
    expect(initial.pendingEvents).toEqual([{ event: "go", data: { ready: true } }]);
    expect(resumeInitialState(undefined)).toMatchObject({ sequence: 0, steps: 0, commandBytes: 0 });
  });

  it("evicts the oldest receipt once the carry bound is reached", () => {
    const receipts = new Map<string, { fingerprint: string; receipt: { requestId: string; eventId: string } }>();
    for (let index = 0; index <= RESUME_LIMITS.receipts; index += 1) rememberReceipt(receipts, `r${index}`, { fingerprint: "fp", receipt: { requestId: `r${index}`, eventId: "e" } });
    expect(receipts.size).toBe(RESUME_LIMITS.receipts);
    expect(receipts.has("r0")).toBe(false);
    expect(receipts.has(`r${RESUME_LIMITS.receipts}`)).toBe(true);
  });

  it("maps oversized or non-JSON carried state to RESUME_STATE_TOO_LARGE", () => {
    expect(() => assertResumePayload({ big: "x".repeat(200_000) })).toThrowError(new WorkflowValidationError("RESUME_STATE_TOO_LARGE"));
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(() => assertResumePayload(cyclic)).toThrowError(new WorkflowValidationError("RESUME_STATE_TOO_LARGE"));
    expect(() => assertResumePayload({ ok: true })).not.toThrow();
  });
});
