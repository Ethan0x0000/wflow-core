import { describe, expect, it } from "vitest";
import { parseDefinition } from "../src/definition.js";
import { WorkflowValidationError } from "../src/errors.js";
import { assertMigratable, migrationOffset, pruneMigrationState } from "../src/migration.js";
import { commandSchema, type Task } from "../src/schema.js";

const command = { type: "migrate" as const, requestId: "migrate-1", tenantId: "tenant", instanceId: "instance", actorId: "admin", toVersion: 2 };

describe("migrate command schema", () => {
  it("accepts a bounded target version and an optional comment", () => {
    expect(commandSchema.parse(command)).toEqual(command);
    expect(commandSchema.parse({ ...command, toVersion: 1, comment: { text: "upgrade" } })).toMatchObject({ toVersion: 1, comment: { text: "upgrade" } });
  });

  it("rejects invalid target versions", () => {
    expect(() => commandSchema.parse({ ...command, toVersion: 0 })).toThrow();
    expect(() => commandSchema.parse({ ...command, toVersion: -1 })).toThrow();
    expect(() => commandSchema.parse({ ...command, toVersion: 1.5 })).toThrow();
    expect(() => commandSchema.parse({ ...command, toVersion: "2" })).toThrow();
    expect(() => commandSchema.parse({ requestId: command.requestId, tenantId: command.tenantId, instanceId: command.instanceId, actorId: command.actorId, type: "migrate" })).toThrow();
    expect(() => commandSchema.parse({ ...command, extra: true })).toThrow();
  });
});

describe("migration preconditions", () => {
  const ready = { suspended: true, activeTasks: 0, activeWaits: 0, depth: 0, currentVersion: 1, toVersion: 2 };

  it("requires suspended, idle and top-level state plus a distinct target version", () => {
    expect(() => assertMigratable(ready)).not.toThrow();
    expect(() => assertMigratable({ ...ready, suspended: false })).toThrowError(new WorkflowValidationError("MIGRATION_REQUIRES_SUSPENDED"));
    expect(() => assertMigratable({ ...ready, activeTasks: 1 })).toThrowError(new WorkflowValidationError("MIGRATION_REQUIRES_IDLE"));
    expect(() => assertMigratable({ ...ready, activeWaits: 1 })).toThrowError(new WorkflowValidationError("MIGRATION_REQUIRES_IDLE"));
    expect(() => assertMigratable({ ...ready, depth: 1 })).toThrowError(new WorkflowValidationError("MIGRATION_REQUIRES_TOP_LEVEL"));
    expect(() => assertMigratable({ ...ready, toVersion: 1 })).toThrowError(new WorkflowValidationError("MIGRATION_INVALID_TARGET"));
  });
});

describe("migration rebase and state pruning", () => {
  const target = parseDefinition({
    schemaVersion: 1, id: "definition", version: 2, name: "Definition",
    nodes: [
      { id: "start", type: "trigger", action: "NONE" },
      { id: "modern", type: "task", mode: "all", assignees: { type: "users", userIds: ["u1"] } },
    ],
  });
  const withResubmit = parseDefinition({
    schemaVersion: 1, id: "definition", version: 2, name: "Definition",
    nodes: [{ id: "start", type: "trigger", action: "NONE" }],
    resubmit: { id: "resubmit", type: "task", mode: "all", assignees: { type: "users", userIds: ["u1"] } },
  });
  const task = (nodeId: string): Task => ({
    id: `task-${nodeId}`, nodeId, type: "approval", mode: "all", assignees: ["u1"], candidates: [], approved: [],
    status: "pending", createdAt: new Date(0).toISOString(), fields: [],
  });

  it("rebases on the tracked top-level node and fails when the target graph lost it", () => {
    expect(migrationOffset(target, "modern")).toBe(1);
    expect(migrationOffset(target, "start")).toBe(0);
    expect(() => migrationOffset(target, "legacy")).toThrowError(new WorkflowValidationError("MIGRATION_TARGET_MISSING"));
    expect(() => migrationOffset(target, undefined)).toThrowError(new WorkflowValidationError("MIGRATION_TARGET_MISSING"));
  });

  it("prunes node-scoped state and keeps the resubmit anchor", () => {
    const completedHumans = new Map([
      ["start", { nodeId: "start", name: "Start", actorIds: ["u1"] }],
      ["legacy", { nodeId: "legacy", name: "Legacy", actorIds: ["u1"] }],
    ]);
    const completedTasks = new Map([["modern", task("modern")], ["legacy", task("legacy")]]);
    const restoredTasks = new Map([["resubmit", task("resubmit")], ["legacy", task("legacy")]]);
    pruneMigrationState(withResubmit, { completedHumans, completedTasks, restoredTasks });
    expect([...completedHumans.keys()]).toEqual(["start"]);
    expect([...completedTasks.keys()]).toEqual([]);
    expect([...restoredTasks.keys()]).toEqual(["resubmit"]);
  });
});
