import { ApplicationFailure } from "@temporalio/common";
import { describe, expect, it } from "vitest";
import { createActivities } from "../src/activities.js";
import type { Adapters, TelemetryEvent } from "../src/ports.js";

const context = { tenantId: "tenant", instanceId: "instance", businessKey: "instance", initiatorId: "initiator", definition: { id: "definition", version: 1 } };
const authorization = { tenantId: "tenant", actorId: "u-1", operation: "read" as const, instanceId: "instance" };
const adapters = (overrides: Partial<Adapters> = {}): Adapters => ({
  definitions: { async get() { return undefined; } },
  authorize: async () => true,
  resolveAssignees: async () => ({ users: [] }),
  actions: new Map(),
  publishEvent: async () => {},
  ...overrides,
});

describe("Adapters.telemetry", () => {
  it("emits start and end around a successful activity with its instance context", async () => {
    const events: TelemetryEvent[] = [];
    const activities = createActivities(adapters({ telemetry: { onActivity: (event) => events.push(event) } }));
    await activities.resolveAssignees({ context, executionId: "n1", nodeId: "review", assignment: { type: "initiator" }, data: {} });
    expect(events).toEqual([
      { activity: "resolveAssignees", phase: "start", context },
      { activity: "resolveAssignees", phase: "end", context, durationMs: expect.any(Number) },
    ]);
  });

  it("emits the error phase with the mapped failure when an activity fails", async () => {
    const events: TelemetryEvent[] = [];
    const activities = createActivities(adapters({
      authorize: async () => { throw new Error("host failure"); },
      telemetry: { onActivity: (event) => events.push(event) },
    }));
    await expect(activities.authorize(authorization)).rejects.toThrow(ApplicationFailure);
    expect(events.map((event) => ({ activity: event.activity, phase: event.phase }))).toEqual([
      { activity: "authorize", phase: "start" },
      { activity: "authorize", phase: "error" },
    ]);
    expect(events[1]!.error).toBeInstanceOf(ApplicationFailure);
    expect(events[1]!.durationMs).toEqual(expect.any(Number));
    expect(events[1]!.context).toBeUndefined();
  });

  it("swallows hook errors so a successful activity still resolves", async () => {
    const activities = createActivities(adapters({ telemetry: { onActivity: () => { throw new Error("hook failure"); } } }));
    await expect(activities.authorize(authorization)).resolves.toBe(true);
  });

  it("swallows hook errors on the failure path without masking the activity error", async () => {
    const activities = createActivities(adapters({
      authorize: async () => { throw new Error("host failure"); },
      telemetry: { onActivity: () => { throw new Error("hook failure"); } },
    }));
    const failure = await activities.authorize(authorization).then(() => undefined, (thrown: unknown) => thrown);
    expect(failure).toBeInstanceOf(ApplicationFailure);
  });
});
