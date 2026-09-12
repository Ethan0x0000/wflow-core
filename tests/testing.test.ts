import { describe, expect, it } from "vitest";
import { parseDefinition } from "../src/definition.js";
import { createMemoryAdapters } from "../src/testing.js";
import type { AssignmentRequest } from "../src/ports.js";
import type { WorkflowEvent } from "../src/schema.js";

const context = { tenantId: "tenant", instanceId: "instance", businessKey: "instance", initiatorId: "initiator", definition: { id: "definition", version: 1 } };
const assignment = (request: Partial<AssignmentRequest> & Pick<AssignmentRequest, "assignment">): AssignmentRequest => ({ context, executionId: "n1", data: {}, ...request });
const event = (eventId: string): WorkflowEvent => ({
  eventId, eventType: "workflow.started", version: 1, occurredAt: new Date().toISOString(),
  tenantId: "tenant", instanceId: "instance", businessKey: "instance", definition: { id: "definition", version: 1 }, sequence: 1, details: {},
});

describe("testing kit memory adapters", () => {
  it("deduplicates projected events by id and keeps insertion order", async () => {
    const memory = createMemoryAdapters();
    await memory.adapters.publishEvent(event("a"));
    await memory.adapters.publishEvent(event("a"));
    await memory.adapters.publishEvent(event("b"));
    expect(memory.events.map((entry) => entry.eventId)).toEqual(["a", "b"]);
  });

  it("allows by default and resolves resolver names through the users map", async () => {
    const memory = createMemoryAdapters({ users: { manager: ["u-1"] } });
    await expect(memory.adapters.authorize({ tenantId: "tenant", actorId: "u-1", operation: "read", instanceId: "instance" })).resolves.toBe(true);
    await expect(memory.adapters.resolveAssignees(assignment({ assignment: { type: "resolver", name: "manager" } }))).resolves.toEqual(["u-1"]);
    await expect(memory.adapters.resolveAssignees(assignment({ assignment: { type: "resolver", name: "missing" }, nodeId: "manager" }))).resolves.toEqual(["u-1"]);
    await expect(memory.adapters.resolveAssignees(assignment({ assignment: { type: "resolver", name: "missing" } }))).resolves.toEqual([]);
  });

  it("honours explicit adapter overrides", async () => {
    const seen: string[] = [];
    const memory = createMemoryAdapters({
      authorize: async () => false,
      publishEvent: async (projected) => { seen.push(projected.eventId); },
    });
    await expect(memory.adapters.authorize({ tenantId: "tenant", actorId: "u-1", operation: "read", instanceId: "instance" })).resolves.toBe(false);
    await memory.adapters.publishEvent(event("a"));
    expect(seen).toEqual(["a"]);
    expect(memory.events.map((entry) => entry.eventId)).toEqual(["a"]);
  });

  it("serves definitions by id and version", async () => {
    const definition = parseDefinition({ schemaVersion: 1, id: "definition", version: 1, name: "Definition", nodes: [{ id: "action", type: "action", action: "noop", input: {} }] });
    const memory = createMemoryAdapters({ definitions: [definition] });
    await expect(memory.adapters.definitions.get("tenant", { id: "definition", version: 1 })).resolves.toBe(definition);
    await expect(memory.adapters.definitions.get("tenant", { id: "definition", version: 2 })).resolves.toBeUndefined();
  });
});
