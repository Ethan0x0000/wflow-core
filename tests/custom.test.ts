import { ApplicationFailure } from "@temporalio/common";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import { createActivities } from "../src/activities.js";
import { AdapterError } from "../src/errors.js";
import { defineNode, type Adapters, type NodeContext } from "../src/ports.js";

const context: NodeContext = {
  tenantId: "tenant", instanceId: "instance", businessKey: "instance", initiatorId: "initiator",
  definition: { id: "definition", version: 1 }, executionId: "n1", nodeId: "custom",
};
const request = {
  context: { tenantId: "tenant", instanceId: "instance", businessKey: "instance", initiatorId: "initiator", definition: { id: "definition", version: 1 } },
  executionId: "n1", nodeId: "custom", kind: "host.verify", config: { amount: 7 }, data: { input: true },
};
const adapters = (overrides: Partial<Adapters> = {}): Adapters => ({
  definitions: { async get() { return undefined; } },
  authorize: async () => true,
  resolveAssignees: async () => ({ users: [] }),
  actions: new Map(),
  publishEvent: async () => {},
  ...overrides,
});
const failureOf = async (run: () => Promise<unknown>): Promise<ApplicationFailure> => {
  const error = await run().then(() => undefined, (thrown: unknown) => thrown);
  expect(error).toBeInstanceOf(ApplicationFailure);
  return error as ApplicationFailure;
};

describe("defineNode", () => {
  it("validates config input and outcome output around the handler", async () => {
    const handler = defineNode({
      input: z.object({ amount: z.number() }),
      output: z.object({ doubled: z.number() }),
      execute: async (received, config, data) => {
        expect(received).toEqual(context);
        expect(data).toEqual({ input: true });
        return { output: { doubled: config.amount * 2 } };
      },
    });
    await expect(handler.execute(context, { amount: 7 }, { input: true })).resolves.toEqual({ output: { doubled: 14 } });
    await expect(handler.execute(context, { amount: "no" }, {})).rejects.toThrow();
    const rejecting = defineNode({
      output: z.object({ doubled: z.number() }),
      execute: async () => ({ output: { doubled: "no" } as unknown as { doubled: number } }),
    });
    await expect(rejecting.execute(context, {}, {})).rejects.toThrow();
  });

  it("passes config through and preserves jumpTo when no schemas are given", async () => {
    const handler = defineNode({ execute: async (_context, config) => ({ output: config, jumpTo: "next" }) });
    await expect(handler.execute(context, { raw: true }, {})).resolves.toEqual({ output: { raw: true }, jumpTo: "next" });
  });
});

describe("executeNode activity boundary", () => {
  it("maps an unregistered kind to a non-retryable UNKNOWN_NODE_KIND", async () => {
    const activities = createActivities(adapters());
    const failure = await failureOf(() => activities.executeNode(request));
    expect(failure.type).toBe("UNKNOWN_NODE_KIND");
    expect(failure.nonRetryable).toBe(true);
    expect(failure.message).not.toContain("host.verify");
  });

  it("runs the registered handler with the flattened NodeContext", async () => {
    const seen: NodeContext[] = [];
    const activities = createActivities(adapters({
      nodeHandlers: new Map([["host.verify", defineNode({
        output: z.object({ ok: z.boolean() }),
        execute: async (nodeContext, config, data) => {
          seen.push(nodeContext);
          return { output: { ok: config.amount === 7 && data.input === true } };
        },
      })]]),
    }));
    await expect(activities.executeNode(request)).resolves.toEqual({ output: { ok: true } });
    expect(seen).toEqual([context]);
  });

  it("keeps AdapterError code and non-retryability without leaking the host message", async () => {
    const activities = createActivities(adapters({
      nodeHandlers: new Map([["host.verify", { execute: async () => { throw new AdapterError("CUSTOM_FAILED", { retryable: false, cause: new Error("secret host detail") }); } }]]),
    }));
    const failure = await failureOf(() => activities.executeNode(request));
    expect(failure.type).toBe("CUSTOM_FAILED");
    expect(failure.nonRetryable).toBe(true);
    expect(failure.message).toBe("Workflow adapter failed");
  });

  it("rejects an outcome with unknown fields as an invalid adapter payload", async () => {
    const activities = createActivities(adapters({
      nodeHandlers: new Map([["host.verify", { execute: async () => ({ output: 1, extra: true } as never) }]]),
    }));
    const failure = await failureOf(() => activities.executeNode(request));
    expect(failure.type).toBe("INVALID_ADAPTER_PAYLOAD");
    expect(failure.nonRetryable).toBe(true);
  });
});
