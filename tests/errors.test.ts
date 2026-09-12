import { ApplicationFailure } from "@temporalio/common";
import { WorkflowExecutionAlreadyStartedError, WorkflowNotFoundError } from "@temporalio/client";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { z } from "zod";
import { createActivities } from "../src/activities.js";
import { isWorkflowAlreadyStarted, isWorkflowNotFound } from "../src/client.js";
import { AdapterError, ERROR_CODES } from "../src/errors.js";
import type { Adapters } from "../src/ports.js";
import type { WorkflowEvent } from "../src/schema.js";

const event = (): WorkflowEvent => ({
  eventId: "run-1:1", eventType: "workflow.started", version: 1, occurredAt: new Date().toISOString(),
  tenantId: "tenant", instanceId: "instance", businessKey: "instance", definition: { id: "definition", version: 1 }, sequence: 1, details: {},
});

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

describe("error catalog", () => {
  it("is a frozen map whose keys equal their codes", () => {
    expect(Object.isFrozen(ERROR_CODES)).toBe(true);
    const entries = Object.entries(ERROR_CODES);
    expect(new Set(entries.map(([, value]) => value)).size).toBe(entries.length);
    for (const [key, value] of entries) expect(value).toBe(key);
  });

  it("covers every literal code thrown by the SDK sources", () => {
    const src = resolve(process.cwd(), "src");
    const patterns = [
      /WorkflowValidationError\(\s*["'`]([A-Z][A-Z0-9_]*)["'`]/g,
      /\bfail\(\s*["'`]([A-Z][A-Z0-9_]*)["'`]/g,
      /ApplicationFailure\.(?:nonRetryable|retryable)\([^,\n]*,\s*["'`]([A-Z][A-Z0-9_]*)["'`]/g,
    ];
    const catalog = new Set<string>(Object.values(ERROR_CODES));
    const found = new Set<string>();
    for (const file of readdirSync(src, { recursive: true })) {
      if (!String(file).endsWith(".ts")) continue;
      const text = readFileSync(resolve(src, String(file)), "utf8");
      for (const pattern of patterns) for (const match of text.matchAll(pattern)) found.add(match[1]!);
    }
    expect(found.size).toBeGreaterThan(50);
    expect([...found].filter((code) => !catalog.has(code))).toEqual([]);
  });
});

describe("AdapterError boundary mapping", () => {
  it("keeps the code and non-retryability without leaking host messages", async () => {
    const activities = createActivities(adapters({ publishEvent: async () => { throw new AdapterError("PROJECTION_REJECTED", { retryable: false, cause: new Error("secret host detail") }); } }));
    const failure = await failureOf(() => activities.publishEvent(event()));
    expect(failure.type).toBe("PROJECTION_REJECTED");
    expect(failure.nonRetryable).toBe(true);
    expect(failure.message).toBe("Workflow adapter failed");
  });

  it("defaults AdapterError to retryable", async () => {
    const activities = createActivities(adapters({ publishEvent: async () => { throw new AdapterError("PROJECTION_BUSY"); } }));
    const failure = await failureOf(() => activities.publishEvent(event()));
    expect(failure.type).toBe("PROJECTION_BUSY");
    expect(failure.nonRetryable).toBe(false);
  });

  it("keeps unclassified and invalid-payload behavior unchanged", async () => {
    const plain = createActivities(adapters({ publishEvent: async () => { throw new Error("secret host detail"); } }));
    const failed = await failureOf(() => plain.publishEvent(event()));
    expect(failed.type).toBe("ADAPTER_FAILED");
    expect(failed.nonRetryable).toBe(false);
    expect(failed.message).toBe("Workflow adapter failed");

    const invalid = createActivities(adapters({ publishEvent: async () => { throw new z.ZodError([]); } }));
    const rejected = await failureOf(() => invalid.publishEvent(event()));
    expect(rejected.type).toBe("INVALID_ADAPTER_PAYLOAD");
    expect(rejected.nonRetryable).toBe(true);
  });
});

describe("isWorkflowNotFound", () => {
  it("matches Temporal WorkflowNotFoundError and its cross-copy name", () => {
    expect(isWorkflowNotFound(new WorkflowNotFoundError("missing", "wf:1:t:i", undefined))).toBe(true);
    expect(isWorkflowNotFound(Object.assign(new Error("missing"), { name: "WorkflowNotFoundError" }))).toBe(true);
    expect(isWorkflowNotFound(new Error("missing"))).toBe(false);
    expect(isWorkflowNotFound(undefined)).toBe(false);
  });
});

describe("isWorkflowAlreadyStarted", () => {
  it("matches Temporal WorkflowExecutionAlreadyStartedError and its cross-copy name", () => {
    expect(isWorkflowAlreadyStarted(new WorkflowExecutionAlreadyStartedError("started", "wf:1:t:i", "genericWorkflowV1"))).toBe(true);
    expect(isWorkflowAlreadyStarted(Object.assign(new Error("started"), { name: "WorkflowExecutionAlreadyStartedError" }))).toBe(true);
    expect(isWorkflowAlreadyStarted(new Error("started"))).toBe(false);
    expect(isWorkflowAlreadyStarted(undefined)).toBe(false);
  });
});
