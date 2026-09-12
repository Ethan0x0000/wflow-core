import { ApplicationFailure } from "@temporalio/common";
import { z } from "zod";
import { assertJsonSize, parseDefinition } from "./definition.js";
import { AdapterError, WorkflowValidationError } from "./errors.js";
import { actionRequestSchema, assignmentRequestSchema, authorizationSchema, integrationRequestSchema, nodeOutcomeSchema, nodeRequestSchema, syncRequestSchema, type Adapters, type EngineActivities, type InstanceContext, type TelemetryEvent } from "./ports.js";
import { eventSchema, idSchema, jsonSchema, referenceSchema } from "./schema.js";

function boundary<T>(run: () => Promise<T>): Promise<T> {
  return run().catch((error: unknown) => {
    if (error instanceof z.ZodError || error instanceof WorkflowValidationError) {
      throw ApplicationFailure.nonRetryable("Invalid workflow adapter payload", "INVALID_ADAPTER_PAYLOAD");
    }
    if (error instanceof ApplicationFailure) throw error;
    // Adapter messages can contain business data; preserve retry behavior without leaking them.
    if (error instanceof AdapterError) {
      throw error.retryable === false
        ? ApplicationFailure.nonRetryable("Workflow adapter failed", error.code)
        : ApplicationFailure.retryable("Workflow adapter failed", error.code);
    }
    throw ApplicationFailure.retryable("Workflow adapter failed", "ADAPTER_FAILED");
  });
}

function requestContext(raw: unknown): InstanceContext | undefined {
  if (typeof raw !== "object" || raw === null || !("context" in raw)) return undefined;
  const context = (raw as { context?: unknown }).context;
  return context !== null && typeof context === "object" ? context as InstanceContext : undefined;
}

/** Emits the telemetry hook around one engine activity; hook errors are swallowed on purpose. */
function observe<T>(telemetry: Adapters["telemetry"], activity: string, raw: unknown, run: () => Promise<T>): Promise<T> {
  const context = requestContext(raw);
  const started = Date.now();
  const emit = (event: TelemetryEvent): void => { try { telemetry?.onActivity?.(event); } catch { /* observability must never break the engine */ } };
  emit({ activity, phase: "start", ...(context ? { context } : {}) });
  return run().then(
    (value) => { emit({ activity, phase: "end", ...(context ? { context } : {}), durationMs: Date.now() - started }); return value; },
    (error: unknown) => { emit({ activity, phase: "error", ...(context ? { context } : {}), error, durationMs: Date.now() - started }); throw error; },
  );
}

export function createActivities(adapters: Adapters): EngineActivities {
  const observeActivity = <T>(activity: string, raw: unknown, run: () => Promise<T>): Promise<T> => observe(adapters.telemetry, activity, raw, run);
  return {
    authorize: (raw) => observeActivity("authorize", raw, () => boundary(async () => {
      assertJsonSize(raw);
      return z.boolean().parse(await adapters.authorize(authorizationSchema.parse(raw)));
    })),
    resolveAssignees: (raw) => observeActivity("resolveAssignees", raw, () => boundary(async () => {
      assertJsonSize(raw);
      const request = assignmentRequestSchema.parse(raw);
      const assignment = request.assignment;
      // Adapters may return a bare id array or { users|userIds, reason } (Java NodeReason).
      const extract = (value: unknown): { users: string[]; reason?: string } => {
        if (Array.isArray(value)) return { users: z.array(idSchema).max(200).parse(value) };
        const record = z.record(z.string(), jsonSchema).parse(value ?? {});
        const users = z.array(idSchema).max(200).parse(record.users ?? record.userIds ?? []);
        return { users, ...(typeof record.reason === "string" ? { reason: record.reason } : {}) };
      };
      const resolved = assignment.type === "users" ? { users: assignment.userIds }
        : assignment.type === "initiator" ? { users: [request.context.initiatorId] } : extract(await adapters.resolveAssignees(request));
      assertJsonSize(resolved.users);
      const mapped = extract(adapters.mapAssignees ? await adapters.mapAssignees(request, resolved.users) : resolved.users);
      return { users: [...new Set(mapped.users)], ...(resolved.reason ? { reason: resolved.reason } : {}) };
    })),
    executeAction: (raw) => observeActivity("executeAction", raw, () => boundary(async () => {
      assertJsonSize(raw);
      const { action, input, ...context } = actionRequestSchema.parse(raw);
      const handler = adapters.actions.get(action);
      if (!handler) throw ApplicationFailure.nonRetryable("Action is not registered", "UNKNOWN_ACTION");
      const output = await handler.execute(context, input);
      assertJsonSize(output);
      return jsonSchema.parse(output);
    })),
    executeNode: (raw) => observeActivity("executeNode", raw, () => boundary(async () => {
      assertJsonSize(raw);
      const { context, executionId, nodeId, kind, config, data } = nodeRequestSchema.parse(raw);
      const handler = adapters.nodeHandlers?.get(kind);
      if (!handler) throw ApplicationFailure.nonRetryable("Node kind is not registered", "UNKNOWN_NODE_KIND");
      const outcome = await handler.execute({ ...context, executionId, nodeId }, config, data);
      assertJsonSize(outcome);
      return nodeOutcomeSchema.parse(outcome);
    })),
    integrate: (raw) => observeActivity("integrate", raw, () => boundary(async () => {
      assertJsonSize(raw);
      const request = integrationRequestSchema.parse(raw);
      if (!adapters.integrate) throw ApplicationFailure.nonRetryable("Host does not implement workflow integrations", "INTEGRATION_NOT_SUPPORTED");
      const output = await adapters.integrate(request);
      assertJsonSize(output ?? null);
      return jsonSchema.parse(output ?? null);
    })),
    syncBusinessData: (raw) => observeActivity("syncBusinessData", raw, () => boundary(async () => {
      assertJsonSize(raw);
      const request = syncRequestSchema.parse(raw);
      if (adapters.syncBusinessData) await adapters.syncBusinessData(request);
    })),
    loadDefinition: (raw) => observeActivity("loadDefinition", raw, () => boundary(async () => {
      const { tenantId, reference } = z.strictObject({ tenantId: idSchema, reference: referenceSchema }).parse(raw);
      const definition = parseDefinition(await adapters.definitions.get(tenantId, reference));
      // version 0 is an unbound subprocess reference: the host resolves the active version itself.
      if (definition.id !== reference.id || (reference.version && definition.version !== reference.version)) throw new WorkflowValidationError("DEFINITION_VERSION_MISMATCH");
      return definition;
    })),
    publishEvent: (raw) => observeActivity("publishEvent", raw, () => boundary(async () => {
      assertJsonSize(raw);
      await adapters.publishEvent(eventSchema.parse(raw));
    })),
  };
}
