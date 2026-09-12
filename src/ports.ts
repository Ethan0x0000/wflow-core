import { z } from "zod";
import { assignmentSchema, commandSchema, dataSchema, idSchema, identitySchema, jsonSchema, referenceSchema, syncEventSchema, syncRuleSchema, type Data, type Definition, type Json, type WorkflowEvent } from "./schema.js";

export const contextSchema = z.strictObject({
  tenantId: idSchema, instanceId: idSchema, businessKey: idSchema, initiatorId: idSchema,
  definition: referenceSchema,
});
export type InstanceContext = z.infer<typeof contextSchema>;
export const authorizationSchema = z.strictObject({
  ...identitySchema.shape, operation: z.enum(["start", "read", "command", "list", "signal"]),
  instanceId: idSchema.optional(), businessKey: idSchema.optional(), definition: referenceSchema.optional(),
  command: commandSchema.optional(),
}).superRefine((request, ctx) => {
  if (request.operation !== "list" && request.instanceId === undefined) {
    ctx.addIssue({ code: "custom", message: "instanceId is required except for list" });
  }
});
export type AuthorizationRequest = z.infer<typeof authorizationSchema>;
export const assignmentRequestSchema = z.strictObject({
  context: contextSchema, executionId: idSchema, nodeId: idSchema.optional(), assignment: assignmentSchema, data: dataSchema,
});
export type AssignmentRequest = z.infer<typeof assignmentRequestSchema>;
export const actionRequestSchema = z.strictObject({
  context: contextSchema, executionId: idSchema, idempotencyKey: z.string().min(1).max(1024),
  action: idSchema, input: dataSchema,
});
export type ActionContext = Omit<z.infer<typeof actionRequestSchema>, "action" | "input">;
export type ActionHandler = { execute(context: ActionContext, input: Data): Promise<Json> };
// Host-defined `custom` nodes: the workflow sends the node context, config and current data to an
// activity, which resolves the `kind` on Adapters.nodeHandlers. `output` is stored under the node's
// resultKey; `jumpTo` rebases the run on a top-level node id like the router target.
export const nodeRequestSchema = z.strictObject({
  context: contextSchema, executionId: idSchema, nodeId: idSchema, kind: idSchema,
  config: dataSchema, data: dataSchema,
});
export type NodeRequest = z.infer<typeof nodeRequestSchema>;
export type NodeContext = z.infer<typeof contextSchema> & { executionId: string; nodeId: string };
export type NodeOutcome<O extends Json = Json> = { output?: O; jumpTo?: string };
export type NodeHandler = { execute(context: NodeContext, config: Data, data: Data): Promise<NodeOutcome> };
// Adapter-side observability hook. It runs in the activity/worker runtime, never in the workflow
// sandbox, and a throwing hook is swallowed so it can never fail an engine activity.
export type TelemetryEvent = {
  activity: string;
  phase: "start" | "end" | "error";
  context?: InstanceContext;
  error?: unknown;
  durationMs?: number;
};
export interface TelemetryHook {
  onActivity?(event: TelemetryEvent): void;
}
export const nodeOutcomeSchema = z.strictObject({ output: jsonSchema.optional(), jumpTo: idSchema.optional() });
// HTTP listeners and cross-instance SIGNAL emission must run outside the deterministic workflow.
export const integrationRequestSchema = z.strictObject({
  context: contextSchema, executionId: idSchema, nodeId: idSchema.optional(),
  source: z.enum(["node", "process", "trigger"]), event: z.string().min(1).max(128),
  type: z.enum(["HTTP", "SIGNAL"]), config: dataSchema, data: dataSchema,
});
export type IntegrationRequest = z.infer<typeof integrationRequestSchema>;
export type IntegrationHandler = (request: IntegrationRequest) => Promise<Json>;
// Business data synchronisation (ProcSetting.SyncRule): preCover -> fieldMapping/range -> DB/EL/API.
export const syncRequestSchema = z.strictObject({
  context: contextSchema, event: syncEventSchema, rule: syncRuleSchema, data: dataSchema,
});
export type SyncRequest = z.infer<typeof syncRequestSchema>;
export type SyncHandler = (request: SyncRequest) => Promise<void>;

export function defineAction<I, O extends Json>(options: {
  input: z.ZodType<I>; output: z.ZodType<O>; execute(context: ActionContext, input: I): Promise<O>;
}): ActionHandler {
  return { execute: async (context, input) => options.output.parse(await options.execute(context, options.input.parse(input))) };
}

/** Mirrors {@link defineAction} for `custom` node handlers: optional config/output validation runs at the activity boundary. */
export function defineNode<I = Data, O extends Json = Json>(options: {
  input?: z.ZodType<I>; output?: z.ZodType<O>;
  execute(context: NodeContext, config: I, data: Data): Promise<NodeOutcome<O>>;
}): NodeHandler {
  return {
    execute: async (context, config, data) => {
      const parsed = options.input ? options.input.parse(config) : config as I;
      const outcome = await options.execute(context, parsed, data);
      return {
        ...(outcome.output === undefined ? {} : { output: options.output ? options.output.parse(outcome.output) : outcome.output }),
        ...(outcome.jumpTo === undefined ? {} : { jumpTo: outcome.jumpTo }),
      };
    },
  };
}

export interface DefinitionStore {
  // The host must retain immutable published versions for the lifetime of instances.
  get(tenantId: string, reference: { id: string; version: number }): Promise<unknown>;
}
export interface Adapters {
  definitions: DefinitionStore;
  authorize(request: AuthorizationRequest): Promise<boolean>;
  resolveAssignees(request: AssignmentRequest): Promise<unknown>;
  mapAssignees?(request: AssignmentRequest, userIds: string[]): Promise<unknown>;
  actions: ReadonlyMap<string, ActionHandler>;
  // Host-defined `custom` node kinds. Unknown kinds (or a missing map) fail with UNKNOWN_NODE_KIND.
  nodeHandlers?: ReadonlyMap<string, NodeHandler>;
  // Executes HTTP event listeners and cross-instance signal triggers. Required when a definition uses them.
  integrate?: IntegrationHandler;
  // Applies ProcSetting.formSync rules. Required when a definition enables business data synchronisation.
  syncBusinessData?: SyncHandler;
  // Commit deduplication and projection writes atomically, keyed by eventId.
  publishEvent(event: WorkflowEvent): Promise<void>;
  // Optional lightweight observability: called around every engine activity. Deeper metrics and
  // tracing belong to Temporal Runtime metrics and interceptors, not this hook.
  telemetry?: TelemetryHook;
}
export interface EngineActivities {
  authorize(request: AuthorizationRequest): Promise<boolean>;
  resolveAssignees(request: AssignmentRequest): Promise<{ users: string[]; reason?: string }>;
  executeAction(request: z.infer<typeof actionRequestSchema>): Promise<Json>;
  executeNode(request: NodeRequest): Promise<NodeOutcome>;
  integrate(request: IntegrationRequest): Promise<Json>;
  syncBusinessData(request: SyncRequest): Promise<void>;
  loadDefinition(request: { tenantId: string; reference: { id: string; version: number } }): Promise<Definition>;
  publishEvent(event: WorkflowEvent): Promise<void>;
}
