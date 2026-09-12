import { z } from "zod";

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
const forbiddenKeys = new Set(["__proto__", "prototype", "constructor"]);
export const keySchema = z.string().min(1).max(128).refine((key) => !forbiddenKeys.has(key));
export const idSchema = z.string().min(1).max(128);
export const jsonSchema: z.ZodType<Json> = z.lazy(() => z.union([
  z.null(), z.boolean(), z.number().finite(), z.string(), z.array(jsonSchema),
  z.record(keySchema, jsonSchema),
]));
export const dataSchema = z.record(keySchema, jsonSchema);
export type Data = z.infer<typeof dataSchema>;
export const valueSchema = z.union([
  z.strictObject({ value: jsonSchema }),
  z.strictObject({ path: z.array(keySchema).min(1).max(16) }),
]);
export type Value = z.infer<typeof valueSchema>;
export type Condition =
  | { op: "and" | "or"; conditions: Condition[] }
  | { op: "not"; condition: Condition }
  | { op: "exists" | "empty"; value: Value }
  | { op: "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "in" | "contains"; left: Value; right: Value }
  | { op: "between"; value: Value; min: Value; max: Value }
  | { op: "before" | "after"; left: Value; right: Value }
  | { op: "timeBetween"; value: Value; start: Value; end: Value }
  | { op: "initiator"; dimension: "user" | "dept" | "role"; compare: "in" | "has"; values: Json[] }
  | { op: "eval"; lang: "el" | "js"; script: string };
export const conditionSchema: z.ZodType<Condition> = z.lazy(() => z.union([
  z.strictObject({ op: z.enum(["and", "or"]), conditions: z.array(conditionSchema).min(1).max(32) }),
  z.strictObject({ op: z.literal("not"), condition: conditionSchema }),
  z.strictObject({ op: z.enum(["exists", "empty"]), value: valueSchema }),
  z.strictObject({ op: z.enum(["eq", "ne", "gt", "gte", "lt", "lte", "in", "contains"]), left: valueSchema, right: valueSchema }),
  z.strictObject({ op: z.literal("between"), value: valueSchema, min: valueSchema, max: valueSchema }),
  z.strictObject({ op: z.enum(["before", "after"]), left: valueSchema, right: valueSchema }),
  z.strictObject({ op: z.literal("timeBetween"), value: valueSchema, start: valueSchema, end: valueSchema }),
  z.strictObject({ op: z.literal("initiator"), dimension: z.enum(["user", "dept", "role"]), compare: z.enum(["in", "has"]), values: z.array(jsonSchema) }),
  z.strictObject({ op: z.literal("eval"), lang: z.enum(["el", "js"]), script: z.string().max(10_000) }),
]));
export const assignmentSchema = z.discriminatedUnion("type", [
  z.strictObject({ type: z.literal("users"), userIds: z.array(idSchema).min(1).max(200) }),
  z.strictObject({ type: z.literal("initiator") }),
  z.strictObject({ type: z.literal("resolver"), name: idSchema, params: dataSchema.optional() }),
]);
export type Assignment = z.infer<typeof assignmentSchema>;
export const fieldSchema = z.strictObject({
  key: keySchema,
  type: z.enum(["string", "number", "boolean", "array", "object"]),
  required: z.boolean().optional(),
  minLength: z.number().int().nonnegative().max(512_000).optional(),
  maxLength: z.number().int().nonnegative().max(512_000).optional(),
  minimum: z.number().finite().optional(),
  maximum: z.number().finite().optional(),
  pattern: z.string().min(1).max(4096).optional(),
}).superRefine((field, ctx) => {
  if ((field.minimum !== undefined || field.maximum !== undefined) && field.type !== 'number') ctx.addIssue({ code: 'custom', message: 'Numeric bounds require a number field' });
  if (field.pattern !== undefined && field.type !== 'string') ctx.addIssue({ code: 'custom', message: 'Pattern requires a string field' });
  if ((field.minLength !== undefined || field.maxLength !== undefined) && field.type !== 'string' && field.type !== 'array') ctx.addIssue({ code: 'custom', message: 'Length bounds require a string or array field' });
  if (field.minLength !== undefined && field.maxLength !== undefined && field.minLength > field.maxLength) ctx.addIssue({ code: 'custom', message: 'Invalid length range' });
  if (field.minimum !== undefined && field.maximum !== undefined && field.minimum > field.maximum) ctx.addIssue({ code: 'custom', message: 'Invalid numeric range' });
});
export type Field = z.infer<typeof fieldSchema>;
const milliseconds = z.number().int().positive().max(31_536_000_000);
export const retrySchema = z.strictObject({
  startToCloseMs: milliseconds,
  maximumAttempts: z.number().int().min(1).max(100),
  initialIntervalMs: milliseconds.optional(),
});
export const listenerSchema = z.strictObject({
  type: z.enum(["EL", "JS", "HTTP"]),
  action: z.string().optional(),
  config: dataSchema.optional(),
});
export type Listener = z.infer<typeof listenerSchema>;
const listenersSchema = z.record(keySchema, z.array(listenerSchema).max(32));
const base = { id: idSchema, name: z.string().min(1).max(256).optional() };
export const humanSchema = z.strictObject({
  ...base,
  type: z.enum(["approval", "task"]),
  assignees: assignmentSchema,
  mode: z.enum(["all", "any", "sequential", "percentage", "candidate"]),
  percentage: z.number().positive().max(100).optional(),
  selfApproval: z.enum(["allow", "exclude"]).optional(),
  emptyAssignees: z.enum(["fail", "skip"]).optional(),
  fields: z.array(fieldSchema).max(100).optional(),
  allowTransfer: z.boolean().optional(),
  allowAddAssignees: z.boolean().optional(),
  allowReturn: z.boolean().optional(),
  needSign: z.boolean().optional(),
  // NEXT (default in the Java reference) completes the task with a reject result and continues the flow.
  rejectRule: z.strictObject({ type: z.enum(["NEXT", "END", "SKIP"]), target: idSchema.optional() }).optional(),
  events: listenersSchema.optional(),
  timeout: z.strictObject({ afterMs: milliseconds, outcome: z.enum(["approve", "reject", "notify"]), repeat: z.number().int().min(1).max(100).optional() }).optional(),
}).superRefine((node, ctx) => {
  if (node.mode === "percentage" && node.percentage === undefined) ctx.addIssue({ code: "custom", message: "Percentage mode requires percentage" });
  if (node.mode !== "percentage" && node.percentage !== undefined) ctx.addIssue({ code: "custom", message: "percentage requires percentage mode" });
  if (node.rejectRule?.type === "SKIP" && !node.rejectRule.target) ctx.addIssue({ code: "custom", message: "SKIP reject rule requires target" });
});
export type HumanNode = z.infer<typeof humanSchema>;
const actionSchema = z.strictObject({
  ...base, type: z.literal("action"), action: idSchema,
  input: z.record(keySchema, valueSchema), resultKey: keySchema.optional(), retry: retrySchema.optional(),
});
const ccSchema = z.strictObject({ ...base, type: z.literal("cc"), recipients: assignmentSchema, events: listenersSchema.optional() });
const delaySchema = z.strictObject({ ...base, type: z.literal("delay"), durationMs: milliseconds });
const delayUntilSchema = z.strictObject({
  ...base, type: z.literal("delayUntil"),
  at: z.string().datetime({ offset: true }).optional(),
  timeOfDay: z.string().regex(/^\d{2}:\d{2}(:\d{2})?$/).optional(),
  tzOffsetMinutes: z.number().int().min(-720).max(840).optional(),
}).superRefine((node, ctx) => {
  if ((node.at === undefined) === (node.timeOfDay === undefined)) ctx.addIssue({ code: "custom", message: "delayUntil requires exactly one of at or timeOfDay" });
});
const decisionSchema = z.strictObject({ ...base, type: z.literal('decision'), outcome: z.enum(['approve', 'reject']) });
const terminateSchema = z.strictObject({ ...base, type: z.literal('terminate'), outcome: z.enum(['approve', 'reject']) });
const signalSchema = z.strictObject({
  name: z.string().min(1).max(256),
  scope: z.enum(["GLOBAL", "PROCESS", "LOCAL", "INSTANCE"]),
  code: z.string().max(128).optional(),
  instId: z.string().max(128).optional(),
});
const triggerSchema = z.strictObject({
  ...base, type: z.literal("trigger"),
  action: z.enum(["EL", "JS", "HTTP", "SIGNAL", "NONE"]),
  script: z.string().max(10_000).optional(),
  http: dataSchema.optional(),
  signal: signalSchema.optional(),
  resultKey: keySchema.optional(),
}).superRefine((node, ctx) => {
  if ((node.action === "EL" || node.action === "JS") && !node.script) ctx.addIssue({ code: "custom", message: "Script trigger requires script" });
  if (node.action === "HTTP" && !node.http) ctx.addIssue({ code: "custom", message: "HTTP trigger requires http" });
  if (node.action === "SIGNAL" && !node.signal) ctx.addIssue({ code: "custom", message: "SIGNAL trigger requires signal" });
});
const waitSchema = z.strictObject({
  ...base, type: z.literal("wait"), event: idSchema,
  timeoutMs: milliseconds.optional(), onTimeout: z.enum(["fail", "continue"]).optional(), resultKey: keySchema.optional(),
});
// Host-defined node: `kind` selects a handler registered on Adapters.nodeHandlers. The handler runs
// in an activity, never in the workflow sandbox; `config`/`data` are bounded JSON and the outcome is
// recorded in history, so replay stays deterministic.
const customSchema = z.strictObject({
  ...base, type: z.literal("custom"), kind: idSchema,
  config: dataSchema.optional(), resultKey: keySchema.optional(), events: listenersSchema.optional(),
});
export type CustomNode = z.infer<typeof customSchema>;
// version 0 marks an unbound subprocess reference: the host resolves the active version at start.
export const referenceSchema = z.strictObject({ id: idSchema, version: z.number().int().nonnegative() });
export type DefinitionReference = z.infer<typeof referenceSchema>;
const childMappingSchema = z.strictObject({
  source: z.string().max(500), target: keySchema,
  isVar: z.boolean().default(false), sync: z.boolean().default(false), fixed: z.boolean().default(false),
});
const childSchema = z.strictObject({
  ...base, type: z.literal("child"), definition: referenceSchema,
  input: z.record(keySchema, valueSchema), resultKey: keySchema.optional(),
  async: z.boolean().optional(), inheritVariables: z.boolean().optional(), inheritBusinessKey: z.boolean().optional(),
  formAutoMapping: z.boolean().optional(), statusSync: z.boolean().optional(),
  initiator: z.strictObject({ type: z.enum(["parent", "fixed"]), userId: idSchema.optional() }).optional(),
  mappings: z.array(childMappingSchema).max(100).optional(),
  events: listenersSchema.optional(),
});
const routerSchema = z.strictObject({
  ...base,
  type: z.literal("router"),
  when: conditionSchema.optional(),
  targetNodeId: idSchema,
});
export type RouterNode = z.infer<typeof routerSchema>;
export type Node = HumanNode | z.infer<typeof decisionSchema> | z.infer<typeof terminateSchema> | z.infer<typeof actionSchema>
  | z.infer<typeof ccSchema> | z.infer<typeof delaySchema> | z.infer<typeof delayUntilSchema> | z.infer<typeof waitSchema>
  | CustomNode | z.infer<typeof childSchema> | z.infer<typeof triggerSchema> | RouterNode
  | { id: string; name?: string; type: "exclusive" | "inclusive"; branches: { when: Condition; nodes: Node[] }[]; otherwise: Node[] }
  | { id: string; name?: string; type: "parallel"; branches: Node[][] }
  | { id: string; name?: string; type: "loop"; while: Condition; maxIterations: number; nodes: Node[] }
  | { id: string; name?: string; type: "forEach"; items: Value; itemKey: string; indexKey?: string; parallel?: boolean; concurrency?: number; completionCondition?: Condition; maxIterations: number; nodes: Node[] }
  | { id: string; name?: string; type: "eventGateway"; branches: { event?: string; timeoutMs?: number; resultKey?: string; nodes: Node[] }[] };
export const nodeSchema: z.ZodType<Node> = z.lazy(() => z.union([
  humanSchema, decisionSchema, terminateSchema, actionSchema, ccSchema, delaySchema, delayUntilSchema, waitSchema,
  customSchema, childSchema, triggerSchema, routerSchema,
  z.strictObject({ ...base, type: z.enum(["exclusive", "inclusive"]),
    branches: z.array(z.strictObject({ when: conditionSchema, nodes: z.array(nodeSchema) })).min(1).max(32),
    otherwise: z.array(nodeSchema),
  }),
  z.strictObject({ ...base, type: z.literal("parallel"), branches: z.array(z.array(nodeSchema)).min(1).max(32) }),
  z.strictObject({ ...base, type: z.literal("loop"), while: conditionSchema,
    maxIterations: z.number().int().min(1).max(1000), nodes: z.array(nodeSchema).min(1) }),
  // OA "明细逐条审批" collection loop: `items` resolves to a JSON array and each iteration binds
  // itemKey/indexKey in a per-iteration scope. Sequential is the default; parallel runs bounded
  // chunks of iterations against local copies and only merges non-iteration keys back.
  z.strictObject({ ...base, type: z.literal("forEach"),
    items: valueSchema, itemKey: keySchema, indexKey: keySchema.optional(),
    parallel: z.boolean().optional(), concurrency: z.number().int().min(1).max(32).optional(),
    completionCondition: conditionSchema.optional(),
    maxIterations: z.number().int().min(1).max(1000), nodes: z.array(nodeSchema).min(1),
  }).superRefine((node, ctx) => {
    if (node.concurrency !== undefined && node.parallel !== true) ctx.addIssue({ code: "custom", message: "concurrency requires parallel" });
    if (node.completionCondition !== undefined && node.parallel === true) ctx.addIssue({ code: "custom", message: "completionCondition supports sequential forEach only" });
    if (node.indexKey !== undefined && node.indexKey === node.itemKey) ctx.addIssue({ code: "custom", message: "indexKey must differ from itemKey" });
  }),
  // BPMN event-based gateway / race: the first registered event wait to receive wins, otherwise
  // the earliest timer branch wins; exactly one of event/timeoutMs per branch.
  z.strictObject({ ...base, type: z.literal("eventGateway"),
    branches: z.array(z.strictObject({
      event: idSchema.optional(), timeoutMs: milliseconds.optional(), resultKey: keySchema.optional(),
      nodes: z.array(nodeSchema).min(1),
    }).superRefine((branch, ctx) => {
      if ((branch.event === undefined) === (branch.timeoutMs === undefined)) ctx.addIssue({ code: "custom", message: "eventGateway branch requires exactly one of event or timeoutMs" });
      if (branch.timeoutMs !== undefined && branch.resultKey !== undefined) ctx.addIssue({ code: "custom", message: "resultKey supports event branches only" });
    })).min(1).max(32),
  }),
]));
export const syncEventSchema = z.enum(["create", "update", "delete", "pass", "reject", "revoke"]);
export type SyncEvent = z.infer<typeof syncEventSchema>;
// ProcSetting.SyncRule: host-side business data synchronisation invoked by the engine.
export const syncRuleSchema = z.strictObject({
  enable: z.boolean(),
  events: z.array(syncEventSchema).max(6),
  type: z.enum(["DB", "EL", "API"]),
  apiUrl: z.string().max(2048).optional(),
  tbName: z.string().max(128).optional(),
  range: z.boolean().optional(),
  preCover: z.boolean().optional(),
  preJs: z.string().max(10_000).optional(),
  el: z.string().max(10_000).optional(),
  fieldMapping: z.array(z.strictObject({ source: keySchema, type: z.string().max(64).optional(), target: keySchema.optional() })).max(200).optional(),
});
export type SyncRule = z.infer<typeof syncRuleSchema>;
const tuningSchema = z.strictObject({
  startToCloseSeconds: z.number().int().positive().max(31_536_000).optional(),
  maximumAttempts: z.number().int().min(1).max(100).optional(),
  initialIntervalMs: milliseconds.optional(),
});
const eventDeliverySchema = tuningSchema.extend({ maximumIntervalMs: milliseconds.optional() });
export const settingsSchema = z.strictObject({
  deduplication: z.strictObject({ type: z.enum(["NONE", "ONCE"]), skip: z.boolean() }).optional(),
  returnSkip: z.boolean().optional(),
  reloadUser: z.boolean().optional(),
  formSync: syncRuleSchema.optional(),
  // Host default timezone offset for delayUntil timeOfDay nodes; nodes may still override per node.
  tzOffsetMinutes: z.number().int().min(-720).max(840).optional(),
  // Engine activity retry policy. Omitted fields keep the 30s/5 attempts/1s initial defaults.
  activity: tuningSchema.optional(),
  // Projection delivery policy. An absent maximumAttempts means retry forever, as before.
  eventDelivery: eventDeliverySchema.optional(),
  // History management: call continueAsNew once the current run's history reaches this many
  // events, but only at a safe top-level node boundary. Absent keeps single-run behavior.
  continueAsNewAfterEvents: z.number().int().min(100).max(50_000).optional(),
});
export type ActivitySettings = z.infer<typeof tuningSchema>;
export type EventDeliverySettings = z.infer<typeof eventDeliverySchema>;
export type Settings = z.infer<typeof settingsSchema>;
export const definitionSchema = z.strictObject({
  schemaVersion: z.literal(1), id: idSchema, version: z.number().int().positive(),
  name: z.string().min(1).max(256), inputFields: z.array(fieldSchema).optional(),
  nodes: z.array(nodeSchema).min(1),
  resubmit: humanSchema.optional(),
  settings: settingsSchema.optional(),
  events: listenersSchema.optional(),
  limits: z.strictObject({ maxSteps: z.number().int().min(1).max(2000), maxCommands: z.number().int().min(1).max(5000) }).optional(),
});
export type Definition = z.infer<typeof definitionSchema>;
export const identitySchema = z.strictObject({ tenantId: idSchema, actorId: idSchema });
export type Identity = z.infer<typeof identitySchema>;
// First-class initiator attributes for `initiator` conditions; the engine accepts either this or
// the legacy `_initiator*` data keys.
export const initiatorSchema = z.strictObject({
  deptLevels: z.array(idSchema).max(200).optional(),
  roles: z.array(idSchema).max(200).optional(),
});
export type Initiator = z.infer<typeof initiatorSchema>;
export type InitiatorContext = Initiator & { initiatorId?: string; deptId?: string };
export const taskSchema = z.strictObject({
  id: idSchema, nodeId: idSchema, type: z.enum(["approval", "task"]),
  mode: z.enum(["all", "any", "sequential", "percentage", "candidate"]),
  assignees: z.array(idSchema).max(200), candidates: z.array(idSchema).max(200), approved: z.array(idSchema).max(200),
  additions: z.array(z.strictObject({
    userId: idSchema, ownerId: idSchema, position: z.enum(["before", "after"]), completed: z.boolean(),
  })).max(200).optional(),
  status: z.enum(["pending", "approved", "rejected", "cancelled"]),
  createdAt: z.string(), deadline: z.string().optional(), fields: z.array(fieldSchema).max(100),
  needSign: z.boolean().optional(), signature: z.string().nullable().optional(),
});
export type Task = z.infer<typeof taskSchema>;
// External event signal payload (Temporal signal WORKFLOW_EVENT_SIGNAL, see protocol.ts): a
// bounded event name plus optional JSON data. The workflow buffers unmatched signals.
export const eventSignalSchema = z.strictObject({
  event: idSchema,
  data: jsonSchema.optional(),
});
export type EventSignal = z.infer<typeof eventSignalSchema>;
// continueAsNew carry-over: only the bounded, replay-relevant slice travels; the pinned
// definition and business `data` stay in their normal input fields.
export const resumeReceiptSchema = z.strictObject({
  requestId: idSchema, fingerprint: z.string().min(1),
  receipt: z.strictObject({ requestId: idSchema, eventId: z.string().min(1).max(1024) }),
});
export const resumeSchema = z.strictObject({
  // Index of the next top-level node in the pinned definition.
  offset: z.number().int().nonnegative().max(5000),
  // Cumulative event sequence keeps host projections ordered; the cumulative step count makes
  // maxSteps a TOTAL budget and commandBytes keeps the 1MB fingerprint cap cumulative across the
  // run chain, so executionId values stay unique. maxCommands remains a per-run count limit.
  sequence: z.number().int().nonnegative().max(2_147_483_647),
  steps: z.number().int().nonnegative().max(2_147_483_647),
  commandBytes: z.number().int().nonnegative().max(1_000_000),
  agreedUsers: z.array(idSchema).max(10_000),
  completedHumans: z.array(z.strictObject({
    nodeId: idSchema, name: z.string().min(1).max(256), actorIds: z.array(idSchema).max(500),
  })).max(500),
  // Withdraw only needs the newest handled node; the tail is kept and older restore snapshots are
  // dropped (documented tradeoff).
  completedTasks: z.array(z.strictObject({ nodeId: idSchema, task: taskSchema })).max(50),
  receipts: z.array(resumeReceiptSchema).max(200),
  // Signals buffered because no active wait matched yet survive a safe-boundary continueAsNew.
  pendingEvents: z.array(eventSignalSchema).max(100).optional(),
});
export type ResumeState = z.infer<typeof resumeSchema>;
export const inputSchema = z.strictObject({
  tenantId: idSchema, instanceId: idSchema, businessKey: idSchema, initiatorId: idSchema,
  definition: definitionSchema, data: dataSchema,
  initiator: initiatorSchema.optional(),
  lineage: z.array(referenceSchema).max(8).optional(),
  // Present only on a continueAsNew run; absent on the initial start, so existing callers are unaffected.
  resume: resumeSchema.optional(),
});
export type WorkflowInput = z.infer<typeof inputSchema>;
const commandBase = { requestId: idSchema, instanceId: idSchema, ...identitySchema.shape, comment: dataSchema.optional() };
const taskCommandBase = { ...commandBase, taskId: idSchema };
export const commandSchema = z.discriminatedUnion("type", [
  z.strictObject({ ...taskCommandBase, type: z.enum(["approve", "complete"]), data: dataSchema.optional(), signature: z.string().optional(),
    otherNodeUsers: z.record(keySchema, z.array(idSchema).max(200)).optional() }),
  z.strictObject({ ...taskCommandBase, type: z.enum(["reject", "claim"]), otherNodeUsers: z.record(keySchema, z.array(idSchema).max(200)).optional() }),
  z.strictObject({ ...taskCommandBase, type: z.literal("transfer"), userId: idSchema }),
  z.strictObject({ ...taskCommandBase, type: z.literal("addAssignee"), userId: idSchema, position: z.enum(["before", "after"]) }),
  z.strictObject({ ...taskCommandBase, type: z.literal("reassign"), fromUserId: idSchema, userId: idSchema }),
  z.strictObject({ ...taskCommandBase, type: z.literal("override"), userId: idSchema, action: z.enum(["approve", "complete", "reject"]), data: dataSchema.optional(), signature: z.string().optional(), otherNodeUsers: z.record(keySchema, z.array(idSchema).max(200)).optional() }),
  z.strictObject({ ...taskCommandBase, type: z.literal("returnTo"), nodeId: idSchema }),
  z.strictObject({ ...commandBase, type: z.literal("withdraw"), nodeId: idSchema }),
  z.strictObject({ ...commandBase, type: z.enum(["suspend", "resume"]) }),
  // Controlled definition migration ("pause -> migrate -> resume"): the engine loads the target
  // version through the definition store and rebases the run at the tracked top-level node.
  z.strictObject({ ...commandBase, type: z.literal("migrate"), toVersion: z.number().int().min(1) }),
  z.strictObject({ ...commandBase, type: z.literal("cancel") }),
  z.strictObject({ ...commandBase, type: z.literal("event"), waitId: idSchema, event: idSchema, data: jsonSchema }),
]);
export type Command = z.infer<typeof commandSchema>;
export type CommandReceipt = { requestId: string; eventId: string };
export type Status = "running" | "completed" | "rejected" | "cancelled" | "failed";
export type Wait = { id: string; nodeId: string; event: string };
export type Snapshot = {
  tenantId: string; instanceId: string; businessKey: string; definition: DefinitionReference;
  initiatorId: string; status: Status; tasks: Task[]; waits: Wait[]; data: Data;
  suspended: boolean;
  returnTargets: { nodeId: string; name: string; actorIds: string[] }[];
};
export type WorkflowResult = { status: "completed" | "rejected" | "cancelled"; data: Data };
export const eventSchema = z.strictObject({
  eventId: z.string().min(1).max(1024), eventType: z.enum([
    "workflow.started", "workflow.completed", "workflow.rejected", "workflow.cancelled", "workflow.failed",
    "workflow.nodeEntered", "workflow.nodeCompleted", "workflow.taskCreated", "workflow.taskChanged",
    "workflow.waitCreated", "workflow.eventReceived", "workflow.cc", "workflow.cancelRequested",
    "workflow.suspended", "workflow.resumed",
    "workflow.returnRequested", "workflow.migrateRequested", "workflow.migrated",
    "workflow.childStarted", "workflow.childCompleted",
  ]),
  version: z.literal(1), occurredAt: z.string().datetime(),
  tenantId: idSchema, instanceId: idSchema, businessKey: idSchema, definition: referenceSchema,
  sequence: z.number().int().positive(), details: dataSchema,
});
export type WorkflowEvent = z.infer<typeof eventSchema>;
