import { ChildWorkflowCancellationType, executeChild, ParentClosePolicy, startChild, uuid4 } from "@temporalio/workflow";
import { mapValues } from "../definition.js";
import { WorkflowValidationError } from "../errors.js";
import { workflowId } from "../protocol.js";
import { jsonSchema, type Data, type Node } from "../schema.js";
import { activityTuning } from "../settings.js";
import { activities } from "../workflow-activities.js";
import { literal, put, Rejected, type WorkflowContext } from "../workflow-context.js";

type ChildNode = Extract<Node, { type: "child" }>;

async function runChild(ctx: WorkflowContext, node: ChildNode, executionId: string, variables: Data): Promise<void> {
  const input = ctx.input;
  const unbound = !node.definition.version;
  const lineage = [...(input.lineage ?? []), ctx.context.definition];
  if (lineage.length > 8 || lineage.some((ref) => ref.id === node.definition.id && (unbound || ref.version === node.definition.version))) throw new WorkflowValidationError("RECURSIVE_CHILD_DEFINITION");
  const definition = await activities.loadDefinition.executeWithOptions(activityTuning(ctx.settings), [{ tenantId: ctx.context.tenantId, reference: node.definition }]);
  const instanceId = uuid4();
  const fixed = node.initiator?.type === "fixed" ? node.initiator.userId : undefined;
  const initiatorId = fixed ?? input.initiatorId;
  // Java ProcessUtil.getFormCtxMappingData: inherited variables, auto-mapped fields, then explicit mapping.
  const childData: Data = {};
  if (node.inheritVariables) Object.assign(childData, variables);
  if (node.formAutoMapping) for (const field of definition.inputFields ?? []) {
    if (field.key in variables && variables[field.key] !== undefined) childData[field.key] = variables[field.key]!;
  }
  for (const mapping of node.mappings ?? []) {
    if (!mapping.target) continue;
    const value = mapping.fixed
      ? mapping.source.startsWith("#") ? variables[mapping.source.slice(1)] : literal(mapping.source)
      : variables[mapping.source];
    if (value !== undefined) childData[mapping.target] = value;
  }
  Object.assign(childData, mapValues(node.input, variables));
  const childInput = { ...input, instanceId, definition, data: childData, initiatorId,
    // A fixed initiator replaces the inherited user, so its dept/role context must not leak down.
    ...(fixed ? { initiator: undefined } : {}),
    businessKey: node.inheritBusinessKey ? input.businessKey : instanceId, lineage };
  const content = { initiator: initiatorId, code: node.definition.id, name: definition.name, version: definition.version, subInstId: instanceId };
  if (node.async) {
    await startChild(ctx.workflow, {
      workflowId: workflowId(ctx.context.tenantId, instanceId), args: [childInput],
      parentClosePolicy: ParentClosePolicy.ABANDON, cancellationType: ChildWorkflowCancellationType.ABANDON,
    });
    if (node.resultKey) put(variables, node.resultKey, { instanceId, async: true });
    await ctx.emit('workflow.childStarted', { nodeId: node.id, executionId, childInstanceId: instanceId, childDefinition: jsonSchema.parse(node.definition), initiatorId, async: true, content });
    return;
  }
  await ctx.emit('workflow.childStarted', { nodeId: node.id, executionId, childInstanceId: instanceId, childDefinition: jsonSchema.parse(node.definition), initiatorId, async: false, content });
  const result = await executeChild(ctx.workflow, {
    workflowId: workflowId(ctx.context.tenantId, instanceId),
    cancellationType: ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    args: [childInput],
  });
  for (const mapping of node.mappings ?? []) if (mapping.sync && mapping.target in result.data) put(variables, mapping.source, result.data[mapping.target]!);
  await ctx.emit('workflow.childCompleted', { nodeId: node.id, executionId, childInstanceId: instanceId, status: result.status });
  // Java processEnd writes the parent record result, but only statusSync propagates the child outcome to the parent flow.
  if (result.status === "rejected" || result.status === "cancelled") { if (node.statusSync) throw new Rejected(); return; }
  if (result.status !== "completed") throw new WorkflowValidationError("CHILD_NOT_COMPLETED");
  put(variables, node.resultKey, result.data);
}

export { runChild };
