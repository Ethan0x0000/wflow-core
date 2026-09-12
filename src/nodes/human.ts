import { condition } from "@temporalio/workflow";
import { WorkflowValidationError } from "../errors.js";
import { jsonSchema, type Data, type HumanNode, type Json, type Task } from "../schema.js";
import { activityTuning } from "../settings.js";
import { actionableUsers, requiredApprovals } from "../tasks.js";
import { activities } from "../workflow-activities.js";
import { cachePreset, nodeEvents, presetFor, Rejected, type WorkflowContext } from "../workflow-context.js";

async function runHuman(ctx: WorkflowContext, node: HumanNode, executionId: string, variables: Data): Promise<void> {
  // WithdrawCmd keeps the decisions of other assignees: reuse the previous task with the withdrawer's record cleared.
  const restored = ctx.restoredTasks.get(node.id);
  let assignees: string[] = [], resolved = 0, autoAgree: string[] = [];
  if (restored) ctx.restoredTasks.delete(node.id);
  else {
    // AssignUserParser.parser: a NODE_USERS preset always wins over rule parsing.
    const stored = presetFor(variables, node.id);
    if (stored) {
      // Presets still pass through agent replacement (EventHandler.taskCreate) but skip rule parsing/dedup.
      assignees = (await activities.resolveAssignees.executeWithOptions(activityTuning(ctx.settings), [{ context: ctx.context, executionId, nodeId: node.id, assignment: { type: "users", userIds: stored }, data: variables }])).users;
      if (node.type === "approval" && node.selfApproval !== "allow") assignees = assignees.filter((user) => user !== ctx.input.initiatorId);
    } else {
      const resolution = await activities.resolveAssignees.executeWithOptions(activityTuning(ctx.settings), [{ context: ctx.context, executionId, nodeId: node.id, assignment: node.assignees, data: variables }]);
      assignees = resolution.users;
      if (resolution.reason) ctx.nodeReasons.set(executionId, resolution.reason);
      if (node.type === "approval" && node.selfApproval !== "allow") assignees = assignees.filter((user) => user !== ctx.input.initiatorId);
      resolved = assignees.length;
      // DeduplicationRule ONCE: remove users who already agreed, or auto-agree their new tasks.
      const dedup = ctx.settings.deduplication;
      if (dedup?.type === "ONCE" && node.type === "approval") {
        const repeated = assignees.filter((user) => ctx.agreedUsers.has(user));
        if (repeated.length) {
          if (dedup.skip) assignees = assignees.filter((user) => !ctx.agreedUsers.has(user));
          else autoAgree = repeated;
        }
      }
      cachePreset(variables, node.id, assignees);
    }
  }
  await ctx.fireListeners(nodeEvents(node), "node", node.id, "calcComplete", executionId, variables);
  if (!restored && !assignees.length) {
    if (node.emptyAssignees === "skip") {
      ctx.nodeReasons.set(executionId, resolved === 0 ? "SKIP_EMPTY" : "SKIP_DISTINCT");
      return;
    }
    throw new WorkflowValidationError("NO_ASSIGNEES");
  }
  const deadline = node.timeout ? Date.now() + node.timeout.afterMs : undefined;
  const task: Task = restored ? { ...restored, id: `task-${executionId}`, ...(deadline === undefined ? {} : { deadline: new Date(deadline).toISOString() }) } : {
    id: `task-${executionId}`, nodeId: node.id, type: node.type, mode: node.mode,
    assignees: node.mode === "candidate" ? [] : assignees,
    candidates: node.mode === "candidate" ? assignees : [], approved: [], status: "pending",
    createdAt: new Date().toISOString(), fields: node.fields ?? [], needSign: node.needSign === true,
    ...(deadline === undefined ? {} : { deadline: new Date(deadline).toISOString() }),
  };
  ctx.activeTasks.set(task.id, { task, node, data: variables });
  const settle = (action: string, extra: Data = {}): Promise<string> => ctx.emit("workflow.taskChanged", { task: jsonSchema.parse(task), action, ...extra });
  try {
    await ctx.emit("workflow.taskCreated", { task: jsonSchema.parse(task) });
    await ctx.fireListeners(nodeEvents(node), "node", node.id, "created", executionId, variables);
    while (autoAgree.length && task.status === "pending") {
      const user = autoAgree.find((candidate) => actionableUsers(task).includes(candidate));
      if (!user) break;
      task.approved.push(user);
      ctx.agreedUsers.add(user);
      await settle("auto_agree", { actorId: user, auto: true, comment: { text: "系统干预：自动同意" } });
      if (task.approved.length >= requiredApprovals(task, node)) task.status = "approved";
    }
    if (deadline === undefined) await condition(() => task.status !== "pending");
    else if (node.timeout?.outcome === "notify") {
      // Mirrors the Java non-interrupting boundary timer: reminders repeat every interval up to `repeat` times.
      const repeat = node.timeout.repeat ?? 20;
      let next = deadline, notifications = 0;
      while (task.status === "pending" && notifications < repeat) {
        if (await condition(() => task.status !== "pending", Math.max(1, next - Date.now()))) break;
        await condition(() => !ctx.suspended);
        if (task.status !== "pending") break;
        await settle("timeout");
        notifications += 1;
        next = Date.now() + node.timeout.afterMs;
      }
      await condition(() => task.status !== "pending");
    } else {
      if (!await condition(() => task.status !== "pending", Math.max(1, deadline - Date.now()))) {
        await condition(() => !ctx.suspended);
        task.status = node.timeout?.outcome === "approve" ? "approved" : "rejected";
        await settle("timeout");
      }
    }
    if (task.status === "approved") for (const user of task.approved) ctx.agreedUsers.add(user);
    await ctx.fireListeners(nodeEvents(node), "node", node.id, "complete", executionId, variables);
    if (task.status === "rejected") {
      if (node.type === "approval" && node.rejectRule?.type === "SKIP" && node.rejectRule.target) {
        ctx.returnTarget = node.rejectRule.target;
        await ctx.emit("workflow.returnRequested", { targetNodeId: node.rejectRule.target, action: "reject_jump" });
        ctx.scope.cancel();
        return;
      }
      // NEXT (the Java default) records the rejection but keeps the flow moving.
      if (node.rejectRule === undefined || node.rejectRule.type === "NEXT") {
        if (ctx.input.definition.nodes.some((n) => n.id === node.id) || ctx.input.definition.resubmit?.id === node.id) ctx.completedHumans.set(node.id, { nodeId: node.id, name: node.name ?? node.id, actorIds: [] });
        return;
      }
      throw new Rejected();
    }
    if (ctx.input.definition.nodes.some((n) => n.id === node.id) || ctx.input.definition.resubmit?.id === node.id) {
      ctx.completedTasks.set(node.id, jsonSchema.parse(task) as Task);
      ctx.completedHumans.set(node.id, { nodeId: node.id, name: node.name ?? node.id, actorIds: [...task.approved] });
    }
  } catch (error) {
    if (task.status === "pending") {
      task.status = "cancelled";
      await ctx.emit("workflow.taskChanged", { task: jsonSchema.parse(task), action: "cancel" });
    }
    throw error;
  } finally {
    ctx.activeTasks.delete(task.id);
    // ProcSetting.reloadUser drops preset assignees once the node is left.
    if (ctx.settings.reloadUser && variables._nodeUsers && typeof variables._nodeUsers === "object" && !Array.isArray(variables._nodeUsers)) delete (variables._nodeUsers as Record<string, Json>)[node.id];
  }
}

export { runHuman };
