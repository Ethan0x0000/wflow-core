import { assertJsonSize, validateFields, WorkflowValidationError } from "./definition.js";
import { dataSchema, type Command, type Data, type HumanNode, type Task } from "./schema.js";

export function requiredApprovals(task: Task, node: HumanNode): number {
  if (task.mode === "any" || task.mode === "candidate") return 1;
  if (task.mode === "percentage") return Math.ceil(task.assignees.length * (node.percentage ?? 100) / 100);
  return task.assignees.length;
}

export function actionableUsers(task: Task): string[] {
  if (task.status !== "pending") return [];
  const remaining = task.assignees.filter((user) => !task.approved.includes(user));
  const base = task.mode === "sequential" ? remaining.slice(0, 1) : remaining;
  const additions = task.additions ?? [];
  const before = additions.filter((a) => !a.completed && a.position === "before" && base.includes(a.ownerId));
  const after = additions.filter((a) => !a.completed && a.position === "after" && task.approved.includes(a.ownerId));
  return [...before.map((a) => a.userId), ...after.map((a) => a.userId),
    ...base.filter((user) => !before.some((a) => a.ownerId === user) && !(task.mode === "sequential" && after.length))];
}

export function applyTaskCommand(task: Task, node: HumanNode, command: Extract<Command, { taskId: string }>, initiatorId: string, data: Data): void {
  const fail = (code: string): never => { throw new WorkflowValidationError(code); };
  if (command.type === 'returnTo') fail('RETURN_REQUIRES_WORKFLOW');
  if (task.status !== "pending") fail("TASK_CLOSED");
  if (command.type === "override") {
    applyTaskCommand(task, node, { requestId: command.requestId, tenantId: command.tenantId, instanceId: command.instanceId, taskId: command.taskId, actorId: command.userId,
      ...(command.action === "reject" ? { type: "reject" } : { type: command.action, ...(command.data ? { data: command.data } : {}), ...(command.signature ? { signature: command.signature } : {}) }) }, initiatorId, data);
    return;
  }
  if (command.type === "reassign") {
    if (task.approved.includes(command.fromUserId) || task.assignees.includes(command.userId) || task.additions?.some((a) => a.userId === command.userId)) fail("INVALID_ASSIGNEE");
    let changed = false;
    const replace = (id: string) => { if (id !== command.fromUserId) return id; changed = true; return command.userId; };
    task.assignees = task.assignees.map(replace);
    task.candidates = task.candidates.map(replace);
    for (const addition of task.additions ?? []) { if (!addition.completed) addition.userId = replace(addition.userId); addition.ownerId = replace(addition.ownerId); }
    if (!changed) fail("NOT_TASK_ASSIGNEE");
    return;
  }
  if (command.type === "claim") {
    // Flowable claimTask only requires an unassigned task; it does not re-check identity links.
    if (task.assignees.length) fail("CANNOT_CLAIM");
    task.assignees = [command.actorId];
    return;
  }
  if (!actionableUsers(task).includes(command.actorId)) fail("NOT_TASK_ASSIGNEE");
  if (command.type === "transfer" || command.type === "addAssignee") {
    if (task.assignees.includes(command.userId) || task.additions?.some((a) => a.userId === command.userId) || (node.type === "approval" && node.selfApproval !== "allow" && command.userId === initiatorId)) fail("INVALID_ASSIGNEE");
    if (command.type === "transfer") {
      if (!node.allowTransfer) fail("TRANSFER_DISABLED");
      task.assignees = task.assignees.map((user) => user === command.actorId ? command.userId : user);
      for (const addition of task.additions ?? []) {
        if (addition.userId === command.actorId) addition.userId = command.userId;
        if (addition.ownerId === command.actorId) addition.ownerId = command.userId;
      }
    } else {
      if (!node.allowAddAssignees || task.mode === "candidate" || !task.assignees.includes(command.actorId)) fail("ADD_ASSIGNEE_DISABLED");
      // Java AddMultiInstanceCmd: only sequential multi-instance nodes support before-add.
      if (command.position === "before" && task.mode !== "sequential") fail("BEFORE_ADD_FORBIDDEN");
      if (task.assignees.length + (task.additions?.length ?? 0) >= 200) fail("TOO_MANY_ASSIGNEES");
      task.additions ??= [];
      task.additions.push({ userId: command.userId, ownerId: command.actorId, position: command.position, completed: false });
    }
    return;
  }
  if (command.type === "reject") {
    if (node.type !== "approval") fail("INVALID_TASK_ACTION");
    task.status = "rejected";
    return;
  }
  if ((node.type === "approval" && command.type !== "approve") || (node.type === "task" && command.type !== "complete")) fail("INVALID_TASK_ACTION");
  if (node.needSign && command.type === "approve" && !("signature" in command && command.signature)) fail("SIGNATURE_REQUIRED");
  if ((command.type === "approve" || command.type === "complete") && "signature" in command) task.signature = command.signature ?? null;
  const patch = "data" in command ? command.data ?? {} : {};
  validateFields(node.fields ?? [], patch, true);
  const merged = { ...data, ...patch };
  assertJsonSize(merged);
  dataSchema.parse(merged);
  Object.assign(data, patch);
  const addition = task.additions?.find((a) => a.userId === command.actorId && !a.completed);
  if (addition) addition.completed = true;
  else task.approved.push(command.actorId);
  if (task.approved.length >= requiredApprovals(task, node) && (task.additions ?? []).every((a) => a.completed)) task.status = "approved";
}
