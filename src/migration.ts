import { WorkflowValidationError } from "./errors.js";
import type { Definition, Task } from "./schema.js";
import type { HumanCompletion } from "./workflow-context.js";

/**
 * Definition migration is only allowed at a fully settled top-level boundary: the run is paused,
 * no human task or event wait is in flight and no nested container (gateway/loop/forEach/child)
 * is mid-flight. Suspend deliberately keeps active tasks, so the idle check is separate.
 */
export function assertMigratable(state: {
  suspended: boolean;
  activeTasks: number;
  activeWaits: number;
  depth: number;
  currentVersion: number;
  toVersion: number;
}): void {
  if (!state.suspended) throw new WorkflowValidationError("MIGRATION_REQUIRES_SUSPENDED");
  if (state.activeTasks > 0 || state.activeWaits > 0) throw new WorkflowValidationError("MIGRATION_REQUIRES_IDLE");
  if (state.depth !== 0) throw new WorkflowValidationError("MIGRATION_REQUIRES_TOP_LEVEL");
  if (state.toVersion === state.currentVersion) throw new WorkflowValidationError("MIGRATION_INVALID_TARGET");
}

/**
 * Rebases the run on the tracked next top-level node id. Node ids are the migration contract: a
 * target graph that no longer has the tracked node cannot be resumed deterministically and fails
 * the instance instead of guessing a position.
 */
export function migrationOffset(target: Definition, nodeId: string | undefined): number {
  const index = target.nodes.findIndex((node) => node.id === nodeId);
  if (index < 0) throw new WorkflowValidationError("MIGRATION_TARGET_MISSING");
  return index;
}

/**
 * Prunes carried state keyed by node id to the nodes the target graph still has. `agreedUsers`
 * is intentionally untouched (deduplication ONCE semantics survive a migration), while a pending
 * return/resubmit position is reset by the caller because its fallback path may be gone.
 */
export function pruneMigrationState(target: Definition, state: {
  completedHumans: Map<string, HumanCompletion>;
  completedTasks: Map<string, Task>;
  restoredTasks: Map<string, Task>;
}): void {
  const known = new Set(target.nodes.map((node) => node.id));
  if (target.resubmit) known.add(target.resubmit.id);
  for (const id of [...state.completedHumans.keys()]) if (!known.has(id)) state.completedHumans.delete(id);
  for (const id of [...state.completedTasks.keys()]) if (!known.has(id)) state.completedTasks.delete(id);
  for (const id of [...state.restoredTasks.keys()]) if (!known.has(id)) state.restoredTasks.delete(id);
}
