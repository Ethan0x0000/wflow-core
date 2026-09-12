import { NativeConnection, Worker, type WorkerOptions } from "@temporalio/worker";
import { createActivities } from "./activities.js";
import type { Adapters } from "./ports.js";

export type WorkflowWorkerOptions = Omit<WorkerOptions, "workflowsPath" | "activities"> & {
  adapters: Adapters;
  workflowsPath?: string;
};

/** Absolute path of the shipped workflow bundle entry, resolved next to the running build. */
export function resolveWorkflowsPath(): string {
  try {
    return require.resolve("./workflows.js");
  } catch {
    // Source execution (vitest, tsx): the compiled bundle lives in the sibling dist/ directory.
    return require.resolve("../dist/workflows.js");
  }
}

export async function createWorkflowWorker(options: WorkflowWorkerOptions): Promise<Worker> {
  const { adapters, workflowsPath, ...workerOptions } = options;
  return Worker.create({
    ...workerOptions,
    workflowsPath: workflowsPath ?? resolveWorkflowsPath(),
    activities: createActivities(adapters),
  });
}

export async function runWorkflowWorker(options: WorkflowWorkerOptions): Promise<void> {
  const worker = await createWorkflowWorker(options);
  await worker.run();
}

export { NativeConnection };
