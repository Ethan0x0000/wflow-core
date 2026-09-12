import { assertJsonSize } from "./definition.js";
import { WorkflowValidationError } from "./errors.js";
import type { ResumeState, Task } from "./schema.js";
import type { HumanCompletion, PendingEvent, ReceiptEntry } from "./workflow-context.js";

// Carried state is bounded so long-lived instances stay well below the 512KB input limit. The
// newest resolved receipt and the newest handled task are always kept (they are the only ones a
// post-continue retry or withdraw can still target).
export const RESUME_LIMITS = Object.freeze({ receipts: 200, completedTasks: 10 });

export type ResumeInputs = {
  offset: number;
  sequence: number;
  steps: number;
  commandBytes: number;
  agreedUsers: ReadonlySet<string>;
  completedHumans: ReadonlyMap<string, HumanCompletion>;
  completedTasks: ReadonlyMap<string, Task>;
  receipts: ReadonlyMap<string, ReceiptEntry>;
  pendingEvents: readonly PendingEvent[];
};

/** Builds the bounded resume payload; `data` and the definition travel in their normal fields. */
export function carryResumeState(state: ResumeInputs): ResumeState {
  const completedHumans = [...state.completedHumans.values()];
  const completedTasks = [...state.completedTasks.entries()].slice(-RESUME_LIMITS.completedTasks)
    .map(([nodeId, task]) => ({ nodeId, task }));
  // Map iteration order does not always match completion order after a task is re-run, so make
  // sure the newest handled node (what withdraw targets) keeps its restore snapshot.
  const newest = completedHumans.at(-1);
  if (newest && !completedTasks.some((entry) => entry.nodeId === newest.nodeId)) {
    const task = state.completedTasks.get(newest.nodeId);
    if (task) completedTasks.push({ nodeId: newest.nodeId, task });
  }
  return {
    offset: state.offset,
    sequence: state.sequence,
    steps: state.steps,
    commandBytes: state.commandBytes,
    agreedUsers: [...state.agreedUsers],
    completedHumans: completedHumans.map(({ nodeId, name, actorIds }) => ({ nodeId, name, actorIds: [...actorIds] })),
    completedTasks,
    receipts: [...state.receipts.entries()].slice(-RESUME_LIMITS.receipts)
      .map(([requestId, entry]) => ({ requestId, fingerprint: entry.fingerprint, receipt: entry.receipt })),
    pendingEvents: state.pendingEvents.map(({ event, data }) => ({ event, data })),
  };
}

/** Rebuilds the per-run maps from carried state; absent resume keeps every budget at zero. */
export function resumeInitialState(resume: ResumeState | undefined): {
  sequence: number;
  steps: number;
  commandBytes: number;
  agreedUsers: Set<string>;
  completedHumans: Map<string, HumanCompletion>;
  completedTasks: Map<string, Task>;
  receipts: Map<string, ReceiptEntry>;
  pendingEvents: PendingEvent[];
} {
  return {
    sequence: resume?.sequence ?? 0,
    steps: resume?.steps ?? 0,
    commandBytes: resume?.commandBytes ?? 0,
    agreedUsers: new Set(resume?.agreedUsers ?? []),
    completedHumans: new Map((resume?.completedHumans ?? []).map((entry) => [entry.nodeId, { nodeId: entry.nodeId, name: entry.name, actorIds: [...entry.actorIds] }])),
    completedTasks: new Map((resume?.completedTasks ?? []).map(({ nodeId, task }) => [nodeId, task])),
    receipts: new Map((resume?.receipts ?? []).map(({ requestId, fingerprint, receipt }) => [requestId, { fingerprint, receipt }])),
    pendingEvents: (resume?.pendingEvents ?? []).map(({ event, data }) => ({ event, data: data ?? null })),
  };
}

/** Records a resolved receipt and evicts the oldest entries beyond the carry bound. */
export function rememberReceipt(receipts: Map<string, ReceiptEntry>, requestId: string, entry: ReceiptEntry): void {
  receipts.set(requestId, entry);
  while (receipts.size > RESUME_LIMITS.receipts) {
    const oldest = receipts.keys().next().value;
    if (oldest === undefined) break;
    receipts.delete(oldest);
  }
}

/**
 * The new-run input must still fit the payload bounds. A too-large carried state fails with a
 * dedicated code instead of being truncated (which would silently lose dedup guarantees).
 */
export function assertResumePayload(input: unknown): void {
  try {
    assertJsonSize(input);
  } catch {
    throw new WorkflowValidationError("RESUME_STATE_TOO_LARGE");
  }
}
