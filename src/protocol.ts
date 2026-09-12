export const WORKFLOW_TYPE = "genericWorkflowV1";
export const COMMAND_UPDATE = "workflowCommandV1";
export const SNAPSHOT_QUERY = "workflowSnapshotV1";
// External event channel: a Temporal signal whose payload is validated by schema.eventSignalSchema.
export const WORKFLOW_EVENT_SIGNAL = "workflowEventV1";

// The tenant length prefix disambiguates ids containing ":", so the wire format stays stable for
// workflows already issued by a running server. `parseWorkflowId` is its exact inverse.
export function workflowId(tenantId: string, instanceId: string): string {
  return `wf:${tenantId.length}:${tenantId}:${instanceId}`;
}

/** Parses a workflow id produced by `workflowId`, returning undefined for malformed ids. */
export function parseWorkflowId(id: string): { tenantId: string; instanceId: string } | undefined {
  if (!id.startsWith("wf:")) return undefined;
  const lengthEnd = id.indexOf(":", 3);
  if (lengthEnd < 0) return undefined;
  const lengthText = id.slice(3, lengthEnd);
  if (!/^\d+$/.test(lengthText)) return undefined;
  const tenantLength = Number(lengthText);
  const tenantStart = lengthEnd + 1;
  const tenantEnd = tenantStart + tenantLength;
  if (id.length <= tenantEnd || id[tenantEnd] !== ":") return undefined;
  return { tenantId: id.slice(tenantStart, tenantEnd), instanceId: id.slice(tenantEnd + 1) };
}
