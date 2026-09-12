import { describe, expect, it } from "vitest";
import { parseWorkflowId, WORKFLOW_EVENT_SIGNAL, workflowId } from "../src/protocol.js";

describe("workflow id protocol", () => {
  it("keeps the external event signal wire name stable for already deployed workers", () => {
    expect(WORKFLOW_EVENT_SIGNAL).toBe("workflowEventV1");
  });

  it("keeps the length-prefixed wire format for ids already issued by the server", () => {
    expect(workflowId("tenant", "leave-1")).toBe("wf:6:tenant:leave-1");
    expect(workflowId("t", "i")).toBe("wf:1:t:i");
  });

  it("round-trips ids containing colons in both parts", () => {
    expect(parseWorkflowId(workflowId("ten:ant", "inst:ance:1"))).toEqual({ tenantId: "ten:ant", instanceId: "inst:ance:1" });
    expect(parseWorkflowId(workflowId("t", "a:b"))).toEqual({ tenantId: "t", instanceId: "a:b" });
    expect(parseWorkflowId(workflowId("", "x"))).toEqual({ tenantId: "", instanceId: "x" });
  });

  it("rejects malformed ids", () => {
    for (const id of ["", "wf", "wf:", "wf:x:tenant:inst", "wf:6:tenant", "wf:6:tenantx:inst", "other:6:tenant:inst"]) {
      expect(parseWorkflowId(id)).toBeUndefined();
    }
  });
});
