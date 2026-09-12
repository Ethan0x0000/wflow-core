import { describe, expect, it } from "vitest";
import { delayUntilTarget } from "../src/nodes/automation.js";

type DelayUntilNode = Parameters<typeof delayUntilTarget>[0];

describe("delayUntil timezone default", () => {
  const node = (props: Record<string, unknown>): DelayUntilNode => ({ id: "wait", type: "delayUntil", timeOfDay: "08:30:00", ...props } as DelayUntilNode);
  const now = Date.parse("2030-01-02T00:00:00Z");

  it("uses the explicit node offset", () => {
    expect(delayUntilTarget(node({ tzOffsetMinutes: 0 }), undefined, now)).toBe(Date.parse("2030-01-02T08:30:00Z"));
    expect(delayUntilTarget(node({ tzOffsetMinutes: 120 }), 0, now)).toBe(Date.parse("2030-01-02T06:30:00Z"));
  });

  it("uses the host definition default when the node has no offset", () => {
    expect(delayUntilTarget(node({}), 0, now)).toBe(Date.parse("2030-01-02T08:30:00Z"));
    expect(delayUntilTarget(node({}), 120, now)).toBe(Date.parse("2030-01-02T06:30:00Z"));
  });

  it("keeps the historical +08:00 fallback", () => {
    expect(delayUntilTarget(node({}), undefined, now)).toBe(Date.parse("2030-01-02T00:30:00Z"));
  });

  it("keeps absolute `at` timestamps unchanged", () => {
    expect(delayUntilTarget(node({ at: "2030-01-02T03:04:05+08:00", timeOfDay: undefined }), 0, 0)).toBe(Date.parse("2030-01-02T03:04:05+08:00"));
  });
});
