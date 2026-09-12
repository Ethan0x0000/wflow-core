import { describe, expect, it } from "vitest";
import { eventSignalSchema } from "../src/schema.js";
import { PENDING_EVENT_LIMIT, receiveEventSignal, takePendingEvent, type EventSignalState, type WaitEntry } from "../src/workflow-context.js";

const wait = (id: string, event: string): WaitEntry => ({ wait: { id, nodeId: id, event }, received: false });

function state(partial: Partial<EventSignalState> = {}): EventSignalState {
  return { activeWaits: new Map(), waitReceipts: 0, pendingEvents: [], status: "running", ...partial };
}

describe("workflow event signal payload", () => {
  it("accepts a bounded event with optional JSON data and rejects malformed payloads", () => {
    expect(eventSignalSchema.parse({ event: "contract.signed" })).toEqual({ event: "contract.signed" });
    expect(eventSignalSchema.parse({ event: "go", data: { by: "u1" } })).toEqual({ event: "go", data: { by: "u1" } });
    expect(() => eventSignalSchema.parse({ event: "" })).toThrow();
    expect(() => eventSignalSchema.parse({ event: "x".repeat(129) })).toThrow();
    expect(() => eventSignalSchema.parse({ event: "go", extra: true })).toThrow();
    expect(() => eventSignalSchema.parse({ event: "go", data: () => null })).toThrow();
  });
});

describe("workflow event signal delivery", () => {
  it("completes the first unreceived matching active wait in registration order", () => {
    const first = wait("wait-1", "go");
    const second = wait("wait-2", "go");
    const ctx = state({ activeWaits: new Map([["wait-1", first], ["wait-2", second]]), waitReceipts: 4 });
    receiveEventSignal(ctx, { event: "go", data: 7 });
    expect(first).toMatchObject({ received: true, value: 7, source: "signal", receivedOrder: 5 });
    expect(second.received).toBe(false);
    receiveEventSignal(ctx, { event: "go" });
    expect(second).toMatchObject({ received: true, value: null, source: "signal", receivedOrder: 6 });
  });

  it("skips active waits that already received and buffers unmatched signals in arrival order", () => {
    const received = { ...wait("wait-1", "go"), received: true };
    const ctx = state({ activeWaits: new Map([["wait-1", received]]) });
    receiveEventSignal(ctx, { event: "go", data: 1 });
    receiveEventSignal(ctx, { event: "other", data: 2 });
    receiveEventSignal(ctx, { event: "go", data: 3 });
    expect(ctx.pendingEvents).toEqual([{ event: "go", data: 1 }, { event: "other", data: 2 }, { event: "go", data: 3 }]);
    expect(takePendingEvent(ctx, "go")).toEqual({ event: "go", data: 1 });
    expect(takePendingEvent(ctx, "missing")).toBeUndefined();
    // Non-matching events stay buffered in their original positions.
    expect(ctx.pendingEvents).toEqual([{ event: "other", data: 2 }, { event: "go", data: 3 }]);
  });

  it("drops the oldest buffered signal once the bound is reached", () => {
    const ctx = state();
    for (let index = 0; index < PENDING_EVENT_LIMIT + 5; index += 1) receiveEventSignal(ctx, { event: `e${index}` });
    expect(ctx.pendingEvents).toHaveLength(PENDING_EVENT_LIMIT);
    expect(ctx.pendingEvents[0]!.event).toBe("e5");
    expect(ctx.pendingEvents.at(-1)!.event).toBe(`e${PENDING_EVENT_LIMIT + 4}`);
  });

  it("ignores malformed payloads and drops signals once the run is terminal", () => {
    const terminal = state({ status: "completed" });
    receiveEventSignal(terminal, { event: "go" });
    expect(terminal.pendingEvents).toEqual([]);
    const running = state();
    receiveEventSignal(running, { event: "" });
    receiveEventSignal(running, { unknown: true });
    expect(running.pendingEvents).toEqual([]);
  });
});
