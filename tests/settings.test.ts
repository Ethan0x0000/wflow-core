import { describe, expect, it } from "vitest";
import { activityTuning, actionTuning, eventDeliveryTuning } from "../src/settings.js";

describe("activity and event delivery tuning", () => {
  it("uses today's defaults when settings are absent", () => {
    expect(activityTuning({})).toEqual({ startToCloseTimeout: 30_000, retry: { maximumAttempts: 5, initialInterval: 1000 } });
    expect(eventDeliveryTuning({})).toEqual({ startToCloseTimeout: 30_000, retry: { initialInterval: 1000, maximumInterval: 60_000 } });
    expect(activityTuning({ activity: {} })).toEqual(activityTuning({}));
    expect(eventDeliveryTuning({ eventDelivery: {} })).toEqual(eventDeliveryTuning({}));
  });

  it("maps settings overrides onto per-call options", () => {
    expect(activityTuning({ activity: { startToCloseSeconds: 5, maximumAttempts: 2, initialIntervalMs: 250 } }))
      .toEqual({ startToCloseTimeout: 5000, retry: { maximumAttempts: 2, initialInterval: 250 } });
    expect(eventDeliveryTuning({ eventDelivery: { startToCloseSeconds: 10, maximumAttempts: 3, initialIntervalMs: 50, maximumIntervalMs: 5000 } }))
      .toEqual({ startToCloseTimeout: 10_000, retry: { initialInterval: 50, maximumInterval: 5000, maximumAttempts: 3 } });
  });

  it("keeps an absent event delivery maximumAttempts retrying forever", () => {
    expect(eventDeliveryTuning({}).retry.maximumAttempts).toBeUndefined();
    expect(eventDeliveryTuning({ eventDelivery: { initialIntervalMs: 50 } }).retry.maximumAttempts).toBeUndefined();
  });

  it("keeps node retry precedence over activity settings for actions", () => {
    const settings = { activity: { startToCloseSeconds: 5, maximumAttempts: 2, initialIntervalMs: 250 } };
    expect(actionTuning(settings, { startToCloseMs: 9000, maximumAttempts: 7 }))
      .toEqual({ startToCloseTimeout: 9000, retry: { maximumAttempts: 7, initialInterval: 250 } });
    expect(actionTuning(settings, { startToCloseMs: 9000, maximumAttempts: 7, initialIntervalMs: 25 }))
      .toEqual({ startToCloseTimeout: 9000, retry: { maximumAttempts: 7, initialInterval: 25 } });
    expect(actionTuning(settings)).toEqual(activityTuning(settings));
  });
});
