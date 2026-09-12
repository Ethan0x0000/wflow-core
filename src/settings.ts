import type { Settings } from "./schema.js";

/** Matches the proxyActivities defaults that predate configurable tuning. */
export const ACTIVITY_DEFAULTS = Object.freeze({ startToCloseSeconds: 30, maximumAttempts: 5, initialIntervalMs: 1000 });
/** Matches the publishEvent proxy defaults; no maximumAttempts means retry forever. */
export const EVENT_DELIVERY_DEFAULTS = Object.freeze({ startToCloseSeconds: 30, initialIntervalMs: 1000, maximumIntervalMs: 60_000 });

export type ActivityTuning = { startToCloseTimeout: number; retry: { maximumAttempts: number; initialInterval: number } };
export type EventDeliveryTuning = { startToCloseTimeout: number; retry: { initialInterval: number; maximumInterval: number; maximumAttempts?: number } };
export type NodeRetry = { startToCloseMs: number; maximumAttempts: number; initialIntervalMs?: number };

/** Per-call options for generic engine activities derived from the pinned workflow settings. */
export function activityTuning(settings: Settings): ActivityTuning {
  const config = settings.activity ?? {};
  return {
    startToCloseTimeout: (config.startToCloseSeconds ?? ACTIVITY_DEFAULTS.startToCloseSeconds) * 1000,
    retry: {
      maximumAttempts: config.maximumAttempts ?? ACTIVITY_DEFAULTS.maximumAttempts,
      initialInterval: config.initialIntervalMs ?? ACTIVITY_DEFAULTS.initialIntervalMs,
    },
  };
}

/** Action nodes keep their explicit `retry` config precedence over settings, field by field. */
export function actionTuning(settings: Settings, retry?: NodeRetry): ActivityTuning {
  const base = activityTuning(settings);
  if (!retry) return base;
  return {
    startToCloseTimeout: retry.startToCloseMs,
    retry: { maximumAttempts: retry.maximumAttempts, initialInterval: retry.initialIntervalMs ?? base.retry.initialInterval },
  };
}

/** Event delivery keeps the historical blocking default when maximumAttempts is unset. */
export function eventDeliveryTuning(settings: Settings): EventDeliveryTuning {
  const config = settings.eventDelivery ?? {};
  return {
    startToCloseTimeout: (config.startToCloseSeconds ?? EVENT_DELIVERY_DEFAULTS.startToCloseSeconds) * 1000,
    retry: {
      initialInterval: config.initialIntervalMs ?? EVENT_DELIVERY_DEFAULTS.initialIntervalMs,
      maximumInterval: config.maximumIntervalMs ?? EVENT_DELIVERY_DEFAULTS.maximumIntervalMs,
      ...(config.maximumAttempts === undefined ? {} : { maximumAttempts: config.maximumAttempts }),
    },
  };
}
