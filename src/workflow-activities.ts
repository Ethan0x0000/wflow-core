import { proxyActivities } from "@temporalio/workflow";
import type { EngineActivities } from "./ports.js";

const activities = proxyActivities<EngineActivities>({
  startToCloseTimeout: "30 seconds",
  retry: { maximumAttempts: 5, initialInterval: "1 second" },
});
// A failed projection must block progression until delivery succeeds or operations intervenes.
const events = proxyActivities<Pick<EngineActivities, "publishEvent">>({
  startToCloseTimeout: "30 seconds", retry: { initialInterval: "1 second", maximumInterval: "1 minute" },
});

export { activities, events };
