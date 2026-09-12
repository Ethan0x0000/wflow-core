import { Client } from "@temporalio/client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import { Worker } from "@temporalio/worker";
import { createActivities } from "../src/activities.js";
import { WorkflowEngineClient } from "../src/client.js";
import { parseDefinition } from "../src/definition.js";
import { workflowId } from "../src/protocol.js";
import type { Adapters } from "../src/ports.js";
import type { Snapshot, WorkflowEvent } from "../src/schema.js";
import { SNAPSHOT_QUERY } from "../src/protocol.js";

const definition = parseDefinition({
  schemaVersion: 1,
  id: "leave-demo",
  version: 1,
  name: "Leave request demo",
  inputFields: [{ key: "days", type: "number", required: true }],
  nodes: [
    {
      id: "manager-approval",
      type: "approval",
      name: "Department manager approval",
      assignees: { type: "resolver", name: "department-manager" },
      mode: "all",
      fields: [{ key: "comment", type: "string" }],
    },
    {
      id: "payroll-sync",
      type: "action",
      name: "Sync leave balance",
      action: "leave.sync-balance",
      input: { days: { path: ["days"] } },
      resultKey: "syncResult",
    },
  ],
});

async function main(): Promise<void> {
  const environment = await TestWorkflowEnvironment.createTimeSkipping();
  const events: WorkflowEvent[] = [];
  const adapters: Adapters = {
    definitions: { async get() { return definition; } },
    async authorize(request) {
      console.log(`[auth] ${request.operation} by ${request.actorId}`);
      return true;
    },
    async resolveAssignees(request) {
      console.log(`[assign] ${request.assignment.type}:${request.assignment.type === "resolver" ? request.assignment.name : ""}`);
      return ["manager-001"];
    },
    actions: new Map([
      ["leave.sync-balance", {
        async execute(_context, input) {
          const days = input.days;
          if (days === undefined) throw new Error("days is required");
          console.log(`[action] leave.sync-balance days=${String(days)}`);
          return { synced: true, days };
        },
      }],
    ]),
    async publishEvent(event) {
      events.push(event);
      console.log(`[event ${String(event.sequence).padStart(2, "0")}] ${event.eventType}`);
    },
  };
  const taskQueue = "wflow-core-demo";
  const worker = await Worker.create({
    connection: environment.nativeConnection,
    namespace: environment.namespace,
    taskQueue,
    workflowsPath: require.resolve("../dist/workflows.js"),
    activities: createActivities(adapters),
  });
  const engine = new WorkflowEngineClient({ client: environment.client as Client, taskQueue, adapters });
  const instanceId = "leave-demo-001";

  try {
    const result = await worker.runUntil(async () => {
      console.log("\n=== starting workflow ===");
      const started = await engine.start({
        tenantId: "demo-hospital",
        instanceId,
        businessKey: "leave-demo-001",
        initiatorId: "employee-001",
        definition,
        data: { days: 3 },
      });
      const handle = environment.client.workflow.getHandle(workflowId("demo-hospital", instanceId));
      let snapshot = await handle.query<Snapshot>(SNAPSHOT_QUERY);
      while (snapshot.tasks.length === 0) snapshot = await handle.query<Snapshot>(SNAPSHOT_QUERY);
      const task = snapshot.tasks[0];
      if (!task) throw new Error("The approval task was not created");
      console.log(`\n=== pending task ===\n${JSON.stringify(task, null, 2)}`);
      console.log("\n=== approving task ===");
      await engine.command({
        type: "approve",
        requestId: "demo-approve-001",
        tenantId: "demo-hospital",
        instanceId,
        actorId: "manager-001",
        taskId: task.id,
        data: { comment: "同意休假" },
      });
      return started.result();
    });
    console.log(`\n=== workflow result ===\n${JSON.stringify(result, null, 2)}`);
    console.log(`\nRecorded ${events.length} durable events.`);
  } finally {
    await environment.teardown();
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
