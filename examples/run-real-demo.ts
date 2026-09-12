import { Client, Connection } from "@temporalio/client";
import { NativeConnection, Worker } from "@temporalio/worker";
import { createActivities } from "../src/activities.js";
import { WorkflowEngineClient } from "../src/client.js";
import { parseDefinition } from "../src/definition.js";
import { workflowId } from "../src/protocol.js";
import type { Adapters } from "../src/ports.js";
import type { Snapshot, WorkflowEvent } from "../src/schema.js";
import { SNAPSHOT_QUERY } from "../src/protocol.js";

const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7233";
const namespace = process.env.TEMPORAL_NAMESPACE ?? "default";
const taskQueue = "wflow-core-real-demo";
const instanceId = `leave-real-${Date.now()}`;
const definition = parseDefinition({
  schemaVersion: 1,
  id: "leave-real-demo",
  version: 1,
  name: "Real Temporal leave approval",
  inputFields: [{ key: "days", type: "number", required: true }],
  nodes: [
    {
      id: "manager-approval",
      type: "approval",
      name: "Department manager approval",
      assignees: { type: "users", userIds: ["manager-001"] },
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
  const connection = await Connection.connect({ address });
  const nativeConnection = await NativeConnection.connect({ address });
  const events: WorkflowEvent[] = [];
  const adapters: Adapters = {
    definitions: { async get() { return definition; } },
    async authorize(request) {
      console.log(`[auth] ${request.operation} by ${request.actorId}`);
      return true;
    },
    async resolveAssignees() { return ["manager-001"]; },
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
  const worker = await Worker.create({
    connection: nativeConnection,
    namespace,
    taskQueue,
    workflowsPath: require.resolve("../dist/workflows.js"),
    activities: createActivities(adapters),
  });
  const temporal = new Client({ connection, namespace });
  const engine = new WorkflowEngineClient({ client: temporal, taskQueue, adapters });
  try {
    const result = await worker.runUntil(async () => {
      console.log(`\n=== connecting to Temporal ${address} ===`);
      const started = await engine.start({
        tenantId: "demo-hospital",
        instanceId,
        businessKey: instanceId,
        initiatorId: "employee-001",
        definition,
        data: { days: 3 },
      });
      const handle = temporal.workflow.getHandle(workflowId("demo-hospital", instanceId));
      let snapshot = await handle.query<Snapshot>(SNAPSHOT_QUERY);
      while (snapshot.tasks.length === 0) snapshot = await handle.query<Snapshot>(SNAPSHOT_QUERY);
      const task = snapshot.tasks[0];
      if (!task) throw new Error("The approval task was not created");
      console.log(`\n=== pending task ${task.id} ===`);
      console.log(JSON.stringify(task, null, 2));
      await engine.command({
        type: "approve",
        requestId: `${instanceId}-approve`,
        tenantId: "demo-hospital",
        instanceId,
        actorId: "manager-001",
        taskId: task.id,
        data: { comment: "同意休假" },
      });
      return started.result();
    });
    console.log(`\n=== workflow result ===\n${JSON.stringify(result, null, 2)}`);
    console.log(`Recorded ${events.length} durable events.`);
    console.log(`Workflow ID: ${workflowId("demo-hospital", instanceId)}`);
  } finally {
    await nativeConnection.close();
    await connection.close();
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
