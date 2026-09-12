import type { Client } from "@temporalio/client";
import { TestWorkflowEnvironment } from "@temporalio/testing";
import type { ConnectionPlugin } from "@temporalio/client";
import type { Worker } from "@temporalio/worker";
import { createWorkflowClient, type WorkflowEngineClient } from "./client.js";
import type { ActionHandler, Adapters, DefinitionStore, IntegrationHandler, NodeHandler, SyncHandler } from "./ports.js";
import type { Definition, WorkflowEvent } from "./schema.js";
import { createWorkflowWorker, resolveWorkflowsPath, type WorkflowWorkerOptions } from "./worker.js";

export interface MemoryAdaptersOptions {
  // A single definition is served for every reference, an array is served by id+version, a store as-is.
  definitions?: DefinitionStore | Definition | readonly Definition[];
  authorize?: Adapters["authorize"];
  resolveAssignees?: Adapters["resolveAssignees"];
  // Resolver names (and node ids) mapped to user ids for the default resolveAssignees implementation.
  users?: Readonly<Record<string, readonly string[]>>;
  actions?: ReadonlyMap<string, ActionHandler> | Iterable<readonly [string, ActionHandler]>;
  // Custom node kinds available to the engine; see Adapters.nodeHandlers.
  nodeHandlers?: ReadonlyMap<string, NodeHandler> | Iterable<readonly [string, NodeHandler]>;
  publishEvent?: Adapters["publishEvent"];
  integrate?: IntegrationHandler;
  syncBusinessData?: SyncHandler;
  onEvent?: (event: WorkflowEvent) => void;
}

export interface MemoryAdapters {
  adapters: Adapters;
  events: WorkflowEvent[];
  definitions: DefinitionStore;
  actions: Map<string, ActionHandler>;
  nodeHandlers: Map<string, NodeHandler>;
}

function isDefinitionStore(value: unknown): value is DefinitionStore {
  return typeof value === "object" && value !== null && typeof (value as DefinitionStore).get === "function";
}

function definitionStore(definitions: MemoryAdaptersOptions["definitions"]): DefinitionStore {
  if (isDefinitionStore(definitions)) return definitions;
  if (Array.isArray(definitions)) {
    const versions = new Map((definitions as readonly Definition[]).map((definition) => [`${definition.id}@${definition.version}`, definition]));
    return { async get(_tenantId, reference) { return versions.get(`${reference.id}@${reference.version}`); } };
  }
  if (definitions) return { async get() { return definitions; } };
  return { async get() { return undefined; } };
}

function resolverUsers(users: Readonly<Record<string, readonly string[]>>): Adapters["resolveAssignees"] {
  return async (request) => {
    if (request.assignment.type === "initiator") return [request.context.initiatorId];
    const name = request.assignment.type === "resolver" ? request.assignment.name : undefined;
    const resolved = (name ? users[name] : undefined) ?? (request.nodeId ? users[request.nodeId] : undefined);
    return resolved ? [...resolved] : [];
  };
}

/** In-memory `Adapters` for tests and demos: allow-by-default authorization, users-map assignment, deduped event log. */
export function createMemoryAdapters(options: MemoryAdaptersOptions = {}): MemoryAdapters {
  const events: WorkflowEvent[] = [];
  const seen = new Set<string>();
  const actions = new Map(options.actions);
  const nodeHandlers = new Map(options.nodeHandlers);
  const definitions = definitionStore(options.definitions);
  const adapters: Adapters = {
    definitions,
    authorize: options.authorize ?? (async () => true),
    resolveAssignees: options.resolveAssignees ?? resolverUsers(options.users ?? {}),
    actions,
    nodeHandlers,
    async publishEvent(event) {
      if (!seen.has(event.eventId)) {
        seen.add(event.eventId);
        events.push(event);
        options.onEvent?.(event);
      }
      if (options.publishEvent) await options.publishEvent(event);
    },
    ...(options.integrate ? { integrate: options.integrate } : {}),
    ...(options.syncBusinessData ? { syncBusinessData: options.syncBusinessData } : {}),
  };
  return { adapters, events, definitions, actions, nodeHandlers };
}

export interface TestEngineOptions {
  // Defaults to a fresh in-memory adapter set when omitted.
  adapters?: Adapters;
  environment?: TestWorkflowEnvironment;
  taskQueue?: string;
  namespace?: string;
  // Time-skipping test server by default; set false for a plain local test server.
  timeSkipping?: boolean;
  // Set false to create the environment and client without starting a worker.
  startWorker?: boolean;
  // Optional extra connection/client/native plugins forwarded to the test environment. Automatic
  // gRPC retry is disabled by default (a plugin strips the retry interceptor list): abandoned retry
  // timers can otherwise fire after teardown and surface as uncaught "Channel has been shut down"
  // errors, failing the suite even when every test passes. Pass a pre-built `environment` to fully
  // customize connection creation.
  plugins?: TestWorkflowEnvironment["options"]["plugins"];
  workflowsPath?: string;
  workerOptions?: Omit<WorkflowWorkerOptions, "adapters" | "connection" | "namespace" | "taskQueue" | "workflowsPath">;
}

export interface TestEngine {
  environment: TestWorkflowEnvironment;
  engine: WorkflowEngineClient;
  worker?: Worker;
  taskQueue: string;
  run<T>(callback: () => Promise<T>): Promise<T>;
  close(): Promise<void>;
}

/** Starts a Temporal test environment, workflow worker and engine client wired to the same adapters. */
export async function createTestEngine(options: TestEngineOptions = {}): Promise<TestEngine> {
  const ownsEnvironment = options.environment === undefined;
  const noGrpcRetry: ConnectionPlugin = {
    name: "wflow-core:no-grpc-retry",
    configureConnection(connectionOptions) { return { ...connectionOptions, interceptors: [] }; },
  };
  const testPlugins = options.plugins ?? [];
  const environment = options.environment ?? (options.timeSkipping === false
    ? await TestWorkflowEnvironment.createLocal({ plugins: [noGrpcRetry, ...testPlugins] })
    : await TestWorkflowEnvironment.createTimeSkipping({ plugins: [noGrpcRetry, ...testPlugins] }));
  const adapters = options.adapters ?? createMemoryAdapters().adapters;
  const taskQueue = options.taskQueue ?? "wflow-core-test";
  const engine = createWorkflowClient({ client: environment.client as Client, taskQueue, adapters });
  const worker = options.startWorker === false ? undefined : await createWorkflowWorker({
    maxCachedWorkflows: 0,
    ...options.workerOptions,
    connection: environment.nativeConnection,
    namespace: options.namespace ?? environment.namespace,
    taskQueue,
    adapters,
    workflowsPath: options.workflowsPath ?? resolveWorkflowsPath(),
  });
  return {
    environment,
    engine,
    ...(worker ? { worker } : {}),
    taskQueue,
    run: (callback) => worker ? worker.runUntil(callback) : callback(),
    async close() {
      if (worker?.getState() === "RUNNING") worker.shutdown();
      if (ownsEnvironment) await environment.teardown();
    },
  };
}
