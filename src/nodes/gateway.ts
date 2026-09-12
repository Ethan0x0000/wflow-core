import { CancellationScope } from "@temporalio/workflow";
import { assertJsonSize, canonical, evaluate, resolveValue } from "../definition.js";
import { WorkflowValidationError } from "../errors.js";
import { jsonSchema, type Data, type Json, type Node } from "../schema.js";
import { put, type WorkflowContext } from "../workflow-context.js";

async function runParallel(ctx: WorkflowContext, branches: Node[][], variables: Data): Promise<void> {
  await runConcurrent(ctx, branches.map((branch) => (local: Data) => ctx.runNodes(branch, local)), variables);
}

// Siblings read the same snapshot; validated disjoint writes merge at the join. `localKeys` keeps
// per-iteration bindings (forEach item/index) out of the parent variables.
async function runConcurrent(ctx: WorkflowContext, runs: ((local: Data) => Promise<void>)[], variables: Data, localKeys?: ReadonlySet<string>): Promise<void> {
  const branchScope = new CancellationScope();
  await branchScope.run(async () => {
    const baseline = jsonSchema.parse(variables) as Data;
    const jobs = runs.map(async (run) => {
      const local = jsonSchema.parse(baseline) as Data;
      await run(local);
      return local;
    });
    let results: Data[];
    try { results = await Promise.all(jobs); }
    catch (error) {
      branchScope.cancel();
      await Promise.allSettled(jobs);
      throw error;
    }
    const merged = { ...variables };
    for (const result of results) for (const [key, value] of Object.entries(result)) {
      if (localKeys?.has(key)) continue;
      if (canonical(baseline[key]) !== canonical(value)) merged[key] = value;
    }
    assertJsonSize(merged);
    Object.assign(variables, merged);
  });
}

type GatewayNode = Extract<Node, { type: "exclusive" | "inclusive" }>;
type LoopNode = Extract<Node, { type: "loop" }>;
type ForEachNode = Extract<Node, { type: "forEach" }>;

async function runExclusive(ctx: WorkflowContext, node: GatewayNode, variables: Data): Promise<void> {
  const branch = node.branches.find((candidate) => evaluate(candidate.when, variables, ctx.initiator));
  await ctx.runNodes(branch?.nodes ?? node.otherwise, variables);
}

async function runInclusive(ctx: WorkflowContext, node: GatewayNode, variables: Data): Promise<void> {
  const branches = node.branches.filter((branch) => evaluate(branch.when, variables, ctx.initiator)).map((branch) => branch.nodes);
  if (branches.length) await runParallel(ctx, branches, variables);
  else await ctx.runNodes(node.otherwise, variables);
}

async function runLoop(ctx: WorkflowContext, node: LoopNode, variables: Data): Promise<void> {
  let iteration = 0;
  while (evaluate(node.while, variables, ctx.initiator)) {
    if (++iteration > node.maxIterations) throw new WorkflowValidationError("LOOP_LIMIT_REACHED");
    await ctx.runNodes(node.nodes, variables);
  }
}

/**
 * forEach over a resolved JSON array. The collection must exist and be an array; exceeding
 * maxIterations is an explicit failure rather than a silent truncation. Sequential iterations
 * share the parent variables (itemKey/indexKey keep the last value); parallel iterations run in
 * chunks of `concurrency` against local copies and never merge itemKey/indexKey back.
 */
async function runForEach(ctx: WorkflowContext, node: ForEachNode, variables: Data): Promise<void> {
  const items = resolveValue(node.items, variables);
  if (!Array.isArray(items)) throw new WorkflowValidationError("FOREACH_REQUIRES_ARRAY");
  if (items.length > node.maxIterations) throw new WorkflowValidationError("FOREACH_LIMIT_REACHED");
  if (node.parallel) {
    const concurrency = node.concurrency ?? 4;
    const localKeys = new Set(node.indexKey === undefined ? [node.itemKey] : [node.itemKey, node.indexKey]);
    for (let offset = 0; offset < items.length; offset += concurrency) {
      const chunk = items.slice(offset, offset + concurrency);
      await runConcurrent(ctx, chunk.map((item: Json, indexInChunk: number) => async (local: Data) => {
        put(local, node.itemKey, item);
        if (node.indexKey !== undefined) put(local, node.indexKey, offset + indexInChunk);
        await ctx.runNodes(node.nodes, local);
      }), variables, localKeys);
    }
    return;
  }
  for (let index = 0; index < items.length; index++) {
    put(variables, node.itemKey, items[index]!);
    if (node.indexKey !== undefined) put(variables, node.indexKey, index);
    await ctx.runNodes(node.nodes, variables);
    if (node.completionCondition && evaluate(node.completionCondition, variables, ctx.initiator)) break;
  }
}

export { runExclusive, runForEach, runInclusive, runLoop, runParallel };
