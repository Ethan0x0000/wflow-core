import { WorkflowValidationError } from "./errors.js";
import { SCRIPT_LIMITS, instrumentScript, translateSpel, validateScript, type ScriptMode } from "./script.js";
import { definitionSchema, jsonSchema, type Condition, type Data, type Definition, type Field, type InitiatorContext, type Json, type Listener, type Node, type Value } from "./schema.js";

export { translateSpel, WorkflowValidationError };

// Bound untrusted JSON before walking recursive schemas or sending Temporal payloads.
export function assertJsonSize(value: unknown): void {
  let entries = 0;
  let bytes = 0;
  const ancestors = new Set<object>();
  const visit = (item: unknown, depth: number): void => {
    if (++entries > 30_000 || depth > 48) throw new WorkflowValidationError("INPUT_TOO_COMPLEX");
    if (typeof item === "string") bytes += item.length * 3;
    else if (item !== null && typeof item === "object") {
      if (ancestors.has(item)) throw new WorkflowValidationError("CYCLIC_INPUT");
      if (!Array.isArray(item) && Object.getPrototypeOf(item) !== Object.prototype && Object.getPrototypeOf(item) !== null) {
        throw new WorkflowValidationError("NON_JSON_INPUT");
      }
      ancestors.add(item);
      for (const [key, child] of Object.entries(item)) { bytes += key.length * 3; visit(child, depth + 1); }
      ancestors.delete(item);
    } else if (item === undefined || typeof item === "function" || typeof item === "symbol" || typeof item === "bigint") {
      throw new WorkflowValidationError("NON_JSON_INPUT");
    }
    if (bytes > 512_000) throw new WorkflowValidationError("INPUT_TOO_LARGE");
  };
  visit(value, 0);
}

export function parseDefinition(raw: unknown): Definition {
  assertJsonSize(raw);
  const definition = definitionSchema.parse(raw);
  const ids = new Set<string>();
  const walk = (nodes: Node[]): Set<string> => {
    const writes = new Set<string>();
    for (const node of nodes) {
      if (ids.has(node.id)) throw new WorkflowValidationError("DUPLICATE_NODE_ID");
      ids.add(node.id);
      if (ids.size > 500) throw new WorkflowValidationError("TOO_MANY_NODES");
      validateNodeScripts(node);
      if ("resultKey" in node && node.resultKey) writes.add(node.resultKey);
      if (node.type === "approval" || node.type === "task") {
        assertUniqueFields(node.fields ?? []);
        for (const field of node.fields ?? []) writes.add(field.key);
      }
      let branches: Set<string>[] = [];
      if (node.type === "parallel") branches = node.branches.map(walk);
      else if (node.type === "inclusive" || node.type === "exclusive") {
        branches = node.branches.map((branch) => walk(branch.nodes));
        for (const key of walk(node.otherwise)) writes.add(key);
      } else if (node.type === "loop" || node.type === "forEach") branches = [walk(node.nodes)];
      else if (node.type === "eventGateway") branches = node.branches.map((branch) => {
        const branchWrites = walk(branch.nodes);
        if (branch.resultKey) branchWrites.add(branch.resultKey);
        return branchWrites;
      });
      // forEach iteration keys become parent variables too (sequential keeps the last value;
      // parallel locals are never merged back, but a later node may still read the key).
      if (node.type === "forEach") { writes.add(node.itemKey); if (node.indexKey) writes.add(node.indexKey); }
      const branchWrites = new Set<string>();
      for (const branch of branches) for (const key of branch) {
        if ((node.type === "parallel" || node.type === "inclusive") && branchWrites.has(key)) {
          throw new WorkflowValidationError("CONCURRENT_VARIABLE_WRITE");
        }
        branchWrites.add(key); writes.add(key);
      }
    }
    return writes;
  };
  assertUniqueFields(definition.inputFields ?? []);
  validateListeners(definition.events);
  walk(definition.nodes);
  if (definition.resubmit) {
    if (definition.resubmit.type !== 'task') throw new WorkflowValidationError('RESUBMIT_REQUIRES_TASK');
    walk([definition.resubmit]);
  }
  return definition;
}

function assertUniqueFields(fields: Field[]): void {
  if (new Set(fields.map((field) => field.key)).size !== fields.length) throw new WorkflowValidationError("DUPLICATE_FIELD");
}

function validateListeners(events: Record<string, Listener[]> | undefined): void {
  for (const listeners of Object.values(events ?? {})) for (const listener of listeners) {
    if (listener.type !== "EL" && listener.type !== "JS") continue;
    const script = listener.action ?? (typeof listener.config?.script === "string" ? listener.config.script : undefined);
    if (script !== undefined) validateScript(listener.type, script);
  }
}

function validateCondition(condition: Condition): void {
  if (condition.op === "eval") { validateScript(condition.lang === "el" ? "EL" : "JS", condition.script); return; }
  if (condition.op === "and" || condition.op === "or") { for (const child of condition.conditions) validateCondition(child); return; }
  if (condition.op === "not") validateCondition(condition.condition);
}

// Every script embedded in a definition is rejected up front, not only at execution time.
function validateNodeScripts(node: Node): void {
  if ("events" in node) validateListeners(node.events);
  if (node.type === "trigger" && (node.action === "EL" || node.action === "JS") && node.script !== undefined) validateScript(node.action, node.script);
  else if (node.type === "exclusive" || node.type === "inclusive") { for (const branch of node.branches) validateCondition(branch.when); }
  else if (node.type === "loop") validateCondition(node.while);
  else if (node.type === "forEach" && node.completionCondition) validateCondition(node.completionCondition);
  else if (node.type === "router" && node.when) validateCondition(node.when);
}

export function validateFields(fields: Field[], data: Data, rejectUnknown: boolean): void {
  if (rejectUnknown && Object.keys(data).some((key) => !fields.some((field) => field.key === key))) {
    throw new WorkflowValidationError("FIELD_NOT_WRITABLE");
  }
  for (const field of fields) {
    const value = data[field.key];
    if (value === undefined || value === null || value === '' || (Array.isArray(value) && value.length === 0)) {
      if (field.required) throw new WorkflowValidationError("REQUIRED_FIELD");
      if (value === undefined || value === null) continue;
    }
    const type = Array.isArray(value) ? "array" : value === null ? "null" : typeof value;
    if (type !== field.type) throw new WorkflowValidationError("INVALID_FIELD_TYPE");
    if ((typeof value === 'string' || Array.isArray(value)) && ((field.minLength !== undefined && value.length < field.minLength) || (field.maxLength !== undefined && value.length > field.maxLength))) throw new WorkflowValidationError('INVALID_FIELD_LENGTH');
    if (typeof value === 'number' && ((field.minimum !== undefined && value < field.minimum) || (field.maximum !== undefined && value > field.maximum))) throw new WorkflowValidationError('INVALID_FIELD_RANGE');
    if (field.pattern !== undefined && typeof value === 'string' && !new RegExp(field.pattern).test(value)) throw new WorkflowValidationError('INVALID_FIELD_PATTERN');
  }
}

export function resolveValue(value: Value, data: Data): Json | undefined {
  if ("value" in value) return value.value;
  let current: Json | undefined = data;
  for (const key of value.path) {
    if (current === null || typeof current !== "object" || !Object.prototype.hasOwnProperty.call(current, key)) return undefined;
    current = (current as Record<string, Json>)[key];
  }
  return current;
}

export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") return `{${Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, child]) => `${JSON.stringify(key)}:${canonical(child)}`).join(",")}}`;
  return JSON.stringify(value) ?? "undefined";
}

const COMPILED_SCRIPT_CACHE_LIMIT = 200;
const compiledScripts = new Map<string, (data: Data) => unknown>();

// Pure memoization: the same (lang, mode, script) always yields the same compiled function, so
// replayed workflow tasks and repeated conditions skip recompilation. FIFO eviction bounds memory
// and evicted entries recompile to equivalent functions.
function compileCached(lang: "EL" | "JS", script: string, mode: ScriptMode): (data: Data) => unknown {
  const key = `${mode}\u0000${lang}\u0000${script}`;
  const cached = compiledScripts.get(key);
  if (cached) return cached;
  validateScript(lang, script);
  const body = instrumentScript(lang, script, mode);
  const raw = new Function("ctx", "__wfBudgetFn", `with(ctx){ const __wfBudget = __wfBudgetFn; ${mode === "statements" ? body : `return ${body};`} }`) as (data: Data, budget: () => void) => unknown;
  const compiled = (data: Data): unknown => {
    let used = 0;
    // The guard callback is recreated per call so each evaluation gets the full iteration budget.
    const budget = (): void => { if (++used > SCRIPT_LIMITS.maxIterations) throw new WorkflowValidationError("SCRIPT_BUDGET_EXCEEDED"); };
    return raw(data, budget);
  };
  if (compiledScripts.size >= COMPILED_SCRIPT_CACHE_LIMIT) {
    const oldest = compiledScripts.keys().next().value;
    if (oldest !== undefined) compiledScripts.delete(oldest);
  }
  compiledScripts.set(key, compiled);
  return compiled;
}

/** Compiles an EL/JS snippet, throwing WorkflowValidationError when it does not parse. */
export function compileScript(lang: "EL" | "JS", script: string): (data: Data) => unknown {
  return compileCached(lang, script, lang === "EL" ? "expression" : "statements");
}
/**
 * Executes the EL/JS snippets attached to trigger nodes, node events and process events.
 * JS snippets follow the Java reference convention of a `handler(ctx)` body; EL snippets
 * are SpEL expressions evaluated against the same context.
 */
export function executeScript(lang: "EL" | "JS", script: string, data: Data): Json {
  try {
    const result = compileScript(lang, script)(data);
    return result === undefined ? null : jsonSchema.parse(result);
  } catch (error) {
    if (error instanceof WorkflowValidationError) throw error;
    throw new WorkflowValidationError("SCRIPT_EXECUTION_FAILED");
  }
}

function extractRawValue(v: unknown): unknown {
  if (v !== null && typeof v === "object") {
    if ("value" in v) return (v as Record<string, unknown>).value;
    if ("id" in v) return (v as Record<string, unknown>).id;
  }
  return v;
}

export function evaluate(condition: Condition, data: Data, initiator?: InitiatorContext): boolean {
  switch (condition.op) {
    case "and": return condition.conditions.every((child) => evaluate(child, data, initiator));
    case "or": return condition.conditions.some((child) => evaluate(child, data, initiator));
    case "not": return !evaluate(condition.condition, data, initiator);
    case "exists": return resolveValue(condition.value, data) !== undefined;
    case "empty": { const value = resolveValue(condition.value, data); return value == null || value === '' || (Array.isArray(value) && value.length === 0); }
    case "between": {
      const val = Number(resolveValue(condition.value, data));
      const min = Number(resolveValue(condition.min, data));
      const max = Number(resolveValue(condition.max, data));
      if (!Number.isFinite(val) || !Number.isFinite(min) || !Number.isFinite(max)) return false;
      return val > min && val < max;
    }
    case "before":
    case "after": {
      const left = resolveValue(condition.left, data);
      const right = resolveValue(condition.right, data);
      if (left == null || right == null) return false;
      const toTimestamp = (v: unknown): number => {
        if (typeof v === "number") return v;
        const str = String(v);
        const withDate = /^\d{2}:\d{2}(:\d{2})?$/.test(str) ? `1970-01-01T${str.length === 5 ? str + ":00" : str}Z` : str;
        return Date.parse(withDate);
      };
      const tLeft = toTimestamp(left), tRight = toTimestamp(right);
      if (!Number.isFinite(tLeft) || !Number.isFinite(tRight)) return false;
      return condition.op === "before" ? tLeft < tRight : tLeft > tRight;
    }
    case "timeBetween": {
      const val = resolveValue(condition.value, data);
      const start = resolveValue(condition.start, data);
      const end = resolveValue(condition.end, data);
      if (val == null || start == null || end == null) return false;
      const toTimestamp = (v: unknown): number => {
        if (typeof v === "number") return v;
        const str = String(v);
        const withDate = /^\d{2}:\d{2}(:\d{2})?$/.test(str) ? `1970-01-01T${str.length === 5 ? str + ":00" : str}Z` : str;
        return Date.parse(withDate);
      };
      const tVal = toTimestamp(val), tStart = toTimestamp(start), tEnd = toTimestamp(end);
      if (!Number.isFinite(tVal) || !Number.isFinite(tStart) || !Number.isFinite(tEnd)) return false;
      // Java compareBetween uses isAfter/isBefore (exclusive bounds).
      return tVal > tStart && tVal < tEnd;
    }
    case "initiator": {
      // First-class context wins; legacy `_initiator*` data keys keep older hosts working.
      const initiatorId = String(initiator?.initiatorId ?? data._initiatorId ?? data.initiatorId ?? data.starter ?? "");
      const deptId = String(initiator?.deptId ?? data._initiatorDeptId ?? data.startDeptId ?? data.deptId ?? "");
      const deptLevels = Array.isArray(initiator?.deptLevels) ? initiator.deptLevels.map(String)
        : Array.isArray(data._initiatorDeptLevels) ? data._initiatorDeptLevels.map(String) : [deptId];
      const roles = Array.isArray(initiator?.roles) ? initiator.roles.map(String)
        : Array.isArray(data._initiatorRoles) ? data._initiatorRoles.map((r) => typeof r === "object" && r !== null && "id" in r ? String((r as Record<string, unknown>).id) : String(r)) : [];
      const targetIds = condition.values.map((v) => typeof v === "object" && v !== null && "id" in v ? String((v as Record<string, unknown>).id) : String(v));
      if (condition.dimension === "user") {
        const has = targetIds.includes(initiatorId);
        return condition.compare === "in" ? has : !has;
      }
      if (condition.dimension === "dept") {
        const has = deptLevels.some((d) => targetIds.includes(d));
        return condition.compare === "in" ? has : !has;
      }
      if (condition.dimension === "role") {
        const has = roles.some((r) => targetIds.includes(r));
        return condition.compare === "has" ? has : !has;
      }
      return false;
    }
    case "eval": {
      try {
        // Conditions are expressions in both languages, so they share the compiled-script cache
        // and EL translation path with `compileScript` instead of compiling the raw SpEL here.
        return Boolean(compileCached(condition.lang === "el" ? "EL" : "JS", condition.script, "expression")(data));
      } catch (error) {
        // Budget exhaustion is a deterministic failure, not a false condition; keep parsing and
        // execution errors on the historical `false` contract.
        if (error instanceof WorkflowValidationError && error.code === "SCRIPT_BUDGET_EXCEEDED") throw error;
        return false;
      }
    }
    default: {
      if (!("left" in condition)) return false;
      const rawLeft = resolveValue(condition.left, data);
      const rawRight = resolveValue(condition.right, data);
      if (rawLeft === undefined || rawRight === undefined) return false;
      const left = extractRawValue(rawLeft);
      const right = extractRawValue(rawRight);
      switch (condition.op) {
        case "eq": return canonical(left) === canonical(right);
        case "ne": return canonical(left) !== canonical(right);
        case "in": return Array.isArray(right) && right.some((item) => canonical(extractRawValue(item)) === canonical(left));
        case "contains": {
          // Mirrors the Java FieldValueType.HAS semantics: strings contain, arrays contain every requested value.
          if (typeof left === "string") {
            if (typeof right === "string") return left.includes(right);
            if (Array.isArray(right)) return right.every((item) => typeof extractRawValue(item) === "string" && left.includes(String(extractRawValue(item))));
            return false;
          }
          if (Array.isArray(left)) {
            if (Array.isArray(right)) return right.every((item) => left.some((entry) => canonical(extractRawValue(entry)) === canonical(extractRawValue(item))));
            return left.some((item) => canonical(extractRawValue(item)) === canonical(right));
          }
          return false;
        }
        default: {
          let numLeft = typeof left === "number" ? left : Number(left);
          if (Array.isArray(left) && left.length === 2 && typeof left[0] === 'string' && typeof left[1] === 'string') {
            const start = Date.parse(left[0]), end = Date.parse(left[1]);
            // Java measures dateTimeRange in days and timeRange in hours.
            if (Number.isFinite(start) && Number.isFinite(end)) numLeft = /^\d{2}:\d{2}/.test(left[0]) ? Math.round((end - start) / 3_600_000) : Math.round((end - start) / 86_400_000);
          }
          const numRight = typeof right === "number" ? right : Number(right);
          if (!Number.isFinite(numLeft) || !Number.isFinite(numRight)) throw new WorkflowValidationError("ORDER_REQUIRES_NUMBERS");
          if (condition.op === "gt") return numLeft > numRight;
          if (condition.op === "gte") return numLeft >= numRight;
          if (condition.op === "lt") return numLeft < numRight;
          return numLeft <= numRight;
        }
      }
    }
  }
}

export function mapValues(mapping: Record<string, Value>, data: Data): Data {
  return Object.fromEntries(Object.entries(mapping).map(([key, value]) => {
    const resolved = resolveValue(value, data);
    if (resolved === undefined) throw new WorkflowValidationError("MISSING_VARIABLE");
    return [key, jsonSchema.parse(resolved)];
  }));
}
