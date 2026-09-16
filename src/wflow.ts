import { assertJsonSize, parseDefinition, WorkflowValidationError } from "./definition.js";
import { dataSchema, fieldSchema, jsonSchema, type Assignment, type Condition, type Definition, type Field, type Json, type Listener, type Node, type Value } from "./schema.js";

export function wflowRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new WorkflowValidationError("INVALID_WFLOW_OBJECT");
  return value as Record<string, unknown>;
}
const obj = (value: unknown) => value == null ? {} : wflowRecord(value);
const arr = (value: unknown): unknown[] => Array.isArray(value) ? value : [];
function str(value: unknown): string {
  if (typeof value !== "string" || !value) throw new WorkflowValidationError("INVALID_WFLOW_STRING");
  return value;
}
/** Host entity ids (users, depts, roles) are Long in the original wflow data, so the editor hands
 *  them over as JSON numbers; the SDK keeps ids as strings, so coerce instead of rejecting. */
function idStr(value: unknown): string {
  if (typeof value === "string" && value) return value;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  throw new WorkflowValidationError("INVALID_WFLOW_STRING");
}
function unsupported(feature: string): never { throw new WorkflowValidationError(`UNSUPPORTED_WFLOW_${feature}`); }
const units: Record<string, number> = { S: 1000, M: 60_000, H: 3_600_000, D: 86_400_000 };
function duration(time: unknown, unit: unknown): number {
  const factor = units[str(unit)];
  if (!factor || typeof time !== "number" || time <= 0) throw new WorkflowValidationError("INVALID_WFLOW_DURATION");
  return time * factor;
}
function javaDateTimeToIso(value: string): string {
  if (!/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(value)) throw new WorkflowValidationError("INVALID_WFLOW_DATE_TIME");
  return `${value.replace(" ", "T")}+08:00`;
}

/** Maps Java ProcEventSetting/NodeEventSetting event entries (type/el/js/http) to SDK listeners. */
function importWflowListenerItem(raw: unknown): Listener {
  const item = obj(raw);
  const type = String(item.type ?? "HTTP").toUpperCase();
  if (type === "EL") return { type: "EL", action: String(item.el ?? "") };
  if (type === "JS") return { type: "JS", action: String(item.js ?? "") };
  if (type === "HTTP") return { type: "HTTP", config: dataSchema.parse(item.http ?? {}) };
  throw new WorkflowValidationError("UNSUPPORTED_WFLOW_EVENT_TYPE");
}

function importWflowListenerLists(raw: unknown): Record<string, Listener[]> {
  const result: Record<string, Listener[]> = {};
  for (const [name, list] of Object.entries(obj(raw))) {
    if (Array.isArray(list) && list.length) result[name] = list.map(importWflowListenerItem);
  }
  return result;
}

export function importWflowProcessEvents(raw: unknown): Record<string, Listener[]> | undefined {
  const events = importWflowListenerLists(raw);
  return Object.keys(events).length ? events : undefined;
}

export function importWflowAssignment(props: Record<string, unknown>): Assignment {
  const rule = props.ruleType ?? "ASSIGN_USER";
  if (rule === "ASSIGN_USER") {
    const ids = arr(props.assignUser).map((user) => idStr(typeof user === "string" ? user : obj(user).id));
    if (ids.length) return { type: "users", userIds: [...new Set(ids)] };
  }
  if (rule === "SELF" || rule === "INITIATOR" || rule === 'ROOT_SELF') return { type: "initiator" };
  return { type: "resolver", name: `wflow:${str(rule)}`, params: dataSchema.parse(props) };
}

export function importWflowCondition(raw: unknown): Condition {
  const props = obj(raw);
  const groups = arr(props.groups).map((rawGroup): Condition => {
    const group = obj(rawGroup);
    const conditions = arr(group.conditions).map((rawItem): Condition => {
      const item = obj(rawItem);
      if (item.group === "INITIATOR") {
        const dimension = item.valueType === "dept" ? "dept" : item.valueType === "role" ? "role" : "user";
        const compare = String(item.compare ?? "IN").toUpperCase() === "HAS" ? "has" : "in";
        const values = arr(item.compareVal).map((v) => jsonSchema.parse(v));
        return { op: "initiator", dimension, compare, values };
      }
      if (item.group === "DEV") {
        const type = String(item.type ?? "JS").toUpperCase();
        const script = String(arr(item.compareVal)[0] ?? "");
        return { op: "eval", lang: type === "EL" ? "el" : "js", script };
      }
      const left: Value = { path: str(item.symbol).replace(/^FORM\./, "").split(".") };
      const values = arr(item.compareVal).map((v) => jsonSchema.parse(v));
      const compare = str(item.compare);
      if (compare === "EM") return { op: 'empty', value: left };
      if (compare === "NEM") return { op: 'not', condition: { op: 'empty', value: left } };
      if (compare === 'BT') {
        if (values.length !== 2) throw new WorkflowValidationError('INVALID_WFLOW_RANGE');
        return { op: 'between', value: left, min: { value: values[0]! }, max: { value: values[1]! } };
      }
      if (compare === 'BF') return { op: 'before', left, right: { value: values[0]! } };
      if (compare === 'AF') return { op: 'after', left, right: { value: values[0]! } };
      if (compare === 'CT') {
        if (values.length !== 2) throw new WorkflowValidationError('INVALID_WFLOW_RANGE');
        return { op: 'timeBetween', value: left, start: { value: values[0]! }, end: { value: values[1]! } };
      }
      if (compare === 'NCT') {
        if (values.length !== 2) throw new WorkflowValidationError('INVALID_WFLOW_RANGE');
        return { op: 'not', condition: { op: 'timeBetween', value: left, start: { value: values[0]! }, end: { value: values[1]! } } };
      }
      const ops = { EQ: "eq", NEQ: "ne", GT: "gt", GT_EQ: "gte", LT: "lt", LT_EQ: "lte", IN: "in", NIN: "in", HAS: "contains", NHAS: "contains" } as const;
      if (!(compare in ops)) unsupported("COMPARE");
      const op = ops[compare as keyof typeof ops];
      const comparison: Condition = { op, left, right: { value: op === "in" ? values : values[0] ?? null } };
      return compare === "NIN" || compare === "NHAS" ? { op: "not", condition: comparison } : comparison;
    });
    if (!conditions.length) throw new WorkflowValidationError("EMPTY_WFLOW_CONDITION");
    return { op: group.logic === false ? "or" : "and", conditions };
  });
  if (!groups.length) throw new WorkflowValidationError("EMPTY_WFLOW_CONDITION");
  return { op: props.logic === false ? "or" : "and", conditions: groups };
}

/** The editor persists ordered node arrays, with branch metadata separate from branch bodies. */
export function importWflowDefinition(raw: unknown): Definition {
  assertJsonSize(raw);
  const source = obj(raw);
  const fields: Field[] = arr(source.fields).map((field) => fieldSchema.parse(field));
  const ids = new Set<string>();
  const links: string[] = [];
  const edges = new Map<string, string>();
  function identify(node: Record<string, unknown>): string {
    const id = str(node.id);
    if (ids.has(id)) throw new WorkflowValidationError("DUPLICATE_NODE_ID");
    ids.add(id);
    if (node.childId) { links.push(str(node.childId)); edges.set(id, str(node.childId)); }
    return id;
  }
  function convert(items: unknown[]): Node[] {
    return items.flatMap((rawNode): Node[] => {
      const node = obj(rawNode), props = obj(node.props);
      const base = { id: identify(node), ...(node.name ? { name: str(node.name) } : {}) };
      if (props.enableMountForm) unsupported("MOUNT_FORM");
      if (node.type === "Start" || node.type === "Join") return [];
      if (node.type === "Approval" || node.type === "Task") {
        if (props.mode === 'AUTO_PASS' || props.mode === 'AUTO_REFUSE') return [{ ...base, type: 'terminate', outcome: props.mode === 'AUTO_PASS' ? 'approve' : 'reject' }];
        if (props.mode && props.mode !== "USER") unsupported("AUTOMATIC_APPROVAL");
        const rejectRule = obj(props.rejectRule);
        const same = obj(props.sameRoot).type;
        const empty = obj(props.noUserHandler).type;
        const taskMode = obj(props.taskMode);
        const modes = { AND: "all", OR: "any", NEXT: "sequential", CUSTOM: "percentage" } as const;
        const oldMode = taskMode.type ?? "AND";
        if (typeof oldMode !== "string" || !(oldMode in modes)) unsupported("TASK_MODE");
        const mode = props.candidate ? "candidate" : modes[oldMode as keyof typeof modes];
        const perms = arr(props.operationPerms).map(obj);
        const enabled = (action: string) => perms.some((perm) => perm.action === action && perm.enable === true);
        const fieldPerms = arr(props.formPerms).map(obj);
        const writable = fields.filter((field) => fieldPerms.some((perm) => (perm.id === field.key || perm.key === field.key) && perm.perm === "E"));
        const timeout = obj(props.timeout);
        if (timeout.enable && !["TO_PASS", "TO_REFUSE", "NOTIFY"].includes(String(timeout.type))) unsupported("TIMEOUT_RULE");
        const parsedEvents = importWflowListenerLists(props.events);
        const reject = rejectRule.type === "SKIP" && rejectRule.target ? { type: "SKIP" as const, target: str(rejectRule.target) }
          : rejectRule.type === "END" ? { type: "END" as const } : { type: "NEXT" as const };
        return [{ ...base, type: node.type === "Approval" ? "approval" : "task", assignees: importWflowAssignment(props), mode,
          ...(mode === "percentage" ? { percentage: Number(taskMode.percentage) } : {}),
          selfApproval: same === "TO_SKIP" ? "exclude" : "allow", emptyAssignees: empty === "TO_NEXT" || same === "TO_SKIP" ? "skip" : "fail",
          fields: writable, allowTransfer: enabled("forward"), allowAddAssignees: enabled("beforeAdd") || enabled("afterAdd"), allowReturn: enabled('fallback'),
          needSign: props.needSign === true,
          rejectRule: reject,
          ...(Object.keys(parsedEvents).length ? { events: parsedEvents } : {}),
          ...(timeout.enable ? { timeout: { afterMs: duration(timeout.time, timeout.timeUnit),
            outcome: timeout.type === "TO_PASS" ? "approve" as const : timeout.type === "TO_REFUSE" ? "reject" as const : "notify" as const,
            ...(timeout.type === "NOTIFY" ? { repeat: 20 } : {}) } } : {}),
        }];
      }
      if (node.type === "Cc") {
        const ccEvents = importWflowListenerLists(props.events);
        return [{ ...base, type: "cc", recipients: importWflowAssignment(props), ...(Object.keys(ccEvents).length ? { events: ccEvents } : {}) }];
      }
      if (node.type === "Waiting") {
        if (props.type === "FIXED") return [{ ...base, type: "delay", durationMs: duration(props.timeout, props.timeUnit) }];
        if (props.type === "DATETIME") return [{ ...base, type: "delayUntil", at: javaDateTimeToIso(str(props.dateTime)) }];
        if (props.type === "TODAY") return [{ ...base, type: "delayUntil", timeOfDay: str(props.time) }];
        if (props.type === "SIGNAL") return [{ ...base, type: "wait", event: str(props.signal) }];
        return unsupported("WAIT_MODE");
      }
      if (node.type === "Trigger") {
        const action = String(props.type ?? "NONE").toUpperCase();
        if (action === "EL") return [{ ...base, type: "trigger", action: "EL", script: str(props.el) }];
        if (action === "JS") return [{ ...base, type: "trigger", action: "JS", script: str(props.jsCode) }];
        if (action === "HTTP") return [{ ...base, type: "trigger", action: "HTTP", http: dataSchema.parse(obj(props.http)) }];
        if (action === "SIGNAL") {
          const signal = obj(props.signal);
          const scope = String(signal.scope ?? "GLOBAL").toUpperCase();
          if (!["GLOBAL", "PROCESS", "LOCAL", "INSTANCE"].includes(scope)) unsupported("SIGNAL_SCOPE");
          return [{ ...base, type: "trigger", action: "SIGNAL", signal: {
            name: str(signal.name), scope: scope as "GLOBAL" | "PROCESS" | "LOCAL" | "INSTANCE",
            ...(signal.code !== undefined && signal.code !== null && String(signal.code) ? { code: str(signal.code) } : {}),
            ...(signal.instId !== undefined && signal.instId !== null && String(signal.instId) ? { instId: str(signal.instId) } : {}),
          } }];
        }
        if (action === "NONE") return [{ ...base, type: "trigger", action: "NONE" }];
        return unsupported("TRIGGER");
      }
      if (node.type === "Gateway") {
        const metadata = arr(props.branch).map(obj), bodies = arr(node.branch);
        if (metadata.length !== bodies.length || !bodies.length || bodies.some((branch) => !Array.isArray(branch))) throw new WorkflowValidationError("INVALID_WFLOW_BRANCHES");
        metadata.forEach(identify);
        const branches = bodies.map((branch) => convert(arr(branch)));
        if (props.type === "Parallel") return [{ ...base, type: "parallel", branches }];
        if (props.type !== "Exclusive" && props.type !== "Inclusive") return unsupported("GATEWAY");
        return [{ ...base, type: props.type === "Exclusive" ? "exclusive" : "inclusive",
          branches: branches.slice(0, -1).map((body, i) => ({ when: importWflowCondition(metadata[i]!.props), nodes: body })),
          otherwise: branches.at(-1)!,
        }];
      }
      if (node.type === "Subproc") {
        const mappings = arr(props.contextMap).flatMap((rawMap) => {
          const map = obj(rawMap);
          if (!map.target) return [];
          const source = typeof map.source === "string" ? map.source : JSON.stringify(jsonSchema.parse(map.source));
          return [{ source: str(source), target: str(map.target), isVar: map.isVar === true, sync: map.sync === true, fixed: map.isFixed === true }];
        });
        const childEvents = importWflowListenerLists(props.events);
        const fixed = props.initiatorType === "FIXED" ? obj(props.fixedUser) : undefined;
        return [{ ...base, type: "child",
          definition: props.isBindVer === true ? { id: str(props.defineId ?? props.code), version: Number(props.version) } : { id: str(props.code), version: 0 },
          input: {}, ...(mappings.length ? { mappings } : {}),
          ...(props.isAsync === true ? { async: true } : {}),
          ...(props.isSyncAllVar === true ? { inheritVariables: true } : {}),
          ...(props.isSyncBizKey === true ? { inheritBusinessKey: true } : {}),
          ...(props.formAutoMapping === true ? { formAutoMapping: true } : {}),
          ...(props.statusSync === true ? { statusSync: true } : {}),
          ...(fixed?.id ? { initiator: { type: "fixed" as const, userId: idStr(fixed.id) } } : {}),
          ...(Object.keys(childEvents).length ? { events: childEvents } : {}) }];
      }
      if (node.type === "Router") {
        const target = obj(props.target);
        return [{ ...base, type: "router", when: props.hasCondition ? importWflowCondition(props) : undefined, targetNodeId: str(target.id) }];
      }
      return unsupported(str(node.type).toUpperCase());
    });
  }
  const converted = convert(arr(source.nodes));
  if (links.some((id) => !ids.has(id))) throw new WorkflowValidationError("DANGLING_WFLOW_LINK");
  for (const id of ids) {
    const visited = new Set<string>();
    let cursor: string | undefined = id;
    while (cursor) { if (visited.has(cursor)) throw new WorkflowValidationError('CYCLIC_WFLOW_LINK'); visited.add(cursor); cursor = edges.get(cursor); }
  }
  const root = arr(source.nodes).map(obj).find((node) => node.type === 'Start');
  return parseDefinition({ schemaVersion: 1, id: str(source.id ?? source.code), version: Number(source.version ?? 1), name: str(source.name), inputFields: fields, nodes: converted,
    ...(root ? { resubmit: { id: str(root.id), name: typeof root.name === 'string' ? root.name : 'Resubmit', type: 'task', assignees: { type: 'initiator' }, mode: 'all', fields } } : {}),
  });
}
