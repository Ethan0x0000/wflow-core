import { describe, expect, it } from "vitest";
import { compileScript, evaluate, executeScript, parseDefinition, translateSpel, WorkflowValidationError } from "../src/definition.js";
import { importWflowCondition } from "../src/wflow.js";
import { SCRIPT_LIMITS, validateScript } from "../src/script.js";

const rejected = new WorkflowValidationError("SCRIPT_REJECTED");
const definitionWith = (node: unknown) => ({ schemaVersion: 1, id: "x", version: 1, name: "x", nodes: [node] });

describe("script static validator", () => {
  it("keeps executing valid JS and EL scripts inside with(ctx)", () => {
    expect(executeScript("JS", "return amount * 2;", { amount: 21 })).toBe(42);
    expect(executeScript("JS", "return name + '!';", { name: "a" })).toBe("a!");
    expect(executeScript("EL", "#amount > 100 and #name.length() > 1", { amount: 200, name: "ab" })).toBe(true);
    expect(executeScript("EL", "#roles.contains('admin')", { roles: ["admin"] })).toBe(true);
    expect(compileScript("JS", "return value;")({ value: 7 })).toBe(7);
  });

  it("does not flag identifiers appearing only in strings or comments", () => {
    expect(executeScript("JS", "return 'process';", {})).toBe("process");
    expect(executeScript("JS", "// process.require(globalThis)\nreturn 'constructor';", {})).toBe("constructor");
    expect(executeScript("JS", "/* globalThis process */ return 'required';", {})).toBe("required");
    expect(executeScript("JS", "return 'prototype'.length;", {})).toBe(9);
  });

  it.each([
    "return globalThis;",
    "return global;",
    "return process;",
    "return require('fs');",
    "return module;",
    "return exports;",
    "return import('fs');",
    "return eval('1');",
    "return Function('return 1')();",
    "return ctx.constructor;",
    "return ctx['constructor'];",
    "return ctx.__proto__;",
    "return ctx.prototype;",
    "return this;",
  ])("rejects dangerous JS pattern: %s", (script) => {
    expect(() => validateScript("JS", script)).toThrow(rejected);
    expect(() => compileScript("JS", script)).toThrow(rejected);
    expect(() => executeScript("JS", script, { ctx: {} })).toThrow(rejected);
  });

  it.each(["#globalThis", "#process", "#require", "#module", "#exports", "#Function", "#constructor", "#__proto__", "#prototype"])(
    "rejects dangerous EL pattern: %s",
    (script) => {
      expect(() => compileScript("EL", `${script} != null`)).toThrow(rejected);
    },
  );

  it("rejects empty, oversized, statement-heavy and deeply nested scripts", () => {
    expect(() => validateScript("JS", "")).toThrow(rejected);
    expect(() => validateScript("JS", "   \n\t ")).toThrow(rejected);
    expect(() => validateScript("EL", "")).toThrow(rejected);
    expect(() => validateScript("JS", `return '${"a".repeat(SCRIPT_LIMITS.maxCharacters)}';`)).toThrow(rejected);
    expect(() => validateScript("JS", "return 1;".repeat(SCRIPT_LIMITS.maxStatements + 1))).toThrow(rejected);
    expect(() => validateScript("JS", "{".repeat(SCRIPT_LIMITS.maxDepth + 8) + "}".repeat(SCRIPT_LIMITS.maxDepth + 8))).toThrow(rejected);
    expect(() => validateScript("JS", "return 1;".repeat(SCRIPT_LIMITS.maxStatements))).not.toThrow();
    expect(() => validateScript("JS", "{".repeat(SCRIPT_LIMITS.maxDepth - 4) + "}".repeat(SCRIPT_LIMITS.maxDepth - 4))).not.toThrow();
  });

  it("rejects dangerous scripts embedded in a definition before execution", () => {
    expect(() => parseDefinition(definitionWith({ id: "t", type: "trigger", action: "JS", script: "return process.env;", resultKey: "x" }))).toThrow(rejected);
    expect(() => parseDefinition(definitionWith({ id: "r", type: "router", targetNodeId: "x", when: { op: "eval", lang: "js", script: "return globalThis;" } }))).toThrow(rejected);
    expect(() => parseDefinition(definitionWith({ id: "l", type: "loop", while: { op: "eval", lang: "el", script: "#process" }, maxIterations: 2, nodes: [{ id: "d", type: "delay", durationMs: 1 }] }))).toThrow(rejected);
    expect(() => parseDefinition(definitionWith({ id: "e", type: "forEach", items: { value: [1] }, itemKey: "item", maxIterations: 2,
      completionCondition: { op: "eval", lang: "js", script: "return globalThis;" }, nodes: [{ id: "d", type: "delay", durationMs: 1 }] }))).toThrow(rejected);
    expect(() => parseDefinition(definitionWith({ id: "g", type: "eventGateway", branches: [
      { event: "go", nodes: [{ id: "t", type: "trigger", action: "JS", script: "return process.env;", resultKey: "x" }] },
    ] }))).toThrow(rejected);
    expect(() => parseDefinition(definitionWith({ id: "a", type: "task", mode: "all", assignees: { type: "users", userIds: ["u"] }, events: { created: [{ type: "JS", action: "return this;" }] } }))).toThrow(rejected);
  });

  it("still accepts safe scripts embedded in a definition", () => {
    const definition = parseDefinition(definitionWith({ id: "t", type: "trigger", action: "JS", script: "return amount + 1;", resultKey: "x" }));
    expect(definition.nodes).toHaveLength(1);
    expect(parseDefinition(definitionWith({ id: "e", type: "forEach", items: { value: [1] }, itemKey: "item", maxIterations: 2,
      completionCondition: { op: "eval", lang: "js", script: "item > 1" }, nodes: [{ id: "d", type: "delay", durationMs: 1 }] })).nodes).toHaveLength(1);
    expect(evaluate({ op: "eval", lang: "js", script: "amount > 1" }, { amount: 2 })).toBe(true);
    expect(evaluate({ op: "eval", lang: "js", script: "process" }, {})).toBe(false);
  });
});

describe("SpEL translation", () => {
  it("translates #variables, boolean operators and method sugar outside literals", () => {
    expect(translateSpel("#amount > 100 and #name != null")).toBe("ctx.amount > 100 && ctx.name != null");
    expect(translateSpel("#name.length() > 3 or #active")).toBe("ctx.name.length > 3 || ctx.active");
    expect(translateSpel("#list.size() == 2")).toBe("ctx.list.length == 2");
    expect(translateSpel("#list.isEmpty()")).toBe("ctx.list.length === 0");
    expect(translateSpel("#roles.contains('admin')")).toBe("ctx.roles.includes('admin')");
    expect(translateSpel("#code.matches('^A')")).toBe("ctx.code.match('^A')");
    expect(translateSpel("#name.equals('x')")).toBe("ctx.name === ('x')");
  });

  it("leaves keywords inside strings, templates and comments untouched", () => {
    expect(translateSpel("name == '#days and candy' and status == 'ok'")).toBe("name == '#days and candy' && status == 'ok'");
    expect(translateSpel('name == "#or #not" or #active')).toBe('name == "#or #not" || ctx.active');
    expect(translateSpel("`#a and ${#b}` or #c")).toBe("`#a and ${#b}` || ctx.c");
    expect(translateSpel("// #a and #b\n#c")).toBe("// #a and #b\nctx.c");
    expect(translateSpel("/* #a or not #b */ #c")).toBe("/* #a or not #b */ ctx.c");
    expect(translateSpel("'#x' == #x")).toBe("'#x' == ctx.x");
  });

  it("binds `not` to its full comparison operand", () => {
    expect(translateSpel("not #a.equals(#b)")).toBe("!(ctx.a === (ctx.b))");
    expect(translateSpel("not #amount > 1")).toBe("!(ctx.amount > 1)");
    expect(translateSpel("#a and not #b or #c")).toBe("ctx.a && !(ctx.b) || ctx.c");
    expect(translateSpel("not (not #a)")).toBe("!((!(ctx.a)))");
    expect(translateSpel("not #a and not (#b or #c)")).toBe("!(ctx.a) && !((ctx.b || ctx.c))");
  });

  it("still rejects the T() bean escape hatch", () => {
    expect(() => translateSpel("T(java.lang.Math).max(1,2)")).toThrow("SCRIPT_EXECUTION_FAILED");
  });
});

describe("EL eval conditions", () => {
  it("evaluates SpEL expressions through the shared compile path", () => {
    expect(evaluate({ op: "eval", lang: "el", script: "#amount > 1" }, { amount: 2 })).toBe(true);
    expect(evaluate({ op: "eval", lang: "el", script: "#amount > 1" }, { amount: 0 })).toBe(false);
    expect(evaluate({ op: "eval", lang: "el", script: "#name.equals('ab') and not #blocked" }, { name: "ab", blocked: false })).toBe(true);
  });

  it("evaluates EL conditions imported from wflow DEV groups", () => {
    const condition = importWflowCondition({ groups: [{ conditions: [{ group: "DEV", type: "EL", compareVal: ["#amount > 1"] }] }] });
    expect(evaluate(condition, { amount: 2 })).toBe(true);
    expect(evaluate(condition, { amount: 0 })).toBe(false);
  });

  it("keeps malformed JS conditions returning false", () => {
    expect(evaluate({ op: "eval", lang: "js", script: "process" }, {})).toBe(false);
  });
});

describe("script execution budget", () => {
  it("fails fast on a non-terminating loop in a trigger script", () => {
    expect(() => executeScript("JS", "while (true) {}", {})).toThrowError("SCRIPT_BUDGET_EXCEEDED");
    expect(() => executeScript("JS", "let i = 0; for (;;) i++;", {})).toThrowError("SCRIPT_BUDGET_EXCEEDED");
    expect(() => executeScript("JS", "do {} while (true)", {})).toThrowError("SCRIPT_BUDGET_EXCEEDED");
  });

  it("fails fast on a non-terminating loop inside an eval condition", () => {
    expect(() => evaluate({ op: "eval", lang: "js", script: "(function(){ while (true) {} })()" }, {})).toThrowError("SCRIPT_BUDGET_EXCEEDED");
  });

  it("keeps finite loops working with head, for-in and for-of forms", () => {
    expect(executeScript("JS", "let n = 0; while (n < 3) n++; return n;", {})).toBe(3);
    expect(executeScript("JS", "let sum = 0; for (let i = 0; i < 5; i++) sum += i; return sum;", {})).toBe(10);
    expect(executeScript("JS", "let total = 0; for (const item of [1,2,3]) total += item; return total;", {})).toBe(6);
    expect(executeScript("JS", "const keys = []; for (const key in {a: 1, b: 2}) keys.push(key); return keys;", {})).toEqual(["a", "b"]);
  });
});

describe("compiled script memoization", () => {
  it("returns the same pure function for repeated compilations", () => {
    expect(compileScript("JS", "return amount * 2;")).toBe(compileScript("JS", "return amount * 2;"));
    expect(executeScript("JS", "return amount * 2;", { amount: 2 })).toBe(4);
  });

  it("stays correct after the bounded cache evicts old entries", () => {
    const first = compileScript("JS", "return 'first';");
    for (let index = 0; index < 250; index++) compileScript("JS", `return ${index};`);
    expect(first({})).toBe("first");
    expect(compileScript("JS", "return 'first';")({})).toBe("first");
  });
});
