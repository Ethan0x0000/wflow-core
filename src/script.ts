import { parse, type Node as AstNode } from "acorn";
import { WorkflowValidationError } from "./errors.js";

// Java GraalVM enforces a statement/time budget at execution time; this is the deterministic
// static equivalent, run inside the workflow sandbox before any script is compiled.
export const SCRIPT_LIMITS = {
  maxCharacters: 10_000,
  maxStatements: 500,
  maxDepth: 32,
  // Injected loop guards throw SCRIPT_BUDGET_EXCEEDED after this many iterations.
  maxIterations: 10_000,
} as const;

// Globals and property names that escape the `with(ctx)` sandbox in the Java reference.
// `import`/`this` are handled as reserved syntax rather than identifiers.
const forbiddenIdentifiers = new Set([
  "globalThis", "global", "process", "require", "module", "exports",
  "eval", "Function", "constructor", "__proto__", "prototype",
]);

const reject = (): never => { throw new WorkflowValidationError("SCRIPT_REJECTED"); };

/**
 * Rejects scripts before compilation: empty, oversized, too many statements, too deeply nested,
 * or referencing dangerous globals/escape hatches. Syntax errors stay SyntaxError so callers keep
 * the historical SCRIPT_EXECUTION_FAILED contract.
 */
export function validateScript(lang: "EL" | "JS", script: string): void {
  if (typeof script !== "string" || script.trim().length === 0 || script.length > SCRIPT_LIMITS.maxCharacters) reject();
  const translated = lang === "EL" ? translateSpel(script) : script;
  const source = lang === "EL" ? `(${translated})` : translated;
  let ast: AstNode;
  try {
    ast = parse(source, { ecmaVersion: "latest", allowReturnOutsideFunction: true });
  } catch {
    throw new SyntaxError("SCRIPT_PARSE_FAILED");
  }
  inspect(ast);
}

function inspect(root: AstNode): void {
  let statements = 0;
  const visit = (value: unknown, depth: number): void => {
    if (Array.isArray(value)) { for (const child of value) visit(child, depth); return; }
    if (value === null || typeof value !== "object") return;
    const record = value as Record<string, unknown>;
    const type = record.type;
    if (typeof type !== "string") return;
    if (depth > SCRIPT_LIMITS.maxDepth) reject();
    if (type === "Identifier" && forbiddenIdentifiers.has(String(record.name))) reject();
    // A top-level `this` inside `with(ctx)` resolves to the function receiver (the global object).
    if (type === "ThisExpression" || type === "ImportExpression" || type === "Import") reject();
    if (type === "MemberExpression" && record.computed === true) {
      const property = record.property as Record<string, unknown> | undefined;
      if (property?.type === "Literal" && typeof property.value === "string" && forbiddenIdentifiers.has(property.value)) reject();
    }
    if (type.endsWith("Statement") || type.endsWith("Declaration")) {
      statements += 1;
      if (statements > SCRIPT_LIMITS.maxStatements) reject();
    }
    for (const key of Object.keys(record)) {
      if (key === "start" || key === "end" || key === "loc" || key === "range") continue;
      visit(record[key], depth + 1);
    }
  };
  visit(root, 0);
}

export type ScriptMode = "expression" | "statements";

type SourceEdit = { start: number; end: number; text: string };

// Budget guards are inserted as text edits at AST offsets rather than regenerated with a code
// generator: loop bodies have unambiguous boundaries, so no output-formatting dependency is needed.
function collectBudgetEdits(root: AstNode): SourceEdit[] {
  const edits: SourceEdit[] = [];
  const guardBody = (body: Record<string, unknown> | undefined | null): void => {
    if (!body || typeof body.start !== "number" || typeof body.end !== "number") return;
    if (body.type === "BlockStatement") { edits.push({ start: body.start + 1, end: body.start + 1, text: "__wfBudget();" }); return; }
    if (body.start === body.end) { edits.push({ start: body.start, end: body.end, text: "{__wfBudget();}" }); return; }
    edits.push({ start: body.start, end: body.start, text: "{__wfBudget();" });
    edits.push({ start: body.end, end: body.end, text: "}" });
  };
  const visit = (value: unknown): void => {
    if (Array.isArray(value)) { for (const child of value) visit(child); return; }
    if (value === null || typeof value !== "object") return;
    const record = value as Record<string, unknown>;
    const type = record.type;
    if (type === "WhileStatement" || type === "DoWhileStatement" || type === "ForStatement") {
      // Guarding the test covers every iteration of while/do-while and `for(;test;)`.
      const test = record.test as Record<string, unknown> | undefined | null;
      if (test && typeof test.start === "number") edits.push({ start: test.start, end: test.start, text: "__wfBudget(), " });
      else guardBody(record.body as Record<string, unknown> | undefined | null);
    } else if (type === "ForInStatement" || type === "ForOfStatement") {
      guardBody(record.body as Record<string, unknown> | undefined | null);
    }
    for (const key of Object.keys(record)) {
      if (key === "start" || key === "end" || key === "loc" || key === "range") continue;
      visit(record[key]);
    }
  };
  visit(root);
  return edits;
}

function applyEdits(source: string, edits: SourceEdit[]): string {
  let result = source;
  for (const edit of [...edits].sort((a, b) => b.start - a.start || b.end - a.end)) {
    result = result.slice(0, edit.start) + edit.text + result.slice(edit.end);
  }
  return result;
}

/**
 * Rewrites a script to the sandbox dialect (SpEL translation for EL) and injects deterministic
 * loop budget guards. `expression` mode parses/wraps the script as a single expression, matching
 * `evaluate`'s eval conditions; `statements` mode keeps the Java handler-body convention.
 */
export function instrumentScript(lang: "EL" | "JS", script: string, mode: ScriptMode): string {
  const translated = lang === "EL" ? translateSpel(script) : script;
  const source = lang === "EL" || mode === "expression" ? `(${translated})` : translated;
  const ast = parse(source, { ecmaVersion: "latest", allowReturnOutsideFunction: true });
  return applyEdits(source, collectBudgetEdits(ast));
}

type SpelToken =
  | { kind: "space" | "comment" | "string" | "template" | "punct" | "number"; text: string }
  | { kind: "ident"; text: string }
  | { kind: "variable"; text: string };

const identifierStart = /[A-Za-z_$]/;
const identifierPart = /[A-Za-z0-9_$]/;

function scanQuoted(script: string, start: number, quote: string): number {
  let index = start + 1;
  while (index < script.length) {
    const ch = script[index]!;
    if (ch === "\\") { index += 2; continue; }
    if (ch === quote) return index + 1;
    index += 1;
  }
  return script.length;
}

function scanTemplateExpression(script: string, start: number): number {
  let depth = 1, index = start;
  while (index < script.length) {
    const ch = script[index]!;
    if (ch === "\\") { index += 2; continue; }
    if (ch === "'" || ch === '"') { index = scanQuoted(script, index, ch); continue; }
    if (ch === "`") { index = scanTemplate(script, index); continue; }
    if (ch === "/" && script[index + 1] === "/") { while (index < script.length && script[index] !== "\n") index += 1; continue; }
    if (ch === "/" && script[index + 1] === "*") { index += 2; while (index < script.length && !(script[index] === "*" && script[index + 1] === "/")) index += 1; index += 2; continue; }
    if (ch === "{") depth += 1;
    else if (ch === "}") { depth -= 1; if (depth === 0) return index + 1; }
    index += 1;
  }
  return script.length;
}

function scanTemplate(script: string, start: number): number {
  let index = start + 1;
  while (index < script.length) {
    const ch = script[index]!;
    if (ch === "\\") { index += 2; continue; }
    if (ch === "`") return index + 1;
    if (ch === "$" && script[index + 1] === "{") { index = scanTemplateExpression(script, index + 2); continue; }
    index += 1;
  }
  return script.length;
}

function scanNumber(script: string, start: number): number {
  let index = start + 1;
  while (index < script.length && /[0-9a-fA-FxXoObB_]/.test(script[index]!)) index += 1;
  if (script[index] === ".") { index += 1; while (index < script.length && /[0-9_]/.test(script[index]!)) index += 1; }
  if (script[index] === "e" || script[index] === "E") {
    let cursor = index + 1;
    if (script[cursor] === "+" || script[cursor] === "-") cursor += 1;
    if (/[0-9]/.test(script[cursor] ?? "")) { index = cursor + 1; while (index < script.length && /[0-9_]/.test(script[index]!)) index += 1; }
  }
  if (script[index] === "n") index += 1;
  return index;
}

// A token-aware scan is required: the legacy regex rewrote SpEL keywords inside string literals
// and comments, corrupting valid expressions such as condition symbols containing "and".
function scanSpel(script: string): SpelToken[] {
  const tokens: SpelToken[] = [];
  let index = 0;
  while (index < script.length) {
    const ch = script[index]!;
    if (/\s/.test(ch)) { let end = index + 1; while (end < script.length && /\s/.test(script[end]!)) end += 1; tokens.push({ kind: "space", text: script.slice(index, end) }); index = end; continue; }
    if (ch === "/" && script[index + 1] === "/") { let end = index + 2; while (end < script.length && script[end] !== "\n") end += 1; tokens.push({ kind: "comment", text: script.slice(index, end) }); index = end; continue; }
    if (ch === "/" && script[index + 1] === "*") { let end = index + 2; while (end < script.length && !(script[end] === "*" && script[end + 1] === "/")) end += 1; end = Math.min(script.length, end + 2); tokens.push({ kind: "comment", text: script.slice(index, end) }); index = end; continue; }
    if (ch === "'" || ch === '"') { const end = scanQuoted(script, index, ch); tokens.push({ kind: "string", text: script.slice(index, end) }); index = end; continue; }
    if (ch === "`") { const end = scanTemplate(script, index); tokens.push({ kind: "template", text: script.slice(index, end) }); index = end; continue; }
    if (ch === "#" && identifierStart.test(script[index + 1] ?? "")) {
      let end = index + 2;
      while (end < script.length && identifierPart.test(script[end]!)) end += 1;
      tokens.push({ kind: "variable", text: script.slice(index, end) }); index = end; continue;
    }
    if (identifierStart.test(ch)) { let end = index + 1; while (end < script.length && identifierPart.test(script[end]!)) end += 1; tokens.push({ kind: "ident", text: script.slice(index, end) }); index = end; continue; }
    if (/[0-9]/.test(ch)) { const end = scanNumber(script, index); tokens.push({ kind: "number", text: script.slice(index, end) }); index = end; continue; }
    tokens.push({ kind: "punct", text: ch });
    index += 1;
  }
  return tokens;
}

function skipSpaces(tokens: SpelToken[], start: number, end: number): number {
  let index = start;
  while (index < end && tokens[index]!.kind === "space") index += 1;
  return index;
}

// SpEL `not` binds looser than comparisons but tighter than and/or, so its operand runs to the
// next top-level boolean operator, closing bracket, comma or expression end.
function findNotOperandEnd(tokens: SpelToken[], start: number, end: number): number {
  let depth = 0;
  for (let index = start; index < end; index += 1) {
    const token = tokens[index]!;
    if (token.kind === "ident") { if (depth === 0 && (token.text === "and" || token.text === "or")) return index; continue; }
    if (token.kind !== "punct") continue;
    if (token.text === "(" || token.text === "[" || token.text === "{") { depth += 1; continue; }
    if (token.text === ")" || token.text === "]" || token.text === "}") { if (depth === 0) return index; depth -= 1; continue; }
    if (depth === 0 && (token.text === "," || token.text === ";" || token.text === "?" || token.text === ":")) return index;
    if (depth === 0 && (token.text === "&" || token.text === "|") && tokens[index + 1]?.kind === "punct" && tokens[index + 1]!.text === token.text) return index;
  }
  return end;
}

type MethodSugar = { text: string; next: number };

function matchMethodSugar(tokens: SpelToken[], index: number, end: number): MethodSugar | undefined {
  const nameIndex = skipSpaces(tokens, index + 1, end);
  const name = tokens[nameIndex];
  if (!name || name.kind !== "ident") return undefined;
  const openIndex = skipSpaces(tokens, nameIndex + 1, end);
  const open = tokens[openIndex];
  if (!open || open.kind !== "punct" || open.text !== "(") return undefined;
  if (name.text === "length" || name.text === "size" || name.text === "isEmpty") {
    const closeIndex = skipSpaces(tokens, openIndex + 1, end);
    const close = tokens[closeIndex];
    if (!close || close.kind !== "punct" || close.text !== ")") return undefined;
    return { text: name.text === "isEmpty" ? ".length === 0" : ".length", next: closeIndex + 1 };
  }
  // `.equals(x)` keeps the argument parens and becomes ` === (x)`; the closing paren already in
  // the source terminates the comparison, so an enclosing `not` can wrap the whole expression.
  if (name.text === "equals") return { text: " === (", next: openIndex + 1 };
  if (name.text === "contains") return { text: ".includes(", next: openIndex + 1 };
  if (name.text === "matches") return { text: ".match(", next: openIndex + 1 };
  return undefined;
}

function renderSpel(tokens: SpelToken[], start: number, end: number): string {
  let out = "";
  let index = start;
  while (index < end) {
    const token = tokens[index]!;
    if (token.kind === "variable") { out += `ctx.${token.text.slice(1)}`; index += 1; continue; }
    if (token.kind === "ident") {
      if (token.text === "and") { out += "&&"; index += 1; continue; }
      if (token.text === "or") { out += "||"; index += 1; continue; }
      if (token.text === "not") {
        const operandStart = skipSpaces(tokens, index + 1, end);
        let operandEnd = findNotOperandEnd(tokens, operandStart, end);
        while (operandEnd > operandStart && tokens[operandEnd - 1]!.kind === "space") operandEnd -= 1;
        if (operandEnd <= operandStart) { out += "!"; index = operandStart; continue; }
        out += `!(${renderSpel(tokens, operandStart, operandEnd)})`;
        index = operandEnd;
        continue;
      }
      out += token.text; index += 1; continue;
    }
    if (token.kind === "punct" && token.text === ".") {
      const sugar = matchMethodSugar(tokens, index, end);
      if (sugar) { out += sugar.text; index = sugar.next; continue; }
      out += "."; index += 1; continue;
    }
    out += token.text;
    index += 1;
  }
  return out;
}

// SpEL (Spring EL) is the Java reference syntax: #var for context variables, and/or/not,
// method-ish collection access. Translate the common surface to the JS equivalent; T() is
// the Java bean-whitelist escape hatch and is intentionally unsupported.
export function translateSpel(script: string): string {
  const tokens = scanSpel(script);
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index]!;
    if (token.kind !== "ident" || token.text !== "T") continue;
    const next = skipSpaces(tokens, index + 1, tokens.length);
    if (next < tokens.length && tokens[next]!.kind === "punct" && tokens[next]!.text === "(") {
      throw new WorkflowValidationError("SCRIPT_EXECUTION_FAILED");
    }
  }
  return renderSpel(tokens, 0, tokens.length);
}
