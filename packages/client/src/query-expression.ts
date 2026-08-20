import type { JsonValue } from "@gonvex/protocol";

export type QueryOperand = { field: string } | { arg: string } | { value: JsonValue };
export type QueryExpression =
  | { op: "eq" | "neq" | "lt" | "lte" | "gt" | "gte"; left: QueryOperand; right: QueryOperand }
  | { op: "in"; value: QueryOperand; values: QueryOperand }
  | { op: "contains" | "containsInsensitive"; value: QueryOperand; search: QueryOperand }
  | { op: "and" | "or"; expressions: QueryExpression[] }
  | { op: "not"; expression: QueryExpression }
  | { op: "serverOnly"; name: string };

export type QuerySort = { field: string; direction: "asc" | "desc" };
export type PortableQueryPlan = {
  entity: string;
  where?: QueryExpression;
  sort?: QuerySort[];
  offset?: number;
  limit?: number;
};

export type OfflineQueryResult<T> = {
  rows: T[];
  completeness: "complete" | "partial";
  supported: boolean;
  unsupportedOperator?: string;
};

export function field(name: string): QueryOperand { return { field: name }; }
export function arg(name: string): QueryOperand { return { arg: name }; }
export function value(literal: JsonValue): QueryOperand { return { value: literal }; }

export function runPortableQuery<T extends Record<string, unknown>>(
  rows: readonly T[],
  plan: PortableQueryPlan,
  args: Record<string, JsonValue>,
  complete: boolean,
): OfflineQueryResult<T> {
  const unsupported = firstUnsupported(plan.where);
  if (unsupported) {
    return { rows: [], completeness: complete ? "complete" : "partial", supported: false, unsupportedOperator: unsupported };
  }
  const filtered = plan.where ? rows.filter((row) => evaluate(plan.where!, row, args)) : [...rows];
  const sorted = [...filtered].sort((left, right) => compareRows(left, right, plan.sort ?? []));
  const offset = Math.max(0, plan.offset ?? 0);
  const limit = Math.max(0, plan.limit ?? sorted.length);
  return {
    rows: sorted.slice(offset, offset + limit),
    completeness: complete ? "complete" : "partial",
    supported: true,
  };
}

export function evaluate(
  expression: QueryExpression,
  row: Record<string, unknown>,
  args: Record<string, JsonValue>,
): boolean {
  switch (expression.op) {
    case "and": return expression.expressions.every((item) => evaluate(item, row, args));
    case "or": return expression.expressions.some((item) => evaluate(item, row, args));
    case "not": return !evaluate(expression.expression, row, args);
    case "serverOnly": return false;
    case "in": {
      const values = resolve(expression.values, row, args);
      return Array.isArray(values) && values.some((candidate) => equal(candidate, resolve(expression.value, row, args)));
    }
    case "contains": return String(resolve(expression.value, row, args) ?? "").includes(String(resolve(expression.search, row, args) ?? ""));
    case "containsInsensitive": return String(resolve(expression.value, row, args) ?? "").toLocaleLowerCase().includes(String(resolve(expression.search, row, args) ?? "").toLocaleLowerCase());
    default: {
      const left = resolve(expression.left, row, args);
      const right = resolve(expression.right, row, args);
      if (expression.op === "eq") return equal(left, right);
      if (expression.op === "neq") return !equal(left, right);
      const comparison = compare(left, right);
      if (expression.op === "lt") return comparison < 0;
      if (expression.op === "lte") return comparison <= 0;
      if (expression.op === "gt") return comparison > 0;
      return comparison >= 0;
    }
  }
}

function resolve(operand: QueryOperand, row: Record<string, unknown>, args: Record<string, JsonValue>) {
  if ("field" in operand) return row[operand.field];
  if ("arg" in operand) return args[operand.arg];
  return operand.value;
}

function firstUnsupported(expression?: QueryExpression): string | undefined {
  if (!expression) return undefined;
  if (expression.op === "serverOnly") return expression.name;
  if (expression.op === "and" || expression.op === "or") {
    for (const child of expression.expressions) {
      const unsupported = firstUnsupported(child);
      if (unsupported) return unsupported;
    }
  }
  if (expression.op === "not") return firstUnsupported(expression.expression);
  return undefined;
}

function compareRows(left: Record<string, unknown>, right: Record<string, unknown>, sorts: QuerySort[]) {
  for (const sort of sorts) {
    const result = compare(left[sort.field], right[sort.field]);
    if (result !== 0) return sort.direction === "desc" ? -result : result;
  }
  return 0;
}

function compare(left: unknown, right: unknown) {
  if (left === right) return 0;
  if (left === null || left === undefined) return -1;
  if (right === null || right === undefined) return 1;
  if (typeof left === "number" && typeof right === "number") return left - right;
  return String(left).localeCompare(String(right));
}

function equal(left: unknown, right: unknown) {
  return JSON.stringify(left) === JSON.stringify(right);
}
