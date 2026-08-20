/** A path into reducer arguments, or a literal row identifier. */
export type OptimisticID = string | number | readonly string[];
export type OptimisticArgument = { readonly $arg: string | readonly string[] };
export type OptimisticValue = unknown | OptimisticArgument | readonly OptimisticValue[] | { readonly [key: string]: OptimisticValue };

export type OptimisticEffectDefinition =
  | { operation: "patch"; entity: string; id: OptimisticID; fields: Readonly<Record<string, OptimisticValue>> }
  | { operation: "upsert"; entity: string; id: OptimisticID; value: Readonly<Record<string, OptimisticValue>> }
  | { operation: "delete"; entity: string; id: OptimisticID };

/** Ordered effects from a module reducer's atomic optimistic transaction. */
export type OptimisticTransactionDefinition = {
  effects: readonly OptimisticEffectDefinition[];
  expectedRevision?: number;
};

type OptimisticRowPatch = { entity?: string; collection?: string; rowId: string; op: "patch" | "insert"; fields: Record<string, unknown> };
type OptimisticUpsertPatch = { entity?: string; collection?: string; rowId: string; op: "upsert"; fields: Record<string, unknown> };
type OptimisticDeletePatch = { entity?: string; collection?: string; rowId: string; op: "delete"; fields?: never };
export type OptimisticPatch = OptimisticRowPatch | OptimisticUpsertPatch | OptimisticDeletePatch;

export function optimisticPatchesFromReference(
  definition: OptimisticTransactionDefinition | undefined,
  args: unknown,
): OptimisticPatch[] {
  return isOptimisticTransactionDefinition(definition) ? optimisticPatchesFromTransaction(definition, args) : [];
}

function optimisticPatchesFromTransaction(transaction: OptimisticTransactionDefinition, args: unknown): OptimisticPatch[] {
  if (!Array.isArray(transaction.effects) || transaction.effects.length === 0) return [];
  const record = isRecord(args) ? args : {};
  const patches: OptimisticPatch[] = [];
  for (const effect of transaction.effects) {
    if (!isOptimisticEffectDefinition(effect) || !effect.entity.trim()) return [];
    const rowId = optimisticRowId(effect.id, record);
    if (!rowId) return [];
    if (effect.operation === "delete") {
      patches.push({ entity: effect.entity, rowId, op: "delete" });
      continue;
    }
    const template = effect.operation === "patch" ? effect.fields : effect.value;
    if (!isRecord(template)) return [];
    const fields = resolveOptimisticValue(template, record);
    if (!isRecord(fields) || containsUndefined(fields)) return [];
    patches.push({ entity: effect.entity, rowId, op: effect.operation === "upsert" ? "upsert" : "patch", fields: normalizeOptimisticFields(fields) });
  }
  return patches;
}

function resolveOptimisticValue(value: unknown, args: Record<string, unknown>): unknown {
  if (Array.isArray(value)) return value.map((item) => resolveOptimisticValue(item, args));
  if (!isRecord(value)) return value;
  if (Object.keys(value).length === 1 && Object.prototype.hasOwnProperty.call(value, "$arg")) {
    const path = value.$arg;
    const segments = Array.isArray(path)
      ? path.filter((part): part is string => typeof part === "string" && part.trim().length > 0)
      : typeof path === "string" ? path.split(".").map((part) => part.trim()).filter(Boolean) : [];
    return segments.length > 0 ? readPath(args, segments) : undefined;
  }
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, resolveOptimisticValue(item, args)]));
}

function containsUndefined(value: unknown): boolean {
  if (value === undefined) return true;
  if (Array.isArray(value)) return value.some(containsUndefined);
  return isRecord(value) && Object.values(value).some(containsUndefined);
}

function optimisticRowId(id: OptimisticID, args: Record<string, unknown>): string {
  const value = Array.isArray(id) ? readPath(args, id) : id;
  return typeof value === "string" || typeof value === "number" ? String(value).trim() : "";
}

function isOptimisticTransactionDefinition(value: unknown): value is OptimisticTransactionDefinition {
  return isRecord(value) && Array.isArray(value.effects);
}

function isOptimisticEffectDefinition(value: unknown): value is OptimisticEffectDefinition {
  if (!isRecord(value) || (value.operation !== "patch" && value.operation !== "upsert" && value.operation !== "delete")) return false;
  if (typeof value.entity !== "string" || (typeof value.id !== "string" && typeof value.id !== "number" && !Array.isArray(value.id))) return false;
  if (Array.isArray(value.id) && value.id.some((part) => typeof part !== "string" || !part.trim())) return false;
  return value.operation === "delete" || isRecord(value.operation === "patch" ? value.fields : value.value);
}

function isRecord(value: unknown): value is Record<string, any> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function readPath(value: Record<string, unknown>, path: readonly string[]): unknown {
  let current: unknown = value;
  for (const part of path) {
    if (!isRecord(current)) return undefined;
    current = current[part];
  }
  return current;
}

function normalizeOptimisticFields(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, normalizeOptimisticValue(item)]));
}

function normalizeOptimisticValue(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(normalizeOptimisticValue);
  if (!isRecord(value)) return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, normalizeOptimisticValue(item)]));
}
