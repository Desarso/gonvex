import type { SQLiteTypedSchema } from './sqlite-typed-schema.js';

export class SQLiteReplicaEngineError extends Error {
  constructor(readonly code: 'FENCED' | 'SCHEMA_MISMATCH' | 'BUDGET' | 'INTENT_CONFLICT' | 'INTENT_CORRUPT' | 'INVALID_ROW' | 'PAGE_CHANGED', message: string) { super(message); this.name = 'SQLiteReplicaEngineError'; }
}
export function pageBudgetError(error: unknown): boolean { return !!error && typeof error === 'object' && 'code' in error && (error.code === 'BUDGET' || error.code === 'sqlite-bridge-budget'); }
export function fail(code: SQLiteReplicaEngineError['code'], message: string): never { throw new SQLiteReplicaEngineError(code, message); }

/** Conservative JS admission accounting before JSON serialization or cloning.
 * Only own plain JSON data is accepted, with bounded depth and property count. */
export function sqlitePayloadBytes(value: unknown, maxBytes: number): number {
  let bytes = 0, nodes = 0;
  const stack = new Set<object>();
  const visit = (current: unknown, depth: number): void => {
    if (++nodes > 100_000 || depth > 32) fail('BUDGET', 'SQLite payload structure exceeds budget');
    bytes += 16;
    if (typeof current === 'string') bytes += current.length * 2;
    else if (typeof current === 'number') { if (!Number.isFinite(current)) fail('INVALID_ROW', 'Nonfinite SQLite JSON number'); }
    else if (current !== null && typeof current !== 'boolean') {
      if (typeof current !== 'object' || !Array.isArray(current) && Object.getPrototypeOf(current) !== Object.prototype && Object.getPrototypeOf(current) !== null)
        fail('INVALID_ROW', 'SQLite payload must be plain JSON data');
      const object = current as object;
      if (Array.isArray(object) && (object.length > 100_000 || object.length * 16 > maxBytes || Object.keys(object).length !== object.length)) fail('BUDGET', 'SQLite arrays must be dense and fit the byte budget');
      if (stack.has(object)) fail('INVALID_ROW', 'Cyclic SQLite payload');
      stack.add(object);
      for (const key of Object.keys(object)) {
        bytes += key.length * 2 + 16;
        if (bytes > maxBytes) fail('BUDGET', 'SQLite payload byte budget exceeded');
        const descriptor = Object.getOwnPropertyDescriptor(object, key)!;
        if (!('value' in descriptor)) fail('INVALID_ROW', 'SQLite payload accessor rejected');
        if (descriptor.value !== undefined || Array.isArray(object)) visit(descriptor.value, depth + 1);
      }
      stack.delete(object);
    }
    if (bytes > maxBytes) fail('BUDGET', 'SQLite payload byte budget exceeded');
  };
  visit(value, 0);
  return bytes;
}

/** Charge decoded JSON before allocating its object graph. Canonical JSON is
 * already validated on ingestion. Strings reserve their encoded UTF-16 size;
 * each structural token reserves container/member/value overhead. This is an
 * intentionally conservative upper estimate, not a JS engine heap measurement. */
export function decodedJSONBytes(text: string, maxBytes: number): number {
  let bytes = text.length * 2 + 64, quoted = false, escaped = false;
  for (let i = 0; i < text.length; i++) {
    const char = text.charCodeAt(i);
    if (quoted) {
      if (escaped) escaped = false;
      else if (char === 92) escaped = true;
      else if (char === 34) quoted = false;
    } else if (char === 34) { quoted = true; bytes += 32; }
    else if (char === 91 || char === 123 || char === 44 || char === 58) bytes += 64;
    if (bytes > maxBytes) fail('BUDGET', 'Decoded SQLite page exceeds budget');
  }
  return bytes;
}
export function decodedPageBytes(records: readonly Record<string, unknown>[], table: SQLiteTypedSchema[string], fields: readonly string[] | undefined, maxBytes: number): number {
  let bytes = 64;
  for (const record of records) {
    bytes += 64;
    if (!fields) bytes += decodedJSONBytes(String(record.body), maxBytes - bytes);
    else for (let i = 0; i < fields.length; i++) {
      if (record['h' + i] === null) continue;
      const field = fields[i]!, value = record['p' + i];
      bytes += field.length * 2 + 64;
      bytes += value === null ? 16 : table.fields[field]!.kind === 'json' ? decodedJSONBytes(String(value), maxBytes - bytes) : typeof value === 'string' ? value.length * 2 + 32 : 16;
    }
    if (bytes > maxBytes) fail('BUDGET', 'Decoded SQLite page exceeds budget');
  }
  return bytes;
}

