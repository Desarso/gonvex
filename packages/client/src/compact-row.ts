import type { ReplicaRow } from './local-replica.js';

// Bound the templates independently of tenant size. JSON parsing allocates the
// row's named fields together, avoiding an out-of-object PropertyArray created
// by repeated spreads and later additions. Nested immutable values stay shared.
const templates = new Map<string, string>();
const strings = new Map<string, string>();
const compactRows = new WeakSet<ReplicaRow>();
let stringBytes = 0;
function sharedString(value: string): string {
  if (value.length > 1024) return value;
  const known = strings.get(value);
  if (known !== undefined) return known;
  const bytes = value.length * 2 + 48;
  while (strings.size >= 4096 || stringBytes + bytes > 1024 * 1024) {
    const first = strings.keys().next().value;
    if (first === undefined) break;
    strings.delete(first); stringBytes -= first.length * 2 + 48;
  }
  strings.set(value, value); stringBytes += bytes;
  return value;
}
export function compactReplicaRow(row: ReplicaRow): ReplicaRow {
  const keys = Object.keys(row);
  if (keys.length < 16 || Object.isFrozen(row) || compactRows.has(row)) return row;
  const signature = JSON.stringify(keys);
  let template = templates.get(signature);
  if (!template) {
    template = '{' + keys.map(key => JSON.stringify(key) + ':null').join(',') + '}';
    if (template.length <= 65536) {
      if (templates.size >= 64) templates.delete(templates.keys().next().value!);
      templates.set(signature, template);
    }
  }
  const result = JSON.parse(template) as ReplicaRow;
  for (const key of keys) {
    const value = row[key]; result[key] = typeof value === 'string' ? sharedString(value) : value;
  }
  compactRows.add(result);
  return result;
}


/** Storage already decoded a fresh owned row with JSON's shared property layout. */
export function admitOwnedReplicaRow(row: ReplicaRow): ReplicaRow {
  if (!Object.isFrozen(row)) for (const key of Object.keys(row)) {
    const value = row[key]; if (typeof value === 'string') row[key] = sharedString(value);
  }
  compactRows.add(row); return row;
}

/** Clone only newly supplied nested values; primitives and old immutable
 * fields need no defensive copies. The published row owns every new subtree. */
export function mergeCompactReplicaRow(previous: ReplicaRow | undefined, incoming: ReplicaRow): ReplicaRow {
  const keys = previous ? [...new Set([...Object.keys(previous), ...Object.keys(incoming)])] : Object.keys(incoming);
  const signature = JSON.stringify(keys);
  let template = templates.get(signature);
  if (!template) {
    template = '{' + keys.map(key => JSON.stringify(key) + ':null').join(',') + '}';
    if (template.length <= 65536) {
      if (templates.size >= 64) templates.delete(templates.keys().next().value!);
      templates.set(signature, template);
    }
  }
  const result = JSON.parse(template) as ReplicaRow;
  const nested: ReplicaRow = Object.create(null);
  for (const key of Object.keys(incoming)) {
    const value = incoming[key];
    if (value !== null && typeof value === 'object') nested[key] = value;
  }
  // Clone the nested graph together, preserving aliases between new fields.
  const copied = Object.keys(nested).length ? structuredClone(nested) : undefined;
  for (const key of keys) {
    const supplied = Object.prototype.hasOwnProperty.call(incoming, key);
    const value = supplied ? incoming[key] : previous![key];
    result[key] = typeof value === 'string' ? sharedString(value)
      : supplied && value !== null && typeof value === 'object' ? copied![key] : value;
  }
  compactRows.add(result); return result;
}
