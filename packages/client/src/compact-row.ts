import type { ReplicaRow } from './local-replica.js';

// Bound the templates independently of tenant size. JSON parsing allocates the
// row's named fields together, avoiding an out-of-object PropertyArray created
// by repeated spreads and later additions. Nested immutable values stay shared.
const templates = new Map<string, string>();
const strings = new Map<string, string>();
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
  if (keys.length < 16 || Object.isFrozen(row)) return row;
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
  return result;
}
