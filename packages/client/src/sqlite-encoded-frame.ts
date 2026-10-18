import { BoundedByteCache, ByteLedger } from './byte-budget.js';
import { decodedJSONBytes, fail } from './sqlite-admission.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';
import type { SQLiteTypedSchema, SQLiteTypedTable } from './sqlite-typed-schema.js';
const utf8Decoder = new TextDecoder('utf-8', { fatal: true });
const keyCache = new BoundedByteCache<number,{raw:Uint8Array;key:string}>('sqlite-wire-keys',8192,32,new ByteLedger(8192));
export const sqliteEncodedKeyCacheStats = () => keyCache.stats();
function decodeUTF8Key(bytes: Uint8Array,start:number,end:number,escaped:boolean):string {
  const length=end-start+1;
  const decode = () => escaped ? JSON.parse(utf8Decoder.decode(bytes.subarray(start,end+1))) as string : utf8Decoder.decode(bytes.subarray(start+1,end));
  if (length>256) return decode();
  let hash=2166136261;
  for(let i=start;i<=end;i++)hash=Math.imul(hash^bytes[i]!,16777619)>>>0;
  const cached=keyCache.get(hash);
  if(cached?.raw.length===length){let equal=true;for(let i=0;i<length;i++)if(cached.raw[i]!==bytes[start+i]){equal=false;break;}if(equal)return cached.key;}
  // Own only the small raw key token, never a view retaining its wire frame.
  const key=decode(),raw=bytes.slice(start,end+1);
  keyCache.put(hash,{raw,key},raw.byteLength+key.length*2+160);return key;
}
const literal = (value: string) => "'" + value.replaceAll("'", "''") + "'";
const path = (...parts: string[]) => literal('$.' + parts.map(name => JSON.stringify(name)).join('.'));
export const sqliteEncodedFrameValueSQL = '(SELECT value FROM main.gv_input_frame WHERE id=1)';

/** The grammar has already passed SQLite json_valid. Scan only structural
 * invariants, without constructing rows or decoding text values. Object-key
 * sets are bounded by the caller's decoded-byte admission and die per frame. */
export function encodedFrameHasCanonicalStructure(text: string | Uint8Array): boolean {
  const code = (i: number) => typeof text === 'string' ? text.charCodeAt(i) : text[i]!;
  const slice = (start: number, end: number) => typeof text === 'string' ? text.slice(start, end) : utf8Decoder.decode(text.subarray(start, end));
  const stack: Array<{ keys?: Set<string>; key: boolean }> = [];
  for (let i = 0; i < text.length; i++) {
    const char = code(i);
    if (char === 34) {
      const start = i; let escaped = false;
      while (++i < text.length) {
        if (code(i) === 92) { escaped = true; i++; }
        else if (code(i) === 34) break;
      }
      const parent = stack.at(-1);
      if (parent?.keys && parent.key) {
        const key = text instanceof Uint8Array ? decodeUTF8Key(text,start,i,escaped) : escaped ? JSON.parse(slice(start, i + 1)) as string : slice(start + 1, i);
        if (parent.keys.has(key)) return false;
        parent.keys.add(key); parent.key = false;
      } else if (stack.length > 32) return false;
    } else if (char === 123 || char === 91) {
      if (stack.length > 32) return false;
      stack.push(char === 123 ? { keys: new Set(), key: true } : { key: false });
    } else if (char === 125 || char === 93) stack.pop();
    else if (char === 44) { const parent = stack.at(-1); if (parent?.keys) parent.key = true; }
    else if (char === 45 || char >= 48 && char <= 57) {
      if (stack.length > 32) return false;
      const start = i;
      while (i + 1 < text.length) {
        const next = code(i + 1);
        if (next >= 48 && next <= 57 || next === 43 || next === 45 || next === 46 || next === 69 || next === 101) i++;
        else break;
      }
      if (!Number.isFinite(Number(slice(start, i + 1)))) return false;
    } else if (char === 110 || char === 116 || char === 102) {
      if (stack.length > 32) return false;
      i += char === 102 ? 4 : 3;
    }
  }
  return stack.length === 0;
}

export function compileSQLiteEncodedFrameValidation(table: SQLiteTypedTable, each: 'json_each' | 'jsonb_each'): string | undefined {
  if (table.fields[table.key]?.kind !== 'text' || Object.keys(table.fields).length > 128) return;
  const id = "json_extract(value,'$.id')", version = "json_extract(value,'$.version')";
  const checks = ["json_type(value,'$.id')='text'", `length(${id})>0`,
    "json_type(value,'$.version') IN ('integer','real')", `${version}>=0 AND ${version}<=9007199254740991 AND ${version}=CAST(${version} AS INTEGER)`,
    "json_type(value,'$.row')='object'", `json_type(value,${path('row',table.key)})='text'`, `json_extract(value,${path('row',table.key)})=${id}`];
  for (const [name, field] of Object.entries(table.fields)) if (field.kind !== 'json') {
    const type = `json_type(value,${path('row',name)})`;
    const types = field.kind === 'text' ? "'text'" : field.kind === 'boolean' ? "'true','false'" : "'integer','real'";
    checks.push(`(${type} IS NULL OR ${type} IN ('null',${types}))`);
  }
  return `SELECT count(*) bad FROM ${each}(${sqliteEncodedFrameValueSQL}) WHERE json_extract(value,'$.table')=? AND CASE WHEN ${checks.map(check => '('+check+')').join(' AND ')} THEN 0 ELSE 1 END`;
}

/** The common wire path never creates a complete row graph. SQL checks strict
 * JSON, numeric finiteness, key uniqueness and declared field types before
 * returning only the bounded text projection needed for Unicode folding.
 * Unusual/legacy inputs return to the canonical JS decoder, preserving its
 * duplicate-key and numeric-key semantics rather than changing them silently. */
export async function writeSQLiteEncodedFrame(db: SQLiteReplicaDatabase, schema: SQLiteTypedSchema,
  each: 'json_each' | 'jsonb_each', encoded: string | Uint8Array, maxBytes: number,
  insert: (entity: string, normalized: string) => Promise<void>,
  validationSQL: (entity: string) => string | undefined,
): Promise<{ rows: number; validationMs: number; projectionMs: number; normalizationMs: number } | undefined> {
  const began = performance.now();
  if (typeof encoded === 'string') decodedJSONBytes(encoded, maxBytes);
  else admitSQLiteUTF8Frame(encoded, maxBytes);
  await db.runAsync('DELETE FROM main.gv_input_frame');
  try {
    // Bind/encode the wire text once. All validation and extraction below read
    // one bounded native JSONB scratch row, removed before this method returns.
    await db.runAsync('WITH source AS MATERIALIZED(SELECT CAST(? AS TEXT) value) INSERT INTO main.gv_input_frame SELECT 1,jsonb(value) FROM source WHERE json_valid(value)', encoded);
    const shape = await db.getFirstAsync<{ kind: string }>(`SELECT json_type(${sqliteEncodedFrameValueSQL}) kind`);
    if (shape?.kind !== 'array') return;
    const size = await db.getFirstAsync<{ n: number; bad: number }>(`SELECT count(*) n,coalesce(sum(type<>'object'),0) bad FROM ${each}(${sqliteEncodedFrameValueSQL})`);
    if (!size || size.n > 512 || size.bad) return;
    if (!encodedFrameHasCanonicalStructure(encoded)) return;
    const entities = await db.getAllAsync<{ entity: unknown }>(`SELECT DISTINCT json_extract(value,'$.table') entity FROM ${each}(${sqliteEncodedFrameValueSQL})`);
    if (entities.length > 32) return;
    // Validate every table before inserting any of them. The caller's enclosing
    // transaction is still the rollback boundary for a later native failure.
    for (const { entity } of entities) {
      if (typeof entity !== 'string' || !Object.hasOwn(schema, entity)) return;
      const sql = validationSQL(entity); if (!sql) return;
      const invalid = await db.getFirstAsync<{ bad: number }>(sql, entity);
      if (invalid?.bad) return;
    }
    const validationMs = performance.now() - began;
    let projectionMs = 0, normalizationMs = 0;
    for (const { entity } of entities as { entity: string }[]) {
      const table = schema[entity]!;
      let normalized = '[]';
      if (table.search?.length) {
        let timed = performance.now();
        const records = await db.getAllAsync<Record<string, unknown>>(`SELECT CAST(key AS INTEGER) pos,${table.search.map((name,index)=>`json_extract(value,${path('row',name)}) s${index}`).join(',')} FROM ${each}(${sqliteEncodedFrameValueSQL}) WHERE json_extract(value,'$.table')=? ORDER BY CAST(key AS INTEGER)`, entity);
        projectionMs += performance.now() - timed; timed = performance.now();
        const values: Array<string[] | null> = Array(size.n).fill(null);
        for (const record of records) values[Number(record.pos)] = table.search.map((_,index)=>String(record['s'+index] ?? '').toLowerCase());
        normalized = JSON.stringify(values); normalizationMs += performance.now() - timed;
      }
      await insert(entity, normalized);
    }
    return { rows: size.n, validationMs, projectionMs, normalizationMs };
  } finally { await db.runAsync('DELETE FROM main.gv_input_frame'); }
}

/** Wire-byte admission without a decoded frame allocation. Only strict UTF-8
 * reaches SQLite; escaped JSON Unicode retains the legacy string semantics. */
export function admitSQLiteUTF8Frame(input: Uint8Array, maxBytes: number): void {
  let bytes = input.byteLength * 2 + 64, quoted = false, escaped = false;
  if (bytes > maxBytes) fail('BUDGET', 'SQLite UTF-8 frame exceeds budget');
  for (let i = 0; i < input.length; i++) {
    const char = input[i]!;
    if (char >= 128) {
      const length = char >= 194 && char <= 223 ? 2 : char >= 224 && char <= 239 ? 3 : char >= 240 && char <= 244 ? 4 : 0;
      if (!length || i + length > input.length) fail('INVALID_ROW', 'Invalid UTF-8 frame');
      const next = input[i + 1]!;
      if (char === 224 && next < 160 || char === 237 && next >= 160 || char === 240 && next < 144 || char === 244 && next >= 144) fail('INVALID_ROW', 'Invalid UTF-8 frame');
      for (let j = 1; j < length; j++) if ((input[i + j]! & 192) !== 128) fail('INVALID_ROW', 'Invalid UTF-8 frame');
      i += length - 1; continue;
    }
    if (quoted) {
      if (escaped) escaped = false;
      else if (char === 92) escaped = true;
      else if (char === 34) quoted = false;
    } else if (char === 34) { quoted = true; bytes += 32; }
    else if (char === 91 || char === 123 || char === 44 || char === 58) bytes += 64;
    if (bytes > maxBytes) fail('BUDGET', 'Decoded SQLite page exceeds budget');
  }
}
