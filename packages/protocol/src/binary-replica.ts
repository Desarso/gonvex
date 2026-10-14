import { decode, encode } from "@msgpack/msgpack";
import type { ServerMessage } from "./index.js";

export const BINARY_REPLICA_MAX_BYTES = 16 * 1024 * 1024;
const MAX_NAMES = 16_384;
const MAX_NODES = 2_000_000;
const MAX_DICTIONARY_CHARS = 1_048_576;
const TYPES = new Set(["replica.snapshotPage", "replica.snapshot", "replica.delta", "replica.transaction"]);

// v1: [version, dictionary offset, new object layouts, new entities, payload].
// Layout keys are name IDs. Objects are [layout ID, values], arrays [-1, values],
// entity and changed-column names [-2, name ID].
// Layout keys retain insertion order, including nested JSON keys used by hashes.
export class BinaryReplicaCodec {
  private layouts: string[][] = [];
  private entities: string[] = [];
  private layoutIds = new Map<string, number>();
  private nameIds = new Map<string, number>();

  private intern(name: string): number {
    let id = this.nameIds.get(name);
    if (id === undefined) { id = this.entities.length; this.entities.push(name); this.nameIds.set(name, id); }
    return id;
  }

  encode(message: ServerMessage): Uint8Array {
    if (!TYPES.has(message.type)) throw new Error("Not a binary replica frame");
    const offset = this.layouts.length;
    const entityOffset = this.entities.length;
    const visit = (value: unknown, depth: number, key?: string): unknown => {
      if (depth > 64) throw new Error("Replica nesting limit");
      if ((key === "entity" || key === "changedColumns") && typeof value === "string") return [-2, this.intern(value)];
      if (Array.isArray(value)) return [-1, value.map(v => visit(v, depth + 1, key))];
      if (value !== null && typeof value === "object") {
        const keys = Object.keys(value).filter(k => (value as Record<string, unknown>)[k] !== undefined);
        const signature = JSON.stringify(keys);
        let id = this.layoutIds.get(signature);
        if (id === undefined) {
          id = this.layouts.length;
          this.layouts.push(keys);
          for (const key of keys) this.intern(key);
          this.layoutIds.set(signature, id);
        }
        return [id, keys.map(k => visit((value as Record<string, unknown>)[k], depth + 1, k))];
      }
      return value;
    };
    const payload = visit(message, 0);
    if (!dictionaryWithinLimits(this.layouts, this.entities)) throw new Error("Replica dictionary limit");
    const bytes = encode([1, offset, this.layouts.slice(offset).map(keys => keys.map(key => this.nameIds.get(key)!)), this.entities.slice(entityOffset), payload]);
    if (bytes.byteLength > BINARY_REPLICA_MAX_BYTES) throw new Error("Replica frame limit");
    return bytes;
  }

  decode(bytes: Uint8Array): ServerMessage {
    if (bytes.byteLength > BINARY_REPLICA_MAX_BYTES) throw new Error("Replica frame limit");
    validateMessagePackLimits(bytes);
    const envelope = decode(bytes, {
      maxStrLength: BINARY_REPLICA_MAX_BYTES, maxBinLength: 0,
      maxArrayLength: MAX_NODES, maxMapLength: 0, maxExtLength: 0,
      useBigInt64: true,
    });
    if (!Array.isArray(envelope) || envelope.length !== 5 || envelope[0] !== 1
      || envelope[1] !== this.layouts.length || !Array.isArray(envelope[2]) || !Array.isArray(envelope[3])) {
      throw new Error("Invalid replica envelope");
    }
    const entities = this.entities.concat(envelope[3]);
    if (entities.length > MAX_NAMES || entities.some(name => typeof name !== "string")) throw new Error("Invalid replica dictionary");
    const additions = envelope[2].map((keys: unknown) => {
      if (!Array.isArray(keys) || keys.length > MAX_NAMES || keys.some(id => !Number.isInteger(id) || id < 0 || id >= entities.length)
        || new Set(keys).size !== keys.length) throw new Error("Invalid replica layout");
      return keys.map(id => entities[id]);
    });
    const layouts = this.layouts.concat(additions);
    if (!dictionaryWithinLimits(layouts, entities)) throw new Error("Replica dictionary limit");
    let nodes = 0;
    const visit = (value: unknown, depth: number): unknown => {
      if (++nodes > MAX_NODES || depth > 64) throw new Error("Replica decode limit");
      if (typeof value === "bigint") {
        // Match JSON.parse for JSON numbers. Cursor precision is validated below.
        return Number(value);
      }
      if (Array.isArray(value)) {
        if (value.length !== 2 || !Number.isInteger(value[0])) throw new Error("Invalid replica value");
        const [id, values] = value;
        if (id === -2) {
          if (!Number.isInteger(values) || values < 0 || values >= entities.length) throw new Error("Invalid entity ID");
          return entities[values];
        }
        if (!Array.isArray(values)) throw new Error("Invalid replica values");
        if (id === -1) return values.map(v => visit(v, depth + 1));
        const keys = layouts[id];
        if (!keys || values.length !== keys.length) throw new Error("Invalid layout ID");
        const object: Record<string, unknown> = {};
        for (let i = 0; i < keys.length; i++) {
          const decoded = visit(values[i], depth + 1);
          if (keys[i] === "__proto__") Object.defineProperty(object, keys[i], { value: decoded, enumerable: true, writable: true, configurable: true });
          else object[keys[i]] = decoded;
        }
        return object;
      }
      if (value !== null && typeof value === "object" || typeof value === "number" && !Number.isFinite(value)) {
        throw new Error("Invalid replica scalar");
      }
      return value;
    };
    const message = visit(envelope[4], 0) as ServerMessage;
    validateReplicaMessage(message);
    // Commit dictionaries only after the whole frame passes validation.
    this.layouts = layouts;
    this.entities = entities;
    return message;
  }
}

type BinaryMessage = Extract<ServerMessage, { type: "replica.snapshot" | "replica.snapshotPage" | "replica.delta" | "replica.transaction" }>;

function validateReplicaMessage(input: ServerMessage): void {
  const message = input as BinaryMessage;
  if (!message || typeof message !== "object" || !TYPES.has(message.type)
    || !message.cursor || typeof message.cursor.epoch !== "string"
    || !Number.isSafeInteger(message.cursor.revision) || message.cursor.revision < 0) throw new Error("Invalid replica frame");
  const fields = message as unknown as Record<string, unknown>;
  for (const key of ["id", "path", "key", "digest", "originCommandId", "snapshotId", "scope", "visibilityScope", "orderBy"]) {
    if (fields[key] !== undefined && typeof fields[key] !== "string") throw new Error("Invalid replica string");
  }
  for (const key of ["deleted", "originCommandIds"]) {
    if (fields[key] !== undefined && (!Array.isArray(fields[key]) || (fields[key] as unknown[]).some(value => typeof value !== "string"))) throw new Error("Invalid replica IDs");
  }
  if (fields.hashes !== undefined && (!fields.hashes || typeof fields.hashes !== "object" || Array.isArray(fields.hashes)
    || Object.values(fields.hashes).some(value => typeof value !== "string"))) throw new Error("Invalid replica hashes");
  if (fields.mode !== undefined && fields.mode !== "eager" && fields.mode !== "progressive"
    || fields.orderDirection !== undefined && fields.orderDirection !== "asc" && fields.orderDirection !== "desc"
    || fields.truncated !== undefined && typeof fields.truncated !== "boolean") throw new Error("Invalid replica metadata");
  for (const key of ["maxRows", "maxBytes"]) {
    if (fields[key] !== undefined && (typeof fields[key] !== "number" || !Number.isFinite(fields[key]))) throw new Error("Invalid replica budget");
  }
  if (message.type === "replica.transaction") {
    if (!Array.isArray(message.changes) || message.changes.some(c => !c || typeof c.entity !== "string"
      || typeof c.id !== "string" || !["insert", "update", "delete"].includes(c.operation)
      || c.changedColumns !== undefined && (!Array.isArray(c.changedColumns) || c.changedColumns.some(column => typeof column !== "string")))) throw new Error("Invalid replica changes");
  } else {
    if (typeof message.id !== "string") throw new Error("Invalid replica ID");
    if (message.type !== "replica.delta" && (!Array.isArray(message.result) || typeof message.key !== "string")) throw new Error("Invalid snapshot");
    if (message.type === "replica.snapshotPage" && (typeof message.snapshotId !== "string"
      || typeof message.scope !== "string" || typeof message.visibilityScope !== "string"
      || !Number.isSafeInteger(message.sequence) || message.sequence < 0 || typeof message.last !== "boolean")) throw new Error("Invalid snapshot page");
    if (message.type === "replica.delta" && (message.upserts !== undefined && !Array.isArray(message.upserts)
      || message.deleted !== undefined && (!Array.isArray(message.deleted) || message.deleted.some(id => typeof id !== "string")))) throw new Error("Invalid delta");
  }
}

function dictionaryWithinLimits(layouts: string[][], entities: string[]): boolean {
  if (layouts.length > MAX_NAMES || entities.length > MAX_NAMES) return false;
  let chars = 0;
  let keys = 0;
  for (const layout of layouts) for (const _key of layout) {
    if (++keys > 65_536 || chars > MAX_DICTIONARY_CHARS) return false;
  }
  for (const entity of entities) { chars += entity.length; if (chars > MAX_DICTIONARY_CHARS) return false; }
  return true;
}

// Check lengths and nesting before the library allocates arrays. v1 has no maps,
// extensions or byte strings. MessagePack still owns scalar decoding.
function validateMessagePackLimits(bytes: Uint8Array): void {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const remaining = [1];
  let offset = 0;
  let nodes = 0;
  const length = (size: number): number => {
    if (offset + size > bytes.length) throw new Error("Truncated replica frame");
    const value = size === 1 ? view.getUint8(offset) : size === 2 ? view.getUint16(offset) : view.getUint32(offset);
    offset += size;
    return value;
  };
  while (remaining.length) {
    if (remaining[remaining.length - 1] === 0) { remaining.pop(); continue; }
    remaining[remaining.length - 1]--;
    if (++nodes > MAX_NODES * 4 || offset >= bytes.length) throw new Error("Replica decode limit");
    const marker = bytes[offset++];
    let arrayLength: number | undefined;
    let skip = 0;
    if (marker <= 0x7f || marker >= 0xe0 || marker === 0xc0 || marker === 0xc2 || marker === 0xc3) { /* scalar */ }
    else if (marker >= 0x90 && marker <= 0x9f) arrayLength = marker & 0xf;
    else if (marker >= 0xa0 && marker <= 0xbf) skip = marker & 0x1f;
    else if (marker === 0xdc || marker === 0xdd) arrayLength = length(marker === 0xdc ? 2 : 4);
    else if (marker === 0xd9 || marker === 0xda || marker === 0xdb) skip = length(marker === 0xd9 ? 1 : marker === 0xda ? 2 : 4);
    else if (marker === 0xca || marker === 0xce || marker === 0xd2) skip = 4;
    else if (marker === 0xcb || marker === 0xcf || marker === 0xd3) skip = 8;
    else if (marker === 0xcc || marker === 0xd0) skip = 1;
    else if (marker === 0xcd || marker === 0xd1) skip = 2;
    else throw new Error("Unsupported replica MessagePack value");
    if (offset + skip > bytes.length) throw new Error("Truncated replica frame");
    offset += skip;
    if (arrayLength !== undefined) {
      if (arrayLength > MAX_NODES || remaining.length >= 140) throw new Error("Replica decode limit");
      remaining.push(arrayLength);
    }
  }
  if (offset !== bytes.length) throw new Error("Trailing replica bytes");
}
