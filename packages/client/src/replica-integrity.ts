import type { JsonValue } from "@gonvex/protocol";

/** Hash normalized rows before advertising a Replica Collection resume. */
export async function replicaRowsHashes(
  rows: readonly JsonValue[],
  keyField: string,
  yieldTask: () => Promise<void> = () => new Promise<void>(resolve => setTimeout(resolve,0)),
): Promise<Record<string, string>> {
  const seen = new Set<string>();
  const entries: Array<readonly [string, string]> = [];
  for (let offset = 0; offset < rows.length; offset += 128) {
    if (offset) await yieldTask();
    entries.push(...await Promise.all(rows.slice(offset, offset + 128).map(async (row) => {
      const key = replicaRowKey(row, keyField);
      if (!key) throw new Error(`replica row is missing key ${JSON.stringify(keyField)}`);
      if (seen.has(key)) throw new Error(`replica collection contains duplicate key ${JSON.stringify(key)}`);
      seen.add(key);
      return [key, await hashValue(stableStringify(row))] as const;
    })));
  }
  return Object.fromEntries(entries);
}

export async function replicaHashesDigest(hashes: Readonly<Record<string, string>>): Promise<string> {
  const pairs = Object.keys(hashes).sort(utf8KeyCompare).map((key) => [key, hashes[key]]);
  return hashValue(stableStringify(pairs as JsonValue));
}

export function replicaRowKey(value: JsonValue, keyField: string) {
  if (!value || Array.isArray(value) || typeof value !== "object") return "";
  const key = value[keyField];
  return key === null || key === undefined ? "" : String(key);
}

async function hashValue(value: string) {
  const subtle = globalThis.crypto?.subtle;
  if (!subtle) throw new Error("web-crypto-unavailable");
  const digest = await subtle.digest("SHA-256", new TextEncoder().encode(value));
  return Array.from(new Uint8Array(digest), (part) => part.toString(16).padStart(2, "0")).join("");
}

function stableStringify(value: JsonValue): string {
  if (typeof value === "string") {
    return JSON.stringify(value)
      .replace(/\u2028/g, "\\u2028")
      .replace(/\u2029/g, "\\u2029");
  }
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  return `{${Object.keys(value)
    .sort(utf8KeyCompare)
    .map((key) => `${stableStringify(key)}:${stableStringify(value[key]!)}`)
    .join(",")}}`;
}

function utf8KeyCompare(left: string, right: string) {
  // ASCII keys have identical UTF-8 and JavaScript lexical ordering.
  if (!/[^\x00-\x7f]/.test(left) && !/[^\x00-\x7f]/.test(right)) return left < right ? -1 : left > right ? 1 : 0;
  const leftBytes = new TextEncoder().encode(left);
  const rightBytes = new TextEncoder().encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    const difference = leftBytes[index]! - rightBytes[index]!;
    if (difference !== 0) return difference;
  }
  return leftBytes.length - rightBytes.length;
}

/** Carry verified unchanged hashes forward; only changed rows need serialization. */
export async function applyReplicaHashDelta(previous: Readonly<Record<string, string>>, upserts: readonly JsonValue[], deleted: readonly string[], key: string, aggregate?: string): Promise<Record<string, string>> {
  const next = { ...previous };
  for (const id of deleted) delete next[id];
  const changed = await replicaRowsHashes(upserts, key);
  const bytes = aggregateBytes(aggregate ?? replicaAggregateDigest(previous));
  for (const id of new Set([...deleted, ...Object.keys(changed)])) {
    if (Object.prototype.hasOwnProperty.call(previous, id)) xorHash(bytes, previous[id]!);
  }
  Object.assign(next, changed);
  for (const hash of Object.values(changed)) xorHash(bytes, hash);
  aggregates.set(next, formatAggregate(bytes));
  const priorSum=sums.get(previous);
  if(priorSum) {
    let sum=BigInt("0x"+priorSum.slice(10));
    for(const id of new Set([...deleted,...Object.keys(changed)])) if(Object.prototype.hasOwnProperty.call(previous,id)) sum-=hash128(previous[id]!);
    for(const hash of Object.values(changed)) sum+=hash128(hash);
    sums.set(next,formatSum(sum));
  }
  return next;
}

// Row hashes include the collection key. XOR is independent of membership order.
const aggregates = new WeakMap<object, string>();
export function replicaAggregateDigest(hashes: Readonly<Record<string, string>>): string {
  const cached = aggregates.get(hashes);
  if (cached) return cached;
  const bytes = new Uint8Array(32);
  for (const hash of Object.values(hashes)) xorHash(bytes, hash);
  const digest = formatAggregate(bytes);
  aggregates.set(hashes, digest);
  return digest;
}
function xorHash(bytes: Uint8Array, hash: string) {
  for (let i = 0; i < 32; i += 1) bytes[i] = bytes[i]! ^ parseInt(hash.slice(i * 2, i * 2 + 2), 16);
}
function aggregateBytes(digest: string) {
  const bytes = new Uint8Array(32);
  xorHash(bytes, digest.slice(5));
  return bytes;
}
function formatAggregate(bytes: Uint8Array) {
  return `xor1:${Array.from(bytes, byte => byte.toString(16).padStart(2, "0")).join("")}`;
}

const sums=new WeakMap<object,string>();
const sumMask=(1n<<128n)-1n;
function hash128(hash:string):bigint {
  if(!/^[0-9a-f]{64}$/.test(hash))throw new TypeError('Replica row hash must be canonical SHA-256 hex');
  return BigInt('0x'+hash.slice(0,32));
}
function formatSum(sum:bigint){return 'sum128-v1:'+(sum&sumMask).toString(16).padStart(32,'0');}
/** Legacy object-map adapter. Native storage maintains the same sum alongside
 * its rows; this cache does not introduce a second per-row hash map. */
export function replicaSumDigest(hashes:Readonly<Record<string,string>>):string {
  const previous=sums.get(hashes);if(previous)return previous;
  let sum=0n;for(const hash of Object.values(hashes))sum+=hash128(hash);
  const digest=formatSum(sum);sums.set(hashes,digest);return digest;
}
