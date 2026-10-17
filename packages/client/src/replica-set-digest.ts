const bucketCount = 256, limbs = 4;
const hex = (value: number) => value.toString(16).padStart(8, '0');
const width = 16 + bucketCount * 16 + 32;

/** Constant-size incremental integrity summary. Storage owns row identity and
 * its last SHA-256, so an upsert subtracts the old hash before adding the new.
 * Negotiated sum128-v1 is separate from the legacy xor1 digest. No caller may
 * advertise this format to an older runtime without capability negotiation.
 * Row hashes must include the stable key and canonical authorized row encoding.
 * This class neither grants coverage nor replaces atomic storage verification. */
export class ReplicaSetDigest {
  private readonly sums = new Uint32Array(limbs * (1 + bucketCount));
  private readonly legacy = new Uint8Array(32);
  private rows = 0;
  get count() { return this.rows; }
  add(sha256: string) { this.change(sha256, 1); }
  remove(sha256: string) { this.change(sha256, -1); }
  private change(hash: string, direction: 1 | -1) {
    if (!/^[0-9a-f]{64}$/.test(hash)) throw new TypeError('Replica row hash must be canonical SHA-256 hex');
    const count = this.rows + direction;
    if (!Number.isSafeInteger(count) || count < 0) throw new RangeError('Replica digest count is invalid');
    const bucket = parseInt(hash.slice(62), 16), base = (bucket + 1) * limbs;
    let carry = 0, bucketCarry = 0;
    // Little-endian 32-bit limbs carry modulo 2^128. All intermediate sums
    // are below 2^33, inside exact JavaScript integer arithmetic.
    for (let i = 0; i < limbs; i++) {
      const value = parseInt(hash.slice((3 - i) * 8, (4 - i) * 8), 16);
      const total = this.sums[i]! + direction * value + carry;
      const partial = this.sums[base + i]! + direction * value + bucketCarry;
      this.sums[i] = total >>> 0; this.sums[base + i] = partial >>> 0;
      carry = total < 0 ? -1 : total > 0xffffffff ? 1 : 0;
      bucketCarry = partial < 0 ? -1 : partial > 0xffffffff ? 1 : 0;
    }
    for (let i = 0; i < 32; i++) this.legacy[i] = this.legacy[i]! ^ parseInt(hash.slice(i * 2, i * 2 + 2), 16);
    this.rows = count;
  }
  digest() { return 'sum128-v1:' + Array.from(this.sums.subarray(0, limbs)).reverse().map(hex).join(''); }
  legacyDigest() { return 'xor1:' + Array.from(this.legacy, value => value.toString(16).padStart(2, '0')).join(''); }
  bucket(index: number) {
    if (!Number.isSafeInteger(index) || index < 0 || index >= bucketCount) throw new RangeError('Invalid replica integrity bucket');
    return Array.from(this.sums.subarray((index + 1) * limbs, (index + 2) * limbs)).reverse().map(hex).join('');
  }
  /** Fixed-width portable little-endian receipt, safe to persist with its rows. */
  encode(): Uint8Array {
    const bytes = new Uint8Array(width + 16), view = new DataView(bytes.buffer);
    view.setUint32(0, 1, true); view.setFloat64(8, this.rows, true);
    for (let i = 0; i < this.sums.length; i++) view.setUint32(16 + i * 4, this.sums[i]!, true);
    bytes.set(this.legacy, 16 + this.sums.byteLength); return bytes;
  }
  static decode(bytes: Uint8Array): ReplicaSetDigest {
    if (bytes.byteLength !== width + 16) throw new TypeError('Invalid replica digest receipt length');
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    if (view.getUint32(0, true) !== 1 || view.getUint32(4, true) !== 0) throw new TypeError('Unsupported replica digest receipt format');
    const result = new ReplicaSetDigest(), count = view.getFloat64(8, true);
    if (!Number.isSafeInteger(count) || count < 0) throw new TypeError('Invalid replica digest receipt count');
    for (let i = 0; i < result.sums.length; i++) result.sums[i] = view.getUint32(16 + i * 4, true);
    result.legacy.set(bytes.subarray(16 + result.sums.byteLength)); result.rows = count;
    return result;
  }
}
