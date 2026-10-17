export type ByteBudgetSnapshot = { name: string; bytes: number; maxBytes: number; entries: number; maxEntries: number; evictions: number; refusals: number };
export class ByteBudgetError extends Error {
  readonly code = 'memory-budget';
  constructor(readonly budget: string) { super('Memory budget exhausted: ' + budget); this.name = 'ByteBudgetError'; }
}

/** Fixed registrations prevent the ledger itself becoming an unbounded cache.
 * Values count their owned storage only; shared buffers register once with the
 * owner rather than being double-charged by every dependent view. */
export class ByteLedger {
  private readonly caches = new Map<string, () => ByteBudgetSnapshot>();
  register(name: string, snapshot: () => ByteBudgetSnapshot): () => void {
    if (!/^[a-z][a-z0-9.-]{0,63}$/.test(name) || this.caches.has(name) || this.caches.size >= 64) throw new RangeError('Invalid or duplicate byte-ledger registration');
    this.caches.set(name, snapshot);
    return () => { if (this.caches.get(name) === snapshot) this.caches.delete(name); };
  }
  snapshot(): ByteBudgetSnapshot[] { return [...this.caches.values()].map(read => read()); }
  get bytes() { let total = 0; for (const read of this.caches.values()) total += read().bytes; return total; }
}

/** LRU with hard byte and entry admission. Pins consume the same budget as
 * evictable values. If pins fill it, admission fails instead of growing RAM or
 * evicting a mounted result. Callers retain their previous visible page on failure.
 * Sizes must include key/value metadata and buffers owned by this cache. */
export class BoundedByteCache<K, V> {
  private readonly values = new Map<K, { value: V; bytes: number; pins: number }>();
  private used = 0;
  private evictions = 0;
  private refusals = 0;
  private closed = false;
  private readonly unregister: () => void;
  constructor(readonly name: string, readonly maxBytes: number, readonly maxEntries: number,
    ledger: ByteLedger, private readonly release?: (value: V) => void) {
    if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || !Number.isSafeInteger(maxEntries) || maxEntries < 1 || maxEntries > 65536) throw new RangeError('Positive finite cache limits required');
    this.unregister = ledger.register(name, () => this.stats());
  }
  stats(): ByteBudgetSnapshot { return { name: this.name, bytes: this.used, maxBytes: this.maxBytes, entries: this.values.size, maxEntries: this.maxEntries, evictions: this.evictions, refusals: this.refusals }; }
  get(key: K): V | undefined {
    const entry = this.values.get(key); if (!entry) return;
    this.values.delete(key); this.values.set(key, entry); return entry.value;
  }
  put(key: K, value: V, bytes: number): void {
    if (this.closed) throw new Error('Byte cache is closed');
    if (!Number.isSafeInteger(bytes) || bytes < 1) throw new RangeError('Cache entries require a conservative positive byte charge');
    const previous = this.values.get(key);
    if (previous?.pins) { this.refusals++; throw new ByteBudgetError(this.name); }
    let reclaimableBytes = previous?.bytes ?? 0, reclaimableEntries = previous ? 1 : 0;
    for (const [oldKey, entry] of this.values) if (oldKey !== key && !entry.pins) { reclaimableBytes += entry.bytes; reclaimableEntries++; }
    if (bytes > this.maxBytes || this.used - reclaimableBytes + bytes > this.maxBytes || this.values.size - reclaimableEntries + 1 > this.maxEntries) {
      this.refusals++; throw new ByteBudgetError(this.name);
    }
    if (previous) this.remove(key, previous);
    for (const [oldKey, entry] of this.values) {
      if (this.used + bytes <= this.maxBytes && this.values.size < this.maxEntries) break;
      if (entry.pins) continue;
      this.remove(oldKey, entry); this.evictions++;
    }
    this.values.set(key, { value, bytes, pins: 0 }); this.used += bytes;
  }
  pin(key: K): (() => void) | undefined {
    const entry = this.values.get(key); if (!entry) return;
    entry.pins++; let released = false;
    return () => { if (!released) { released = true; entry.pins--; } };
  }
  delete(key: K): boolean {
    const entry = this.values.get(key); if (!entry || entry.pins) return false;
    this.remove(key, entry); return true;
  }
  shrink(targetBytes = 0) {
    if (!Number.isSafeInteger(targetBytes) || targetBytes < 0) throw new RangeError('Invalid shrink target');
    for (const [key, entry] of this.values) {
      if (this.used <= targetBytes) break;
      if (!entry.pins) { this.remove(key, entry); this.evictions++; }
    }
  }
  private remove(key: K, entry: { value: V; bytes: number }) {
    this.values.delete(key); this.used -= entry.bytes;
    try { this.release?.(entry.value); } catch { /* Resource cleanup cannot corrupt accounting. */ }
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    for (const [key, entry] of this.values) this.remove(key, entry);
    this.unregister();
  }
}
