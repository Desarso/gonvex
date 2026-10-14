/** Copy-on-write pages retain published versions without per-row tree nodes.
 * Hash buckets map IDs to integer slots; slot pages preserve Map insertion order.
 * Only edited pages/buckets copy. Directory copies are proportional to pages. */
const pageSize = 256;
type Page<V> = { owner: object; ids: Array<string | undefined>; values: Array<V | undefined> };
type Bucket = { owner: object; slots: Map<string, number> };
function bucketNumber(id: string, count: number): number {
  let hash = 2166136261;
  for (let i = 0; i < id.length; i++) hash = Math.imul(hash ^ id.charCodeAt(i), 16777619);
  return (hash >>> 0) % count;
}

export class ResidentMap<V> extends Map<string, V> {
  private buckets: Array<Bucket | undefined> = [];
  private bucketCount = 16;
  private pages: Page<V>[] = [];
  private owner: object = {};
  private lineage: object = {};
  private count = 0;
  private nextPosition = 0;
  readonly changedIDs = new Set<string>();

  fork(): ResidentMap<V> {
    this.owner = {};
    const next = new ResidentMap<V>();
    next.lineage = this.lineage;
    next.buckets = this.buckets.slice(); next.pages = this.pages.slice(); next.bucketCount = this.bucketCount;
    next.count = this.count; next.nextPosition = this.nextPosition;
    return next;
  }
  sharesLineage(other: Map<string,V>): boolean { return other instanceof ResidentMap && this.lineage === other.lineage; }
  override get size() { return this.count; }
  override get(id: string): V | undefined {
    const slot = this.buckets[bucketNumber(id,this.bucketCount)]?.slots.get(id);
    return slot === undefined ? undefined : this.pages[slot >>> 8]!.values[slot % pageSize];
  }
  position(id: string): number { return this.buckets[bucketNumber(id,this.bucketCount)]?.slots.get(id) ?? -1; }
  override has(id: string): boolean { return this.position(id) !== -1; }
  private writableBucket(number: number): Map<string, number> {
    let bucket = this.buckets[number];
    if (!bucket || bucket.owner !== this.owner) {
      bucket = { owner: this.owner, slots: new Map(bucket?.slots) };
      this.buckets[number] = bucket;
    }
    return bucket.slots;
  }
  private writablePage(slot: number): Page<V> {
    const number = slot >>> 8;
    let page = this.pages[number];
    if (!page || page.owner !== this.owner) {
      page = { owner: this.owner, ids: page?.ids.slice() ?? [], values: page?.values.slice() ?? [] };
      this.pages[number] = page;
    }
    return page;
  }
  override set(id: string, value: V): this {
    this.changedIDs.add(id);
    const number = bucketNumber(id,this.bucketCount);
    let slot = this.buckets[number]?.slots.get(id);
    if (slot === undefined) {
      slot = this.nextPosition++;
      if (this.count >= this.bucketCount * 128 && this.bucketCount < 1024) this.rehash(this.bucketCount * 4);
      this.writableBucket(bucketNumber(id,this.bucketCount)).set(id, slot);
      this.count++;
    }
    const page = this.writablePage(slot);
    page.ids[slot % pageSize] = id; page.values[slot % pageSize] = value;
    return this;
  }
  override delete(id: string): boolean {
    const number = bucketNumber(id,this.bucketCount);
    const slot = this.buckets[number]?.slots.get(id);
    if (slot === undefined) return false;
    this.changedIDs.add(id);
    this.writableBucket(number).delete(id);
    const page = this.writablePage(slot);
    page.ids[slot % pageSize] = undefined; page.values[slot % pageSize] = undefined;
    this.count--;
    // Bound tombstones under long-running delete/reinsert workloads. Old page
    // directories remain valid for captured reducer and persistence views.
    if (this.nextPosition > 1024 && this.nextPosition > this.count * 2) this.compact();
    return true;
  }
  private rehash(count: number): void {
    const prior = this.buckets;
    this.buckets = []; this.bucketCount = count;
    for (const bucket of prior) if (bucket) for (const [id,slot] of bucket.slots) {
      this.writableBucket(bucketNumber(id,count)).set(id,slot);
    }
  }
  private compact(): void {
    const next = new ResidentMap<V>();
    for (const [id, value] of this.entries()) next.set(id, value);
    this.pages = next.pages; this.buckets = next.buckets; this.owner = next.owner;
    this.nextPosition = next.nextPosition; this.bucketCount = next.bucketCount;
  }
  override clear(): void {
    this.buckets = []; this.pages = []; this.count = 0; this.nextPosition = 0; this.bucketCount = 16;
    this.lineage = {}; this.changedIDs.clear();
  }
  override *entries(): MapIterator<[string, V]> {
    for (const page of this.pages) for (let i = 0; i < page.ids.length; i++) {
      const id = page.ids[i];
      if (id !== undefined) yield [id, page.values[i]!];
    }
  }
  override *keys(): MapIterator<string> {
    for (const page of this.pages) for (const id of page.ids) if (id !== undefined) yield id;
  }
  override *values(): MapIterator<V> {
    for (const page of this.pages) for (let i = 0; i < page.ids.length; i++) {
      if (page.ids[i] !== undefined) yield page.values[i]!;
    }
  }
  override [Symbol.iterator](): MapIterator<[string, V]> { return this.entries(); }
  override forEach(callback: (value: V, key: string, map: Map<string, V>) => void, thisArg?: unknown): void {
    for (const [id, value] of this.entries()) callback.call(thisArg, value, id, this);
  }
}
