/** An indexed min heap contains only unpinned residents. Parallel slot arrays
 * avoid a record per row; writes and reads update the existing integer handle. */
export class RowRecency {
  private readonly entries = new Map<string, Map<string, number>>();
  private count = 0;
  private readonly entities: string[] = [];
  private readonly ids: string[] = [];
  private readonly versions: Array<number | undefined> = [];
  private readonly executionVersions: Array<number | undefined> = [];
  private readonly times: number[] = [];
  private readonly positions: number[] = [];
  private readonly free: number[] = [];
  private readonly heap: number[] = [];
  private readonly aliases = new Map<number, { entity: string; id: string }>();
  private clock = 0;
  get size() { return this.count; }
  private parts(key: string): [string,string] {
    const separator = key.indexOf('\0');
    return separator < 0 ? ['',key] : [key.slice(0,separator),key.slice(separator+1)];
  }
  private lookup(key: string): number | undefined {
    const separator = key.indexOf('\0');
    return separator < 0 ? this.entries.get('')?.get(key) : this.entries.get(key.slice(0,separator))?.get(key.slice(separator+1));
  }
  has(key: string): boolean { return this.lookup(key) !== undefined; }
  version(key: string, execution = false): number | undefined { const slot = this.lookup(key); return slot === undefined ? undefined : (execution ? this.executionVersions : this.versions)[slot]; }
  setVersion(key: string, version: number, execution = false): boolean {
    const slot = this.lookup(key);
    if (slot === undefined) return false;
    (execution ? this.executionVersions : this.versions)[slot] = version; return true;
  }
  clear(): void {
    this.entries.clear(); this.aliases.clear(); this.entities.length = 0; this.ids.length = 0; this.versions.length = 0; this.executionVersions.length = 0; this.count = 0;
    this.times.length = 0; this.positions.length = 0; this.free.length = 0;
    this.heap.length = 0; this.clock = 0;
  }
  set(key: string, row: { entity: string; id: string }, pinned: boolean): void {
    let slot = this.lookup(key);
    if (slot === undefined) {
      const [entity,id] = this.parts(key);
      slot = this.free.pop() ?? this.ids.length;
      let table = this.entries.get(entity);
      if (!table) { table = new Map(); this.entries.set(entity,table); }
      const canonical = key === `${row.entity}\0${row.id}`;
      table.set(canonical ? row.id : id,slot); this.count++;
      this.entities[slot] = canonical ? row.entity : entity; this.ids[slot] = canonical ? row.id : id;
      this.positions[slot] = -1; this.versions[slot] = undefined; this.executionVersions[slot] = undefined;
      if (!canonical) this.aliases.set(slot,row);
    }
    this.times[slot] = ++this.clock;
    this.pin(key,pinned);
    if (this.positions[slot]! >= 0) this.down(this.positions[slot]!);
  }
  touch(key: string): void {
    const slot = this.lookup(key);
    if (slot === undefined) return;
    this.times[slot] = ++this.clock;
    if (this.positions[slot]! >= 0) this.down(this.positions[slot]!);
  }
  pin(key: string, pinned: boolean): void {
    const slot = this.lookup(key);
    if (slot === undefined) return;
    if (pinned && this.positions[slot]! >= 0) this.remove(this.positions[slot]!);
    else if (!pinned && this.positions[slot]! < 0) { this.heap.push(slot); this.up(this.heap.length - 1); }
  }
  delete(key: string): void {
    const slot = this.lookup(key);
    if (slot === undefined) return;
    if (this.positions[slot]! >= 0) this.remove(this.positions[slot]!);
    const entity = this.entities[slot]!, id = this.ids[slot]!;
    const table = this.entries.get(entity)!;
    table.delete(id); if (!table.size) this.entries.delete(entity);
    this.count--; this.aliases.delete(slot); this.entities[slot] = ''; this.ids[slot] = '';
    this.versions[slot] = undefined; this.executionVersions[slot] = undefined; this.free.push(slot);
    if (!this.count) this.clear();
  }
  oldest(): { key: string; entity: string; id: string } | undefined {
    const slot = this.heap[0];
    if (slot === undefined) return undefined;
    const entity = this.entities[slot]!, id = this.ids[slot]!;
    const key = entity ? `${entity}\0${id}` : id;
    return { key, ...(this.aliases.get(slot) ?? { entity, id }) };
  }
  private place(position: number, slot: number): void { this.heap[position] = slot; this.positions[slot] = position; }
  private up(position: number): void {
    const slot = this.heap[position]!;
    while (position > 0) {
      const parent = (position - 1) >>> 1;
      if (this.times[this.heap[parent]!]! <= this.times[slot]!) break;
      this.place(position,this.heap[parent]!); position = parent;
    }
    this.place(position,slot);
  }
  private down(position: number): void {
    const slot = this.heap[position]!;
    while (position * 2 + 1 < this.heap.length) {
      let child = position * 2 + 1;
      if (child + 1 < this.heap.length && this.times[this.heap[child+1]!]! < this.times[this.heap[child]!]!) child++;
      if (this.times[slot]! <= this.times[this.heap[child]!]!) break;
      this.place(position,this.heap[child]!); position = child;
    }
    this.place(position,slot);
  }
  private remove(position: number): void {
    const removed = this.heap[position]!;
    const last = this.heap.pop()!;
    this.positions[removed] = -1;
    if (position === this.heap.length) return;
    this.place(position,last);
    const parent = (position - 1) >>> 1;
    if (position > 0 && this.times[last]! < this.times[this.heap[parent]!]!) this.up(position);
    else this.down(position);
  }
}
