type Entry = { key: string; entity: string; id: string; time: number; position: number };

/** An indexed min heap contains only unpinned residents. Reads update in place;
 * pin/release keeps the original access time. No stale heap entries accumulate. */
export class RowRecency {
  private readonly entries = new Map<string, Entry>();
  private readonly heap: Entry[] = [];
  private clock = 0;
  get size() { return this.entries.size; }
  clear(): void { this.entries.clear(); this.heap.length = 0; this.clock = 0; }
  set(key: string, row: { entity: string; id: string }, pinned: boolean): void {
    this.delete(key);
    const entry = { key, ...row, time: ++this.clock, position: -1 };
    this.entries.set(key, entry);
    if (!pinned) this.add(entry);
  }
  touch(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    entry.time = ++this.clock;
    if (entry.position >= 0) this.down(entry.position);
  }
  pin(key: string, pinned: boolean): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    if (pinned && entry.position >= 0) this.remove(entry.position);
    else if (!pinned && entry.position < 0) this.add(entry);
  }
  delete(key: string): void {
    const entry = this.entries.get(key);
    if (!entry) return;
    if (entry.position >= 0) this.remove(entry.position);
    this.entries.delete(key);
  }
  oldest(): Entry | undefined { return this.heap[0]; }
  private place(position: number, entry: Entry): void { this.heap[position] = entry; entry.position = position; }
  private up(position: number): void {
    const entry = this.heap[position]!;
    while (position > 0) {
      const parent = (position - 1) >>> 1;
      if (this.heap[parent]!.time <= entry.time) break;
      this.place(position, this.heap[parent]!); position = parent;
    }
    this.place(position, entry);
  }
  private down(position: number): void {
    const entry = this.heap[position]!;
    while (position * 2 + 1 < this.heap.length) {
      let child = position * 2 + 1;
      if (child + 1 < this.heap.length && this.heap[child + 1]!.time < this.heap[child]!.time) child++;
      if (entry.time <= this.heap[child]!.time) break;
      this.place(position, this.heap[child]!); position = child;
    }
    this.place(position, entry);
  }
  private add(entry: Entry): void { this.heap.push(entry); this.up(this.heap.length - 1); }
  private remove(position: number): void {
    const removed = this.heap[position]!;
    const last = this.heap.pop()!;
    removed.position = -1;
    if (position === this.heap.length) return;
    this.place(position, last);
    const parent = (position - 1) >>> 1;
    if (position > 0 && last.time < this.heap[parent]!.time) this.up(position);
    else this.down(position);
  }
}
