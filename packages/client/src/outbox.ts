import type { Dexie as DexieDatabase, Table } from "dexie";
import type { OptimisticPatch } from "./optimistic.js";

export type MutationOutboxOptions = {
  databaseName?: string;
  indexedDB?: IDBFactory;
  IDBKeyRange?: typeof IDBKeyRange;
  enabled?: boolean;
};

export type MutationOutboxEntry = {
  /** Auto-incremented sequence number. Lower ids always happened first. */
  id: number;
  path: string;
  args: unknown;
  idempotencyKey: string;
  /** Entity identifiers whose writes must retain enqueue order. */
  entityKeys: string[];
  /** Optimistic UI state restored while this entry awaits a server result. */
  patches?: OptimisticPatch[];
  createdAt: number;
  attempts: number;
  nextAttemptAt: number;
  lastError?: string;
  state: "pending" | "inflight";
};

export type EnqueueMutation = {
  path: string;
  args: unknown;
  idempotencyKey?: string;
  entityKeys?: string[];
  patches?: OptimisticPatch[];
};

export type MutationOutbox = {
  enqueue(mutation: EnqueueMutation): Promise<MutationOutboxEntry>;
  loadAll(): Promise<MutationOutboxEntry[]>;
  nextReady(now: number): Promise<MutationOutboxEntry | undefined>;
  markInflight(id: number): Promise<void>;
  ack(id: number): Promise<void>;
  fail(id: number, error: string): Promise<void>;
  count(): Promise<number>;
  subscribe(listener: () => void): () => void;
};

type NewMutationOutboxEntry = Omit<MutationOutboxEntry, "id"> & { id?: number };

type MutationOutboxDatabase = DexieDatabase & {
  entries: Table<MutationOutboxEntry, number, NewMutationOutboxEntry>;
};

/**
 * A durable, totally ordered mutation queue.
 *
 * The auto-incremented id is the enqueue order and is never reused while the
 * database exists. `nextReady` may skip unrelated writes, but it never skips a
 * lower-id write touching the same entity. An inflight entry remains a causal
 * barrier until it is acknowledged, and `loadAll` recovers abandoned inflight
 * work after a crash by returning it to pending.
 *
 * IndexedDB is an optimization for durability, not a prerequisite for the
 * optimistic mutation path. Disabled or failed storage permanently degrades
 * this instance to the same queue semantics in memory for the current session.
 */
export class DexieMutationOutbox implements MutationOutbox {
  private readonly databaseName: string;
  private readonly indexedDB?: IDBFactory;
  private readonly keyRange?: typeof IDBKeyRange;
  private readonly listeners = new Set<() => void>();
  private readonly memoryEntries = new Map<number, MutationOutboxEntry>();
  private database?: MutationOutboxDatabase;
  private databasePromise?: Promise<MutationOutboxDatabase>;
  private memoryOnly: boolean;
  private nextMemoryId = 1;

  constructor(options: MutationOutboxOptions = {}) {
    this.databaseName = options.databaseName ?? "gonvex-outbox";
    this.indexedDB = options.indexedDB;
    this.keyRange = options.IDBKeyRange;
    this.memoryOnly = options.enabled === false
      || !(options.indexedDB ?? globalThis.indexedDB)
      || !(options.IDBKeyRange ?? globalThis.IDBKeyRange);
  }

  async enqueue(mutation: EnqueueMutation): Promise<MutationOutboxEntry> {
    const createdAt = Date.now();
    const entry: NewMutationOutboxEntry = {
      path: mutation.path,
      args: cloneValue(mutation.args),
      idempotencyKey: mutation.idempotencyKey ?? createIdempotencyKey(),
      entityKeys: [...(mutation.entityKeys ?? [])],
      patches: mutation.patches?.map(clonePatch),
      createdAt,
      attempts: 0,
      nextAttemptAt: createdAt,
      state: "pending",
    };

    if (this.memoryOnly) return this.enqueueInMemory(entry);
    try {
      const database = await this.open();
      const id = await database.entries.add(entry);
      const stored = { ...entry, id } as MutationOutboxEntry;
      this.remember(stored);
      this.notify();
      return cloneEntry(stored);
    } catch {
      this.degradeToMemory();
      return this.enqueueInMemory(entry);
    }
  }

  async loadAll(): Promise<MutationOutboxEntry[]> {
    if (this.memoryOnly) return this.loadAllFromMemory();
    try {
      const database = await this.open();
      let changed = false;
      let entries: MutationOutboxEntry[] = [];
      await database.transaction("rw", database.entries, async () => {
        entries = await database.entries.orderBy("id").toArray();
        entries = entries.map((entry) => {
          if (entry.state !== "inflight") return entry;
          changed = true;
          return { ...entry, state: "pending" as const };
        });
        if (changed) await database.entries.bulkPut(entries);
      });
      this.replaceMemoryEntries(entries);
      if (changed) this.notify();
      return entries.map(cloneEntry);
    } catch {
      this.degradeToMemory();
      return this.loadAllFromMemory();
    }
  }

  async nextReady(now: number): Promise<MutationOutboxEntry | undefined> {
    if (this.memoryOnly) return cloneOptionalEntry(firstReady(this.sortedMemoryEntries(), now));
    try {
      const database = await this.open();
      const entries = await database.entries.orderBy("id").toArray();
      this.replaceMemoryEntries(entries);
      return cloneOptionalEntry(firstReady(entries, now));
    } catch {
      this.degradeToMemory();
      return cloneOptionalEntry(firstReady(this.sortedMemoryEntries(), now));
    }
  }

  async markInflight(id: number): Promise<void> {
    if (this.memoryOnly) {
      this.markInflightInMemory(id);
      return;
    }
    try {
      const database = await this.open();
      const entry = await database.entries.get(id);
      if (!entry || entry.state === "inflight") return;
      const updated = { ...entry, state: "inflight" as const };
      await database.entries.put(updated);
      this.remember(updated);
      this.notify();
    } catch {
      this.degradeToMemory();
      this.markInflightInMemory(id);
    }
  }

  async ack(id: number): Promise<void> {
    if (this.memoryOnly) {
      this.ackInMemory(id);
      return;
    }
    try {
      const database = await this.open();
      const entry = await database.entries.get(id);
      if (!entry) return;
      await database.entries.delete(id);
      this.memoryEntries.delete(id);
      this.notify();
    } catch {
      this.degradeToMemory();
      this.ackInMemory(id);
    }
  }

  async fail(id: number, error: string): Promise<void> {
    if (this.memoryOnly) {
      this.failInMemory(id, error);
      return;
    }
    try {
      const database = await this.open();
      const entry = await database.entries.get(id);
      if (!entry) return;
      const updated = failedEntry(entry, error, Date.now());
      await database.entries.put(updated);
      this.remember(updated);
      this.notify();
    } catch {
      this.degradeToMemory();
      this.failInMemory(id, error);
    }
  }

  async count(): Promise<number> {
    if (this.memoryOnly) return this.memoryEntries.size;
    try {
      return await (await this.open()).entries.count();
    } catch {
      this.degradeToMemory();
      return this.memoryEntries.size;
    }
  }

  subscribe(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  private enqueueInMemory(entry: NewMutationOutboxEntry): MutationOutboxEntry {
    const stored = { ...entry, id: this.nextMemoryId++ } as MutationOutboxEntry;
    this.remember(stored);
    this.notify();
    return cloneEntry(stored);
  }

  private loadAllFromMemory(): MutationOutboxEntry[] {
    let changed = false;
    for (const [id, entry] of this.memoryEntries) {
      if (entry.state !== "inflight") continue;
      this.memoryEntries.set(id, { ...entry, state: "pending" });
      changed = true;
    }
    if (changed) this.notify();
    return this.sortedMemoryEntries().map(cloneEntry);
  }

  private markInflightInMemory(id: number) {
    const entry = this.memoryEntries.get(id);
    if (!entry || entry.state === "inflight") return;
    this.memoryEntries.set(id, { ...entry, state: "inflight" });
    this.notify();
  }

  private ackInMemory(id: number) {
    if (!this.memoryEntries.delete(id)) return;
    this.notify();
  }

  private failInMemory(id: number, error: string) {
    const entry = this.memoryEntries.get(id);
    if (!entry) return;
    this.memoryEntries.set(id, failedEntry(entry, error, Date.now()));
    this.notify();
  }

  private sortedMemoryEntries() {
    return [...this.memoryEntries.values()].sort((left, right) => left.id - right.id);
  }

  private remember(entry: MutationOutboxEntry) {
    this.memoryEntries.set(entry.id, cloneEntry(entry));
    this.nextMemoryId = Math.max(this.nextMemoryId, entry.id + 1);
  }

  private replaceMemoryEntries(entries: MutationOutboxEntry[]) {
    this.memoryEntries.clear();
    for (const entry of entries) this.remember(entry);
  }

  private notify() {
    for (const listener of this.listeners) {
      try {
        listener();
      } catch {
        // A subscriber must not be able to break mutation delivery.
      }
    }
  }

  private degradeToMemory() {
    this.memoryOnly = true;
    this.database?.close();
    this.database = undefined;
    this.databasePromise = undefined;
  }

  private async open(): Promise<MutationOutboxDatabase> {
    if (!this.databasePromise) {
      this.databasePromise = this.createDatabase().catch((error) => {
        this.databasePromise = undefined;
        throw error;
      });
    }
    return this.databasePromise;
  }

  private async createDatabase(): Promise<MutationOutboxDatabase> {
    const indexedDBValue = this.indexedDB ?? globalThis.indexedDB;
    const keyRangeValue = this.keyRange ?? globalThis.IDBKeyRange;
    if (!indexedDBValue || !keyRangeValue) throw new Error("indexeddb-unavailable");
    const { Dexie } = await import("dexie");
    const database = new Dexie(this.databaseName, {
      indexedDB: indexedDBValue,
      IDBKeyRange: keyRangeValue,
    }) as MutationOutboxDatabase;
    database.version(1).stores({
      entries: "++id, state, nextAttemptAt",
    });
    database.on("versionchange", () => database.close());
    await database.open();
    this.database = database;
    return database;
  }
}

export function createMutationOutbox(options: MutationOutboxOptions = {}): MutationOutbox {
  return new DexieMutationOutbox(options);
}

function firstReady(entries: MutationOutboxEntry[], now: number) {
  const blockedEntityKeys = new Set<string>();
  for (const entry of entries) {
    if (
      entry.state === "pending"
      && entry.nextAttemptAt <= now
      && !entry.entityKeys.some((key) => blockedEntityKeys.has(key))
    ) {
      return entry;
    }
    for (const key of entry.entityKeys) blockedEntityKeys.add(key);
  }
  return undefined;
}

function failedEntry(entry: MutationOutboxEntry, error: string, now: number): MutationOutboxEntry {
  const attempts = entry.attempts + 1;
  return {
    ...entry,
    attempts,
    state: "pending",
    nextAttemptAt: now + Math.min(30_000, 1_000 * (2 ** attempts)),
    lastError: error,
  };
}

function createIdempotencyKey() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function cloneEntry(entry: MutationOutboxEntry): MutationOutboxEntry {
  return {
    ...entry,
    args: cloneValue(entry.args),
    entityKeys: [...entry.entityKeys],
    patches: entry.patches?.map(clonePatch),
  };
}

function cloneOptionalEntry(entry: MutationOutboxEntry | undefined) {
  return entry ? cloneEntry(entry) : undefined;
}

function cloneValue<T>(value: T): T {
  try {
    return globalThis.structuredClone(value);
  } catch {
    return value;
  }
}

function clonePatch(patch: OptimisticPatch): OptimisticPatch {
  if (patch.op === "delete") return { ...patch };
  return { ...patch, fields: cloneValue(patch.fields) };
}
