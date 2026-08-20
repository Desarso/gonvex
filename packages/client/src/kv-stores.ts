import type { ReducerOutboxEntry, OutboxStore } from "./outbox.js";

/** Minimal host persistence boundary used by the reducer outbox and adapters. */
export type GonvexKv = {
  get(table: string, key: string): Promise<string | undefined>;
  set(table: string, key: string, value: string): Promise<void>;
  delete(table: string, key: string): Promise<void>;
  list(table: string): Promise<Array<{ key: string; value: string }>>;
  clear(table: string, keyPrefix?: string): Promise<void>;
};

export const kvReducerOutboxTable = "reducer_outbox";

export function createMemoryGonvexKv(): GonvexKv {
  const tables = new Map<string, Map<string, string>>();
  const table = (name: string) => {
    let rows = tables.get(name);
    if (!rows) { rows = new Map(); tables.set(name, rows); }
    return rows;
  };
  return {
    async get(name, key) { return table(name).get(key); },
    async set(name, key, value) { table(name).set(key, value); },
    async delete(name, key) { table(name).delete(key); },
    async list(name) { return [...table(name)].map(([key, value]) => ({ key, value })); },
    async clear(name, prefix) {
      const rows = table(name);
      if (!prefix) { rows.clear(); return; }
      for (const key of rows.keys()) if (key.startsWith(prefix)) rows.delete(key);
    },
  };
}

export function createKvOutboxStore(kv: GonvexKv): OutboxStore {
  return {
    async load() {
      const entries: ReducerOutboxEntry[] = [];
      for (const { value } of await kv.list(kvReducerOutboxTable)) {
        try {
          const entry = JSON.parse(value) as ReducerOutboxEntry;
          if (entry && typeof entry.id === "number") entries.push(entry);
        } catch { /* ignore corrupt disposable rows */ }
      }
      return entries.sort((left, right) => left.id - right.id);
    },
    async put(entry) { await kv.set(kvReducerOutboxTable, String(entry.id), JSON.stringify(entry)); },
    async delete(id) { await kv.delete(kvReducerOutboxTable, String(id)); },
    async clear(scope) {
      if (scope === undefined) { await kv.clear(kvReducerOutboxTable); return; }
      for (const { key, value } of await kv.list(kvReducerOutboxTable)) {
        try { if ((JSON.parse(value) as ReducerOutboxEntry)?.scope === scope) await kv.delete(kvReducerOutboxTable, key); } catch { /* ignore */ }
      }
    },
  };
}
