import { Dexie, type Table } from "dexie";
import type { OutboxStore, ReducerOutboxEntry } from "../outbox.js";

type Receipt = { key: string; path: string; args: string; result: unknown };

/** Receipts are recovery metadata. The established IDB outbox remains canonical. */
export class EngineReceipts {
  private readonly database: Dexie & { receipts: Table<Receipt, string> };
  constructor(name: string) {
    this.database = new Dexie(`${name}-engine-receipts`, { cache: "disabled" }) as typeof this.database;
    this.database.version(1).stores({ receipts: "&key" });
  }
  private key(scope: string, command: string) { return JSON.stringify([scope, command]); }
  get(scope: string, command: string) { return this.database.receipts.get(this.key(scope, command)); }
  async put(scope: string, command: string, path: string, args: unknown, result: unknown) {
    await this.database.receipts.put({ key: this.key(scope, command), path, args: JSON.stringify(args), result });
  }
  wrap(store: OutboxStore): OutboxStore {
    const remember = async (ids: readonly number[]) => {
      const selected = new Set(ids);
      const entries = store.getMany ? await store.getMany(ids) : await store.load();
      for (const entry of entries) if (entry && selected.has(entry.id) && entry.engineOutcome) {
        await this.put(entry.scope, entry.idempotencyKey, entry.path, entry.args, entry.engineOutcome.result);
      }
    };
    return { ...store,
      ...(store.replacePending ? { replacePending: async (id: number, draft: Omit<ReducerOutboxEntry, "id">) => {
        await remember([id]); return store.replacePending!(id, draft);
      } } : {}),
      // Persist the receipt first. A crash can leave both records, never neither.
      delete: async id => { await remember([id]); await store.delete(id); },
      deleteMany: async ids => { await remember(ids); if (store.deleteMany) await store.deleteMany(ids); else for (const id of ids) await store.delete(id); },
    };
  }
  async find(store: OutboxStore, scope: string, command: string, path: string, args: unknown) {
    const receipt = await this.get(scope, command);
    const entry: ReducerOutboxEntry | undefined = receipt ? undefined : (await store.load(scope)).find(entry => entry.idempotencyKey === command);
    const outcome = receipt ?? (entry?.engineOutcome ? { path: entry.path, args: JSON.stringify(entry.args), result: entry.engineOutcome.result } : undefined);
    if (outcome && (outcome.path !== path || outcome.args !== JSON.stringify(args))) throw new Error("Engine command ID reused with different arguments");
    return outcome;
  }
  close() { this.database.close(); }
}
