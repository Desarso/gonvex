import { Dexie, type Table } from "dexie";
import type { OutboxStore, ReducerOutboxEntry } from "../outbox.js";

type Receipt = { key: string; path: string; args: string; result: unknown; retainedAt: number; commandAt: number };

/** Receipts are recovery metadata. The established IDB outbox remains canonical. */
export class EngineReceipts {
  private readonly journal: Dexie;
  private readonly database: Dexie & { receipts: Table<Receipt, string>; metadata: Table<{ key: string; retryFloor: number }, string> };
  constructor(name: string, private readonly limit = 1024, private readonly maxAgeMs = 30 * 24 * 60 * 60 * 1000) {
    this.journal = new Dexie(name, { cache: "disabled" });
    this.database = new Dexie(`${name}-engine-receipts`, { cache: "disabled" }) as typeof this.database;
    this.database.version(1).stores({ receipts: "&key" });
    this.database.version(2).stores({ receipts: "&key,retainedAt", metadata: "&key" }).upgrade(transaction =>
      transaction.table("receipts").toCollection().modify({ retainedAt: Date.now(), commandAt: Date.now() }));
  }
  async openJournal() { await this.journal.open(); }
  private key(scope: string, command: string) { return JSON.stringify([scope, command]); }
  async get(scope: string, command: string) {
    const receipt = await this.database.receipts.get(this.key(scope, command));
    return receipt && receipt.retainedAt >= Date.now() - this.maxAgeMs ? receipt : undefined;
  }
  async assertCanAdmit(commandAt: number) {
    const floor = await this.database.metadata.get("retryFloor");
    if (!Number.isSafeInteger(commandAt) || commandAt < Date.now() - this.maxAgeMs || commandAt <= (floor?.retryFloor ?? -Infinity)) {
      throw new Error("Engine command recovery receipt expired; check its server outcome before issuing a new command");
    }
  }
  private pending: Array<{ receipt: Receipt; resolve(): void; reject(error: unknown): void }> = [];
  private scheduled = false;
  private async flush() {
    const pending = this.pending.splice(0); this.scheduled = false;
    try {
      await this.database.transaction("rw", [this.database.receipts, this.database.metadata], async () => {
        await this.database.receipts.bulkPut(pending.map(item => item.receipt));
        const removed = await this.database.receipts.where("retainedAt").below(Date.now() - this.maxAgeMs).toArray();
        await this.database.receipts.bulkDelete(removed.map(receipt => receipt.key));
        const excess = await this.database.receipts.count() - this.limit;
        if (excess > 0) {
          const excessRows = await this.database.receipts.orderBy("retainedAt").limit(excess).toArray();
          removed.push(...excessRows);
          await this.database.receipts.bulkDelete(excessRows.map(receipt => receipt.key));
        }
        if (removed.length) {
          const prior = await this.database.metadata.get("retryFloor");
          await this.database.metadata.put({ key: "retryFloor", retryFloor: Math.max(prior?.retryFloor ?? 0, ...removed.map(receipt => receipt.commandAt)) });
        }
      });
      for (const item of pending) item.resolve();
    } catch (error) { for (const item of pending) item.reject(error); }
  }
  async put(scope: string, command: string, path: string, args: unknown, result: unknown, commandAt = Date.now()) {
    await new Promise<void>((resolve, reject) => {
      this.pending.push({ receipt: { key: this.key(scope, command), path, args: JSON.stringify(args), result, retainedAt: Date.now(), commandAt }, resolve, reject });
      if (!this.scheduled) { this.scheduled = true; queueMicrotask(() => { void this.flush(); }); }
    });
  }
  readonly admissions = new Map<string, { result: unknown; commandNow?: number }>();
  wrap(store: OutboxStore): OutboxStore {
    const admitted = <T extends Omit<ReducerOutboxEntry, "id">>(entry: T): T => {
      const outcome = this.admissions.get(entry.idempotencyKey);
      return outcome ? { ...entry, engineOutcome: structuredClone(outcome) } : entry;
    };
    const remember = async (ids: readonly number[]) => {
      const selected = new Set(ids);
      // Read only recovery metadata here. Deletion still goes through the
      // contract-fenced canonical store after the receipt transaction commits.
      const entries: Array<ReducerOutboxEntry | undefined> = store.getMany ? await store.getMany(ids)
        : this.journal.isOpen() ? await this.journal.table("entries").bulkGet([...ids]) : await store.load();
      await Promise.all(entries.map(entry => entry && selected.has(entry.id) && entry.engineOutcome
        ? this.put(entry.scope, entry.idempotencyKey, entry.path, entry.args, entry.engineOutcome.result, entry.engineOutcome.commandNow ?? entry.localExecution?.now ?? entry.createdAt) : undefined));
    };
    return { ...store,
      ...(store.append ? { append: async (entry: Omit<ReducerOutboxEntry, "id">) => store.append!(admitted(entry)) } : {}),
      put: entry => store.put(admitted(entry)),
      ...(store.replacePending ? { replacePending: async (id: number, draft: Omit<ReducerOutboxEntry, "id">) => {
        await remember([id]); return store.replacePending!(id, admitted(draft));
      } } : {}),
      // Persist the receipt first. A crash can leave both records, never neither.
      delete: async id => { await remember([id]); await store.delete(id); },
      deleteMany: async ids => { await remember(ids); if (store.deleteMany) await store.deleteMany(ids); else for (const id of ids) await store.delete(id); },
    };
  }
  async findReceipt(scope: string, command: string, path: string, args: unknown) {
    const receipt = await this.get(scope, command);
    if (receipt && (receipt.path !== path || receipt.args !== JSON.stringify(args))) throw new Error("Engine command ID reused with different arguments");
    return receipt;
  }
  close() { this.database.close(); this.journal.close(); }
}
