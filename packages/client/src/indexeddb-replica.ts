import { Dexie, type EntityTable } from "dexie";
import type { LocalReplicaStorage, ReplicaScope, ReplicaSnapshot, ReplicaTransaction } from "./local-replica.js";

type SnapshotRecord = { scope: ReplicaScope; snapshot: ReplicaSnapshot };
const defaultReplicaScope: ReplicaScope = "default";

/** Atomic web persistence for the normalized Gonvex Local Replica. */
export class IndexedDBLocalReplicaStorage implements LocalReplicaStorage {
  private readonly database: Dexie & { snapshots: EntityTable<SnapshotRecord, "scope"> };

  constructor(name = "gonvex-local-replica") {
    const database = new Dexie(name) as Dexie & { snapshots: EntityTable<SnapshotRecord, "scope"> };
    database.version(1).stores({ snapshots: "&key" });
    database.version(2).stores({ snapshots: "&scope" }).upgrade(async (transaction) => {
      // v1 had one unscoped `current` record. It is only eligible for the
      // compatibility/default namespace; authenticated scopes never read it.
      await transaction.table("snapshots").toCollection().modify((record: SnapshotRecord & { key?: string }) => {
        record.scope = defaultReplicaScope;
        delete record.key;
      });
    });
    this.database = database;
  }

  async load(scope: ReplicaScope = defaultReplicaScope): Promise<ReplicaSnapshot | undefined> {
    return (await this.database.snapshots.get(scope))?.snapshot;
  }

  async applyTransaction(_transaction: ReplicaTransaction, snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.database.transaction("rw", this.database.snapshots, async () => {
      await this.database.snapshots.put({ scope, snapshot });
    });
  }

  async replaceSnapshot(snapshot: ReplicaSnapshot, scope: ReplicaScope = defaultReplicaScope): Promise<void> {
    await this.applyTransaction({ cursor: snapshot.cursor ?? { epoch: "materialized", revision: 1 }, changes: [] }, snapshot, scope);
  }

  close() {
    this.database.close();
  }
}

export function indexedDBLocalReplica(name?: string): LocalReplicaStorage {
  return new IndexedDBLocalReplicaStorage(name);
}
