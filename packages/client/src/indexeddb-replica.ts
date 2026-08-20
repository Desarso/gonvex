import { Dexie, type EntityTable } from "dexie";
import type { LocalReplicaStorage, ReplicaSnapshot, ReplicaTransaction } from "./local-replica.js";

type SnapshotRecord = { key: "current"; snapshot: ReplicaSnapshot };

/** Atomic web persistence for the normalized Gonvex Local Replica. */
export class IndexedDBLocalReplicaStorage implements LocalReplicaStorage {
  private readonly database: Dexie & { snapshots: EntityTable<SnapshotRecord, "key"> };

  constructor(name = "gonvex-local-replica") {
    const database = new Dexie(name) as Dexie & { snapshots: EntityTable<SnapshotRecord, "key"> };
    database.version(1).stores({ snapshots: "&key" });
    this.database = database;
  }

  async load(): Promise<ReplicaSnapshot | undefined> {
    return (await this.database.snapshots.get("current"))?.snapshot;
  }

  async applyTransaction(_transaction: ReplicaTransaction, snapshot: ReplicaSnapshot): Promise<void> {
    await this.database.transaction("rw", this.database.snapshots, async () => {
      await this.database.snapshots.put({ key: "current", snapshot });
    });
  }

  async replaceSnapshot(snapshot: ReplicaSnapshot): Promise<void> {
    await this.applyTransaction({ cursor: snapshot.cursor ?? { epoch: "materialized", revision: 1 }, changes: [] }, snapshot);
  }

  close() {
    this.database.close();
  }
}

export function indexedDBLocalReplica(name?: string): LocalReplicaStorage {
  return new IndexedDBLocalReplicaStorage(name);
}
