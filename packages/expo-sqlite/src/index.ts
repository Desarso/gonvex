import type {
  LiveQueryMembership,
  LocalReplicaStorage,
  ReplicaRow,
  ReplicaSnapshot,
  ReplicaTransaction,
} from "@gonvex/client";

export interface ExpoSQLiteDatabase {
  execAsync(sql: string): Promise<void>;
  runAsync(sql: string, ...params: unknown[]): Promise<unknown>;
  getFirstAsync<T>(sql: string, ...params: unknown[]): Promise<T | null>;
  getAllAsync<T>(sql: string, ...params: unknown[]): Promise<T[]>;
  withTransactionAsync(task: () => Promise<void>): Promise<void>;
}

type EntityRecord = { entity: string; id: string; value: string };
type QueryRecord = { signature: string; value: string };
type MetaRecord = { value: string };

/**
 * Transactional, normalized SQLite persistence for Expo. Every server
 * transaction updates entities, Live Query memberships, and the replica
 * cursor inside one SQLite transaction.
 */
export class ExpoSQLiteLocalReplicaStorage implements LocalReplicaStorage {
  private initialized?: Promise<void>;

  constructor(private readonly database: ExpoSQLiteDatabase) {}

  private initialize() {
    return this.initialized ??= this.database.execAsync(`
      CREATE TABLE IF NOT EXISTS _gonvex_replica_entities (
        entity TEXT NOT NULL, id TEXT NOT NULL, value TEXT NOT NULL,
        PRIMARY KEY (entity, id)
      );
      CREATE TABLE IF NOT EXISTS _gonvex_replica_queries (
        signature TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS _gonvex_replica_meta (
        key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL
      );
    `);
  }

  async load(): Promise<ReplicaSnapshot | undefined> {
    await this.initialize();
    const cursor = await this.database.getFirstAsync<MetaRecord>(
      `SELECT value FROM _gonvex_replica_meta WHERE key = 'cursor'`,
    );
    const entities: Record<string, Record<string, ReplicaRow>> = {};
    const entityRecords = await this.database.getAllAsync<EntityRecord>(
      `SELECT entity, id, value FROM _gonvex_replica_entities ORDER BY entity, id`,
    );
    for (const row of entityRecords) {
      (entities[row.entity] ??= {})[row.id] = JSON.parse(row.value) as ReplicaRow;
    }
    const liveQueries: Record<string, LiveQueryMembership> = {};
    const queryRecords = await this.database.getAllAsync<QueryRecord>(
      `SELECT signature, value FROM _gonvex_replica_queries ORDER BY signature`,
    );
    for (const row of queryRecords) {
      liveQueries[row.signature] = JSON.parse(row.value) as LiveQueryMembership;
    }
    if (!cursor && entityRecords.length === 0 && queryRecords.length === 0) return undefined;
    return { cursor: cursor ? JSON.parse(cursor.value) : undefined, entities, liveQueries };
  }

  async applyTransaction(transaction: ReplicaTransaction, _snapshot: ReplicaSnapshot): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      const previous = await this.database.getFirstAsync<MetaRecord>(
        `SELECT value FROM _gonvex_replica_meta WHERE key = 'cursor'`,
      );
      if (previous && JSON.parse(previous.value).epoch !== transaction.cursor.epoch) {
        await this.database.runAsync(`DELETE FROM _gonvex_replica_entities`);
        await this.database.runAsync(`DELETE FROM _gonvex_replica_queries`);
      }
      for (const change of transaction.changes) {
        if (change.operation === "delete") {
          await this.database.runAsync(
            `DELETE FROM _gonvex_replica_entities WHERE entity = ? AND id = ?`,
            change.entity, change.id,
          );
        } else {
          await this.database.runAsync(
            `INSERT INTO _gonvex_replica_entities (entity, id, value) VALUES (?, ?, ?)
             ON CONFLICT(entity, id) DO UPDATE SET value = excluded.value`,
            change.entity, change.id, JSON.stringify(change.newValue),
          );
        }
      }
      for (const membership of transaction.memberships ?? []) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_queries (signature, value) VALUES (?, ?)
           ON CONFLICT(signature) DO UPDATE SET value = excluded.value`,
          membership.signature, JSON.stringify(membership),
        );
      }
      await this.database.runAsync(
        `INSERT INTO _gonvex_replica_meta (key, value) VALUES ('cursor', ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
        JSON.stringify(transaction.cursor),
      );
    });
  }

  async replaceSnapshot(snapshot: ReplicaSnapshot): Promise<void> {
    await this.initialize();
    await this.database.withTransactionAsync(async () => {
      await this.database.runAsync(`DELETE FROM _gonvex_replica_entities`);
      await this.database.runAsync(`DELETE FROM _gonvex_replica_queries`);
      await this.database.runAsync(`DELETE FROM _gonvex_replica_meta WHERE key = 'cursor'`);
      for (const [entity, rows] of Object.entries(snapshot.entities)) {
        for (const [id, value] of Object.entries(rows)) {
          await this.database.runAsync(
            `INSERT INTO _gonvex_replica_entities (entity, id, value) VALUES (?, ?, ?)`,
            entity, id, JSON.stringify(value),
          );
        }
      }
      for (const [signature, membership] of Object.entries(snapshot.liveQueries)) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_queries (signature, value) VALUES (?, ?)`,
          signature, JSON.stringify(membership),
        );
      }
      if (snapshot.cursor) {
        await this.database.runAsync(
          `INSERT INTO _gonvex_replica_meta (key, value) VALUES ('cursor', ?)
           ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
          JSON.stringify(snapshot.cursor),
        );
      }
    });
  }
}

export function expoSQLite(database: ExpoSQLiteDatabase): LocalReplicaStorage {
  return new ExpoSQLiteLocalReplicaStorage(database);
}
