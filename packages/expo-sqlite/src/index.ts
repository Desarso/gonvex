import {
  SQLiteLocalReplicaStorage,
  type SQLiteReplicaDatabase,
  type LocalReplicaStorage,
} from "@gonvex/client";

// Preserve the Expo entry point while sharing normalized persistence with web.
export { SQLiteLocalReplicaStorage as ExpoSQLiteLocalReplicaStorage };
export interface ExpoSQLiteDatabase extends SQLiteReplicaDatabase {}
export * from "./outbox.js";

export function expoSQLite(database: ExpoSQLiteDatabase): LocalReplicaStorage {
  return new SQLiteLocalReplicaStorage(database);
}
