// Worker/storage entry point. Keep SQLite and schema/query code off the main
// client entry point and out of tabs that never become storage owners.
export { compileSQLiteTypedSchema, validateSQLiteTypedSchema, sqliteTableName, sqliteFieldName, sqliteCountName, sqliteSearchName } from './sqlite-typed-schema.js';
export type { SQLiteTypedSchema, SQLiteTypedTable, SQLiteField } from './sqlite-typed-schema.js';
export { compileSQLiteTypedPage, compileSQLiteTypedView } from './sqlite-typed-query.js';
export type { SQLitePredicate, SQLitePagePlan, SQLiteKeyset, SQLiteOrder, SQLiteTypedStatement, SQLiteBinding } from './sqlite-typed-query.js';
export { SQLiteTypedReplicaEngine, SQLiteReplicaEngineError, sqlitePayloadBytes } from './sqlite-typed-engine.js';
export type { SQLiteTypedWrite, SQLiteTypedPage, SQLiteTypedViewPage, SQLiteBootstrapState } from './sqlite-typed-engine.js';
export { openOfficialSQLiteHost, officialSQLiteDatabase } from './sqlite-official-host.js';
export type { OfficialSQLiteHost } from './sqlite-official-host.js';
export { sqliteTypedRowLayout, sqliteTypedReplicaStorage } from './sqlite-typed-row-layout.js';
export { SQLiteReplicaBroker, SQLiteBrokerError } from './sqlite-broker.js';
export type { SQLiteBrokerOptions, SQLiteBrokerState } from './sqlite-broker.js';
export { ReplicaSetDigest } from './replica-set-digest.js';
export { lowerSQLiteLivePlan } from './sqlite-plan-lowering.js';
export { SQLiteWindowStore } from './sqlite-window-store.js';
export type { SQLiteWindowHeader, SQLiteWindowProgress, SQLiteWindowPage } from './sqlite-window-store.js';

export {SQLiteNativeReplicaReader} from './sqlite-native-replica-reader.js';
export type {SQLiteNativeWindowRead,SQLiteNativeWindowResult,SQLiteNativeEntityRead,SQLiteNativeEntityResult,SQLiteEntitySlot} from './sqlite-native-replica-reader.js';
export {SQLiteIntentJournal} from './sqlite-intent-journal.js';
export type {SQLiteIntentReceipt,SQLiteIntentPage} from './sqlite-intent-journal.js';
export {SQLitePredictions} from './sqlite-predictions.js';
export {SQLiteReducerCore,SQLiteReducerPreparationRequiredError} from './sqlite-reducer-core.js';
export type {SQLiteReducerInvocation,SQLiteReducerReceipt,SQLiteReducerContext} from './sqlite-reducer-core.js';

export {SQLiteReducerOwner} from './sqlite-reducer-owner.js';
export type {SQLiteReducerOwnerOptions,SQLiteOwnerReducerCall,SQLiteTabAdmission} from './sqlite-reducer-owner.js';
export {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
export type {SQLiteSafetyPage,SQLiteSafetyEntry} from './sqlite-safety-journal.js';
export {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
export type {SQLiteMigrationSourcePage} from './sqlite-idb-migration-source.js';
export {SQLiteLegacyIntentMigration} from './sqlite-legacy-intent-migration.js';
