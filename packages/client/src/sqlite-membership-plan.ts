import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLitePagePlan} from './sqlite-typed-query.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import {decodedJSONBytes,fail} from './sqlite-admission.js';
/** The caller already owns the canonical read transaction. Generation and
 * table/key checks precede any row read; a replaced window is never returned
 * as an empty collection. The physical row alias may differ from this scope. */
export async function assertSQLiteMembership(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema,scope:string,plan:SQLitePagePlan){
 if(!plan.membership)return;
 if(!await db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gv_windows'"))fail('PAGE_CHANGED','Native membership is not installed');
 const record=await db.getFirstAsync<{header:string}>(`SELECT g.header FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation)
 WHERE w.scope=? AND w.signature=? AND w.generation=? AND g.discarded=0`,scope,plan.membership.signature,plan.membership.generation);
 if(!record)fail('PAGE_CHANGED','Native membership generation changed');decodedJSONBytes(record.header,16384);const header=JSON.parse(record.header);
 if(header.entity!==plan.table||header.key!==schema[plan.table]?.key)fail('INVALID_ROW','Native membership differs from the requested table');
 if(plan.membership.requireComplete&&header.completeness!=='complete')fail('PAGE_CHANGED','Native collection coverage is incomplete');
}
