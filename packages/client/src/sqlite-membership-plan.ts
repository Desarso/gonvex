import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLitePagePlan} from './sqlite-typed-query.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import {decodedJSONBytes,fail} from './sqlite-admission.js';
export type SQLiteMembershipSource={subscription:string;snapshot:string};
/** The caller already owns the canonical read transaction. Generation and
 * table/key checks precede any row read; a replaced window is never returned
 * as an empty collection. The physical row alias may differ from this scope. */
export async function assertSQLiteMembership(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema,scope:string,plan:SQLitePagePlan){
 if(!plan.membership)return;
 const installed=await db.getAllAsync<{name:string}>("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('gv_windows','gv_snapshot_window_alias')");const alias=installed.some(row=>row.name==='gv_snapshot_window_alias');
 if(!installed.some(row=>row.name==='gv_windows'))fail('PAGE_CHANGED','Native membership is not installed');
 const record=await db.getFirstAsync<{header:string;subscription:string|null;snapshot:string|null}>(`SELECT g.header,${alias?'a.subscription,a.snapshot':'NULL subscription,NULL snapshot'} FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation) ${alias?'LEFT JOIN gv_snapshot_window_alias a USING(scope,signature,generation)':''}
 WHERE w.scope=? AND w.signature=? AND w.generation=? AND g.discarded=0`,scope,plan.membership.signature,plan.membership.generation);
 if(!record)fail('PAGE_CHANGED','Native membership generation changed');decodedJSONBytes(record.header,16384);const header=JSON.parse(record.header);
 if(header.entity!==plan.table||header.key!==schema[plan.table]?.key)fail('INVALID_ROW','Native membership differs from the requested table');
 if(plan.membership.requireComplete&&header.completeness!=='complete')fail('PAGE_CHANGED','Native collection coverage is incomplete');
 return record.subscription!==null?{subscription:record.subscription,snapshot:record.snapshot!}:undefined;
}
