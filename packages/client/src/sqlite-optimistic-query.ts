import {sqliteSearchAccess} from './sqlite-search-access.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import type {SQLiteTypedPage} from './sqlite-typed-engine.js';
import {compileSQLiteTypedPage,compileSQLiteOptimisticPage,type SQLitePagePlan,type SQLiteQueryOverlay} from './sqlite-typed-query.js';
import {decodedPageBytes,fail,pageBudgetError,sqlitePayloadBytes} from './sqlite-admission.js';
import {assertSQLiteMembership} from './sqlite-membership-plan.js';
/** Caller owns the fenced transaction. SQL merges indexed canonical and
 * prediction sources, including relations. Only one admitted page decodes. */
export async function readSQLiteOptimisticPage(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema,scope:string,physical:string,plan:SQLitePagePlan,sequence:number,predictionSequence:number,overlay?:SQLiteQueryOverlay,sources?:Readonly<Record<string,string>>):Promise<SQLiteTypedPage>{
 if(plan.after&&(plan.after.sequence!==sequence||plan.after.predictionSequence!==predictionSequence))fail('PAGE_CHANGED','Optimistic keyset generation changed');
 await assertSQLiteMembership(db,schema,scope,plan);const table=schema[plan.table];if(!table)throw new TypeError('Unknown optimistic query table');
 const access=await sqliteSearchAccess(db,schema,plan);
 const fields=plan.select?[...new Set([table.key,...plan.select])]:undefined;let take=plan.limit,records:Record<string,unknown>[],bytes:number;
 for(;;){
  try{
   const limited={...plan,limit:take};const statement=overlay?compileSQLiteOptimisticPage(schema,physical,limited,access,false,scope,overlay,sources):compileSQLiteTypedPage(schema,physical,limited,access,false,scope,undefined,sources);
   records=await db.getAllAsync<Record<string,unknown>>(statement.sql,...statement.bindings);bytes=Math.max(sqlitePayloadBytes(records,131072),decodedPageBytes(records,table,fields,131072));break;
  }catch(error){if(take===1||!pageBudgetError(error))throw error;take=Math.max(1,Math.floor(take/2));}
 }
 const rows=records.map(record=>!fields?JSON.parse(String(record.body)):Object.fromEntries(fields.flatMap((field,n)=>{
  if(record['h'+n]===null)return [];const value=record['p'+n],kind=table.fields[field]!.kind;return [[field,value===null?null:kind==='json'?JSON.parse(String(value)):kind==='boolean'?Boolean(value):value]];
 })));
 const last=records.at(-1);return {rows,sequence,bytes,cursor:last&&records.length===take?{sequence,predictionSequence,id:String(last.id),values:plan.order.map((_order,n)=>last['k'+n] as string|number|null)}:null};
}
