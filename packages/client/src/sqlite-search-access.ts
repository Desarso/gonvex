import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLitePagePlan} from './sqlite-typed-query.js';
import {sqliteIdentifier as q,sqliteSearchName,type SQLiteTypedSchema} from './sqlite-typed-schema.js';
/** A bounded native probe chooses between a selective trigram candidate set
 * and an ordered scan that stops after a page for common/short terms. */
export async function sqliteSearchAccess(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema,plan:SQLitePagePlan):Promise<'trigram'|'ordered'>{
 if(!plan.search)return 'trigram';if(Array.from(plan.search).length<3)return 'ordered';
 const table=schema[plan.table];if(!table?.search?.length)throw new TypeError('Invalid SQLite search table');
 const pattern='*'+plan.search.toLowerCase().replace(/[\[\]*?]/g,char=>char==='['?'[[]':char===']'?'[]]':'['+char+']')+'*';
 const candidates=await db.getFirstAsync<{n:number}>(`SELECT count(*) n FROM (${table.search.map((_,i)=>`SELECT rowid FROM (SELECT rowid FROM ${q(sqliteSearchName(plan.table))} WHERE ${q('s'+i)} GLOB ? LIMIT 1025)`).join(' UNION ')} LIMIT 1025)`,...table.search.map(()=>pattern));
 return candidates!.n>1024?'ordered':'trigram';
}
