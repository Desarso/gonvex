import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLitePagePlan,SQLitePredicate} from './sqlite-typed-query.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
export const sqliteEntityAliasSchema=`CREATE TABLE IF NOT EXISTS gv_entity_alias(scope TEXT NOT NULL,entity TEXT NOT NULL,physical TEXT NOT NULL,PRIMARY KEY(scope,entity)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_entity_retired(scope TEXT NOT NULL,entity TEXT NOT NULL,physical TEXT NOT NULL,PRIMARY KEY(entity,physical)) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS gv_entity_alias_physical ON gv_entity_alias(physical,entity);`;
/** Entity activation may replace one large table while other table aliases
 * and the existing whole-scope bootstrap mapping remain intact. */
export async function sqlitePhysicalEntityScope(db:SQLiteReplicaDatabase,scope:string,entity?:string){
 if(entity===undefined){const row=await db.getFirstAsync<{physical:string}>('SELECT json_quote(physical) physical FROM gv_scope_alias WHERE scope=?',scope);return row?JSON.parse(row.physical) as string:scope;}
 const row=await db.getFirstAsync<{physical:string}>(`SELECT json_quote(physical) physical FROM (
 SELECT physical,0 priority FROM gv_entity_alias WHERE scope=? AND entity=?
 UNION ALL SELECT physical,1 priority FROM gv_scope_alias WHERE scope=?) ORDER BY priority LIMIT 1`,scope,entity,scope);return row?JSON.parse(row.physical) as string:scope;
}
/** Resolve only the bounded tables referenced by this plan, not a schema or
 * collection corpus. The compiler can then use different physical sources in
 * relation predicates without a per-row JS callback. */
export async function sqliteQueryEntityScopes(db:SQLiteReplicaDatabase,scope:string,plan:SQLitePagePlan):Promise<{physical:string;sources?:Readonly<Record<string,string>>}>{
 const entities=new Set([plan.table]);const visit=(predicate:SQLitePredicate|undefined)=>{
  if(!predicate)return;if(predicate.op==='relation'){entities.add(predicate.table);visit(predicate.where);}else if(predicate.op==='and'||predicate.op==='or')predicate.children.forEach(visit);else if(predicate.op==='not')visit(predicate.child);
 };visit(plan.where);if(entities.size>64)fail('BUDGET','Query entity aliases exceed allowance');
 const fallback=await sqlitePhysicalEntityScope(db,scope),aliases=await db.getAllAsync<{entity:string;physical:string}>('SELECT entity,json_quote(physical) physical FROM gv_entity_alias WHERE scope=? AND entity IN (SELECT value FROM json_each(?))',scope,JSON.stringify([...entities]));
 sqlitePayloadBytes(aliases,65536);if(!aliases.length)return {physical:fallback};
 const sources:Record<string,string>=Object.create(null);for(const entity of entities)sources[entity]=fallback;for(const alias of aliases)sources[alias.entity]=JSON.parse(alias.physical);
 return {physical:sources[plan.table]!,sources};
}
