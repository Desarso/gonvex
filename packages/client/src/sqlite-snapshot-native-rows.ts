import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteTypedTable} from './sqlite-typed-schema.js';
import {compileSQLiteTypedSchema,sqliteFieldName,sqliteIdentifier as q,sqliteSearchName,sqliteTableName} from './sqlite-typed-schema.js';
import {sqliteBodySkeletonSQL} from './sqlite-typed-body.js';
import {fail} from './sqlite-admission.js';
import {yieldSQLiteTask} from './sqlite-task-yield.js';
const literal=(value:string)=>"'"+value.replaceAll("'","''")+"'";
const path=(field:string)=>literal('$.'+JSON.stringify(field));
export const snapshotOverlapSchema='CREATE TEMP TABLE IF NOT EXISTS gv_snapshot_overlap(id TEXT PRIMARY KEY,position INTEGER NOT NULL) WITHOUT ROWID';
type Source={scope:string;subscription:string;snapshot:string;entity:string;revision:number;sequence:number};

/** Fresh rows take their baseline authority directly into the typed table.
 * Only pre-existing rows or tombstones need the bounded per-field JS merge.
 * The temporary relation holds IDs/positions, never duplicate row bodies. */
export async function insertFreshSnapshotRows(db:SQLiteReplicaDatabase,table:SQLiteTypedTable,source:Source):Promise<{freshRows:number;restoreIndexes():Promise<void>}|undefined>{
 if(!db.ingressUnicodeFold||table.fields[table.key]?.kind!=='text'||Object.keys(table.fields).length>128)return;
 const selection='s.scope=? AND s.subscription=? AND s.snapshot=?',args=[source.scope,source.subscription,source.snapshot];
 const name=q(sqliteTableName(source.entity)),fields=Object.entries(table.fields);
 const checks=[`json_type(s.body,${path(table.key)})='text'`,`json_extract(s.body,${path(table.key)})=s.id`];
 for(const [field,shape]of fields)if(shape.kind!=='json'){
  const type=`json_type(s.body,${path(field)})`,types=shape.kind==='text'?"'text'":shape.kind==='boolean'?"'true','false'":"'integer','real'";
  checks.push(`(${type} IS NULL OR ${type} IN ('null',${types}))`);
  if(shape.kind==='number')checks.push(`(${type} IS NULL OR ${type}='null' OR abs(json_extract(s.body,${path(field)}))<=1.7976931348623157e308)`);
 }
 const invalid=await db.getFirstAsync(`SELECT 1 FROM gv_snapshot_rows s WHERE ${selection} AND CASE WHEN ${checks.join(' AND ')} THEN 0 ELSE 1 END LIMIT 1`,...args);
 if(invalid)fail('INVALID_ROW','Snapshot field type differs from the generated schema');
 const physical=(await db.getFirstAsync<{physical:string}>('SELECT physical FROM gv_scope_alias WHERE scope=?',source.scope))?.physical??source.scope;
 await db.runAsync('DELETE FROM temp.gv_snapshot_overlap');
 await db.runAsync(`INSERT INTO temp.gv_snapshot_overlap SELECT s.id,s.position FROM gv_snapshot_rows s WHERE ${selection} AND (
  EXISTS(SELECT 1 FROM ${name} t WHERE t.scope=? AND t.id=s.id)
  OR EXISTS(SELECT 1 FROM gv_canonical_tombstones t WHERE t.scope=? AND t.entity=? AND t.id=s.id)
  OR EXISTS(SELECT 1 FROM _gonvex_replica_meta m WHERE m.scope=? AND m.key='row:'||json_array(?,s.id)))`,...args,physical,source.scope,source.entity,source.scope,source.entity);
 const fresh=await db.getFirstAsync<{n:number}>(`SELECT count(*) n FROM gv_snapshot_rows s WHERE ${selection} AND NOT EXISTS(SELECT 1 FROM temp.gv_snapshot_overlap o WHERE o.id=s.id)`,...args);
 // A large initial collection can coexist with the first rendered page.
 // Rebuilding is safe only while the entire typed table still has <=512 rows,
 // so another already-populated tenant never pays for this optimization.
 const existing=await db.getFirstAsync<{n:number}>(`SELECT count(*) n FROM (SELECT 1 FROM ${name} LIMIT 513)`);
 const deferred=fresh!.n>512&&(existing?.n??513)<=512;
 if(deferred){
  const stem=sqliteTableName(source.entity);
  for(let i=0;i<table.indexes.length;i++)await db.execAsync(`DROP INDEX IF EXISTS ${q(stem+'_i_'+i)}`);
  if(table.search?.length){
   for(const suffix of ['a','u','d'])await db.execAsync(`DROP TRIGGER IF EXISTS ${q(stem+'_fts_'+suffix)}`);
   await db.execAsync(`DROP TABLE IF EXISTS ${q(sqliteSearchName(source.entity))}`);
  }
 }
 const columns=['scope','id','body','version',...fields.map(([field])=>sqliteFieldName(field)),...(table.search??[]).map((_,i)=>'s'+i)];
 const values=fields.map(([field,shape])=>{
  const extract=`json_extract(s.body,${path(field)})`;
  return shape.kind==='json'?`CASE WHEN json_type(s.body,${path(field)}) IS NULL OR json_type(s.body,${path(field)})='null' THEN NULL ELSE json_quote(${extract}) END`:extract;
 });
 values.push(...(table.search??[]).map(field=>{
  const value=`coalesce(json_extract(s.body,${path(field)}),'')`;
  return db.ingressNativeFastFold?`coalesce(gv_ingress_lower_fast(${value}),gv_ingress_lower(${value}))`:`gv_ingress_lower(${value})`;
 }));
 await db.runAsync(`INSERT INTO ${name}(${columns.map(q).join(',')})
  SELECT ?,s.id,${sqliteBodySkeletonSQL('s.body',table)},?,${values.join(',')} FROM gv_snapshot_rows s WHERE ${selection}
  AND NOT EXISTS(SELECT 1 FROM temp.gv_snapshot_overlap o WHERE o.id=s.id)`,physical,source.revision,...args);
 await db.runAsync(`INSERT INTO _gonvex_replica_changes SELECT ?,'row',?,s.id,? FROM gv_snapshot_rows s WHERE ${selection}
  AND NOT EXISTS(SELECT 1 FROM temp.gv_snapshot_overlap o WHERE o.id=s.id)
  ON CONFLICT(scope,kind,entity,id) DO UPDATE SET sequence=excluded.sequence`,source.scope,source.entity,source.sequence,...args);
 return {freshRows:fresh!.n,async restoreIndexes(){
  if(!deferred)return;
  const ddl=compileSQLiteTypedSchema({[source.entity]:table});
  if(ddl.indexes)await db.execAsync(ddl.indexes);
  if(ddl.search)await db.execAsync(ddl.search);
  if(table.search?.length){
   const fts=q(sqliteSearchName(source.entity)),columns=table.search.map((_,i)=>q('s'+i)).join(',');
   let after=0;
   for(;;){
    const last=(await db.getFirstAsync<{last:number|null}>(`SELECT max(rid) last FROM (SELECT rid FROM ${name} WHERE rid>? ORDER BY rid LIMIT 512)`,after))?.last;
    if(last==null)break;
    await db.runAsync(`INSERT INTO ${fts}(rowid,${columns}) SELECT rid,${columns} FROM ${name} WHERE rid>? AND rid<=? ORDER BY rid`,after,last);
    after=last;await yieldSQLiteTask();
   }
  }
 }};
}
