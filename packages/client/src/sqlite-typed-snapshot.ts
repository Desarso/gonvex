import {ByteLedger,BoundedByteCache} from './byte-budget.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import {sqlitePhysicalEntityScope} from './sqlite-entity-scope.js';
import {sqliteBodySkeletonSQL,sqliteRowBodySQL} from './sqlite-typed-body.js';
import {sqliteFieldName,sqliteIdentifier as q,sqliteTableName,sqliteSearchName,compileSQLiteTypedSchema,type SQLiteTypedSchema} from './sqlite-typed-schema.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
const literal=(value:string)=>value.includes('\u0000')?"CAST(X'"+[...new TextEncoder().encode(value)].map(byte=>byte.toString(16).padStart(2,'0')).join('')+"' AS TEXT)":"'"+value.replaceAll("'","''")+"'";
const path=(field:string)=>literal('$.'+JSON.stringify(field));
export const typedSnapshotSchema=`CREATE TABLE IF NOT EXISTS gv_snapshot_typed(
 scope TEXT NOT NULL,subscription TEXT NOT NULL,snapshot TEXT NOT NULL,entity TEXT NOT NULL,physical TEXT NOT NULL UNIQUE,
 PRIMARY KEY(scope,subscription,snapshot)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_snapshot_scope_clock(id INTEGER PRIMARY KEY CHECK(id=1),value INTEGER NOT NULL);`;
/** Generated SQL consumes the same bounded frame as the hash/membership stage.
 * Only the tiny skeleton lives in the typed row; the generic stage keeps its
 * identity/hash/position and no second row body. */
type SnapshotWritePlan={check:string;insert:string;maxRid:string;fts?:string};
export function compileSQLiteTypedSnapshot(schema:SQLiteTypedSchema):SnapshotWritePlan[]{
 return Object.entries(schema).filter(([,table])=>table.fields[table.key]?.kind==='text'&&Object.keys(table.fields).length<=128).map(([entity,table])=>{
  const target=`b.scope=i.scope AND b.subscription=json_extract(i.value,'$.id') AND b.snapshot=json_extract(i.value,'$.snapshotId') AND b.entity=${literal(entity)}`;
  const fields=Object.entries(table.fields),extract=(field:string)=>`json_extract(r.body,${path(field)})`;
  const checks=[`json_type(r.body,${path(table.key)})='text'`];
  for(const [field,shape]of fields)if(shape.kind!=='json'){
   const type=`json_type(r.body,${path(field)})`,types=shape.kind==='text'?"'text'":shape.kind==='boolean'?"'true','false'":"'integer','real'";
   checks.push(`(${type} IS NULL OR ${type} IN ('null',${types}))`);
   if(shape.kind==='number')checks.push(`(${type} IS NULL OR ${type}='null' OR abs(${extract(field)})<=1.7976931348623157e308)`);
  }
  const values=fields.map(([field,shape])=>shape.kind==='json'?`CASE WHEN json_type(r.body,${path(field)}) IS NULL OR json_type(r.body,${path(field)})='null' THEN NULL ELSE json_quote(${extract(field)}) END`:extract(field));
  values.push(...(table.search??[]).map(field=>`coalesce(gv_ingress_lower_fast(coalesce(${extract(field)},'')),gv_ingress_lower(coalesce(${extract(field)},'')))`));
  const columns=['scope','id','body','version',...fields.map(([field])=>sqliteFieldName(field)),...(table.search??[]).map((_,i)=>'s'+i)];
  const input=`WITH input AS MATERIALIZED(SELECT b.physical,jsonb(j.value) body FROM gv_snapshot_fast_input i JOIN gv_snapshot_typed b ON ${target} JOIN json_each(i.value,'$.canonicalRows') j)`;
  return {check:`${input} SELECT 1 FROM input r WHERE CASE WHEN ${checks.join(' AND ')} THEN 0 ELSE 1 END LIMIT 1`,
 insert:`${input} INSERT INTO ${q(sqliteTableName(entity))}(${columns.map(q).join(',')}) SELECT r.physical,${extract(table.key)},${sqliteBodySkeletonSQL('r.body',table)},CAST((SELECT json_extract(value,'$.cursor.revision') FROM gv_snapshot_fast_input WHERE id=1) AS INTEGER),${values.join(',')}
 FROM input r WHERE (SELECT replay FROM gv_snapshot_fast_state WHERE id=1)=0`,maxRid:`SELECT coalesce(max(rid),0) rid FROM ${q(sqliteTableName(entity))}`,
 ...(table.search?.length?{fts:`INSERT INTO ${q(sqliteSearchName(entity))}(rowid,${table.search.map((_,i)=>q('s'+i)).join(',')}) SELECT rid,${table.search.map((_,i)=>q('s'+i)).join(',')} FROM ${q(sqliteTableName(entity))} NOT INDEXED WHERE scope=? AND rid>? ORDER BY rid`}:{})};
 });
}
/** Owner-only setup, before accepting the first credited frame. Trusted native
 * plan admission supplies the entity; it is never inferred from row data. */
export async function prepareSQLiteTypedSnapshot(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema,input:{scope:string;subscription:string;snapshot:string;entity:string}):Promise<string>{
 sqlitePayloadBytes(input,8192);const table=Object.hasOwn(schema,input.entity)&&schema[input.entity];
 if(!table||table.fields[table.key]?.kind!=='text'||Object.keys(table.fields).length>128||!db.ingressNativeSnapshot||!db.ingressNativeFastFold)fail('INVALID_ROW','Typed snapshot is not supported by this generated table/host');
 if(!input.scope||input.scope.length>512||!input.subscription||input.subscription.length>256||!input.snapshot||input.snapshot.length>128)throw new TypeError('Invalid typed snapshot identity');
 const old=await db.getFirstAsync<{entity:string;physical:string}>('SELECT entity,json_quote(physical) physical FROM gv_snapshot_typed WHERE scope=? AND subscription=? AND snapshot=?',input.scope,input.subscription,input.snapshot);
 if(old){if(old.entity!==input.entity)fail('INTENT_CONFLICT','Typed snapshot entity changed');return JSON.parse(old.physical) as string;}
 if(await db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE name='gv_snapshot_activation_receipts' AND type='table'")&&await db.getFirstAsync('SELECT 1 FROM gv_snapshot_activation_receipts WHERE scope=? AND subscription=? AND snapshot=?',input.scope,input.subscription,input.snapshot))fail('PAGE_CHANGED','Snapshot publication receipt already exists');
 if(await db.getFirstAsync('SELECT 1 FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=?',input.scope,input.subscription,input.snapshot))fail('PAGE_CHANGED','Cannot bind an already started snapshot');
 const n=await db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_snapshot_typed');// Eight admitted connections may each own128 bounded source descriptors.
 // Stages live on disk; allow one obsolete generation per active source for
 // bounded idle cleanup, rather than rejecting a second45-table tenant.
 if(n!.n>=2048)fail('BUDGET','Collect obsolete typed snapshot stages first');
 const existing=await db.getFirstAsync<{n:number}>(`SELECT count(*) n FROM (SELECT 1 FROM ${q(sqliteTableName(input.entity))} LIMIT 513)`);
 if(existing!.n<=512){
  const markers=await db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_snapshot_index_deferred');if(markers!.n>=512)fail('BUDGET','Deferred index entities exceed allowance');
  await db.runAsync('INSERT OR IGNORE INTO gv_snapshot_index_deferred VALUES(?)',input.entity);
  for(let i=0;i<table.indexes.length;i++)await db.execAsync(`DROP INDEX IF EXISTS ${q(sqliteTableName(input.entity)+'_i_'+i)}`);
 }
 const clock=await db.getFirstAsync<{value:number}>('INSERT INTO gv_snapshot_scope_clock VALUES(1,1) ON CONFLICT(id) DO UPDATE SET value=value+1 WHERE value<9007199254740991 RETURNING value');
 if(!clock||!Number.isSafeInteger(clock.value)||clock.value<1)fail('FENCED','Snapshot physical scope clock exhausted');
 const physical='\u0000s:'+clock.value.toString(36);await db.runAsync('INSERT INTO gv_snapshot_typed VALUES(?,?,?,?,?)',input.scope,input.subscription,input.snapshot,input.entity,physical);return physical;
}

/** Caller owns the snapshot publication transaction. Existing authority stays
 * in its old partition until bounded overlap merges have finished. */
export async function activateSQLiteTypedSnapshot(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema,input:{scope:string;subscription:string;snapshot:string;entity:string;sequence:number;rows:number}){
 if(!await db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE type='table' AND name='gv_snapshot_typed'"))return;
 const binding=await db.getFirstAsync<{entity:string;physical:string}>('SELECT entity,json_quote(physical) physical FROM gv_snapshot_typed WHERE scope=? AND subscription=? AND snapshot=?',input.scope,input.subscription,input.snapshot);
 if(!binding)return;
 const table=schema[input.entity];if(!table||binding.entity!==input.entity)fail('INVALID_ROW','Typed snapshot entity differs from its admitted plan');
 const physical=JSON.parse(binding.physical) as string,previous=await sqlitePhysicalEntityScope(db,input.scope,input.entity);
 if(previous===physical)fail('PAGE_CHANGED','Typed snapshot partition is already active');
 const name=q(sqliteTableName(input.entity)),selection='s.scope=? AND s.subscription=? AND s.snapshot=?',args=[input.scope,input.subscription,input.snapshot];
 const count=await db.getFirstAsync<{n:number}>(`SELECT count(*) n FROM ${name} WHERE scope=?`,physical);if(count!.n!==input.rows)fail('INVALID_ROW','Typed snapshot row count differs from its certificate');
 await db.runAsync('DELETE FROM temp.gv_snapshot_overlap');
 await db.runAsync(`INSERT INTO temp.gv_snapshot_overlap SELECT s.id,s.position FROM gv_snapshot_rows s WHERE ${selection} AND (
 EXISTS(SELECT 1 FROM ${name} t WHERE t.scope=? AND t.id=s.id)
 OR EXISTS(SELECT 1 FROM gv_canonical_tombstones t WHERE t.scope=? AND t.entity=? AND t.id=s.id)
 OR EXISTS(SELECT 1 FROM _gonvex_replica_meta m WHERE m.scope=? AND m.key='row:'||json_array(?,s.id)))`,...args,previous,input.scope,input.entity,input.scope,input.entity);
 const overlap=await db.getFirstAsync<{n:number}>('SELECT count(*) n FROM temp.gv_snapshot_overlap');
 await db.runAsync(`INSERT INTO _gonvex_replica_changes SELECT ?,'row',?,s.id,? FROM gv_snapshot_rows s WHERE ${selection}
 AND NOT EXISTS(SELECT 1 FROM temp.gv_snapshot_overlap o WHERE o.id=s.id) ON CONFLICT(scope,kind,entity,id) DO UPDATE SET sequence=excluded.sequence`,input.scope,input.entity,input.sequence,...args);
 const body=`(SELECT ${sqliteRowBodySQL(table,'typed')} FROM ${name} typed WHERE typed.scope=${literal(physical)} AND typed.id=gv_snapshot_rows.id)`;
 return {freshRows:input.rows-overlap!.n,body,async restoreIndexes(){
  // Canonical overlap writes still target the old partition. Copy their final
  // authority result, plus rows retained by other windows, entirely in SQL.
  const columns=['scope','id','body','version',...Object.keys(table.fields).map(sqliteFieldName),...(table.search??[]).map((_,i)=>'s'+i)];
  const rest=columns.slice(1).map(column=>'old.'+q(column)).join(',');
  await db.runAsync(`DELETE FROM ${name} WHERE scope=? AND id IN (SELECT id FROM temp.gv_snapshot_overlap)`,physical);
  await db.runAsync(`INSERT INTO ${name}(${columns.map(q).join(',')}) SELECT ?,${rest} FROM ${name} old WHERE old.scope=?
   AND (EXISTS(SELECT 1 FROM temp.gv_snapshot_overlap o WHERE o.id=old.id) OR NOT EXISTS(SELECT 1 FROM gv_snapshot_rows s WHERE ${selection} AND s.id=old.id))`,physical,previous,...args);
  const indexes=compileSQLiteTypedSchema({[input.entity]:table}).indexes;if(indexes)await db.execAsync(indexes);
  await db.runAsync('DELETE FROM gv_snapshot_index_deferred WHERE entity=?',input.entity);
  if(table.search?.length&&await db.getFirstAsync('SELECT 1 FROM gv_fts_deferred WHERE scope=? AND entity=?',physical,input.entity)){
   const columns=table.search.map((_,i)=>q('s'+i)).join(','),fts=q(sqliteSearchName(input.entity));
   let after=0;for(;;){const last=(await db.getFirstAsync<{last:number|null}>(`SELECT max(rid) last FROM (SELECT rid FROM ${name} NOT INDEXED WHERE scope=? AND rid>? ORDER BY rid LIMIT 512)`,physical,after))?.last;if(last==null)break;
    await db.runAsync(`INSERT INTO ${fts}(rowid,${columns}) SELECT rid,${columns} FROM ${name} NOT INDEXED WHERE scope=? AND rid>? AND rid<=? ORDER BY rid`,physical,after,last);after=last;
   }
   await db.runAsync('DELETE FROM gv_fts_deferred WHERE scope=? AND entity=?',physical,input.entity);
  }
  await db.runAsync('INSERT INTO gv_entity_alias VALUES(?,?,?) ON CONFLICT(scope,entity) DO UPDATE SET physical=excluded.physical',input.scope,input.entity,physical);
  await db.runAsync('INSERT INTO gv_entity_retired VALUES(?,?,?) ON CONFLICT(entity,physical) DO NOTHING',input.scope,input.entity,previous);
 }};
}


export class SQLiteTypedSnapshotWriter {
 private readonly ledger=new ByteLedger(262144);
 private readonly plans=new BoundedByteCache<string,SnapshotWritePlan>('typed-snapshot-plans',262144,16,this.ledger);
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly schema:SQLiteTypedSchema){}
 stats(){return this.plans.stats();}
 async append(){
  const db=this.db,schema=this.schema;
  const row=await db.getFirstAsync<{entity:string;key:string;physical:string;replay:number}>(`SELECT b.entity,json_extract(i.value,'$.key') key,json_quote(b.physical) physical,(SELECT replay FROM gv_snapshot_fast_state WHERE id=1) replay FROM gv_snapshot_fast_input i JOIN gv_snapshot_typed b ON b.scope=i.scope AND b.subscription=json_extract(i.value,'$.id') AND b.snapshot=json_extract(i.value,'$.snapshotId')`);
  if(!row)return;const table=Object.hasOwn(schema,row.entity)&&schema[row.entity];if(!table||row.key!==table.key)fail('INVALID_ROW','Unknown typed snapshot entity or mismatched key');
  if(row.replay)return;
  let plan=this.plans.get(row.entity);if(!plan){plan=compileSQLiteTypedSnapshot({[row.entity]:table})[0];if(!plan)fail('INVALID_ROW','Unsupported typed snapshot entity');this.plans.put(row.entity,plan,(row.entity.length+plan.check.length+plan.insert.length+plan.maxRid.length+(plan.fts?.length??0))*2+512);}
  if(await db.getFirstAsync(plan.check))fail('INVALID_ROW','Snapshot field type differs from generated schema');
  const physical=JSON.parse(row.physical) as string;
  if(plan.fts&&!await db.getFirstAsync('SELECT 1 FROM gv_fts_deferred WHERE scope=? AND entity=?',physical,row.entity)){
   const before=(await db.getFirstAsync<{rid:number}>(plan.maxRid))!.rid;
   await db.runAsync('INSERT INTO gv_fts_deferred VALUES(?,?)',physical,row.entity);
   await db.runAsync(plan.insert);await db.runAsync(plan.fts,physical,before);
   await db.runAsync('DELETE FROM gv_fts_deferred WHERE scope=? AND entity=?',physical,row.entity);
  }else await db.runAsync(plan.insert);
 }
}


export async function sqliteTypedSnapshotBody(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema|undefined,scope:string,subscription:string,snapshot:string){
 if(!await db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE name='gv_snapshot_typed' AND type='table'"))return;
 const row=await db.getFirstAsync<{entity:string;physical:string}>('SELECT entity,json_quote(physical) physical FROM gv_snapshot_typed WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);if(!row)return;
 const table=schema&&Object.hasOwn(schema,row.entity)&&schema[row.entity];if(!table)fail('INVALID_ROW','Typed snapshot requires its generated schema');
 if((await db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE name='gv_snapshot_activation_receipts' AND type='table'")&&await db.getFirstAsync('SELECT 1 FROM gv_snapshot_activation_receipts WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot))||await db.getFirstAsync('SELECT 1 FROM gv_entity_alias WHERE entity=? AND physical=?',row.entity,JSON.parse(row.physical)))fail('PAGE_CHANGED','Typed snapshot has already become mutable canonical state');
 return `(SELECT ${sqliteRowBodySQL(table,'typed')} FROM ${q(sqliteTableName(row.entity))} typed WHERE typed.scope=${literal(JSON.parse(row.physical))} AND typed.id=gv_snapshot_rows.id)`;
}
/** Delete only a bounded private page. An active alias retains its canonical
 * partition when the obsolete hash/position stage is collected. */
export async function discardSQLiteTypedSnapshot(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema|undefined,scope:string,subscription:string,snapshot:string):Promise<boolean>{
 if(!await db.getFirstAsync("SELECT 1 FROM sqlite_master WHERE name='gv_snapshot_typed' AND type='table'"))return true;
 const row=await db.getFirstAsync<{entity:string;physical:string}>('SELECT entity,json_quote(physical) physical FROM gv_snapshot_typed WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);if(!row)return true;
 if(!schema||!Object.hasOwn(schema,row.entity))fail('INVALID_ROW','Typed stage cleanup requires its generated schema');const physical=JSON.parse(row.physical) as string;
 if(!await db.getFirstAsync('SELECT 1 FROM gv_entity_alias WHERE entity=? AND physical=?',row.entity,physical)){
  const name=q(sqliteTableName(row.entity));await db.runAsync(`DELETE FROM ${name} WHERE scope=? AND rid IN (SELECT rid FROM ${name} WHERE scope=? LIMIT 128)`,physical,physical);
  if(await db.getFirstAsync(`SELECT 1 FROM ${name} WHERE scope=? LIMIT 1`,physical))return false;
 }
 await db.runAsync('DELETE FROM gv_fts_deferred WHERE scope=? AND entity=?',physical,row.entity);
 await db.runAsync('DELETE FROM gv_snapshot_typed WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);
 if(!await db.getFirstAsync('SELECT 1 FROM gv_snapshot_typed WHERE entity=? LIMIT 1',row.entity)&&await db.getFirstAsync('SELECT 1 FROM gv_snapshot_index_deferred WHERE entity=?',row.entity)){
  const indexes=compileSQLiteTypedSchema({[row.entity]:schema[row.entity]!}).indexes;if(indexes)await db.execAsync(indexes);await db.runAsync('DELETE FROM gv_snapshot_index_deferred WHERE entity=?',row.entity);
 }return true;
}


/** Owner idle work, one bounded page in the caller's fenced transaction. */
export async function collectSQLiteRetiredEntityRows(db:SQLiteReplicaDatabase,schema:SQLiteTypedSchema,limit=128):Promise<boolean>{
 if(!Number.isSafeInteger(limit)||limit<1||limit>512)throw new RangeError('Invalid retired-row collection page');
 const retired=await db.getFirstAsync<{scope:string;entity:string;physical:string}>('SELECT scope,entity,json_quote(physical) physical FROM gv_entity_retired ORDER BY entity,physical LIMIT 1');if(!retired)return true;
 if(!Object.hasOwn(schema,retired.entity))fail('INVALID_ROW','Retired entity requires its generated schema');const physical=JSON.parse(retired.physical) as string;
 if(await db.getFirstAsync('SELECT 1 FROM gv_entity_alias WHERE entity=? AND physical=?',retired.entity,physical)||await db.getFirstAsync(`SELECT 1 FROM gv_scope_alias a WHERE a.physical=? AND NOT EXISTS(SELECT 1 FROM gv_entity_alias e WHERE e.scope=a.scope AND e.entity=?)`,physical,retired.entity))fail('FENCED','Retired partition still has an active alias');
 const table=q(sqliteTableName(retired.entity));await db.runAsync(`DELETE FROM ${table} WHERE scope=? AND rid IN (SELECT rid FROM ${table} WHERE scope=? LIMIT ?)`,physical,physical,limit);
 if(!await db.getFirstAsync(`SELECT 1 FROM ${table} WHERE scope=? LIMIT 1`,physical))await db.runAsync('DELETE FROM gv_entity_retired WHERE entity=? AND physical=?',retired.entity,physical);
 return !await db.getFirstAsync('SELECT 1 FROM gv_entity_retired LIMIT 1');
}
