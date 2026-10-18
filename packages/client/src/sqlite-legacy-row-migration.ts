import {SQLiteWindowStore} from './sqlite-window-store.js';
import type {ReplicaRow,LocalReplicaSession} from './local-replica.js';
import type {ReplicaRecordVersion,VersionedReplicaRecord} from './replica-record.js';
import {mergeReplicaRecord} from './replica-record.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import type {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
import {sqlitePhysicalEntityScope} from './sqlite-entity-scope.js';
import {sqliteIdentifier,sqliteTableName} from './sqlite-typed-schema.js';
import {decodedJSONBytes,fail,pageBudgetError,sqlitePayloadBytes} from './sqlite-admission.js';

type Input={source:SQLiteIndexedDBMigrationSource;identityScope:string;session:LocalReplicaSession};
type Progress={key:string;scope:string;epoch:string;total:number;seen:number;copied:number;superseded:number;deferred:number;ignored:number;after:string|null;root:string;finished:number};
type LegacyRow={scope:string;entity:string;id:string;value:string;authority?:ReplicaRecordVersion;deleted?:boolean};
type Row={source:LegacyRow;row:ReplicaRow|null;authority?:ReplicaRecordVersion};
type Change={entity:string;id:string;operation:'update'|'delete';newValue?:ReplicaRow};
const schema=`CREATE TABLE IF NOT EXISTS gv_legacy_row_migrations(key TEXT PRIMARY KEY,scope TEXT NOT NULL,epoch TEXT NOT NULL,total INTEGER NOT NULL,seen INTEGER NOT NULL,copied INTEGER NOT NULL,superseded INTEGER NOT NULL,deferred INTEGER NOT NULL,ignored INTEGER NOT NULL,after TEXT,root TEXT NOT NULL,finished INTEGER NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_legacy_row_receipts(migration TEXT NOT NULL,entity TEXT NOT NULL,id TEXT NOT NULL,source_hash TEXT NOT NULL,target_hash TEXT NOT NULL,status TEXT NOT NULL,PRIMARY KEY(migration,entity,id)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_legacy_row_verify ON gv_legacy_row_receipts(migration,status,entity,id);`;
const stable=(value:unknown):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(stable).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+stable((value as Record<string,unknown>)[key])).join(',')+'}';
async function hash(value:string){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');}
/** Background copy from a fenced legacy replica. Intents and the authenticated
 * session must have cut over first. Each bounded page verifies its actual SQL
 * readback before its resume cursor commits. Unstamped or obsolete-epoch rows
 * remain in the read-only source until a complete server certificate supersedes
 * them; they can never overwrite newer authority or grant table completeness. */
export class SQLiteLegacyRowMigration {
 private readonly initialized:Promise<void>;private verificationAfter?:[string,string];private verificationKey?:string;
 constructor(private readonly owner:SQLiteOwnerProgramContext,private readonly artifactHash:string){this.initialized=owner.runOwner(0,async()=>{await owner.canonical.ready();await new SQLiteWindowStore(owner.database,()=>owner.engine.verifyOwner()).ready();await owner.database.execAsync(schema);});void this.initialized.catch(()=>{});}
 ready(){return this.initialized;}
 private key(input:Input){sqlitePayloadBytes({identityScope:input.identityScope,session:input.session},65536);if(!input.identityScope||input.identityScope.length>512||input.session.artifactHash!==this.artifactHash)fail('SCHEMA_MISMATCH','Legacy row migration needs the admitted current artifact');return JSON.stringify([input.source.name,input.source.fenceVersion,input.identityScope,input.session.directive.visibilityScope,this.artifactHash]);}
 private async guard(input:Input){await this.owner.engine.verifyOwner();const saved=await this.owner.canonical.loadSession(input.identityScope);if(!saved||stable(saved)!==stable(input.session))fail('FENCED','Durable session must cut over before legacy replica rows');const epoch=await this.owner.database.getFirstAsync<{value:string}>("SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key='currentEpoch'",input.session.directive.visibilityScope);if(epoch&&epoch.value!==input.session.directive.epoch)fail('PAGE_CHANGED','Legacy rows must wait for the admitted canonical epoch');}
 private async begin(input:Input):Promise<Progress>{
  const key=this.key(input),scope=input.session.directive.visibilityScope,epoch=input.session.directive.epoch;await this.initialized;
  const old=await this.owner.runOwner(0,async()=>{await this.guard(input);return this.owner.database.getFirstAsync<Progress>('SELECT * FROM gv_legacy_row_migrations WHERE key=?',key);});if(old)return old;
  const total=await input.source.countScope('entities',scope);if(!Number.isSafeInteger(total)||total<0)fail('INTENT_CORRUPT','Invalid fenced replica row count');
  let begun!:Progress;await this.owner.runOwner(65536,()=>this.owner.database.withTransactionAsync(async()=>{await this.guard(input);await this.owner.database.runAsync('INSERT OR IGNORE INTO gv_legacy_row_migrations VALUES(?,?,?,?,0,0,0,0,0,NULL,?,0)',key,scope,epoch,total,'0'.repeat(64));begun=(await this.owner.database.getFirstAsync<Progress>('SELECT * FROM gv_legacy_row_migrations WHERE key=?',key))!;}));return begun;
 }
 private row(value:unknown,scope:string):Row{
  sqlitePayloadBytes(value,131072);const record=value as LegacyRow;if(!record||record.scope!==scope||typeof record.entity!=='string'||!record.entity||record.entity.length>256||typeof record.id!=='string'||!record.id||record.id.length>1024||typeof record.value!=='string')throw new TypeError('Invalid normalized legacy row');decodedJSONBytes(record.value,131072);const parsed=JSON.parse(record.value) as ReplicaRow|null;
  if(parsed!==null&&(typeof parsed!=='object'||Array.isArray(parsed)))throw new TypeError('Invalid legacy row body');if(record.deleted&&parsed!==null)fail('INTENT_CORRUPT','Legacy tombstone contains row data');
  const table=Object.hasOwn(this.owner.engine.schema,record.entity)?this.owner.engine.schema[record.entity]:undefined,row=parsed&&table?Object.fromEntries(Object.keys(parsed).filter(field=>Object.hasOwn(table.fields,field)).map(field=>[field,parsed[field]!])):parsed;
  if(row&&table&&String(row[table.key])!==record.id)fail('INTENT_CORRUPT','Legacy row key differs from its envelope');
  const authority=record.authority;if(authority){if(typeof authority.epoch!=='string'||!authority.epoch||authority.epoch.length>256||!authority.fields||typeof authority.fields!=='object'||Array.isArray(authority.fields))throw new TypeError('Invalid legacy row authority');for(const revision of [...Object.values(authority.fields),...(authority.base===undefined?[]:[authority.base]),...(authority.deleted===undefined?[]:[authority.deleted])])if(!Number.isSafeInteger(revision)||revision<0)throw new TypeError('Invalid legacy authority revision');}
  return {source:record,row,authority};
 }
 private coverage(scope:string,entity:string,epoch:string){
  const columns=JSON.stringify(Object.keys(this.owner.engine.schema[entity]!.fields));
  return this.owner.database.getFirstAsync<{revision:number;epoch:string;header:string}>(`SELECT json_extract(g.header,'$.cursor.revision') revision,json_extract(g.header,'$.cursor.epoch') epoch,g.header
   FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation) WHERE w.scope=? AND json_extract(g.header,'$.entity')=?
    AND json_extract(g.header,'$.entityCoverage')='complete' AND json_extract(g.header,'$.completeness')='complete'
    AND json_extract(g.header,'$.artifactHash')=? AND json_extract(g.header,'$.cursor.epoch')=?
    AND NOT EXISTS(SELECT 1 FROM json_each(?) required WHERE NOT EXISTS(SELECT 1 FROM json_each(g.header,'$.columns') actual WHERE actual.value=required.value))
   ORDER BY revision DESC LIMIT 1`,scope,entity,this.artifactHash,epoch,columns);
 }
 private async reconcileDeferred(input:Input,previous:Progress):Promise<{progress:Progress;verified:boolean}>{
  if(this.verificationKey!==previous.key){this.verificationKey=previous.key;this.verificationAfter=undefined;}let progress!:Progress,nextAfter=this.verificationAfter;await this.owner.runOwner(262144,()=>this.owner.database.withTransactionAsync(async()=>{
   await this.guard(input);const current=await this.owner.database.getFirstAsync<Progress>('SELECT * FROM gv_legacy_row_migrations WHERE key=?',previous.key);if(!current||current.root!==previous.root)fail('FENCED','Legacy verification advanced concurrently');progress={...current};
   const read=(after:[string,string]|undefined)=>this.owner.database.getAllAsync<{entity:string;id:string;source_hash:string}>("SELECT entity,id,source_hash FROM gv_legacy_row_receipts WHERE migration=? AND status='deferred' AND (entity,id)>(?,?) ORDER BY entity,id LIMIT 16",progress.key,...(after??['','']));let deferred=await read(this.verificationAfter);if(!deferred.length&&this.verificationAfter)deferred=await read(undefined);nextAfter=deferred.length?[deferred.at(-1)!.entity,deferred.at(-1)!.id]:undefined;
   for(const row of deferred){const certificate=await this.coverage(progress.scope,row.entity,progress.epoch);if(!certificate)continue;const proof=await hash(certificate.header);await this.owner.database.runAsync("UPDATE gv_legacy_row_receipts SET status='superseded',target_hash=? WHERE migration=? AND entity=? AND id=? AND status='deferred'",proof,progress.key,row.entity,row.id);progress.deferred--;progress.superseded++;progress.root=await hash(progress.root+stable({key:[row.entity,row.id],source:row.source_hash,certificate:proof}));}
   await this.guard(input);await this.owner.database.runAsync('UPDATE gv_legacy_row_migrations SET deferred=?,superseded=?,root=? WHERE key=?',progress.deferred,progress.superseded,progress.root,progress.key);
  }));this.verificationAfter=nextAfter;return {progress,verified:progress.finished===1&&progress.deferred===0};
 }
 /** One page/transaction per call. Caller schedules subsequent steps outside
  * the owner FIFO, letting first-sync frames and user intents make progress. */
 async step(input:Input):Promise<{progress:Progress;verified:boolean}>{
  const captured={...input,session:structuredClone(input.session)},progress=await this.begin(captured);if(progress.finished)return progress.deferred?this.reconcileDeferred(captured,progress):{progress,verified:true};let limit=16;
  for(;;){try{
   const after=progress.after===null?undefined:JSON.parse(progress.after) as IDBValidKey,page=await captured.source.scopePage('entities',progress.scope,after,limit,131072),rows=page.records.map(record=>{const row=this.row(record.value,progress.scope);if(!Array.isArray(record.key)||record.key.length!==3||record.key[0]!==progress.scope||record.key[1]!==row.source.entity||record.key[2]!==row.source.id)fail('INTENT_CORRUPT','Legacy compound identity differs from its body');return row;});
   sqlitePayloadBytes(rows,262144);let result!:Progress;
   await this.owner.runOwner(1048576,()=>this.owner.database.withTransactionAsync(async()=>{
    await this.guard(captured);const current=await this.owner.database.getFirstAsync<Progress>('SELECT * FROM gv_legacy_row_migrations WHERE key=?',progress.key);if(!current||current.after!==progress.after||current.seen!==progress.seen)fail('FENCED','Legacy row migration advanced concurrently');
    const next={...current},changes=new Map<number,Map<string,Change>>(),expected=new Map<string,VersionedReplicaRecord>(),selected:Array<{entry:Row;status:string;sourceHash:string}>=[];
    const add=(revision:number,change:Change)=>{let group=changes.get(revision);if(!group)changes.set(revision,group=new Map());const key=JSON.stringify([change.entity,change.id]),prior=group.get(key);group.set(key,prior&&prior.operation!=='delete'&&change.operation!=='delete'?{...change,newValue:{...prior.newValue,...change.newValue}}:change);};
    for(const entry of rows){
     const {source,row,authority}=entry,key=JSON.stringify([source.entity,source.id]),table=Object.hasOwn(this.owner.engine.schema,source.entity)?this.owner.engine.schema[source.entity]:undefined,sourceHash=await hash(stable({row,authority:authority??null}));let status='copied';
     if(!table){next.ignored++;status='ignored';}
     else{
      const coverage=await this.coverage(progress.scope,source.entity,progress.epoch);
      const prior=(await this.owner.canonical.loadBoundedEntityRows(progress.scope,source.entity,[source.id]))[0]?.row??null;
      const highest=authority?Math.max(authority.base??0,authority.deleted??0,...Object.values(authority.fields)):Number.MAX_SAFE_INTEGER;
      if(coverage?.revision!==null&&coverage?.revision!==undefined&&(!authority||authority.epoch!==progress.epoch||coverage.revision>=highest&&!prior)){next.superseded++;status='superseded';}
      else if(!authority||authority.epoch!==progress.epoch){next.deferred++;status='deferred';}
      else{
       const metadata=await this.owner.database.getFirstAsync<{value:string}>('SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key=?',progress.scope,'row:'+key),physical=await sqlitePhysicalEntityScope(this.owner.database,progress.scope,source.entity),version=await this.owner.database.getFirstAsync<{version:number}>(`SELECT version FROM ${sqliteIdentifier(sqliteTableName(source.entity))} WHERE scope=? AND id=?`,physical,source.id);
       if(metadata)decodedJSONBytes(metadata.value,131072);const before:VersionedReplicaRecord={row:prior,authority:metadata?JSON.parse(metadata.value):{epoch:progress.epoch,base:version?.version??0,fields:{}}};expected.set(key,before);
       if(row===null){if(authority.deleted===undefined)fail('INTENT_CORRUPT','Legacy tombstone has no authority');add(authority.deleted,{entity:source.entity,id:source.id,operation:'delete'});}
       else for(const [field,value] of Object.entries(row)){const revision=Object.hasOwn(authority.fields,field)?authority.fields[field]:authority.base;if(revision===undefined)fail('INTENT_CORRUPT','Legacy projected field has no authority');add(revision,{entity:source.entity,id:source.id,operation:'update',newValue:{[table.key]:row[table.key]!,[field]:value}});}
       next.copied++;
      }
     }selected.push({entry,status,sourceHash});
    }
    for(const revision of [...changes.keys()].sort((a,b)=>a-b)){const group=[...changes.get(revision)!.values()];for(const change of group){const key=JSON.stringify([change.entity,change.id]);expected.set(key,mergeReplicaRecord(expected.get(key),change.operation==='delete'?null:change.newValue!,{epoch:progress.epoch,revision}));}await this.owner.canonical.applyTransaction({cursor:{epoch:progress.epoch,revision},changes:group},{entities:{},liveQueries:{}},progress.scope);}
    for(const item of selected){const {source}=item.entry,key=JSON.stringify([source.entity,source.id]);let targetHash='';if(item.status==='copied'){const wanted=expected.get(key)!,actual=(await this.owner.canonical.loadBoundedEntityRows(progress.scope,source.entity,[source.id]))[0]?.row??null;const metadata=await this.owner.database.getFirstAsync<{value:string}>('SELECT value FROM _gonvex_replica_meta WHERE scope=? AND key=?',progress.scope,'row:'+key);if(!metadata)fail('INTENT_CORRUPT','Migrated SQLite authority is missing');decodedJSONBytes(metadata.value,131072);targetHash=await hash(stable({row:actual,authority:JSON.parse(metadata.value)}));if(targetHash!==await hash(stable(wanted)))fail('INTENT_CORRUPT','Migrated SQLite row or authority hash differs from its merge');}
     await this.owner.database.runAsync('INSERT INTO gv_legacy_row_receipts VALUES(?,?,?,?,?,?)',progress.key,source.entity,source.id,item.sourceHash,targetHash,item.status);next.root=await hash(next.root+stable({key,source:item.sourceHash,target:targetHash,status:item.status}));next.seen++;
    }
    next.after=page.records.length?JSON.stringify(page.records.at(-1)!.key):progress.after;if(page.nextKey===null){if(next.seen!==next.total)fail('INTENT_CORRUPT','Fenced legacy row count changed');next.finished=1;}
    await this.guard(captured);await this.owner.database.runAsync('UPDATE gv_legacy_row_migrations SET seen=?,copied=?,superseded=?,deferred=?,ignored=?,after=?,root=?,finished=? WHERE key=?',next.seen,next.copied,next.superseded,next.deferred,next.ignored,next.after,next.root,next.finished,next.key);result=next;
   }),[progress.scope]);return {progress:result,verified:!!result.finished&&result.deferred===0};
  }catch(error){if(limit===1||!pageBudgetError(error))throw error;limit=Math.max(1,Math.floor(limit/2));}}
 }
}
