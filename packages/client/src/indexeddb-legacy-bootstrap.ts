import type {LocalReplicaSession} from './local-replica.js';
import type {ReducerOutboxEntry} from './outbox.js';
import type {JsonValue} from '@gonvex/protocol';
import {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
import {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {IndexedDBReplicaEngine,type IndexedDBLegacyRow} from './indexeddb-replica-engine.js';
import {decodedJSONBytes,fail,sqlitePayloadBytes} from './sqlite-admission.js';

type Sources={replica?:SQLiteIndexedDBMigrationSource;outbox?:SQLiteIndexedDBMigrationSource};
const stable=(value:unknown):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(stable).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+stable((value as Record<string,unknown>)[key])).join(',')+'}';
const hash=async(value:string)=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');
/** Fences old SDK databases before an IndexedDB owner admits an identity. The
 * independent safety journal receives every old command before the new owner
 * restores a cached session. Interrupted scans are idempotent; sources remain
 * read-only and are never deleted as part of the cutover. */
export class IndexedDBLegacyBootstrap {
 private opening?:Promise<Sources>;private closed=false;
 constructor(private readonly safety:SQLiteSafetyJournal,private readonly artifactHash:string,private readonly replicaName='gonvex-local-replica',private readonly outboxName='gonvex-outbox',private readonly factory:IDBFactory=globalThis.indexedDB,private readonly ranges:typeof IDBKeyRange=globalThis.IDBKeyRange){}
 private open(){
  if(this.closed)fail('FENCED','Fallback legacy bootstrap closed');
  if(!this.opening){const operation=(async()=>{let replica:SQLiteIndexedDBMigrationSource|undefined,outbox:SQLiteIndexedDBMigrationSource|undefined;try{replica=await SQLiteIndexedDBMigrationSource.open(this.replicaName,this.factory,this.ranges);if(this.closed)fail('FENCED','Fallback legacy bootstrap closed');outbox=await SQLiteIndexedDBMigrationSource.open(this.outboxName,this.factory,this.ranges);if(this.closed)fail('FENCED','Fallback legacy bootstrap closed');return {replica,outbox};}catch(error){replica?.close();outbox?.close();throw error;}})();this.opening=operation;void operation.catch(()=>{if(this.opening===operation)this.opening=undefined;});}return this.opening;
 }
 async prepare(intentScope:string,session?:LocalReplicaSession,current:()=>boolean=()=>true){
  const check=()=>{if(this.closed||!current())fail('FENCED','Fallback legacy migration admission changed');};check();if(!intentScope||intentScope.length>512)throw new TypeError('Invalid fallback legacy identity');
  const sources=await this.open();check();let selected=session;
  if(!selected&&sources.replica?.stores().includes('meta')){const record=await sources.replica.get('meta',[intentScope,'session']) as {scope?:string;key?:string;value?:unknown}|undefined;if(record){if(record.scope!==intentScope||record.key!=='session'||typeof record.value!=='string')fail('INTENT_CORRUPT','Fallback legacy session key differs');decodedJSONBytes(record.value,32768);selected=JSON.parse(record.value) as LocalReplicaSession;}}
  if(!selected)return;
  if(selected.artifactHash!==this.artifactHash)fail('SCHEMA_MISMATCH','Fallback legacy session needs current artifact');
  if(sources.outbox?.stores().includes('entries')){let after:IDBValidKey|undefined;for(;;){const page=await sources.outbox.indexedScopePage('entries',intentScope,after,16,131072);for(const record of page.records){const entry=record.value as ReducerOutboxEntry;if(!entry||entry.scope!==intentScope)fail('INTENT_CORRUPT','Fallback legacy outbox scope differs');sqlitePayloadBytes(entry,131072);if(typeof entry.idempotencyKey!=='string'||!entry.idempotencyKey||typeof entry.path!=='string'||!Number.isSafeInteger(entry.createdAt))throw new TypeError('Invalid fallback legacy intent');if(entry.localExecution&&(entry.localExecution.identity.auth.account?.id!==selected.identity.auth.account?.id||entry.localExecution.identity.tenant?.id!==selected.identity.tenant?.id))fail('FENCED','Fallback legacy intent identity differs');const invocation={scope:selected.directive.visibilityScope,path:entry.path,commandId:entry.idempotencyKey,args:entry.args as JsonValue,now:entry.localExecution?.now??entry.createdAt,...(entry.localExecution?.intentEntropy===undefined?{}:{intentEntropy:entry.localExecution.intentEntropy})};check();await this.safety.appendLegacy(invocation,entry,selected,intentScope);check();}if(page.nextKey===null)break;after=page.nextKey;}}
  await this.safety.seedSession(intentScope,selected);check();return structuredClone(selected);
 }
 /** One source page per call. The caller schedules the next turn outside the
  * owner FIFO so native bootstrap and interactions can make progress. */
 async stepRows(engine:IndexedDBReplicaEngine,intentScope:string,session:LocalReplicaSession,current:()=>boolean=()=>true){
  const check=()=>{if(this.closed||!current())fail('FENCED','Fallback legacy row admission changed');};check();const sources=await this.open(),source=sources.replica;if(!source?.stores().includes('entities'))return {verified:true,progress:null};
  if(session.artifactHash!==this.artifactHash)fail('SCHEMA_MISMATCH','Fallback legacy rows need current artifact');const admitted=await this.safety.session(intentScope);if(!admitted||stable(admitted)!==stable(session))fail('FENCED','Fallback legacy row migration session changed');const scope=session.directive.visibilityScope,epoch=session.directive.epoch,key=[source.name,source.fenceVersion,scope].join('\u0000'),previous=await engine.legacyProgress(key);if(previous?.finished)return {verified:previous.deferred===0&&previous.ignored===0,progress:previous};
  const total=previous?.total??await source.countScope('entities',scope),after=previous?.after??null,page=await source.scopePage('entities',scope,after===null?undefined:JSON.parse(after),16,131072),rows:IndexedDBLegacyRow[]=[];
  for(const record of page.records){const value=record.value as {scope?:string;entity?:string;id?:string;value?:string;authority?:IndexedDBLegacyRow['authority'];deleted?:boolean};if(!Array.isArray(record.key)||record.key.length!==3||value.scope!==scope||record.key[0]!==scope||record.key[1]!==value.entity||record.key[2]!==value.id||typeof value.value!=='string')fail('INTENT_CORRUPT','Fallback legacy row key differs from its body');decodedJSONBytes(value.value,131072);const row=JSON.parse(value.value);if(row!==null&&(typeof row!=='object'||Array.isArray(row))||value.deleted&&row!==null)fail('INTENT_CORRUPT','Fallback legacy row value is invalid');rows.push({scope,entity:value.entity!,id:value.id!,row,authority:value.authority,sourceHash:await hash(stable({row,authority:value.authority??null}))});}
  check();const next=page.records.length?JSON.stringify(page.records.at(-1)!.key):after,progress=await engine.importLegacyRows(key,epoch,total,after,rows,next,page.nextKey===null,current);check();return {verified:progress.finished&&progress.deferred===0&&progress.ignored===0,progress};
 }
 close(){if(this.closed)return;this.closed=true;void this.opening?.then(sources=>{sources.replica?.close();sources.outbox?.close();},()=>{});}
}
