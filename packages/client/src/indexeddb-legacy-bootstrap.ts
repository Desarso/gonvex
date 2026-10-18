import type {LocalReplicaSession} from './local-replica.js';
import type {ReducerOutboxEntry} from './outbox.js';
import type {JsonValue} from '@gonvex/protocol';
import {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
import {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {decodedJSONBytes,fail,sqlitePayloadBytes} from './sqlite-admission.js';

type Sources={replica?:SQLiteIndexedDBMigrationSource;outbox?:SQLiteIndexedDBMigrationSource};
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
 close(){if(this.closed)return;this.closed=true;void this.opening?.then(sources=>{sources.replica?.close();sources.outbox?.close();},()=>{});}
}
