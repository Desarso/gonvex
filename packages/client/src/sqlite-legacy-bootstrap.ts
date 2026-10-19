import type {LocalReplicaSession} from './local-replica.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import type {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
import {SQLiteLegacyIntentMigration} from './sqlite-legacy-intent-migration.js';
import {SQLiteLegacyRowMigration} from './sqlite-legacy-row-migration.js';
import {decodedJSONBytes,fail} from './sqlite-admission.js';

type Options={owner:SQLiteOwnerProgramContext;journal:SQLiteIntentJournal;artifactHash:string;replicaName:string;outboxName:string;factory?:IDBFactory;ranges?:typeof IDBKeyRange};
/** Owner-only legacy cutover. Finish prepare before admitting this identity's
 * native reads or writes. Replica copying runs one bounded step per background
 * turn, independently of the first visible page. Source databases stay intact. */
export class SQLiteLegacyBootstrap {
 private sources?:Promise<{replica?:SQLiteIndexedDBMigrationSource;outbox?:SQLiteIndexedDBMigrationSource}>;
 private readonly intents:SQLiteLegacyIntentMigration;
 private readonly rows:SQLiteLegacyRowMigration;
 private closed=false;
 constructor(private readonly options:Options){
  this.intents=new SQLiteLegacyIntentMigration({database:options.owner.database,canonical:options.owner.canonical,journal:options.journal,runOwner:options.owner.runOwner,fence:()=>options.owner.engine.verifyOwner()});
  this.rows=new SQLiteLegacyRowMigration(options.owner,options.artifactHash);
 }
 private open(){
  if(this.closed)fail('FENCED','Legacy bootstrap is closed');
  if(!this.sources){const operation=(async()=>{let replica:SQLiteIndexedDBMigrationSource|undefined,outbox:SQLiteIndexedDBMigrationSource|undefined;try{
   replica=await SQLiteIndexedDBMigrationSource.open(this.options.replicaName,this.options.factory,this.options.ranges);
   if(this.closed)fail('FENCED','Legacy bootstrap is closed');
   outbox=await SQLiteIndexedDBMigrationSource.open(this.options.outboxName,this.options.factory,this.options.ranges);
   if(this.closed)fail('FENCED','Legacy bootstrap is closed');return {replica,outbox};
  }catch(error){replica?.close();outbox?.close();throw error;}})();this.sources=operation;void operation.catch(()=>{if(this.sources===operation)this.sources=undefined;});}return this.sources;
 }
 /** A current authoritative session may be supplied after reconnect. An old
  * cached artifact cannot authorize offline reads with new generated code. */
 async prepare(identityScope:string,session?:LocalReplicaSession,current:()=>boolean=()=>true):Promise<LocalReplicaSession|undefined>{
  const check=()=>{if(this.closed||!current())fail('FENCED','Legacy bootstrap admission changed');};check();
  const source=await this.open();check();let selected=session;
  if(!selected&&source.replica?.stores().includes('meta')){
   const record=await source.replica.get('meta',[identityScope,'session']) as {scope?:string;key?:string;value?:unknown}|undefined;
   if(record){if(record.scope!==identityScope||record.key!=='session'||typeof record.value!=='string')fail('INTENT_CORRUPT','Legacy session envelope differs from its key');decodedJSONBytes(record.value,32768);selected=JSON.parse(record.value) as LocalReplicaSession;}
  }
  if(!selected)return;
  if(selected.artifactHash!==this.options.artifactHash)fail('SCHEMA_MISMATCH','Legacy offline session requires authoritative authentication for the current artifact');
  if(this.closed)fail('FENCED','Legacy bootstrap is closed');
  if(source.outbox?.stores().includes('entries')){const input={source:source.outbox,store:'entries',identityScope,session:selected,current};if(!await this.intents.hasCutover(input))await this.intents.migrate(input);}
  else{
   await this.options.owner.runOwner(32768,()=>this.options.owner.database.withTransactionAsync(async()=>{
    await this.options.owner.engine.verifyOwner();check();const saved=await this.options.owner.canonical.loadSession(identityScope);
    if(saved&&JSON.stringify(saved)!==JSON.stringify(selected))fail('FENCED','A newer native admission won legacy migration');
    await this.options.owner.canonical.saveSession(identityScope,selected!);check();
   }));
  }
  check();return structuredClone(selected);
 }
 async step(identityScope:string,session:LocalReplicaSession){
  const source=await this.open();if(!source.replica?.stores().includes('entities'))return {verified:true};
  return this.rows.step({source:source.replica,identityScope,session});
 }
 close(){if(this.closed)return;this.closed=true;void this.sources?.then(source=>{source.replica?.close();source.outbox?.close();},()=>{});}
}
