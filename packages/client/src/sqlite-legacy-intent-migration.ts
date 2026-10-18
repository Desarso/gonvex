import type {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
import type {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import type {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import type {SQLiteReplicaDatabase,SQLiteLocalReplicaStorage} from './sqlite-replica-storage.js';
import type {LocalReplicaSession} from './local-replica.js';
import type {ReducerOutboxEntry} from './outbox.js';
import type {JsonValue} from '@gonvex/protocol';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
type Input={source:SQLiteIndexedDBMigrationSource;store:string;identityScope:string;session:LocalReplicaSession};
type Options={database:SQLiteReplicaDatabase;canonical:SQLiteLocalReplicaStorage;journal:SQLiteIntentJournal;safety:SQLiteSafetyJournal;fence:()=>Promise<unknown>};
/** Run before enabling the native scope. The old source is already fenced.
 * Every intent/session first enters independent IDB durability, then the
 * complete SQLite intent/session cutover commits atomically. Replica rows are
 * deliberately a later migration phase. Retrying either phase is idempotent. */
export class SQLiteLegacyIntentMigration {
 private ready?:Promise<void>;
 constructor(private readonly options:Options){}
 private initialize(){return this.ready??=(async()=>{await this.options.journal.ready();await this.options.canonical.ready();await this.options.database.withTransactionAsync(async()=>{await this.options.fence();await this.options.database.execAsync('CREATE TABLE IF NOT EXISTS gv_legacy_intent_migrations(key TEXT PRIMARY KEY,scope TEXT NOT NULL,source_rows INTEGER NOT NULL,copied INTEGER NOT NULL) WITHOUT ROWID');});})().catch(error=>{this.ready=undefined;throw error;});}
 async migrate(input:Input):Promise<{sourceRows:number;selected:number;copied:number;previouslySettled:number;alreadyCopied:boolean}>{
  sqlitePayloadBytes({identityScope:input.identityScope,session:input.session,store:input.store},131072);
  const scope=input.session.directive.visibilityScope;
  if(!input.identityScope||input.identityScope.length>512||typeof scope!=='string'||!scope||scope.length>512||scope.includes('\u0000'))throw new TypeError('Invalid legacy intent migration scope');
  const session=JSON.parse(JSON.stringify(input.session)) as LocalReplicaSession;
  const key=JSON.stringify([input.source.name,input.source.fenceVersion,input.store,input.identityScope]);
  let after:IDBValidKey|undefined,sourceRows=0,selected=0,previouslySettled=0;
  await this.options.safety.saveSession(input.identityScope,session);
  // Only the already authenticated identity mapping may select old records.
  // Other identities remain untouched in the fenced source, never guessed.
  for(;;){
   const page=await input.source.page(input.store,after,16,131072);
   for(const record of page.records){
    sourceRows++;const entry=record.value as ReducerOutboxEntry;
    if(!entry||entry.scope!==input.identityScope)continue;
    sqlitePayloadBytes(entry,131072);
    if(typeof entry.idempotencyKey!=='string'||!entry.idempotencyKey||typeof entry.path!=='string'||!Number.isSafeInteger(entry.createdAt))throw new TypeError('Invalid fenced legacy intent');
    if(entry.localExecution&&(entry.localExecution.identity.auth.account?.id!==session.identity.auth.account?.id||entry.localExecution.identity.tenant?.id!==session.identity.tenant?.id))fail('FENCED','Legacy intent identity differs from its cached session');
    const invocation={scope,path:entry.path,commandId:entry.idempotencyKey,args:entry.args as JsonValue,now:entry.localExecution?.now??entry.createdAt,...(entry.localExecution?.intentEntropy===undefined?{}:{intentEntropy:entry.localExecution.intentEntropy})};
    const receipt=await this.options.safety.appendLegacy(invocation,entry,session,input.identityScope);selected++;previouslySettled+=Number(receipt.settled);
   }
   if(page.nextKey===null)break;after=page.nextKey;
  }
  await this.initialize();let copied=0,alreadyCopied=false;
  await this.options.database.withTransactionAsync(async()=>{
   await this.options.fence();
   const old=await this.options.database.getFirstAsync<{source_rows:number;copied:number}>('SELECT source_rows,copied FROM gv_legacy_intent_migrations WHERE key=?',key);
   if(old){if(old.source_rows!==sourceRows)fail('FENCED','Fenced migration source changed');alreadyCopied=true;copied=old.copied;return;}
   await this.options.canonical.saveSession(input.identityScope,session);
   if(JSON.stringify(await this.options.canonical.loadSession(input.identityScope))!==JSON.stringify(session))fail('FENCED','Migrated session verification differs');
   let sequence=0;
   for(;;){
    const page=await this.options.safety.page(input.identityScope,sequence);
    for(const record of page.entries){
     const legacy=record.legacy;if(!legacy||legacy.scope!==input.identityScope)continue;
     const {id:_legacyID,...draft}=legacy;
     await this.options.journal.append({...draft,scope:input.identityScope,state:legacy.state==='inflight'?'pending':legacy.state,...(legacy.localExecution?{localExecution:{...legacy.localExecution,scope}}:{})});copied++;
    }
    if(page.nextSequence===null)break;sequence=page.nextSequence;
   }
   if(copied+previouslySettled!==selected)fail('FENCED','Legacy intent migration count differs');
   await this.options.database.runAsync('INSERT INTO gv_legacy_intent_migrations VALUES(?,?,?,?)',key,scope,sourceRows,copied);
  });return {sourceRows,selected,copied,previouslySettled,alreadyCopied};
 }
}
