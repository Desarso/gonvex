import {DatabaseSync} from 'node:sqlite';
import {expect,it} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
import {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import {SQLiteLegacyIntentMigration} from './sqlite-legacy-intent-migration.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {LocalReplicaSession} from './local-replica.js';
const identityScope='identity\u0000ws://private\u0000project\u0000tenant\u0000account';
const session:LocalReplicaSession={directive:{protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'e'},identity:{auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}},artifactHash:'artifact'};
const tables={tasks:{key:'id',fields:{id:{kind:'text'},title:{kind:'text'}},indexes:[]}} as const;
async function sqlite(safety:SQLiteSafetyJournal){
 const sql=new DatabaseSync(':memory:');const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const engine=new SQLiteTypedReplicaEngine(db,tables);await engine.open('migrate');const canonical=sqliteTypedReplicaStorage(db,tables),journal=new SQLiteIntentJournal(db,()=>engine.verifyOwner());await canonical.ready();await journal.ready();const migration=new SQLiteLegacyIntentMigration({database:db,canonical,journal,safety,fence:()=>engine.verifyOwner()});return {sql,db,journal,canonical,migration};
}
async function source(factory:IDBFactory){
 const db=await new Promise<IDBDatabase>((resolve,reject)=>{const opening=factory.open('old-outbox',20);opening.onupgradeneeded=()=>opening.result.createObjectStore('entries',{keyPath:'id'});opening.onsuccess=()=>resolve(opening.result);opening.onerror=()=>reject(opening.error);});
 await new Promise<void>((resolve,reject)=>{const tx=db.transaction('entries','readwrite');for(let n=1;n<=30;n++)tx.objectStore('entries').add({id:n,scope:n%7===0?'foreign-account':identityScope,path:'tasks/create',idempotencyKey:'intent-'+n,args:{title:'Synthetic '+n},entityKeys:['tasks:stable-'+n],patches:[{entity:'tasks',rowId:'stable-'+n,op:'insert',fields:{id:'stable-'+n,title:'Synthetic '+n}}],localExecution:{scope:'visible',commandId:'intent-'+n,now:100+n,identity:session.identity,artifactHash:'old-artifact'},createdAt:100+n,attempts:n===1?2:0,nextAttemptAt:100+n,state:n===1?'inflight':n===2?'failed':n===3?'rejected':'pending',...(n===1?{sentAt:110}:{})});tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});db.close();return (await SQLiteIndexedDBMigrationSource.open('old-outbox',factory,IDBKeyRange))!;
}
it('moves fenced sessions and original offline intents before replica rows, with exact identity isolation and resumable settlement',async()=>{
 const factory=new IDBFactory(),safety=new SQLiteSafetyJournal('safety',factory,IDBKeyRange),legacy=await source(factory),f=await sqlite(safety);let recovered:Awaited<ReturnType<typeof sqlite>>|undefined;
 try{
  const input={source:legacy,store:'entries',identityScope,session};const first=await f.migration.migrate(input);expect(first).toEqual({sourceRows:30,selected:26,copied:26,previouslySettled:0,alreadyCopied:false});
  expect(await f.canonical.loadSession(identityScope)).toEqual(session);expect(await safety.session(identityScope)).toEqual(session);
  const rows=[];let after=0;for(;;){const page=await f.journal.page(identityScope,after);rows.push(...page.entries);if(page.nextId===null)break;after=page.nextId;}
  expect(rows).toHaveLength(26);expect(rows.every(row=>row.scope===identityScope&&row.localExecution?.scope==='visible')).toBe(true);expect(rows[0]).toMatchObject({state:'pending',attempts:2,sentAt:110,idempotencyKey:'intent-1',patches:[{rowId:'stable-1'}]});expect(rows[1].state).toBe('failed');expect(rows[2].state).toBe('rejected');
  await safety.settle(identityScope,'intent-1');await f.journal.acknowledge(identityScope,[rows[0].id]);
  expect(await f.migration.migrate(input)).toMatchObject({alreadyCopied:true,previouslySettled:1});expect((await f.journal.summary(identityScope)).count).toBe(25);expect(await legacy.count('entries')).toBe(30);
  recovered=await sqlite(safety);expect(await recovered.migration.migrate(input)).toMatchObject({copied:25,previouslySettled:1,alreadyCopied:false});expect((await recovered.journal.summary(identityScope)).count).toBe(25);expect((await recovered.journal.page(identityScope)).entries.some(row=>row.idempotencyKey==='intent-1')).toBe(false);
 }finally{recovered?.sql.close();f.sql.close();legacy.close();safety.close();}
});
it('keeps independent recovery durable when the SQLite cutover rolls back late, then retries without duplicates',async()=>{
 const factory=new IDBFactory(),safety=new SQLiteSafetyJournal('safety',factory,IDBKeyRange),legacy=await source(factory),f=await sqlite(safety);
 try{
  // Initialize the marker table without accepting a migration, then fail its
  // final durable marker after every intent and session has been copied.
  await f.db.execAsync('CREATE TABLE gv_legacy_intent_migrations(key TEXT PRIMARY KEY,scope TEXT NOT NULL,source_rows INTEGER NOT NULL,copied INTEGER NOT NULL) WITHOUT ROWID; CREATE TRIGGER fail_cutover BEFORE INSERT ON gv_legacy_intent_migrations BEGIN SELECT RAISE(ABORT,\'quota surrogate\'); END;');
  const input={source:legacy,store:'entries',identityScope,session};await expect(f.migration.migrate(input)).rejects.toThrow('quota surrogate');
  expect((await f.journal.summary(identityScope)).count).toBe(0);expect(await f.canonical.loadSession(identityScope)).toBeUndefined();expect(await safety.count(identityScope)).toBe(26);expect(await legacy.count('entries')).toBe(30);
  await f.db.execAsync('DROP TRIGGER fail_cutover');expect(await f.migration.migrate(input)).toMatchObject({copied:26,alreadyCopied:false});expect((await f.journal.summary(identityScope)).count).toBe(26);
 }finally{f.sql.close();legacy.close();safety.close();}
});
