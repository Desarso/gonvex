import {DatabaseSync} from 'node:sqlite';
import {createReducerOutbox} from './outbox.js';
import {expect,it} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
import {SQLiteSessionJournal} from './sqlite-session-journal.js';
import {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import {SQLiteLegacyBootstrap} from './sqlite-legacy-bootstrap.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {SQLiteLegacyIntentMigration} from './sqlite-legacy-intent-migration.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {LocalReplicaSession} from './local-replica.js';
const identityScope='identity\u0000ws://private\u0000project\u0000tenant\u0000account';
const session:LocalReplicaSession={directive:{protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'e'},identity:{auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}},artifactHash:'artifact'};
const tables={tasks:{key:'id',fields:{id:{kind:'text'},title:{kind:'text'}},indexes:[]}} as const;
async function sqlite(){
 const sql=new DatabaseSync(':memory:');const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const engine=new SQLiteTypedReplicaEngine(db,tables);await engine.open('migrate');const canonical=sqliteTypedReplicaStorage(db,tables),journal=new SQLiteIntentJournal(db,()=>engine.verifyOwner());await canonical.ready();await journal.ready();const migration=new SQLiteLegacyIntentMigration({database:db,canonical,journal,fence:()=>engine.verifyOwner()});return {sql,db,engine,journal,canonical,migration};
}
async function source(factory:IDBFactory){
 const db=await new Promise<IDBDatabase>((resolve,reject)=>{const opening=factory.open('old-outbox',20);opening.onupgradeneeded=()=>opening.result.createObjectStore('entries',{keyPath:'id'}).createIndex('scope','scope');opening.onsuccess=()=>resolve(opening.result);opening.onerror=()=>reject(opening.error);});
 await new Promise<void>((resolve,reject)=>{const tx=db.transaction('entries','readwrite');for(let n=1;n<=30;n++)tx.objectStore('entries').add({id:n,scope:n%7===0?'foreign-account':identityScope,path:'tasks/create',idempotencyKey:'intent-'+n,args:{title:n%7===0?'x'.repeat(200000):'Synthetic '+n},entityKeys:['tasks:stable-'+n],patches:[{entity:'tasks',rowId:'stable-'+n,op:'insert',fields:{id:'stable-'+n,title:'Synthetic '+n}}],localExecution:{scope:'visible',commandId:'intent-'+n,now:100+n,identity:session.identity,artifactHash:'old-artifact'},createdAt:100+n,attempts:n===1?2:0,nextAttemptAt:100+n,state:n===1?'inflight':n===2?'failed':n===3?'rejected':'pending',...(n===1?{sentAt:110}:{})});tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});db.close();return (await SQLiteIndexedDBMigrationSource.open('old-outbox',factory,IDBKeyRange))!;
}
it('moves fenced sessions and original offline intents before replica rows, with exact identity isolation and resumable settlement',async()=>{
 const factory=new IDBFactory(),legacy=await source(factory),f=await sqlite();
 try{
  const input={source:legacy,store:'entries',identityScope,session};const first=await f.migration.migrate(input);expect(first).toEqual({sourceRows:30,selected:26,copied:26,previouslySettled:0,alreadyCopied:false});
  expect(await f.canonical.loadSession(identityScope)).toEqual(session);
  const rows=[];let after=0;for(;;){const page=await f.journal.page(identityScope,after);rows.push(...page.entries);if(page.nextId===null)break;after=page.nextId;}
  expect(rows).toHaveLength(26);expect(rows.every(row=>row.scope===identityScope&&row.localExecution?.scope==='visible')).toBe(true);expect(rows[0]).toMatchObject({state:'pending',attempts:2,sentAt:110,idempotencyKey:'intent-1',patches:[{rowId:'stable-1'}]});expect(rows[1].state).toBe('failed');expect(rows[2].state).toBe('rejected');
  await f.journal.acknowledge(identityScope,[rows[0].id]);
  expect(await f.migration.migrate(input)).toMatchObject({alreadyCopied:true,previouslySettled:1,copied:0});expect((await f.journal.summary(identityScope)).count).toBe(25);expect(await legacy.count('entries')).toBe(30);

 }finally{f.sql.close();legacy.close();}
});
it('retains the read-only source when the atomic SQLite cutover rolls back, then retries without duplicates',async()=>{
 const factory=new IDBFactory(),legacy=await source(factory),f=await sqlite();
 try{
  // Initialize the marker table without accepting a migration, then fail its
  // final durable marker after every intent and session has been copied.
  await f.db.execAsync('CREATE TABLE gv_legacy_intent_migrations(key TEXT PRIMARY KEY,scope TEXT NOT NULL,source_rows INTEGER NOT NULL,copied INTEGER NOT NULL) WITHOUT ROWID; CREATE TRIGGER fail_cutover BEFORE INSERT ON gv_legacy_intent_migrations BEGIN SELECT RAISE(ABORT,\'quota surrogate\'); END;');
  const input={source:legacy,store:'entries',identityScope,session};await expect(f.migration.migrate(input)).rejects.toThrow('quota surrogate');
  expect((await f.journal.summary(identityScope)).count).toBe(0);expect(await f.canonical.loadSession(identityScope)).toBeUndefined();expect(await legacy.count('entries')).toBe(30);
  await f.db.execAsync('DROP TRIGGER fail_cutover');expect(await f.migration.migrate(input)).toMatchObject({copied:26,alreadyCopied:false});expect((await f.journal.summary(identityScope)).count).toBe(26);
 }finally{f.sql.close();legacy.close();}
});

it('automatically cuts over the session and scoped intents before copying rows, without hydrating legacy windows',async()=>{
 const factory=new IDBFactory(),legacy=await source(factory),f=await sqlite();legacy.close();
 const old=await new Promise<IDBDatabase>((resolve,reject)=>{const opening=factory.open('old-replica',50);opening.onupgradeneeded=()=>{opening.result.createObjectStore('meta',{keyPath:['scope','key']});opening.result.createObjectStore('entities',{keyPath:['scope','entity','id']});opening.result.createObjectStore('windows',{keyPath:['scope','signature']});};opening.onsuccess=()=>resolve(opening.result);opening.onerror=()=>reject(opening.error);});
 await new Promise<void>((resolve,reject)=>{const tx=old.transaction(['meta','entities','windows'],'readwrite');tx.objectStore('meta').put({scope:identityScope,key:'session',value:JSON.stringify(session)});tx.objectStore('meta').put({scope:'visible',key:'cursor',value:JSON.stringify({epoch:'e',revision:1})});tx.objectStore('entities').put({scope:'visible',entity:'tasks',id:'a',value:JSON.stringify({id:'a',title:'Offline row'}),authority:{epoch:'e',base:1,fields:{}}});tx.objectStore('windows').put({scope:'visible',signature:'huge',value:'x'.repeat(1000000)});tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});old.close();
 let lane=Promise.resolve();const owner={database:f.db,engine:f.engine,canonical:f.canonical,runOwner<T>(_bytes:number,run:()=>Promise<T>){const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;}} as SQLiteOwnerProgramContext;
 const bootstrap=new SQLiteLegacyBootstrap({owner,journal:f.journal,artifactHash:'artifact',replicaName:'old-replica',outboxName:'old-outbox',factory,ranges:IDBKeyRange});
 try{
  expect(await bootstrap.prepare(identityScope)).toEqual(session);expect((await f.journal.summary(identityScope)).count).toBe(26);
  expect(await f.canonical.loadEntityRows('visible','tasks',['a'])).toEqual([]);
  expect(await bootstrap.step(identityScope,session)).toMatchObject({verified:true,progress:{seen:1,copied:1}});
  expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual({id:'a',title:'Offline row'});
  expect(await bootstrap.prepare(identityScope)).toEqual(session);expect((await f.journal.summary(identityScope)).count).toBe(26);
  expect(await f.engine.integrity()).toBe('ok');
 }finally{bootstrap.close();f.sql.close();}
});
it('never replaces a newer durable admission or revives a revoked cache during legacy cutover',async()=>{
 const factory=new IDBFactory(),legacy=await source(factory),f=await sqlite();
 try{
  const safety=new SQLiteSessionJournal({database:f.db,canonical:f.canonical,engine:f.engine,runOwner:(_bytes:number,run:()=>Promise<any>)=>run()} as SQLiteOwnerProgramContext);
  const newer={...session,lastOnlineAtMs:999};await safety.saveSession(identityScope,newer,true);await f.canonical.saveSession(identityScope,newer);
  await expect(f.migration.migrate({source:legacy,store:'entries',identityScope,session})).rejects.toMatchObject({code:'FENCED'});
  expect(await safety.session(identityScope)).toEqual(newer);expect(await f.canonical.loadSession(identityScope)).toEqual(newer);expect((await f.journal.summary(identityScope)).count).toBe(0);
  await safety.revokeSession(identityScope);await expect(f.migration.migrate({source:legacy,store:'entries',identityScope,session:newer})).rejects.toMatchObject({code:'FENCED'});expect(await safety.sessionRevoked(identityScope)).toBe(true);
 }finally{legacy.close();f.sql.close();}
});

it('imports a late durable legacy enqueue once and never resurrects an acknowledged intent',async()=>{
 const factory=new IDBFactory(),legacy=await source(factory),f=await sqlite();
 const input={source:legacy,store:'entries',identityScope,session};
 let db:IDBDatabase|undefined;
 try{
  await f.migration.migrate(input);
  const first=(await f.journal.find(identityScope,'intent-1'))!;
  await f.journal.acknowledge(identityScope,[first.id]);
  db=await new Promise<IDBDatabase>((resolve,reject)=>{const r=factory.open('old-outbox');r.onsuccess=()=>resolve(r.result);r.onerror=()=>reject(r.error);});
  const write=async(change:(store:IDBObjectStore)=>void)=>new Promise<void>((resolve,reject)=>{const tx=db!.transaction('entries','readwrite');change(tx.objectStore('entries'));tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});
  await write(store=>{const get=store.get(1);get.onsuccess=()=>store.put({...get.result,attempts:9,state:'pending',nextAttemptAt:999});store.put({id:31,scope:identityScope,path:'tasks/create',args:{title:'Late offline edit'},idempotencyKey:'late',entityKeys:[],createdAt:999,attempts:0,nextAttemptAt:999,state:'pending'});});
  expect(await f.migration.migrate(input)).toMatchObject({sourceRows:31,copied:1,previouslySettled:1});
  expect(await f.journal.find(identityScope,'intent-1')).toBeUndefined();
  const late=(await f.journal.find(identityScope,'late'))!;expect(late.args).toEqual({title:'Late offline edit'});
  await f.journal.acknowledge(identityScope,[late.id]);
  const restarted=new SQLiteLegacyIntentMigration({database:f.db,canonical:f.canonical,journal:f.journal,fence:()=>f.engine.verifyOwner()});
  expect(await restarted.migrate(input)).toMatchObject({copied:0,previouslySettled:2});
  await write(store=>{const get=store.get(31);get.onsuccess=()=>store.put({...get.result,args:{title:'Different command with reused key'}});});
  await expect(restarted.migrate(input)).rejects.toMatchObject({code:'INTENT_CONFLICT'});
  expect(await f.journal.find(identityScope,'late')).toBeUndefined();
 }finally{db?.close();legacy.close();f.sql.close();}
});

it('background bootstrap imports a public old-tab enqueue after cutover and preserves its durable source',async()=>{
 const factory=new IDBFactory(),f=await sqlite();
 const options={databaseName:'public-old-outbox',indexedDB:factory,IDBKeyRange};
 const old=createReducerOutbox(options);
 await old.enqueue({scope:identityScope,path:'tasks/create',args:{title:'Before'},idempotencyKey:'before'});
 const owner={database:f.db,engine:f.engine,canonical:f.canonical,runOwner:<T>(_bytes:number,run:()=>Promise<T>)=>run()} as SQLiteOwnerProgramContext;
 const bootstrap=new SQLiteLegacyBootstrap({owner,journal:f.journal,artifactHash:'artifact',replicaName:'absent',outboxName:options.databaseName,factory,ranges:IDBKeyRange});
 try{
  await bootstrap.prepare(identityScope,session);
  const lateTab=createReducerOutbox(options);
  await lateTab.enqueue({scope:identityScope,path:'tasks/create',args:{title:'After'},idempotencyKey:'after'});
  expect(await bootstrap.step(identityScope,session)).toMatchObject({verified:true,imported:1,watching:true});
  const imported=(await f.journal.find(identityScope,'after'))!;expect(imported.args).toEqual({title:'After'});
  await f.journal.acknowledge(identityScope,[imported.id]);
  expect(await bootstrap.step(identityScope,session)).toMatchObject({imported:0,watching:true});
  expect(await f.journal.find(identityScope,'after')).toBeUndefined();
  expect(await lateTab.count(identityScope)).toBe(2);
 }finally{bootstrap.close();f.sql.close();}
});
