import {DatabaseSync} from 'node:sqlite';
import {expect,it,vi} from 'vitest';
import {SQLiteWorkerSessions} from './sqlite-worker-sessions.js';
import {SQLiteSessionJournal} from './sqlite-session-journal.js';
import {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteTransportSession} from './sqlite-transport-host.js';
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
const saved={directive:{protocolVersion:1 as const,scope:'tenant',visibilityScope:'visible',epoch:'e'},identity,artifactHash:'artifact',lastOnlineAtMs:1000};
const admitted=(intentScope='identity\u0000a',generation=1):SQLiteTransportSession=>({...saved,intentScope,connectionGeneration:generation,capabilities:{replicaNativeRows:1}});
async function fixture(){
 const sql=new DatabaseSync(':memory:');
 const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const typed={tasks:{key:'id',fields:{id:{kind:'text' as const}},indexes:[]}},engine=new SQLiteTypedReplicaEngine(db,typed);await engine.open('sessions');const canonical=sqliteTypedReplicaStorage(db,typed);await canonical.ready();
 let lane=Promise.resolve();
 const runOwner=<T>(_bytes:number,run:()=>Promise<T>)=>{const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;};
 const safety=new SQLiteSessionJournal({database:db,engine,canonical,runOwner} as SQLiteOwnerProgramContext);
 const journal=new SQLiteIntentJournal(db,()=>engine.verifyOwner());await runOwner(0,()=>journal.ready());
 const options={artifactHash:'artifact',offlineMaxAgeMs:5000,database:db,canonical,safety,runOwner<T>(_bytes:number,run:()=>Promise<T>){const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;},fence:()=>engine.verifyOwner()};
 const registry=new SQLiteWorkerSessions(options);registry.open('one');registry.attachTab('tab','one');registry.authenticating('one','identity\u0000a',1);
 return {sql,db,canonical,safety,journal,registry,options,close(){sql.close();}};
}
it('admits only durable correlated identity, maps tabs centrally and preserves offline editing policy',async()=>{
 const f=await fixture();try{
  expect(()=>f.registry.resolveTab('tab')).toThrow('unavailable');expect(f.registry.admitsFrames('one',1)).toBe(false);
  const pending=f.registry.activate('one',admitted(),()=>true);expect(f.registry.admitsFrames('one',1)).toBe(false);expect(await pending).toBe(true);
  const current=f.registry.resolveTab('tab',true,99999999);expect(current).toMatchObject({scope:'visible',intentScope:'identity\u0000a',online:true,identity});expect(await f.canonical.loadSession('identity\u0000a')).toEqual(saved);
  current.identity.member!.id='caller mutation';expect(f.registry.resolveTab('tab').identity.member!.id).toBe('member');
  f.registry.disconnected('one');expect(f.registry.admitsFrames('one',1)).toBe(false);expect(f.registry.resolveTab('tab',true,6000).online).toBe(false);
  expect(()=>f.registry.resolveTab('tab',true,6001)).toThrow('Offline editing window expired');expect(()=>f.registry.resolveTab('tab',true,999)).toThrow('Offline editing window expired');expect(f.registry.resolveTab('tab',false,99999999).scope).toBe('visible');
  f.registry.authenticating('one','identity\u0000a',2);expect(f.registry.resolveTab('tab',true,1001).online).toBe(false);expect(f.registry.admitsFrames('one',2)).toBe(false);
  await expect(f.registry.activate('one',admitted(),()=>true)).rejects.toMatchObject({code:'FENCED'});
  expect(await f.registry.activate('one',admitted('identity\u0000a',2),()=>true)).toBe(true);expect(f.registry.resolveTab('tab').generation).not.toBe(current.generation);
  f.registry.authenticating('one','identity\u0000other',3);expect(()=>f.registry.resolveTab('tab')).toThrow('unavailable');expect(await f.registry.restore('one')).toBe(false);
 }finally{f.close();}
});
it('rolls back stale SQLite admission and fences queued IDB admission before publishing any rows',async()=>{
 const f=await fixture();try{
  const original=f.canonical.saveSession.bind(f.canonical);let release!:()=>void,reached!:()=>void;const barrier=new Promise<void>(resolve=>{release=resolve;}),entered=new Promise<void>(resolve=>{reached=resolve;});
  const spy=vi.spyOn(f.canonical,'saveSession').mockImplementation(async(...args)=>{await original(...args);reached();await barrier;});
  const activation=f.registry.activate('one',admitted(),()=>true);void activation.catch(()=>{});await entered;
  f.registry.authenticating('one','identity\u0000other',2);release();await expect(activation).rejects.toMatchObject({code:'FENCED'});spy.mockRestore();expect(await f.canonical.loadSession('identity\u0000a')).toBeUndefined();expect(()=>f.registry.resolveTab('tab')).toThrow('unavailable');
  await expect(f.registry.activate('one',admitted('identity\u0000other',2),()=>false)).rejects.toMatchObject({code:'FENCED'});expect(await f.safety.session('identity\u0000other')).toBeUndefined();
 }finally{vi.restoreAllMocks();f.close();}
});
it('quarantines every connection for a revoked identity immediately, retains commands, and restores only after fresh admission',async()=>{
 const f=await fixture();try{
  await f.registry.activate('one',admitted(),()=>true);f.registry.open('two');f.registry.attachTab('other-tab','two');f.registry.authenticating('two','identity\u0000a',1);await f.registry.restore('two');
  await f.journal.append({scope:'identity\u0000a',path:'tasks/create',args:{},idempotencyKey:'pending',createdAt:1000,nextAttemptAt:1000,attempts:0,state:'pending',patches:[],entityKeys:[]});
  const revoked=f.registry.quarantine('one',1);expect(()=>f.registry.resolveTab('tab')).toThrow('unavailable');expect(()=>f.registry.resolveTab('other-tab')).toThrow('unavailable');
  await revoked;expect((await f.journal.summary('identity\u0000a')).count).toBe(1);expect(await f.canonical.loadSession('identity\u0000a')).toBeUndefined();
  f.registry.close('two');f.registry.open('two');f.registry.attachTab('other-tab','two');f.registry.authenticating('two','identity\u0000a',1);expect(await f.registry.restore('two')).toBe(false);
  await expect(f.registry.activate('one',admitted(),()=>true)).rejects.toMatchObject({code:'FENCED'});
  f.registry.authenticating('one','identity\u0000a',2);await f.registry.activate('one',admitted('identity\u0000a',2),()=>true);expect(f.registry.resolveTab('tab').online).toBe(true);expect((await f.journal.summary('identity\u0000a')).count).toBe(1);
  await f.registry.quarantine('one',1);expect(f.registry.resolveTab('tab').online).toBe(true);
 }finally{f.close();}
});
it('orders a new admission after pending revocation durability and rejects obsolete restore callbacks',async()=>{
 const f=await fixture();try{
  await f.registry.activate('one',admitted(),()=>true);let release!:()=>void;const barrier=new Promise<void>(resolve=>{release=resolve;});const original=f.safety.revokeSession.bind(f.safety);
  const spy=vi.spyOn(f.safety,'revokeSession').mockImplementation(async scope=>{await barrier;return original(scope);});
  const revoked=f.registry.quarantine('one',1);f.registry.authenticating('one','identity\u0000a',2);const newer=f.registry.activate('one',admitted('identity\u0000a',2),()=>true);release();await revoked;await newer;spy.mockRestore();expect(await f.safety.sessionRevoked('identity\u0000a')).toBe(false);expect(await f.canonical.loadSession('identity\u0000a')).toEqual(saved);
  f.registry.disconnected('one');f.registry.authenticating('one','identity\u0000a',3);let current=true;const restoring=f.registry.restore('one',()=>current);current=false;await expect(restoring).rejects.toMatchObject({code:'FENCED'});
 }finally{vi.restoreAllMocks();f.close();}
});
it('bounds connections, tabs and session bytes without evicting active admission',async()=>{
 const f=await fixture();try{
  for(let n=2;n<=8;n++)f.registry.open('connection-'+n);expect(()=>f.registry.open('extra')).toThrow('allowance');for(let n=1;n<64;n++)f.registry.attachTab('tab-'+n,'one');expect(()=>f.registry.attachTab('extra','one')).toThrow('allowance');
  await expect(f.registry.activate('one',{...admitted(),identity:{...identity,member:{...identity.member,permissions:{oversized:'x'.repeat(20000)}}}},()=>true)).rejects.toMatchObject({code:'BUDGET'});
  await f.registry.activate('one',admitted(),()=>true);for(const budget of f.registry.budgets()){expect(budget.bytes).toBeLessThanOrEqual(budget.maxBytes);expect(budget.entries).toBeLessThanOrEqual(budget.maxEntries);}
  expect(f.registry.resolveTab('tab').scope).toBe('visible');f.registry.close('one');expect(()=>f.registry.resolveTab('tab')).toThrow('no authenticated');
 }finally{f.close();}
});

it('rejects an already queued native frame after same-socket reauthentication or tenant change',async()=>{
 const f=await fixture();try{
  await f.registry.activate('one',admitted(),()=>true);const frame=f.registry.frameAdmission('one',1);expect(frame.scope).toBe('visible');f.registry.assertFrameAdmission('one',1,frame.token);
  f.registry.authenticating('one','identity\u0000a',1);expect(()=>f.registry.frameAdmission('one',1)).toThrow('no durable');expect(()=>f.registry.assertFrameAdmission('one',1,frame.token)).toThrow('changed');
  await f.registry.activate('one',admitted(),()=>true);expect(()=>f.registry.assertFrameAdmission('one',1,frame.token)).toThrow('changed');expect(f.registry.frameAdmission('one',1).token).not.toBe(frame.token);
  f.registry.authenticating('one','identity\u0000other',2);expect(()=>f.registry.assertFrameAdmission('one',1,frame.token)).toThrow('changed');
 }finally{f.close();}
});
it('rejects incompatible cached artifacts and control-plane sessions instead of granting local writes',async()=>{
 const f=await fixture();try{
  await f.safety.saveSession('identity\u0000a',{...saved,artifactHash:'other'});await expect(f.registry.restore('one')).rejects.toMatchObject({code:'SCHEMA_MISMATCH'});expect(()=>f.registry.resolveTab('tab',true)).toThrow('unavailable');
  expect(await f.registry.activate('one',{...admitted(),directive:undefined,identity:undefined},()=>true)).toBe(false);expect(f.registry.admitsFrames('one',1)).toBe(false);expect(()=>f.registry.resolveTab('tab')).toThrow('unavailable');
 }finally{f.close();}
});

it('restores a legacy session without an online timestamp for reads, with the existing expired offline-write policy',async()=>{
 const f=await fixture();try{
  const {lastOnlineAtMs:_,...legacy}=saved;await f.canonical.saveSession('identity\u0000a',legacy);expect(await f.registry.restore('one')).toBe(true);expect(f.registry.resolveTab('tab').scope).toBe('visible');expect(()=>f.registry.resolveTab('tab',true,6001)).toThrow('Offline editing window expired');
 }finally{f.close();}
});

it('imports a fenced cached session before network admission without refreshing its offline timestamp or admitting frames',async()=>{
 const f=await fixture();try{expect(await f.registry.importCached('one',saved)).toBe(true);expect(await f.safety.session('identity\u0000a')).toEqual(saved);expect(await f.canonical.loadSession('identity\u0000a')).toEqual(saved);expect(await f.registry.restore('one')).toBe(true);expect(f.registry.resolveTab('tab',true,2000)).toMatchObject({online:false,scope:'visible'});expect(f.registry.admitsFrames('one',1)).toBe(false);expect(await f.registry.importCached('one',saved)).toBe(false);expect(()=>f.registry.resolveTab('tab',true,6001)).toThrow('expired');}finally{f.close();}
});
it('does not let cached migration revive revocation or overwrite a newer authoritative admission',async()=>{
 const f=await fixture();try{await f.safety.revokeSession('identity\u0000a');expect(await f.registry.importCached('one',saved)).toBe(false);expect(await f.canonical.loadSession('identity\u0000a')).toBeUndefined();await f.registry.activate('one',{...admitted(),lastOnlineAtMs:3000},()=>true);expect(await f.registry.importCached('one',saved)).toBe(false);expect((await f.safety.session('identity\u0000a'))?.lastOnlineAtMs).toBe(3000);expect((await f.canonical.loadSession('identity\u0000a'))?.lastOnlineAtMs).toBe(3000);}finally{f.close();}
});

it('keeps cached-read admission stable across transport retries and changes it for verified visibility changes',async()=>{
 const f=await fixture();try{
  await f.registry.activate('one',admitted(),()=>true);const first=f.registry.resolveTab('tab');
  f.registry.disconnected('one');f.registry.authenticating('one','identity\u0000a',2);expect(f.registry.resolveTab('tab').readGeneration).toBe(first.readGeneration);expect(f.registry.resolveTab('tab').generation).not.toBe(first.generation);expect(f.registry.admitsFrames('one',2)).toBe(false);
  await f.registry.restore('one');expect(f.registry.resolveTab('tab').readGeneration).toBe(first.readGeneration);
  await f.registry.activate('one',admitted('identity\u0000a',2),()=>true);expect(f.registry.resolveTab('tab').readGeneration).toBe(first.readGeneration);
  f.registry.authenticating('one','identity\u0000a',3);const next=admitted('identity\u0000a',3);next.directive={...next.directive!,visibilityScope:'changed-visibility'};await f.registry.activate('one',next,()=>true);expect(f.registry.resolveTab('tab').readGeneration).not.toBe(first.readGeneration);
  await f.registry.quarantine('one',3);expect(()=>f.registry.resolveTab('tab')).toThrow('unavailable');
 }finally{f.close();}
});
