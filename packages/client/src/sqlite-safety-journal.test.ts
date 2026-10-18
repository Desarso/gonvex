import {expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange,IDBObjectStore} from 'fake-indexeddb';
import {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import type {LocalReplicaSession} from './local-replica.js';
const session:LocalReplicaSession={directive:{protocolVersion:1,scope:'tenant',visibilityScope:'s',epoch:'e'},identity:{auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}},artifactHash:'artifact'};
const command=(n:number)=>({scope:'s',path:'tasks/create',commandId:'command-'+n,args:{text:'Synthetic '+n},now:100+n});
function fixture(){const factory=new IDBFactory();return {factory,journal:new SQLiteSafetyJournal('safety',factory,IDBKeyRange),reopen:()=>new SQLiteSafetyJournal('safety',factory,IDBKeyRange)};}
it('atomically retains authenticated session and immutable commands across reopen, replay and settlement',async()=>{
 const f=fixture();let reopened:SQLiteSafetyJournal|undefined;try{
  const input=command(1),pending=f.journal.append(input,session,'s');input.args.text='caller mutation';const first=await pending;
  expect((await f.journal.page('s')).entries[0].invocation.args).toEqual({text:'Synthetic 1'});expect(await f.journal.session('s')).toEqual(session);expect(await f.journal.session('other')).toBeUndefined();
  f.journal.close();reopened=f.reopen();expect(await reopened.append(command(1),session,'s')).toEqual(first);expect(await reopened.count('s')).toBe(1);
  await expect(reopened.append({...command(1),args:{text:'different'}},session,'s')).rejects.toMatchObject({code:'INTENT_CONFLICT'});
  expect(await reopened.settle('other','command-1')).toBe(false);expect(await reopened.settle('s','command-1')).toBe(true);expect(await reopened.settle('s','command-1')).toBe(false);
  expect(await reopened.append(command(1),session,'s')).toEqual({...first,settled:true});expect(await reopened.count('s')).toBe(0);expect(await reopened.page('s')).toEqual({entries:[],nextSequence:null});
 }finally{f.journal.close();reopened?.close();}
});
it('rolls back the whole journal acceptance on quota failure and never degrades to memory',async()=>{
 const f=fixture();try{
  await f.journal.append(command(1),session,'s');const original=IDBObjectStore.prototype.put;
  const failure=vi.spyOn(IDBObjectStore.prototype,'put').mockImplementation(function(this:IDBObjectStore,...args:any[]){if(this.name==='sessions')throw new DOMException('quota surrogate','QuotaExceededError');return original.apply(this,args as [any]);});
  await expect(f.journal.append(command(2),{...session,artifactHash:'new'},'s')).rejects.toMatchObject({name:'QuotaExceededError'});failure.mockRestore();
  expect(await f.journal.count('s')).toBe(1);expect(await f.journal.session('s')).toEqual(session);expect((await f.journal.page('s')).entries.map(value=>value.invocation.commandId)).toEqual(['command-1']);
  await f.journal.append(command(2),session,'s');expect(await f.journal.count('s')).toBe(2);
 }finally{vi.restoreAllMocks();f.journal.close();}
});
it('pages 5,000 pending commands without loading their bodies as a corpus',async()=>{
 const f=fixture();try{
  for(let n=0;n<5000;n++)await f.journal.append(command(n),session,'s');expect(await f.journal.count('s')).toBe(5000);
  let after=0,count=0,pages=0;for(;;){const page=await f.journal.page('s',after);expect(page.entries.length).toBeLessThanOrEqual(8);count+=page.entries.length;pages++;if(page.nextSequence===null)break;after=page.nextSequence;}
  expect(count).toBe(5000);expect(pages).toBe(625);expect((await f.journal.page('s',4998)).entries.map(value=>value.invocation.commandId)).toEqual(['command-4998','command-4999']);
 }finally{f.journal.close();}
},30000);
it('shrinks large command pages, verifies hashes before recovery, and refuses unavailable durability',async()=>{
 const f=fixture();try{
  for(let n=0;n<4;n++)await f.journal.append({...command(n),args:{text:'x'.repeat(40000)}},session,'s');
  const page=await f.journal.page('s',0,32);expect(page.entries.length).toBeGreaterThan(0);expect(page.entries.length).toBeLessThan(4);
  const db=await new Promise<IDBDatabase>((resolve,reject)=>{const opening=f.factory.open('safety');opening.onsuccess=()=>resolve(opening.result);opening.onerror=()=>reject(opening.error);});
  await new Promise<void>((resolve,reject)=>{const tx=db.transaction('bodies','readwrite');tx.objectStore('bodies').put({sequence:1,value:JSON.stringify({...command(0),args:{text:'y'.repeat(40000)}})});tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});db.close();
  await expect(f.journal.page('s')).rejects.toMatchObject({code:'INTENT_CORRUPT'});
  await expect(f.journal.append({...command(9),args:{text:'x'.repeat(100000)}},session,'s')).rejects.toMatchObject({code:'BUDGET'});
  const unavailable=new SQLiteSafetyJournal('none',undefined as any,undefined as any);await expect(unavailable.append(command(1),session,'s')).rejects.toMatchObject({code:'UNSUPPORTED'});unavailable.close();
 }finally{f.journal.close();}
});

it('preserves a fenced legacy intent envelope and its identity scope across independent recovery',async()=>{
 const f=fixture();try{
  const invocation=command(1),legacy={id:81,scope:'identity\u0000ws://private\u0000account',path:invocation.path,args:invocation.args,idempotencyKey:invocation.commandId,entityKeys:['tasks:stable'],patches:[{entity:'tasks',rowId:'stable',op:'insert' as const,fields:{id:'stable',title:'Synthetic'}}],localExecution:{scope:'s',commandId:invocation.commandId,now:invocation.now,artifactHash:'old-artifact',identity:session.identity},createdAt:invocation.now,attempts:2,nextAttemptAt:200,state:'inflight' as const,sentAt:150};
  const receipt=await f.journal.appendLegacy(invocation,legacy,session,'s');expect(await f.journal.append(invocation,session,'s')).toEqual(receipt);
  f.journal.close();const restored=f.reopen();try{expect((await restored.page('s')).entries).toEqual([{invocation,legacy}]);await expect(restored.appendLegacy(invocation,{...legacy,attempts:3},session,'s')).rejects.toMatchObject({code:'INTENT_CONFLICT'});expect(await restored.settle('s',invocation.commandId)).toBe(true);expect((await restored.page('s')).entries).toEqual([]);}finally{restored.close();}
 }finally{f.journal.close();}
});

it('keeps one recovery receipt when a stable identity receives a new visibility scope',async()=>{
 const f=fixture();try{
  const identityScope='identity\u0000deployment\u0000tenant\u0000account',input=command(1),first=await f.journal.append(input,session,identityScope);
  const moved={...input,scope:'new-visible'},updated={...session,directive:{...session.directive,visibilityScope:'new-visible'}};
  expect(await f.journal.append(moved,updated,identityScope)).toEqual(first);expect(await f.journal.count(identityScope)).toBe(1);expect((await f.journal.page(identityScope)).entries).toEqual([{invocation:input}]);
  await expect(f.journal.append({...moved,args:{text:'different'}},updated,identityScope)).rejects.toMatchObject({code:'INTENT_CONFLICT'});
 }finally{f.journal.close();}
});

it('persists definitive revocation without deleting pending commands or allowing legacy restore to revive it',async()=>{
 const f=fixture();let reopened:SQLiteSafetyJournal|undefined;try{
  await f.journal.append(command(1),session,'s');await f.journal.revokeSession('s');f.journal.close();reopened=f.reopen();
  expect(await reopened.session('s')).toBeUndefined();expect(await reopened.sessionRevoked('s')).toBe(true);expect(await reopened.count('s')).toBe(1);
  await expect(reopened.append(command(2),session,'s')).rejects.toMatchObject({code:'FENCED'});
  await expect(reopened.saveSession('s',session)).rejects.toMatchObject({code:'FENCED'});
  await expect(reopened.saveSession('s',session,true,()=>false)).rejects.toMatchObject({code:'FENCED'});expect(await reopened.sessionRevoked('s')).toBe(true);
  await reopened.saveSession('s',session,true);expect(await reopened.sessionRevoked('s')).toBe(false);await reopened.append(command(2),session,'s');expect(await reopened.count('s')).toBe(2);
 }finally{f.journal.close();reopened?.close();}
});
