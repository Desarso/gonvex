import {DatabaseSync} from 'node:sqlite';
import {expect,it} from 'vitest';
import {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {readyIntents,type ReducerOutboxEntry,type IntentDependencies} from './outbox.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
async function fixture(){
 const sql=new DatabaseSync(':memory:');let largestRead=0,owner=1;
 const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){const rows=sql.prepare(query).all(...args as any[]) as T[];largestRead=Math.max(largestRead,rows.length);return rows;},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const fence=async()=>{if(owner!==1)throw Object.assign(Error('stale owner'),{code:'FENCED'});};const journal=new SQLiteIntentJournal(db,fence);await journal.ready();return {sql,db,journal,get largestRead(){return largestRead;},displace(){owner++;}};
}
const draft=(i:number,changes:Partial<Omit<ReducerOutboxEntry,'id'>>={})=>({scope:'s',path:'tasks/create',args:{name:'Task '+i},idempotencyKey:'intent-'+i,entityKeys:['tasks:'+i],createdAt:100,nextAttemptAt:100,attempts:0,state:'pending' as const,...changes});
it('keeps 5,000 queued intents on disk and only reads bounded delivery/inspection pages',async()=>{
 const f=await fixture();try{
  await f.db.withTransactionAsync(async()=>{for(let i=0;i<5000;i++)await f.journal.append(draft(i));});
  expect(await f.journal.summary('s')).toEqual({count:5000,queued:5000,failed:0,rejected:0,oldest:100});
  const ready=await f.journal.readyBatch('s',100,8);expect(ready.map(entry=>entry.id)).toEqual([1,2,3,4,5,6,7,8]);
  const claimed=await f.journal.claim('s',100,8);expect(claimed.map(entry=>entry.id)).toEqual(ready.map(entry=>entry.id));expect(claimed.every(entry=>entry.state==='inflight'&&entry.sentAt===100)).toBe(true);expect((await f.journal.claim('s',100,8)).map(entry=>entry.id)).toEqual([9,10,11,12,13,14,15,16]);await f.journal.recoverInflight('s');expect((await f.journal.readyBatch('s',100,8)).map(entry=>entry.id)).toEqual(ready.map(entry=>entry.id));
  const first=await f.journal.page('s',0,7),last=await f.journal.page('s',4998,7);expect(first.entries).toHaveLength(7);expect(first.nextId).toBe(7);expect(last.entries.map(entry=>entry.id)).toEqual([4999,5000]);expect(last.nextId).toBeNull();expect(f.largestRead).toBeLessThanOrEqual(8);
  expect(await f.journal.readyBatch('other',100)).toEqual([]);expect((await f.journal.summary('other')).count).toBe(0);
 }finally{f.sql.close();}
},15000);
it('SQL scheduling agrees with the existing causal scheduler for mixed reads/writes/scans, legacy keys, barriers and states',async()=>{
 const f=await fixture();let seed=0x7431;const next=()=>seed=(Math.imul(seed,1664525)+1013904223)>>>0;
 try{
  for(let round=0;round<60;round++){
   const entries:ReducerOutboxEntry[]=[];const scope='scope-'+round;
   for(let i=0;i<24;i++){
    const table=next()%2?'tasks':'members',row=String(next()%5),mode=next()%7,state=(['pending','pending','pending','inflight','committed','failed','rejected'] as const)[next()%7];
    const dependencies:IntentDependencies={reads:[],writes:[],scans:[]};
    if(mode===0)dependencies.reads.push({entity:table,rowId:row});else if(mode===1)dependencies.scans.push(table);else dependencies.writes.push({entity:table,rowId:row});
    const input=draft(i,{scope,state,nextAttemptAt:next()%4===0?200:100,entityKeys:mode===5?['__gonvex_local_intents']:[table+':'+row],...(mode===6?{}:{dependencies})});const receipt=await f.journal.append(input);entries.push({...input,id:receipt.id});
   }
   expect((await f.journal.readyBatch(scope,100,8)).map(entry=>entry.id)).toEqual(readyIntents(entries,100,8).map(entry=>entry.id));
  }
 }finally{f.sql.close();}
},15000);
it('composes the intent receipt and predicted writes atomically, retaining replay dedupe after acknowledgment',async()=>{
 const f=await fixture();try{
  await f.db.execAsync('CREATE TABLE predictions(id TEXT PRIMARY KEY)');const input=draft(1);let receipt;
  await expect(f.db.withTransactionAsync(async()=>{await f.journal.append(input);await f.db.runAsync("INSERT INTO predictions VALUES('predicted')");throw Error('reducer failed');})).rejects.toThrow('reducer failed');
  expect((await f.journal.summary('s')).count).toBe(0);expect(f.sql.prepare('SELECT count(*) n FROM gv_reducer_journal_receipts').get()).toEqual({n:0});expect(f.sql.prepare('SELECT count(*) n FROM predictions').get()).toEqual({n:0});
  await f.db.withTransactionAsync(async()=>{receipt=await f.journal.append(input);await f.db.runAsync("INSERT INTO predictions VALUES('predicted')");});expect(await f.journal.append(input)).toEqual(receipt);
  await f.journal.acknowledge('s',[receipt!.id]);expect(await f.journal.append(input)).toEqual(receipt);expect((await f.journal.summary('s')).count).toBe(0);
  await expect(f.journal.append({...input,args:{name:'changed'}})).rejects.toMatchObject({code:'INTENT_CONFLICT'});
 }finally{f.sql.close();}
});
it('persists compare-and-set transitions, rebuilds causal indexes, and recovers inflight without losing its identity',async()=>{
 const f=await fixture();try{
  const a=await f.journal.append(draft(1,{dependencies:{reads:[],writes:[{entity:'tasks',rowId:'same'}],scans:[]}}));
  const b=await f.journal.append(draft(2,{dependencies:{reads:[{entity:'tasks',rowId:'same'}],writes:[],scans:[]}}));
  expect((await f.journal.readyBatch('s',100)).map(entry=>entry.id)).toEqual([a.id]);
  await f.journal.update('s',a.id,['pending'],entry=>({...entry,state:'inflight',sentAt:101}));expect(await f.journal.readyBatch('s',100)).toEqual([]);
  expect(await f.journal.update('s',a.id,['pending'],entry=>({...entry,state:'rejected'}))).toBeUndefined();
  await f.journal.recoverInflight('s');expect((await f.journal.page('s')).entries[0]).toMatchObject({id:a.id,state:'pending',sentAt:101,attempts:0});
  await expect(f.journal.update('s',a.id,['pending'],entry=>{entry.path='changed';return entry;})).rejects.toMatchObject({code:'INTENT_CONFLICT'});
  await f.journal.update('s',a.id,['pending'],entry=>({...entry,state:'committed'}));expect((await f.journal.readyBatch('s',100)).map(entry=>entry.id)).toEqual([b.id]);
  f.displace();await expect(f.journal.readyBatch('s',100)).rejects.toMatchObject({code:'FENCED'});
 }finally{f.sql.close();}
});
it('snapshots admitted drafts before awaiting and rejects oversize/invalid requests before a durable insert',async()=>{
 const f=await fixture();try{
  const input=draft(1),pending=f.journal.append(input);input.args.name='caller mutated';await pending;expect((await f.journal.page('s')).entries[0].args).toEqual({name:'Task 1'});
  await expect(f.journal.append(draft(2,{args:{text:'x'.repeat(100000)}}))).rejects.toMatchObject({code:'BUDGET'});
  await expect(f.journal.page('s',0,33)).rejects.toThrow('1..32');await expect(f.journal.readyBatch('s',NaN)).rejects.toThrow('time');expect((await f.journal.summary('s')).count).toBe(1);
 }finally{f.sql.close();}
});
