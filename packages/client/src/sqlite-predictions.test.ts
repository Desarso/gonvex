import {DatabaseSync} from 'node:sqlite';
import {expect,it} from 'vitest';
import {memoryReadView,overlayReadView} from '@gonvex/local-runtime/read-view';
import {SQLitePredictions} from './sqlite-predictions.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import type {LocalPatch} from '@gonvex/local-runtime';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},n:{kind:'number'},data:{kind:'json'}},indexes:[{columns:['n']}],search:['name'],searchContent:'external',searchDetail:'column'}} as const;
const coverage={tasks:{key:'id',complete:true}};
async function fixture(){
 const sql=new DatabaseSync(':memory:');const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open('predictions');const canonical=sqliteTypedReplicaStorage(db,schema);await canonical.ready();const fence=()=>engine.verifyOwner(),predictions=new SQLitePredictions(db,canonical,schema,fence),journal=new SQLiteIntentJournal(db,fence);await predictions.ready();await journal.ready();
 const rows=Array.from({length:20},(_,n)=>({id:String(n).padStart(2,'0'),name:'Task '+n,n,data:{a:[n]}}));await canonical.applyTransaction({cursor:{epoch:'e',revision:1},changes:rows.map(row=>({entity:'tasks',id:row.id,operation:'insert' as const,newValue:row}))},{entities:{},liveQueries:{}},'s');
 return {sql,db,engine,canonical,predictions,journal,rows};
}
it('agrees with the portable overlay for inserted, updated, deleted and chained predicted rows without changing canonical authority',async()=>{
 const f=await fixture();try{
  const patches:LocalPatch[]=[{entity:'tasks',rowId:'01',op:'patch',fields:{name:'café 東京',n:99}},{entity:'tasks',rowId:'02',op:'delete'},{entity:'tasks',rowId:'new',op:'insert',fields:{id:'new',name:'offline',n:-1,data:null}},{entity:'tasks',rowId:'new',op:'patch',fields:{name:'offline edited'}}];await f.predictions.apply('s',1,patches);
  const reference=overlayReadView(memoryReadView(new Map([['tasks',new Map(f.rows.map(row=>[row.id,row]))]]),coverage),coverage,patches);
  for(const read of [{table:'tasks',where:{column:'n',op:'gte',value:0},orderBy:[{column:'n',direction:'desc'}],limit:5},{table:'tasks',where:{column:'id',op:'in',values:['01','02','new']}},{table:'tasks',columns:['id','name','data']} ] as const){expect(await f.predictions.withReadView('s',coverage,view=>view.select(read))).toEqual(await reference.select(read));}
  expect((await f.predictions.entities('s','tasks',['01','02','new'])).map(entry=>entry.id)).toEqual(['01','new']);
  expect((await f.canonical.loadEntityRows('s','tasks',['01','02','new'])).map(entry=>entry.row.name)).toEqual(['Task 1','Task 2']);expect((await f.predictions.entities('other','tasks',['01','new']))).toEqual([]);expect(await f.engine.integrity()).toBe('ok');
 }finally{f.sql.close();}
});
it('commits or rolls back predictions and durable intents together and can rebuild derived state after reopen',async()=>{
 const f=await fixture();try{
  const draft={scope:'s',path:'tasks/create',args:{},idempotencyKey:'offline',entityKeys:['tasks:new'],createdAt:1,attempts:0,nextAttemptAt:1,state:'pending' as const};
  await expect(f.db.withTransactionAsync(async()=>{const intent=await f.journal.append(draft);await f.predictions.apply('s',intent.id,[{entity:'tasks',rowId:'new',op:'insert',fields:{id:'new',name:'offline',n:21}}]);throw Error('abort reducer');})).rejects.toThrow('abort reducer');
  expect(await f.predictions.entities('s','tasks',['new'])).toEqual([]);expect((await f.journal.summary('s')).count).toBe(0);
  const intent=await f.journal.append(draft);await f.predictions.apply('s',intent.id,[{entity:'tasks',rowId:'new',op:'insert',fields:{id:'new',name:'offline',n:21}}]);
  const reopened=new SQLitePredictions(f.db,f.canonical,schema,()=>f.engine.verifyOwner());expect((await reopened.entities('s','tasks',['new']))[0].row.name).toBe('offline');
  await reopened.reset('s');expect(await reopened.entities('s','tasks',['new'])).toEqual([]);expect((await f.journal.summary('s')).count).toBe(1);expect(await f.engine.integrity()).toBe('ok');
 }finally{f.sql.close();}
});
it('preserves missing-field coverage and rejects a patch of a missing row or identity replacement atomically',async()=>{
 const f=await fixture();try{
  await f.predictions.apply('s',1,[{entity:'tasks',rowId:'partial',op:'insert',fields:{id:'partial',name:'partial'}}]);
  expect(await f.predictions.withReadView('s',coverage,view=>view.select({table:'tasks',columns:['id','n'],where:{column:'id',op:'eq',value:'partial'}}))).toEqual({complete:false,rows:[{id:'partial',n:null}]});
  expect(Object.hasOwn((await f.predictions.entities('s','tasks',['partial']))[0].row,'n')).toBe(false);
  await expect(f.predictions.apply('s',2,[{entity:'tasks',rowId:'01',op:'delete'},{entity:'tasks',rowId:'missing',op:'patch',fields:{name:'bad'}}])).rejects.toMatchObject({code:'INVALID_ROW'});expect((await f.predictions.entities('s','tasks',['01']))[0].row.name).toBe('Task 1');
  await expect(f.predictions.apply('s',2,[{entity:'tasks',rowId:'01',op:'patch',fields:{id:'other'}}])).rejects.toThrow('identity');
 }finally{f.sql.close();}
});
