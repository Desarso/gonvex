import {lowerSQLiteLivePlan} from './sqlite-plan-lowering.js';
import {runOfflineLiveQuery} from './query-expression.js';
import {DatabaseSync} from 'node:sqlite';
import {expect,it,vi} from 'vitest';
import {memoryReadView,overlayReadView} from '@gonvex/local-runtime/read-view';
import {SQLitePredictions} from './sqlite-predictions.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteIntentJournal} from './sqlite-intent-journal.js';
import type {LocalPatch} from '@gonvex/local-runtime';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import {sqliteTableName,type SQLiteTypedSchema} from './sqlite-typed-schema.js';
const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},n:{kind:'number'},data:{kind:'json'}},indexes:[{columns:['n']}],search:['name'],searchContent:'external',searchDetail:'column'}} as const;
const coverage={tasks:{key:'id',complete:true}};
async function fixture(tables:SQLiteTypedSchema=schema){
 const sql=new DatabaseSync(':memory:');const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const engine=new SQLiteTypedReplicaEngine(db,tables);await engine.open('predictions');const canonical=sqliteTypedReplicaStorage(db,tables);await canonical.ready();const fence=()=>engine.verifyOwner(),predictions=new SQLitePredictions(db,canonical,tables,fence),journal=new SQLiteIntentJournal(db,fence);await predictions.ready();await journal.ready();
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

it('clears only prediction-bearing tables and preserves canonical and other-principal rows',async()=>{
 const f=await fixture({...schema,unused:schema.tasks});try{
  await f.predictions.apply('s',1,[{entity:'tasks',rowId:'new',op:'insert',fields:{id:'new',name:'draft',n:21}},{entity:'tasks',rowId:'01',op:'patch',fields:{name:'edited'}},{entity:'tasks',rowId:'02',op:'delete'}]);
  await f.predictions.apply('other',2,[{entity:'tasks',rowId:'other-new',op:'insert',fields:{id:'other-new',name:'other draft',n:22}}]);
  const run=vi.spyOn(f.db,'runAsync');await f.predictions.reset('s');
  expect(run.mock.calls.some(([sql])=>sql.includes('DELETE FROM "'+sqliteTableName('unused')+'"'))).toBe(false);
  expect(await f.db.getFirstAsync('SELECT count(*) n FROM "'+sqliteTableName('tasks')+'" WHERE scope=?','\u0000prediction:s')).toEqual({n:0});
  expect((await f.predictions.entities('s','tasks',['01','02','new'])).map(row=>row.row.name)).toEqual(['Task 1','Task 2']);
  expect((await f.predictions.entities('other','tasks',['other-new']))[0].row.name).toBe('other draft');
  expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_prediction_members WHERE scope=?','s')).toEqual({n:0});
  expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_prediction_fields WHERE scope=?','s')).toEqual({n:0});
  expect(await f.engine.integrity()).toBe('ok');run.mockRestore();
 }finally{f.sql.close();}
});

it('does not write or advance a prediction generation when resetting an empty overlay',async()=>{
 const f=await fixture();try{
  const before=await f.db.getFirstAsync('SELECT total_changes() n');await f.predictions.reset('s');
  expect(await f.db.getFirstAsync('SELECT total_changes() n')).toEqual(before);
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

it('walks typed optimistic keysets with filtering, substring search and exact replacement/deletion semantics',async()=>{
 const f=await fixture();try{
  await f.predictions.apply('s',1,[{entity:'tasks',rowId:'01',op:'patch',fields:{n:99,name:'café 東京'}},{entity:'tasks',rowId:'02',op:'delete'},{entity:'tasks',rowId:'new',op:'insert',fields:{id:'new',name:'offline edited',n:-1}}]);
  const plan={table:'tasks',order:[{field:'n',direction:'asc' as const}],limit:7};let after:import('./sqlite-typed-query.js').SQLiteKeyset|undefined;const ids:string[]=[];
  for(;;){const page=await f.predictions.page('s',{...plan,...(after?{after}:{})});ids.push(...page.rows.map(row=>String(row.id)));if(!page.cursor)break;after=page.cursor;}
  expect(ids).toEqual(['new','00',...Array.from({length:17},(_,n)=>String(n+3).padStart(2,'0')),'01']);expect(new Set(ids).size).toBe(20);
  expect((await f.predictions.page('s',{...plan,search:'東京'})).rows.map(row=>row.id)).toEqual(['01']);expect((await f.predictions.page('s',{...plan,search:'offline'})).rows.map(row=>row.id)).toEqual(['new']);
  expect((await f.predictions.page('s',{...plan,where:{field:'n',op:'gte',value:19},select:['id','n']})).rows).toEqual([{id:'19',n:19},{id:'01',n:99}]);
  expect((await f.predictions.page('s',{...plan,where:{op:'relation',field:'id',table:'tasks',relatedField:'id',where:{field:'n',op:'gte',value:90}}})).rows.map(row=>row.id)).toEqual(['01']);
  const first=await f.predictions.page('s',plan);await f.predictions.apply('s',2,[{entity:'tasks',rowId:'03',op:'patch',fields:{n:-5}}]);await expect(f.predictions.page('s',{...plan,after:first.cursor!})).rejects.toMatchObject({code:'PAGE_CHANGED'});
  await expect(f.engine.page('s',{...plan,after:first.cursor!})).rejects.toThrow('Optimistic cursors');expect((await f.canonical.loadEntityRows('s','tasks',['01']))[0].row.n).toBe(1);
 }finally{f.sql.close();}
});
it('keeps optimistic position views on disk and invalidates them on a prediction-only generation',async()=>{
 const f=await fixture();try{
  const plan={table:'tasks',order:[{field:'n',direction:'asc' as const}],limit:3};const first=await f.predictions.viewPage('s','grid',plan,10);expect(first.total).toBe(20);expect(first.rows.map(row=>row.id)).toEqual(['10','11','12']);
  const generation=await f.engine.verifyOwner();await f.predictions.apply('s',1,[{entity:'tasks',rowId:'01',op:'patch',fields:{n:99}},{entity:'tasks',rowId:'02',op:'delete'}]);expect(await f.engine.verifyOwner()).toEqual(generation);
  const next=await f.predictions.viewPage('s','grid',plan,10);expect(next.total).toBe(19);expect(next.rows.map(row=>row.id)).toEqual(['12','13','14']);await f.predictions.reset('s');expect((await f.predictions.viewPage('s','grid',plan,10)).rows.map(row=>row.id)).toEqual(['10','11','12']);
 }finally{f.sql.close();}
});
it('keeps admitted membership for replacements and permits only explicit matching local inserts outside it',async()=>{
 const {SQLiteWindowStore}=await import('./sqlite-window-store.js'),f=await fixture();try{
  const windows=new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner());const header={signature:'subset',entity:'tasks',key:'id',kind:'replica' as const,source:'server' as const,completeness:'complete' as const};await windows.begin('s',header,'one');const state=await windows.append('s','subset','one',0,JSON.stringify({ids:['00','01','02','03','04']}));await windows.publish('s','subset',state);
  await f.predictions.apply('s',1,[{entity:'tasks',rowId:'01',op:'patch',fields:{n:-1}},{entity:'tasks',rowId:'02',op:'delete'},{entity:'tasks',rowId:'13',op:'patch',fields:{n:-99}},{entity:'tasks',rowId:'local',op:'insert',fields:{id:'local',name:'matching local',n:2}},{entity:'tasks',rowId:'outside',op:'insert',fields:{id:'outside',name:'outside predicate',n:99}}]);
  const plan={table:'tasks',membership:{signature:'subset',generation:'one',requireComplete:true},where:{field:'n',op:'lt' as const,value:5},order:[{field:'n',direction:'asc' as const}],limit:50};
  expect((await f.predictions.page('s',plan)).rows.map(row=>row.id)).toEqual(['01','00','03','04']);expect((await f.predictions.page('s',plan,true)).rows.map(row=>row.id)).toEqual(['01','00','local','03','04']);expect((await f.predictions.viewPage('s','subset-view',plan,0,true)).total).toBe(5);
 }finally{f.sql.close();}
});

it('preserves newly received untouched fields while applying only intent-owned fields to pending source pages',async()=>{
 const f=await fixture();try{
  await f.predictions.apply('s',1,[{entity:'tasks',rowId:'01',op:'patch',fields:{n:99}},{entity:'tasks',rowId:'02',op:'delete'},{entity:'tasks',rowId:'new',op:'insert',fields:{id:'new',name:'Offline new',n:3,data:null}}]);
  const received=[{id:'01',row:{id:'01',name:'New server title',n:10,data:{fresh:true}}},{id:'02',row:f.rows[2]},{id:'new',row:{id:'new',name:'Server generated default',n:5,data:null}}];
  expect(await f.predictions.overlayRows('s','tasks',received)).toEqual([{id:'01',row:{id:'01',name:'New server title',n:99,data:{fresh:true}}},{id:'02',row:null},{id:'new',row:{id:'new',name:'Offline new',n:3,data:null}}]);
  await f.predictions.apply('s',2,[{entity:'tasks',rowId:'01',op:'patch',fields:{name:'Local title'}}]);expect((await f.predictions.overlayRows('s','tasks',received))[0].row).toEqual({id:'01',name:'Local title',n:99,data:{fresh:true}});
  await f.predictions.apply('s',3,[{entity:'tasks',rowId:'03',op:'patch',fields:{}}]);const fresh=[{id:'03',row:{id:'03',name:'New untouched server title',n:900,data:null}}];expect(await f.predictions.overlayRows('s','tasks',fresh)).toEqual(fresh);
  await f.db.runAsync('DELETE FROM gv_prediction_fields WHERE scope=? AND id=?','s','01');await expect(f.predictions.overlayRows('s','tasks',received)).rejects.toMatchObject({code:'PAGE_CHANGED'});
  await f.predictions.reset('s');expect(await f.predictions.overlayRows('s','tasks',received)).toEqual(received);expect(await f.db.getFirstAsync('SELECT count(*) n FROM gv_prediction_fields')).toEqual({n:0});
 }finally{f.sql.close();}
});

it('matches IDB offline-column boolean search over canonical and newly predicted SQLite rows',async()=>{
 const f=await fixture();try{
  f.sql.function('gv_ingress_lower',(value:any)=>String(value??'').toLowerCase());
  const predicted={id:'new',name:'Offline CAFÉ 東京',n:-1,data:null};await f.predictions.apply('s',1,[{entity:'tasks',rowId:'new',op:'insert',fields:predicted}]);
  const source:any={table:'tasks',key:'id',columns:['id','name','n','data'],sort:{defaultColumn:'n',defaultDirection:'asc',allowedColumns:['n']},window:{offsetArgument:'offset',limitArgument:'limit',defaultLimit:128,maxLimit:128,count:'exact'},search:{argument:'search',booleanTerms:true,columns:[],offlineColumns:['id','name','n'],sources:[{table:'serverDocuments',key:'taskId',column:'searchText'}]}};
  for(const search of ['offline','CAFÉ','東京','offline $and 東京','new $or Task 19','-1']){
   const args={search,offset:0,limit:128},lowered=lowerSQLiteLivePlan(schema,source,args,{}, {textOrder:'binary',offlineSearch:true});expect(lowered.supported).toBe(true);if(!lowered.supported)throw Error(lowered.reason);
   const actual=await f.predictions.page('s',lowered.plan),expected=runOfflineLiveQuery([...f.rows,predicted],source,args,'complete');
   expect(actual.rows.map(row=>row.id)).toEqual(expected.rows.map(row=>row.id));expect(lowered.partialSearch).toBe(true);expect(expected.completeness).toBe('partial');
  }
 }finally{f.sql.close();}
});

it('filters a corpus larger than the page byte cap with keysets and SQL COUNT instead of hydrating it',async()=>{
 const f=await fixture();try{
  for(let offset=20;offset<100;offset+=5)await f.engine.apply('s',Array.from({length:5},(_,n)=>({table:'tasks',id:String(offset+n),version:1,row:{id:String(offset+n),name:'wide'.repeat(1200),n:offset+n,data:null}})));
  const plan={table:'tasks',where:{field:'n',op:'gte' as const,value:20},order:[{field:'n',direction:'asc' as const}],limit:128};
  const count=await f.predictions.count('s',plan);expect(count).toBe(80);let after:any;const ids:string[]=[];
  for(;;){const page=await f.predictions.page('s',{...plan,...(after?{after}:{})});expect(page.rows.length).toBeLessThan(80);ids.push(...page.rows.map(row=>String(row.id)));if(!page.cursor)break;after=page.cursor;}
  expect(ids).toEqual(Array.from({length:80},(_,n)=>String(n+20)));expect(new Set(ids).size).toBe(80);
 }finally{f.sql.close();}
});

it('executes every portable task-grid structured filter with search and predicted rows on SQLite',async()=>{
 const f=await fixture();try{
  f.sql.function('gv_ingress_lower',(value:any)=>String(value??'').toLowerCase());
  const predicted={id:'new',name:'Offline task',n:25,data:null};await f.predictions.apply('s',1,[{entity:'tasks',rowId:'new',op:'insert',fields:predicted}]);
  const cases=[['name','contains','TASK'],['name','notContains','Task 1'],['name','equals','Task 1'],['name','notEquals','Task 1'],['name','startsWith','task 1'],['name','endsWith','19'],['name','empty',''],['name','notEmpty',''],['name','oneOf','["Task 1","Offline task"]'],['n','oneOf','[1,25]'],['n','lessThan','5'],['n','lessThanOrEqual','5'],['n','greaterThan','5'],['n','greaterThanOrEqual','5'],['n','inRange','4','7'],['n','equals','01'],['n','notEquals','01']];
  const operators=[...new Set(cases.map(c=>c[1]))];
  const source:any={table:'tasks',key:'id',columns:['id','name','n','data'],sort:{defaultColumn:'n',defaultDirection:'asc',allowedColumns:['n']},window:{offsetArgument:'offset',limitArgument:'limit',defaultLimit:128,maxLimit:128,count:'exact'},search:{argument:'search',columns:['name'],offlineColumns:['name']},filters:{argument:'filters',allowedColumns:['name','n'],allowedOperators:operators}};
  for(const [column,operator,value,valueTo] of cases){
   const args={search:'task',offset:0,limit:128,filters:[{column:column!,operator:operator!,value:value!,...(valueTo?{valueTo}:{})}]};
   const lowered=lowerSQLiteLivePlan(schema,source,args,{}, {textOrder:'binary',offlineSearch:true});expect(lowered.supported,operator).toBe(true);if(!lowered.supported)throw Error(lowered.reason);
   const actual=await f.predictions.page('s',lowered.plan),expected=runOfflineLiveQuery([...f.rows,predicted],source,args,'complete');
   expect(actual.rows.map(row=>row.id),operator+' '+value).toEqual(expected.rows.map(row=>row.id));
  }
  const combined=lowerSQLiteLivePlan(schema,source,{search:'offline',filters:[{column:'n',operator:'oneOf',value:'[25]'},{column:'name',operator:'notEmpty',value:''}]},{},{textOrder:'binary',offlineSearch:true});
  if(!combined.supported)throw Error(combined.reason);expect((await f.predictions.page('s',combined.plan)).rows.map(row=>row.id)).toEqual(['new']);
 }finally{f.sql.close();}
});
