import {DatabaseSync} from 'node:sqlite';
import {expect,it} from 'vitest';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {SQLiteSnapshotActivator} from './sqlite-snapshot-activation.js';
import {SQLiteWindowStore,type SQLiteWindowHeader} from './sqlite-window-store.js';
import {replicaRowsHashes,replicaSumDigest} from './replica-integrity.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {ReplicaRow} from './local-replica.js';
const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},detail:{kind:'text'},value:{kind:'number'},config:{kind:'json'}},indexes:[{columns:['name']}],counts:[{columns:['name']}],search:['name','detail'],searchContent:'external',searchDetail:'column'}} as const;
async function fixture(native=false){
 const sql=new DatabaseSync(':memory:');const bodies:number[]=[];
 const raw:SQLiteReplicaDatabase={async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},
  async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},
  async getAllAsync<T>(query:string,...args:unknown[]){const rows=sql.prepare(query).all(...args as any[]) as T[];if(query.startsWith('SELECT id,body,position FROM gv_snapshot_rows'))bodies.push(rows.length);return rows;},
  async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}},
 };
 if(native){Object.assign(raw,{ingressUnicodeFold:true});sql.function('gv_ingress_lower',value=>String(value??'').toLowerCase());}
 const db=sqliteOwnerTransactions(raw),engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open('activation');
 const canonical=sqliteTypedReplicaStorage(db,schema);await canonical.ready();
 const fence=()=>engine.verifyOwner(),stager=new SQLiteSnapshotPages(db,fence),windows=new SQLiteWindowStore(db,fence),activator=new SQLiteSnapshotActivator(db,canonical,fence,native?schema:undefined);
 async function stage(rows:ReplicaRow[],snapshot='one',last=true,revision=10){
  const digest=replicaSumDigest(await replicaRowsHashes(rows,'id'));
  for(let offset=0;offset<rows.length;offset+=50){
   const final=last&&offset+50>=rows.length;
   await stager.append('s',{type:'replica.snapshotPage',id:'collection',snapshotId:snapshot,scope:'tenant',visibilityScope:'s',key:'id',cursor:{epoch:'e',revision},sequence:offset/50,last:final,result:rows.slice(offset,offset+50),...(final?{digest}:{}),maxRows:10000,maxBytes:10000000});
  }
  const header:SQLiteWindowHeader={signature:'collection',entity:'tasks',key:'id',kind:'replica',source:'server',completeness:'complete',cursor:{epoch:'e',revision},aggregateDigest:digest};
  return {scope:'s',subscription:'collection',snapshot,header};
 }
 const write=(revision:number,changes:any[])=>canonical.applyTransaction({cursor:{epoch:'e',revision},changes},{entities:{},liveQueries:{}},'s');
 return {sql,db,engine,canonical,stager,windows,activator,stage,write,bodies};
}
it('atomically merges bounded authoritative pages and publishes native membership without a corpus receipt',async()=>{
 const test=await fixture();try{
  await test.write(20,[{entity:'tasks',id:'existing',operation:'insert',newValue:{id:'existing',name:'newer'}},{entity:'tasks',id:'deleted',operation:'delete'}]);
  const rows:ReplicaRow[]=Array.from({length:300},(_,id)=>({id:String(id),name:'snapshot '+id,value:id}));
  rows.push({id:'existing',name:'older',detail:'missing projection'},{id:'deleted',name:'do not resurrect'});
  const input=await test.stage(rows),receipt=await test.activator.activate(input);
  expect(receipt.changes).toMatchObject({reset:true,entities:{},windows:{}});expect(JSON.stringify(receipt).length).toBeLessThan(1024);
  const values=await test.canonical.loadEntityRows('s','tasks',['existing','deleted','299']);
  expect(values.find(entry=>entry.id==='existing')?.row).toEqual({id:'existing',name:'newer',detail:'missing projection'});
  expect(values.some(entry=>entry.id==='deleted')).toBe(false);
  expect(JSON.parse((await test.db.getFirstAsync<{value:string}>("SELECT value FROM _gonvex_replica_meta WHERE scope='s' AND key=?",'row:'+JSON.stringify(['tasks','deleted'])))!.value).deleted).toBe(20);expect(values.find(entry=>entry.id==='299')?.row).toMatchObject({name:'snapshot 299'});
  const window=await test.windows.header('s','collection');expect(window).toMatchObject({rows:302,uniqueRows:302,header:input.header});
  expect((await test.windows.page('s','collection','one',299,3)).ids).toEqual(['299','existing','deleted']);
  expect(Math.max(...test.bodies)).toBeLessThanOrEqual(100);
  expect(JSON.parse((await test.db.getFirstAsync<{value:string}>("SELECT value FROM _gonvex_replica_meta WHERE scope='s' AND key='cursor'"))!.value).revision).toBe(20);
  await test.write(30,[{entity:'tasks',id:'existing',operation:'update',newValue:{name:'latest'}}]);
  expect(await test.activator.activate(input)).toEqual(receipt);
  expect((await test.canonical.loadEntityRows('s','tasks',['existing']))[0]?.row?.name).toBe('latest');
  await expect(test.activator.activate({...input,header:{...input.header,entity:'other'}})).rejects.toMatchObject({code:'PAGE_CHANGED'});
  expect(await test.engine.integrity()).toBe('ok');
 }finally{test.sql.close();}
});
it('rolls back canonical merges and window publication together if the native header is newer',async()=>{
 const test=await fixture();try{
  const input=await test.stage([{id:'new',name:'snapshot'}]);
  await test.windows.begin('s',{...input.header,cursor:{epoch:'e',revision:30}},'newer');
  const progress=await test.windows.append('s','collection','newer',0,JSON.stringify({ids:['already-visible']}));await test.windows.publish('s','collection',progress);
  await expect(test.activator.activate(input)).rejects.toMatchObject({code:'PAGE_CHANGED'});
  expect(await test.canonical.loadEntityRows('s','tasks',['new'])).toEqual([]);
  expect((await test.windows.header('s','collection'))?.generation).toBe('newer');
  expect(await test.db.getFirstAsync('SELECT 1 FROM gv_snapshot_activation_receipts')).toBeNull();
 }finally{test.sql.close();}
});
it('refuses incomplete stages and rolls back earlier pages when a later row violates the generated schema',async()=>{
 const test=await fixture();try{
  const incomplete=await test.stage([{id:'pending',name:'pending'}],'incomplete',false);
  await expect(test.activator.activate(incomplete)).rejects.toMatchObject({code:'PAGE_CHANGED'});
  const rows:ReplicaRow[]=Array.from({length:200},(_,id)=>({id:String(id),name:'valid'}));rows.push({id:'invalid',name:true});
  const input=await test.stage(rows,'invalid');await expect(test.activator.activate(input)).rejects.toThrow('field type');
  expect(await test.canonical.loadEntityRows('s','tasks',['0','199','invalid'])).toEqual([]);expect(await test.windows.header('s','collection')).toBeUndefined();
 }finally{test.sql.close();}
});
it('checks the owner fence even for a durable replay and clears old native coverage on an epoch replacement',async()=>{
 const test=await fixture();try{
  const input=await test.stage([{id:'one',name:'one'}]);await test.activator.activate(input);
  await test.write(1,[{entity:'tasks',id:'two',operation:'insert',newValue:{id:'two',name:'two'}}].map(change=>change));
  // A new server epoch retires coverage from the prior authorized generation.
  await test.canonical.applyTransaction({cursor:{epoch:'next',revision:1},changes:[{entity:'tasks',id:'next',operation:'insert',newValue:{id:'next'}}]},{entities:{},liveQueries:{}},'s');
  expect(await test.windows.header('s','collection')).toBeUndefined();
  await expect(test.activator.activate(input)).rejects.toMatchObject({code:'FENCED'});
  const next=new SQLiteTypedReplicaEngine(test.db,schema);await next.open('replacement');
  await expect(test.activator.activate(input)).rejects.toMatchObject({code:'FENCED'});
 }finally{test.sql.close();}
});

it('retires absent members through field authority while preserving newer rows and other-window references',async()=>{
 const test=await fixture();try{
  await test.write(5,[{entity:'tasks',id:'removed',operation:'insert',newValue:{id:'removed',name:'old'}},{entity:'tasks',id:'shared',operation:'insert',newValue:{id:'shared',name:'shared'}}]);
  await test.write(20,[{entity:'tasks',id:'newer',operation:'insert',newValue:{id:'newer',name:'newer'}}]);
  const input=await test.stage([{id:'current',name:'current'}]);
  const old={...input.header,cursor:{epoch:'e',revision:5}};
  await test.windows.begin('s',old,'old');
  const oldProgress=await test.windows.append('s','collection','old',0,JSON.stringify({ids:['removed','shared','newer']}));await test.windows.publish('s','collection',oldProgress);
  await test.windows.begin('s',{...old,signature:'other-window'},'other');
  const otherProgress=await test.windows.append('s','other-window','other',0,JSON.stringify({ids:['shared']}));await test.windows.publish('s','other-window',otherProgress);
  await test.activator.activate(input);
  const values=await test.canonical.loadEntityRows('s','tasks',['removed','shared','newer','current']);
  expect(values.map(entry=>entry.id)).toEqual(['shared','newer','current']);
  expect((await test.windows.page('s','collection','one',0)).ids).toEqual(['current']);
  expect((await test.windows.page('s','other-window','other',0)).ids).toEqual(['shared']);
 }finally{test.sql.close();}
});

it('inserts fresh typed rows natively with baseline authority and decodes only the overlap',async()=>{
 const test=await fixture(true);try{
  await test.write(20,[{entity:'tasks',id:'existing',operation:'insert',newValue:{id:'existing',name:'newer'}},{entity:'tasks',id:'deleted',operation:'delete'}]);
  const rows:ReplicaRow[]=Array.from({length:300},(_,id)=>({id:String(id),name:'CAFÉ snapshot '+id,value:id,config:{nested:[true,null,id]},unknown:'preserved'}));
  rows.push({id:'existing',name:'old',detail:'old projection'},{id:'deleted',name:'old'});
  const input=await test.stage(rows),receipt=await test.activator.activate(input);
  expect(receipt.changes).toMatchObject({reset:true,entities:{},windows:{}});
  expect(test.bodies).toEqual([2]);
  expect((await test.canonical.loadEntityRows('s','tasks',['existing']))[0]?.row).toEqual({id:'existing',name:'newer',detail:'old projection'});
  expect(await test.canonical.loadEntityRows('s','tasks',['deleted'])).toEqual([]);
  expect((await test.canonical.loadEntityRows('s','tasks',['299']))[0]?.row).toEqual(rows[299]);
  expect(await test.db.getFirstAsync("SELECT value FROM _gonvex_replica_meta WHERE scope='s' AND key=?",'row:'+JSON.stringify(['tasks','299']))).toBeNull();
  const freshBase=await test.db.getFirstAsync<{version:number}>("SELECT version FROM gv_t_007400610073006b0073 WHERE scope='s' AND id='299'");expect(freshBase?.version).toBe(10);
  await test.write(5,[{entity:'tasks',id:'299',operation:'update',newValue:{name:'stale',detail:'missing older field'}}]);
  expect((await test.canonical.loadEntityRows('s','tasks',['299']))[0]?.row).toMatchObject({name:'CAFÉ snapshot 299',detail:'missing older field'});
  expect((await test.engine.page('s',{table:'tasks',search:'café',order:[{field:'id',direction:'asc'}],textOrder:'binary',limit:50})).rows).toHaveLength(50);
  expect(await test.engine.integrity()).toBe('ok');
  expect((await test.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM temp.gv_snapshot_overlap'))?.n).toBe(0);
 }finally{test.sql.close();}
});
it('rolls back native fresh insertion and all publication state on a later generation conflict',async()=>{
 const test=await fixture(true);try{
  await test.write(20,[{entity:'tasks',id:'existing',operation:'insert',newValue:{id:'existing',name:'original café'}}]);
  const input=await test.stage(Array.from({length:700},(_,id)=>({id:'fresh-'+id,name:'fresh café',value:id})));
  const header={...input.header,cursor:{epoch:'e',revision:30}};
  await test.windows.begin('s',header,'newer');const progress=await test.windows.append('s','collection','newer',0,JSON.stringify({ids:[]}));await test.windows.publish('s','collection',progress);
  await expect(test.activator.activate(input)).rejects.toMatchObject({code:'PAGE_CHANGED'});
  expect(await test.canonical.loadEntityRows('s','tasks',['fresh-0','fresh-699'])).toEqual([]);
  expect((await test.engine.page('s',{table:'tasks',search:'original café',order:[{field:'id',direction:'asc'}],textOrder:'binary',limit:50})).rows.map(row=>row.id)).toEqual(['existing']);
  expect((await test.windows.header('s','collection'))?.generation).toBe('newer');expect(await test.engine.integrity()).toBe('ok');
 }finally{test.sql.close();}
});

it('builds indexes and FTS once after a large native activation while keeping the newer first-page row',async()=>{
 const test=await fixture(true);try{
  await test.write(20,[{entity:'tasks',id:'first-page',operation:'insert',newValue:{id:'first-page',name:'CAFÉ newer first page'}}]);
  const rows:ReplicaRow[]=Array.from({length:700},(_,id)=>({id:String(id),name:'CAFÉ snapshot '+id}));rows.push({id:'first-page',name:'stale first page',detail:'projection'});
  const input=await test.stage(rows);await test.activator.activate(input);
  expect((await test.canonical.loadEntityRows('s','tasks',['first-page']))[0]?.row).toEqual({id:'first-page',name:'CAFÉ newer first page',detail:'projection'});
  expect((await test.engine.page('s',{table:'tasks',search:'café',order:[{field:'id',direction:'asc'}],textOrder:'binary',limit:50})).rows).toHaveLength(50);
  expect(await test.engine.integrity()).toBe('ok');
  // Restored triggers must maintain the rebuilt index on subsequent writes.
  await test.write(30,[{entity:'tasks',id:'first-page',operation:'update',newValue:{name:'Unique replacement needle'}}]);
  expect((await test.engine.page('s',{table:'tasks',search:'replacement needle',order:[{field:'id',direction:'asc'}],textOrder:'binary',limit:50})).rows.map(row=>row.id)).toEqual(['first-page']);
  expect(await test.engine.integrity()).toBe('ok');
 }finally{test.sql.close();}
});

it('requires initialization before a parent ingress transaction so rollback cannot poison cached schema readiness',async()=>{
 const test=await fixture();try{
  const input=await test.stage([{id:'one',name:'one'}]);
  await expect(test.db.withTransactionAsync(async()=>{await test.activator.activate(input);})).rejects.toMatchObject({code:'FENCED'});
  await test.activator.ready();
  await test.db.withTransactionAsync(async()=>{await test.activator.activate(input);});
  expect((await test.windows.header('s','collection'))?.rows).toBe(1);
 }finally{test.sql.close();}
});
it('prunes disconnected canonical orphans only for a generated complete entity source and preserves newer authority',async()=>{
 for(const native of [false,true]){
  const f=await fixture(native);try{
   const first=await f.stage([{id:'old-member',name:'old window'}]);await f.activator.activate(first);
   await f.write(15,[{entity:'tasks',id:'orphan',operation:'insert',newValue:{id:'orphan',name:'arrived outside old membership'}}]);await f.write(25,[{entity:'tasks',id:'newer',operation:'insert',newValue:{id:'newer',name:'newer than replacement'}}]);
   const replacement=await f.stage([{id:'kept',name:'replacement'}],'replacement',true,20);
   replacement.header.entityCoverage='complete';await f.activator.activate(replacement);
   expect((await f.canonical.loadEntityRows('s','tasks',['old-member','orphan','newer','kept'])).map(entry=>entry.id)).toEqual(['newer','kept']);
   expect((await f.windows.page('s','collection','replacement',0,10)).ids).toEqual(['kept']);expect(await f.engine.integrity()).toBe('ok');
  }finally{f.sql.close();}
 }
});
it('keeps canonical rows outside a filtered source and refuses a partial full-entity claim',async()=>{
 const f=await fixture();try{
  await f.write(5,[{entity:'tasks',id:'outside',operation:'insert',newValue:{id:'outside',name:'another source'}}]);const source=await f.stage([{id:'inside',name:'filtered'}]);await f.activator.activate(source);expect((await f.canonical.loadEntityRows('s','tasks',['outside']))[0].row.name).toBe('another source');
  const invalid=await f.stage([{id:'next',name:'partial'}],'partial');invalid.header.entityCoverage='complete';invalid.header.completeness='partial';await expect(f.activator.activate(invalid)).rejects.toThrow('Invalid snapshot activation');expect((await f.canonical.loadEntityRows('s','tasks',['outside']))[0].row.name).toBe('another source');
 }finally{f.sql.close();}
});
