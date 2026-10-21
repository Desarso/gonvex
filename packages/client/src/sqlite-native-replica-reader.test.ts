import {sqlitePayloadBytes} from './sqlite-admission.js';
import {DatabaseSync} from 'node:sqlite';
import {expect,it} from 'vitest';
import {SQLiteNativeReplicaReader} from './sqlite-native-replica-reader.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteWindowStore} from './sqlite-window-store.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'}},indexes:[]}} as const;
async function fixture(n=300,text='value'){
 const sql=new DatabaseSync(':memory:');let decodedBodies=0,compatibilityReads=0;
 const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},
 async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},
 async getAllAsync<T>(query:string,...args:unknown[]){const result=sql.prepare(query).all(...args as any[]) as T[];if(query.startsWith('SELECT scope,entity,id,value')||query.startsWith('SELECT t.id,'))decodedBodies+=result.length;if(query.includes('FROM _gonvex_replica_entities'))compatibilityReads++;return result;},
 async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase);
 const engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open('reader');const canonical=sqliteTypedReplicaStorage(db,schema),windows=new SQLiteWindowStore(db,()=>engine.verifyOwner()),reader=new SQLiteNativeReplicaReader(db,canonical,engine);await reader.ready();
 const batch=Math.max(1,Math.min(50,Math.floor(100000/(text.length*2+1024))));
 for(let offset=0;offset<n;offset+=batch)await engine.apply('s',Array.from({length:Math.min(batch,n-offset)},(_,i)=>({table:'tasks',id:String(offset+i),version:1,row:{id:String(offset+i),name:text}})));
 async function publish(generation:string){
  const header={signature:'collection',entity:'tasks',key:'id',kind:'replica',source:'server',completeness:'complete'} as const;await windows.begin('s',header,generation);
  let progress;for(let offset=0;offset<n;offset+=100)progress=await windows.append('s','collection',generation,offset/100,JSON.stringify({ids:Array.from({length:Math.min(100,n-offset)},(_,i)=>String(offset+i))}));
  await windows.publish('s','collection',progress!);
 }
 await publish('one');return {sql,db,engine,canonical,reader,publish,get decodedBodies(){return decodedBodies;},get compatibilityReads(){return compatibilityReads;}};
}
it('reads a bounded ordered native page, retaining tombstone slots without fetching the membership corpus',async()=>{
 const f=await fixture();try{
  await f.canonical.applyTransaction({cursor:{epoch:'e',revision:3},changes:[{entity:'tasks',id:'1',operation:'delete'},{entity:'tasks',id:'2',operation:'update',newValue:{id:'2',name:'newer'}}]},{entities:{},liveQueries:{}},'s');
  const before=f.decodedBodies;const first=await f.reader.window({scope:'s',signature:'collection',limit:3});
  expect(first).toMatchObject({window:{generation:'one',total:300},offset:0,nextOffset:3,rows:[{id:'0',value:{id:'0',name:'value'}},{id:'1',value:null},{id:'2',value:{id:'2',name:'newer'}}]});expect(f.decodedBodies-before).toBe(2); // Only two live bodies, never the tombstone or the 300-member corpus.
  expect(f.compatibilityReads).toBe(0); // A point read never expands the full-schema compatibility view.
  const next=await f.reader.window({scope:'s',signature:'collection',generation:'one',offset:3,limit:3,expected:first!.generation});expect(next!.rows.map(row=>row.id)).toEqual(['3','4','5']);
  await expect(f.reader.window({scope:'s',signature:'collection',offset:3})).rejects.toThrow('generation');
  await f.publish('two');await expect(f.reader.window({scope:'s',signature:'collection',generation:'one',offset:3})).rejects.toMatchObject({code:'PAGE_CHANGED'});
  expect(await f.reader.window({scope:'other',signature:'collection'})).toBeUndefined();
 }finally{f.sql.close();}
});
it('shrinks large pages before decoding bodies and rejects a single oversized entity',async()=>{
 const f=await fixture(40,'x'.repeat(6000));try{
  const page=await f.reader.window({scope:'s',signature:'collection',limit:40});expect(page!.rows.length).toBeGreaterThan(0);expect(page!.rows.length).toBeLessThan(40);expect(f.decodedBodies).toBe(page!.rows.length);
  await f.canonical.applyTransaction({cursor:{epoch:'e',revision:3},changes:[{entity:'tasks',id:'0',operation:'update',newValue:{name:'x'.repeat(100000)}}]},{entities:{},liveQueries:{}},'s');
  const before=f.decodedBodies;await expect(f.reader.entities({scope:'s',entity:'tasks',ids:['0']})).rejects.toMatchObject({code:'BUDGET'});expect(f.decodedBodies).toBe(before);
 }finally{f.sql.close();}
});
it('fences changed committed generations and displaced owners, and bounds entity requests',async()=>{
 const f=await fixture(2);try{
  const page=await f.reader.entities({scope:'s',entity:'tasks',ids:['1','missing','1']});expect(page.rows.map(row=>row.id)).toEqual(['1','missing','1']);expect(page.rows[1].value).toBeNull();
  await f.engine.apply('s',[{table:'tasks',id:'2',version:2,row:{id:'2',name:'later'}}]);
  await expect(f.reader.entities({scope:'s',entity:'tasks',ids:['1'],expected:page.generation})).rejects.toMatchObject({code:'PAGE_CHANGED'});
  await expect(f.reader.entities({scope:'s',entity:'tasks',ids:Array(129).fill('1')})).rejects.toThrow('bounded entity');
  await f.db.runAsync("UPDATE gv_storage_meta SET epoch=epoch+1,owner='replacement' WHERE id=1");await expect(f.reader.window({scope:'s',signature:'collection'})).rejects.toMatchObject({code:'FENCED'});
 }finally{f.sql.close();}
});

it('reads predicted replacements and tombstones through the same bounded window and entity slots',async()=>{
 const {SQLitePredictions}=await import('./sqlite-predictions.js'),f=await fixture(3);try{
  const predictions=new SQLitePredictions(f.db,f.canonical,schema,()=>f.engine.verifyOwner());await predictions.ready();f.reader.usePredictions(predictions);
  await predictions.apply('s',1,[{entity:'tasks',rowId:'0',op:'patch',fields:{name:'predicted'}},{entity:'tasks',rowId:'1',op:'delete'},{entity:'tasks',rowId:'local',op:'insert',fields:{id:'local',name:'offline'}}]);
  const page=await f.reader.window({scope:'s',signature:'collection',limit:3});expect(page!.rows).toEqual([{id:'0',value:{id:'0',name:'predicted'}},{id:'1',value:null},{id:'2',value:{id:'2',name:'value'}}]);
  expect((await f.reader.entities({scope:'s',entity:'tasks',ids:['local','1','0']})).rows).toEqual([{id:'local',value:{id:'local',name:'offline'}},{id:'1',value:null},{id:'0',value:{id:'0',name:'predicted'}}]);
  expect((await f.canonical.loadEntityRows('s','tasks',['0']))[0].row.name).toBe('value');
  await predictions.reset('s');expect((await f.reader.entities({scope:'s',entity:'tasks',ids:['0','1','local']})).rows.map(row=>row.value?.name??null)).toEqual(['value','value',null]);
 }finally{f.sql.close();}
});

it('continues byte-bounded wide entity hydration without skipping IDs or losing its generation',async()=>{
 const f=await fixture(128,'wide'.repeat(1500));try{
  const ids=Array.from({length:128},(_,n)=>String(n)),seen:string[]=[];let offset=0,generation:any;
  for(;;){const page=await f.reader.entities({scope:'s',entity:'tasks',ids,offset,...(generation?{expected:generation}:{})});generation??=page.generation;
   expect(sqlitePayloadBytes(page,131072)).toBeLessThanOrEqual(131072);expect(page.rows.length).toBeLessThan(128);seen.push(...page.rows.map(row=>row.id));
   if(page.nextOffset===null)break;expect(page.nextOffset).toBeGreaterThan(offset);offset=page.nextOffset!;
  }expect(seen).toEqual(ids);expect(new Set(seen).size).toBe(128);
 }finally{f.sql.close();}
});
