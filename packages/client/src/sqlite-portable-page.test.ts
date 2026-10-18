import { afterEach, expect, it } from 'vitest';
import type {Database} from '@sqlite.org/sqlite-wasm';
import {initializePinnedSQLite} from '../test/sqlite-wasm-host.mjs';
import type { DataPredicate, DataRead, JsonObject } from '@gonvex/module-sdk';
import { memoryReadView } from '@gonvex/local-runtime/read-view';
import { officialSQLiteDatabase } from './sqlite-official-host.js';
import { SQLiteTypedReplicaEngine } from './sqlite-typed-engine.js';
import { sqliteTypedReplicaStorage } from './sqlite-typed-row-layout.js';
const sqlite3=await initializePinnedSQLite(), connections:Database[]=[];
afterEach(()=>{for(const db of connections.splice(0))if(db.pointer)db.close();});
const schema={records:{key:'_id',fields:{_id:{kind:'text'},title:{kind:'text'},rank:{kind:'number'},done:{kind:'boolean'},data:{kind:'json'},payload:{kind:'text'}},indexes:[{columns:['rank']},{columns:['title']},{columns:['done']}]}} as const;
async function fixture(rows:JsonObject[]) {
  const db=new sqlite3.oo1.DB(':memory:');connections.push(db);
  const host=officialSQLiteDatabase(sqlite3,db,{maxRows:32,maxBytes:8192});
  const engine=new SQLiteTypedReplicaEngine(host.database,schema);await engine.open('owner');
  await engine.importScope('s',(async function*(){for(let at=0;at<rows.length;at+=8)yield JSON.stringify(rows.slice(at,at+8).map(row=>({table:'records',id:row._id,version:1,row})));})());
  return {db,engine,storage:sqliteTypedReplicaStorage(host.database,schema)};
}
it('matches portable scalar, Unicode, JSON, null, residual and projection semantics in official WASM',async()=>{
  const titles=['İSTANBUL','CAFÉ',' 東京 ','ΟΣ','😀','\uE000','a\u0000b','plain'];
  const rows=Array.from({length:80},(_,n)=>({_id:String(n).padStart(3,'0'),rank:n%11-5,done:n%2===0,title:titles[n%titles.length],data:n%3===0?null:[n%5,'café'],...(n%7===0?{unknown:'extra'}:{})})) as JsonObject[];
  // Missing and explicit null values must remain distinguishable to coverage.
  rows.push({_id:'missing'},{_id:'null',title:null,rank:null,done:null,data:null});
  const {storage,engine}=await fixture(rows);
  const coverage={records:{key:'_id',complete:true}};
  const reference=memoryReadView(new Map([['records',new Map(rows.map(row=>[String(row._id),row]))]]),coverage);
  const predicates:DataPredicate[]=[
    {column:'rank',op:'eq',value:2},{column:'rank',op:'ne',value:2},{column:'rank',op:'gt',value:0},
    {column:'rank',op:'lte',value:-1},{column:'rank',op:'in',values:[-3,1,null,'wrong',true]},
    {column:'title',op:'eq',value:'a\u0000b'},{column:'title',op:'lt',value:'\uE000'},
    {column:'title',op:'in',values:['😀','CAFÉ',null]},{column:'title',op:'isNull'},{column:'title',op:'notNull'},
    {column:'title',op:'eq',value:'café',transform:'lower'},
    {column:'title',op:'eq',value:'東京',transform:'trim'},
    {column:'title',op:'matches',pattern:'^[A-Z]'},
    {column:'data',op:'arrayContains',value:2},{column:'data',op:'isNull'},
    {column:'done',op:'eq',value:false},{column:'done',op:'lt',value:true},
    {column:'unknown',op:'eq',value:'extra'},{column:'rank',op:'eq',value:null},
    {and:[{column:'rank',op:'gte',value:0},{column:'done',op:'eq',value:true}]},
    {or:[{column:'rank',op:'eq',value:-3},{column:'title',op:'eq',value:'東京',transform:'trim'}]},
  ];
  for(const where of predicates)for(const columns of [undefined,['_id','title','data'],['_id','unknown']] as const){
    const read:DataRead={table:'records',where,columns,orderBy:[{column:'title',direction:'desc'}],limit:7};
    expect(await storage.withReadView('s',coverage,view=>view.select(read,['001'])),JSON.stringify(read)).toEqual(await reference.select(read,['001']));
  }
  for(const where of [{and:[{column:'rank',op:'eq',value:'invalid'},{column:'rank',op:'eq',value:999}]},{and:[{column:'rank',op:'eq',value:'invalid',transform:'lower'},{column:'rank',op:'eq',value:999}]}] as DataPredicate[]){
    await expect(storage.withReadView('s',coverage,view=>view.select({table:'records',where}))).rejects.toThrow();
  }
  expect(await engine.integrity()).toBe('ok');
});
it('projects narrow columns before the native result budget and uses generated indexes without a scalar copy',async()=>{
  const rows=Array.from({length:80},(_,n)=>({_id:String(n).padStart(3,'0'),rank:n,title:'Title '+n,payload:'x'.repeat(9000)}));
  const {storage,db}=await fixture(rows);
  const result=await storage.withReadView('s',{records:{key:'_id',complete:true}},view=>view.select({table:'records',columns:['_id','rank'],where:{column:'rank',op:'gte',value:70},orderBy:[{column:'rank',direction:'desc'}],limit:3}));
  expect(result).toEqual({complete:true,rows:[{_id:'079',rank:79},{_id:'078',rank:78},{_id:'077',rank:77}]});
  expect(db.selectValue("SELECT count(*) FROM sqlite_master WHERE name='_gonvex_replica_lookup'")).toBe(0);
});
