import {afterEach,expect,it} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {IndexedDBCollectionPages} from './indexeddb-collection-page.js';
import {IndexedDBLiveWindows} from './indexeddb-live-windows.js';
import {ReplicaSetDigest} from './replica-set-digest.js';
import type {FunctionReference} from './index.js';
const closes:Array<()=>void>=[];afterEach(()=>{for(const close of closes.splice(0))close();});
const schema={tasks:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'},name:{kind:'text'}},indexes:[],search:['name']}} as const;
const ref:FunctionReference={kind:'query',path:'__local.tasks',delivery:'replica',replica:{table:'tasks',key:'id',columns:['id','n','name'],maxRows:1000,maxBytes:1000000}};
async function fixture(){const engine=new IndexedDBLiveWindows(schema,'collection-pages',new IDBFactory(),IDBKeyRange);closes.push(()=>engine.close());await engine.claim('owner');const rows=Array.from({length:150},(_,n)=>({id:'id-'+String(n).padStart(3,'0'),n,name:n%2?'odd':'even'})),encoded=rows.map(row=>JSON.stringify(row)),receipt=new ReplicaSetDigest();for(const value of encoded)receipt.add(Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join(''));const identity={scope:'visible',source:'source',snapshot:'base'},header={signature:'signature',entity:'tasks',key:'id',kind:'replica' as const,source:'server' as const,cursor:{epoch:'e',revision:1},aggregateDigest:receipt.digest(),completeness:'complete' as const,columns:['id','n','name'],entityCoverage:'complete' as const,artifactHash:'artifact'};await engine.beginSnapshot(identity,header,{maxRows:1000,maxBytes:1000000});await engine.appendSnapshot(identity,0,encoded.slice(0,100),false);await engine.appendSnapshot(identity,1,encoded.slice(100),true,receipt.digest());await engine.atomic(undefined,tx=>engine.publishSnapshotInTransaction(tx,identity));return {engine,pages:new IndexedDBCollectionPages(engine,schema)};}
it('streams beyond the first disk page, applies keyset and predicted inserts with a bounded winner set',async()=>{
 const f=await fixture(),base={scope:'visible',epoch:'e',artifactHash:'artifact',signature:'signature',ref,args:{},plan:{order:[{field:'n',direction:'desc' as const}],limit:2}};
 const first=await f.pages.page(base,new Set(['tasks']));expect(first?.rows.map(row=>row.n)).toEqual([149,148]);expect(first?.cursor?.id).toBe('id-148');
 const second=await f.pages.page({...base,generation:first!.generation,plan:{...base.plan,after:first!.cursor!}},new Set(['tasks']));expect(second?.rows.map(row=>row.n)).toEqual([147,146]);
 await f.engine.atomic(undefined,tx=>f.engine.predictionsInTransaction(tx,'visible','offline',[{entity:'tasks',rowId:'local',op:'insert',fields:{id:'local',n:200,name:'even'}}]));const updated=await f.pages.page({...base,plan:{...base.plan,search:'even'}},new Set(['tasks']));expect(updated?.rows.map(row=>row.n)).toEqual([200,148]);
 await expect(f.pages.page({...base,generation:first!.generation,plan:{...base.plan,after:first!.cursor!}},new Set(['tasks']))).rejects.toMatchObject({code:'PAGE_CHANGED'});
});
