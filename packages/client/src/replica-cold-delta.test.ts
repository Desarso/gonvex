import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {Dexie} from 'dexie';
import {IndexedDBLocalReplicaStorage} from './indexeddb-replica';
import {LocalReplica,type ReplicaWindow} from './local-replica';
let storage:IndexedDBLocalReplicaStorage;
beforeEach(()=>{
  Dexie.dependencies.indexedDB=new IDBFactory();Dexie.dependencies.IDBKeyRange=IDBKeyRange;
  storage=new IndexedDBLocalReplicaStorage('cold-delta-'+Math.random(),'mounted');
});
afterEach(()=>storage.close());
const rows=Array.from({length:20},(_,i)=>({_id:'row-'+i,name:'Name '+String(i).padStart(2,'0'),details:'Persisted detail '+i}));
async function seed(ordered=false){
 const window:ReplicaWindow={signature:'all',kind:'replica',entity:'items',key:'_id',ids:rows.map(r=>r._id),completeness:'complete',source:'server',cursor:{epoch:'e',revision:1},...(ordered?{orderBy:'name',orderDirection:'asc' as const}:{})};
 await storage.replaceWindow(window,{entities:{items:Object.fromEntries(rows.map(r=>[r._id,r]))},liveQueries:{all:window}},'scope',rows);
 const replica=new LocalReplica(storage,{maxResidentRows:2});await replica.activateScope('scope');
 expect(replica.entityRows('items')).toHaveLength(0);
 return replica;
}
it('preserves cold members across a cursor-only delta without loading rows',async()=>{
 const replica=await seed();const load=vi.spyOn(storage,'loadEntityRows');
 await replica.applyWindowDelta({signature:'all',entity:'items',key:'_id',upserts:[],deleted:[],cursor:{epoch:'e',revision:2}});
 expect(replica.getWindow('all')?.ids).toEqual(rows.map(r=>r._id));
 expect(load).not.toHaveBeenCalled();expect(replica.entityRows('items')).toHaveLength(0);
 expect((await storage.loadWindowRows('scope','all'))?.rows).toHaveLength(20);
});
it('updates and deletes cold rows without replacing full membership with the resident subset',async()=>{
 const replica=await seed();
 await replica.applyWindowDelta({signature:'all',entity:'items',key:'_id',upserts:[{_id:'row-0',name:'Updated'},{_id:'new',name:'New'}],deleted:['row-1'],cursor:{epoch:'e',revision:2}});
 const ids=[...rows.filter(r=>r._id!=='row-1').map(r=>r._id),'new'];
 expect(replica.getWindow('all')?.ids).toEqual(ids);
 expect(replica.entityRows('items').length).toBeLessThanOrEqual(2);
 expect(replica.entity('items','row-0')?.details).toBe('Persisted detail 0');
 const persisted=await storage.loadWindowRows('scope','all');expect(persisted?.rows.map(r=>r._id)).toEqual(ids);
 expect(persisted?.rows.find(r=>r._id==='row-0')?.name).toBe('Updated');
});
it.each(['asc','desc'] as const)('orders changed cold rows through bounded indexed lookups, %s',async(direction)=>{
 const replica=await seed(true);
 if(direction==='desc')await replica.replaceWindow({signature:'all',entity:'items',key:'_id',kind:'replica',rows:[...rows].reverse(),completeness:'complete',source:'server',cursor:{epoch:'e',revision:1},orderBy:'name',orderDirection:direction,scope:'scope'});
 const load=vi.spyOn(storage,'loadEntityRows'),all=vi.spyOn(storage,'loadWindowRows');
 const changed=[{_id:'row-0',name:'ZZZ'},{_id:'row-19',name:'AAA'}];
 await replica.applyWindowDelta({signature:'all',entity:'items',key:'_id',upserts:changed,deleted:[],cursor:{epoch:'e',revision:2}});
 const values=rows.map(row=>changed.find(r=>r._id===row._id)??row).sort((a,b)=>(a.name<b.name?-1:a.name>b.name?1:0)*(direction==='desc'?-1:1));
 expect(replica.getWindow('all')?.ids).toEqual(values.map(r=>r._id));
 expect(replica.entityRows('items').length).toBeLessThanOrEqual(2);
 expect(all).not.toHaveBeenCalled();expect(load.mock.calls.every(([, ,ids])=>ids.length<=1)).toBe(true);
});
