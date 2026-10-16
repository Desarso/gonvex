import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {Dexie} from 'dexie';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {IndexedDBLocalReplicaStorage} from './indexeddb-replica';
import type {ReplicaWindow} from './local-replica';
const dependencies={...Dexie.dependencies};
let storage:IndexedDBLocalReplicaStorage;
beforeEach(()=>{Dexie.dependencies.indexedDB=new IDBFactory();Dexie.dependencies.IDBKeyRange=IDBKeyRange;storage=new IndexedDBLocalReplicaStorage('cursor-only-'+Math.random());});
afterEach(()=>{vi.restoreAllMocks();storage.close();Object.assign(Dexie.dependencies,dependencies);});
const window:ReplicaWindow={signature:'items',kind:'replica',entity:'items',key:'id',ids:Array.from({length:1000},(_,i)=>'item-'+i),hashes:Object.fromEntries(Array.from({length:1000},(_,i)=>['item-'+i,'ab'.repeat(32)])),cursor:{epoch:'e',revision:1},completeness:'complete',source:'server'};
async function seed(windows=[window]){await storage.replaceSnapshot({entities:{},liveQueries:Object.fromEntries(windows.map(w=>[w.signature,w])),cursor:window.cursor},'tenant');}
function advance(windows:ReplicaWindow[],revision:number){return (storage.advanceCursorWatermark??storage.advanceWatermark).call(storage,windows,{epoch:'e',revision},'tenant');}

it('advances a cursor without inspecting or decoding large membership and hash values',async()=>{
 await seed();const table=(storage as any).database.windows,prior=await table.get(['tenant','items']);
 const parse=vi.spyOn(JSON,'parse');
 const cursor={epoch:'e',revision:12345};const next={...window,cursor};
 next.ids=new Proxy(window.ids,{get(target,key){if(key==='length')return target.length;throw Error('Cursor checkpoint inspected membership values');}});
 Object.defineProperty(next,'hashes',{get(){throw Error('Cursor checkpoint inspected hashes');}});
 await advance([next],cursor.revision);
 expect(parse.mock.calls.some(([value])=>value===prior.value)).toBe(false);
 const after=await table.get(['tenant','items']);
 expect(after.value.slice(after.cursorPrefixLength)).toBe(prior.value.slice(prior.cursorPrefixLength));
 expect(JSON.parse(after.value)).toEqual({...window,cursor});
 expect((await storage.load('tenant'))?.liveQueries.items).toEqual({...window,cursor});
 expect((await storage.readChanges('tenant',0)).windows.items?.cursor).toEqual(cursor);
});

it('keeps newer cursors and rolls back every window when a later write fails',async()=>{
 const other={...window,signature:'other'};await seed([window,other]);
 const table=(storage as any).database.windows,put=table.put.bind(table);
 vi.spyOn(table,'put').mockImplementation((record:any)=>{if(record.signature==='other')throw Error('late quota failure');return put(record);});
 await expect(advance([window,other].map(w=>({...w,cursor:{epoch:'e',revision:3}})),3)).rejects.toThrow('late quota failure');
 expect((await storage.load('tenant'))?.cursor).toEqual(window.cursor);
 expect((await storage.load('tenant'))?.liveQueries.items.cursor).toEqual(window.cursor);
 vi.restoreAllMocks();
 await advance([{...window,cursor:{epoch:'e',revision:9}}],9);
 await expect(advance([{...window,cursor:{epoch:'e',revision:2}}],2)).resolves.toBeUndefined();
 expect((await storage.load('tenant'))?.cursor).toEqual({epoch:'e',revision:9});
 expect((await storage.load('tenant'))?.liveQueries.items).toEqual({...window,cursor:{epoch:'e',revision:9}});
});

it('upgrades an ordinary legacy JSON record without losing certificates or metadata',async()=>{
 await seed();const table=(storage as any).database.windows;
 const old=await table.get(['tenant','items']);delete old.cursorPrefixLength;old.value=JSON.stringify(window);await table.put(old);
 await advance([{...window,cursor:{epoch:'e',revision:2}}],2);
 expect((await storage.load('tenant'))?.liveQueries.items).toEqual({...window,cursor:{epoch:'e',revision:2}});
 const record=await table.get(['tenant','items']);expect(record.cursorPrefixLength).toBeGreaterThan(0);
});

it('keeps the full metadata path for integrity confirmations and changed membership',async()=>{
 await seed();await advance([{...window,cursor:{epoch:'e',revision:2}}],2);
 const changed={...window,ids:['item-1'],hashes:{'item-1':'cd'.repeat(32)},truncated:true,cursor:{epoch:'e',revision:3}};
 await storage.advanceWatermark([changed],changed.cursor,'tenant');
 expect((await storage.load('tenant'))?.liveQueries.items).toEqual(changed);
 await advance([{...changed,cursor:{epoch:'e',revision:4}}],4);
 expect((await storage.load('tenant'))?.liveQueries.items).toEqual({...changed,cursor:{epoch:'e',revision:4}});
});
