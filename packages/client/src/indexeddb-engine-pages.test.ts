import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { IDBFactory, IDBKeyRange } from 'fake-indexeddb';
import { Dexie } from 'dexie';
import { IndexedDBEngineReplicaStorage } from './indexeddb-engine';
import type { ReplicaWindow } from './local-replica';
let storage: IndexedDBEngineReplicaStorage;
const originalDependencies = { indexedDB: Dexie.dependencies.indexedDB, IDBKeyRange: Dexie.dependencies.IDBKeyRange };
beforeEach(() => {
  Dexie.dependencies.indexedDB = new IDBFactory(); Dexie.dependencies.IDBKeyRange = IDBKeyRange;
  storage = new IndexedDBEngineReplicaStorage('row-pages-'+Math.random());
});
afterEach(() => { vi.restoreAllMocks(); storage.close(); Object.assign(Dexie.dependencies, originalDependencies); });
async function seed(rows: Array<{id: string; title: string}>, scope='tenant', ids=rows.map(row=>row.id), hashes?: Record<string,string>) {
  const window: ReplicaWindow = { signature: 'items', kind:'replica', entity:'items', key:'id', ids,
    completeness:'complete', source:'server', cursor:{epoch:'e',revision:1}, hashes };
  await storage.replaceWindow(window,{entities:{},liveQueries:{},cursor:window.cursor},scope,rows);
}

it('reads only the requested disk range without reconstructing the full window', async () => {
  const rows = Array.from({length:1_000},(_,i)=>({id:'row-'+i,title:'Title '+i}));
  await seed(rows);
  const materialize = vi.spyOn(storage as any,'materializeWindow').mockImplementation(()=>{throw Error('Unbounded materialization');});
  const gets = vi.spyOn((storage as any).database.entities,'get');
  const page = await storage.loadReplicaRowsPage('tenant','items',{offset:700,limit:5,maxBytes:10_000});
  expect(page).toMatchObject({offset:700,nextOffset:705,totalUniqueRows:1_000,ids:rows.slice(700,705).map(row=>row.id),rows:rows.slice(700,705),missingIds:[]});
  expect(page?.window).not.toHaveProperty('ids');expect(page?.window).not.toHaveProperty('hashes');
  expect(gets).toHaveBeenCalledTimes(5);expect(materialize).not.toHaveBeenCalled();
  expect(await storage.loadReplicaRowsPage('other','items',{offset:0,limit:5,maxBytes:10_000})).toBeUndefined();
});

it('stops at the byte budget and reports an oversized row without skipping it', async () => {
  const small={id:'a',title:'small'},large={id:'b',title:'large'.repeat(1000)};
  await seed([small,large]);const bytes=JSON.stringify(small).length*2+256;
  const first=await storage.loadReplicaRowsPage('tenant','items',{offset:0,limit:2,maxBytes:bytes});
  expect(first).toMatchObject({ids:['a'],rows:[small],nextOffset:1,totalUniqueRows:2,decodedBytes:bytes,blockedId:'b'});
  const blocked=await storage.loadReplicaRowsPage('tenant','items',{offset:1,limit:2,maxBytes:bytes});
  expect(blocked).toMatchObject({ids:[],rows:[],nextOffset:1,decodedBytes:0,blockedId:'b'});
  const retry=await storage.loadReplicaRowsPage('tenant','items',{offset:1,limit:1,maxBytes:50_000});
  expect(retry).toMatchObject({ids:['b'],rows:[large],nextOffset:2});
});

it('counts unique membership, excluding hash-only certificates and repeated positions', async () => {
  await seed([{id:'a',title:'A'},{id:'b',title:'B'}],'tenant',['a','b','a'],{a:'11'.repeat(32),b:'22'.repeat(32),retired:'33'.repeat(32)});
  expect(await storage.loadReplicaRowsPage('tenant','items',{offset:0,limit:10,maxBytes:10_000}))
    .toMatchObject({ids:['a','b'],totalUniqueRows:2,nextOffset:2});
  const count=await storage.loadReplicaRowsPage('tenant','items',{offset:0,limit:0,maxBytes:0});
  expect(count).toMatchObject({ids:[],rows:[],totalUniqueRows:2,decodedBytes:0});
});

it('reports missing normalized rows rather than presenting them as complete data', async () => {
  await seed([{id:'a',title:'A'},{id:'b',title:'B'}]);
  await (storage as any).database.entities.delete(['tenant','items','b']);
  const page=await storage.loadReplicaRowsPage('tenant','items',{offset:0,limit:2,maxBytes:10_000});
  expect(page).toMatchObject({ids:['a','b'],rows:[{id:'a',title:'A'}],missingIds:['b'],totalUniqueRows:2,nextOffset:2});
});

it.each([{offset:-1,limit:1,maxBytes:1000},{offset:0,limit:513,maxBytes:1000},{offset:0,limit:1,maxBytes:Infinity},{offset:.5,limit:1,maxBytes:1000}])('rejects invalid bounds %j',async options=>{
  await expect(storage.loadReplicaRowsPage('tenant','items',options)).rejects.toThrow(RangeError);
});

it('rejects a continuation after durable rows or membership change', async () => {
  await seed([{id:'a',title:'A'},{id:'b',title:'B'}]);
  const first=(await storage.loadReplicaRowsPage('tenant','items',{offset:0,limit:1,maxBytes:10000}))!;
  expect(first.storageSequence).toBeGreaterThan(0);
  expect(await storage.loadReplicaRowsPage('tenant','items',{offset:1,limit:1,maxBytes:10000,expectedStorageSequence:first.storageSequence}))
    .toMatchObject({rows:[{id:'b',title:'B'}],storageSequence:first.storageSequence});
  await seed([{id:'a',title:'New A'},{id:'b',title:'New B'}]);
  await expect(storage.loadReplicaRowsPage('tenant','items',{offset:1,limit:1,maxBytes:10000,expectedStorageSequence:first.storageSequence}))
    .rejects.toMatchObject({name:'ReplicaPageChangedError',code:'replica-page-changed',expectedStorageSequence:first.storageSequence});
  expect(await storage.loadReplicaRowsPage('tenant','items',{offset:0,limit:1,maxBytes:10000}))
    .toMatchObject({rows:[{id:'a',title:'New A'}]});
});

it('detects a row-only update and keeps sequence guards isolated by scope', async () => {
  await seed([{id:'a',title:'A'},{id:'b',title:'B'}]);
  const first=(await storage.loadReplicaRowsPage('tenant','items',{offset:0,limit:1,maxBytes:10000}))!;
  await seed([{id:'a',title:'Other'}],'other');
  await expect(storage.loadReplicaRowsPage('tenant','items',{offset:1,limit:1,maxBytes:10000,expectedStorageSequence:first.storageSequence})).resolves.toMatchObject({rows:[{id:'b',title:'B'}]});
  await storage.applyTransaction({cursor:{epoch:'e',revision:2},changes:[{entity:'items',id:'b',operation:'update',newValue:{title:'Changed'}}]}, {entities:{},liveQueries:{}}, 'tenant');
  await expect(storage.loadReplicaRowsPage('tenant','items',{offset:1,limit:1,maxBytes:10000,expectedStorageSequence:first.storageSequence}))
    .rejects.toMatchObject({code:'replica-page-changed'});
});

it.each([-1,0.5,Infinity,NaN])('rejects invalid continuation sequence %s',async expectedStorageSequence=>{
  await expect(storage.loadReplicaRowsPage('tenant','items',{offset:0,limit:1,maxBytes:10000,expectedStorageSequence})).rejects.toThrow(RangeError);
});
