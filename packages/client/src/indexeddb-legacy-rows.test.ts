import {afterEach,expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange,IDBObjectStore} from 'fake-indexeddb';
import {IndexedDBLiveWindows} from './indexeddb-live-windows.js';
const closes:Array<()=>void>=[];afterEach(()=>{for(const close of closes.splice(0))close();vi.restoreAllMocks();});
it('rolls the imported row and progress marker back together on a late storage failure',async()=>{
 const engine=new IndexedDBLiveWindows({tasks:{key:'id',fields:{id:{},title:{}}}},'legacy-rollback',new IDBFactory(),IDBKeyRange);closes.push(()=>engine.close());await engine.claim('owner');const key='old\u00002\u0000visible',row={scope:'visible',entity:'tasks',id:'a',row:{id:'a',title:'Offline'},authority:{epoch:'e',base:1,fields:{}},sourceHash:'a'.repeat(64)};
 const put=IDBObjectStore.prototype.put,spy=vi.spyOn(IDBObjectStore.prototype,'put').mockImplementation(function(this:IDBObjectStore,...args:any[]){if(this.name==='meta'&&args[0]?.key==='legacy-rows:'+key)throw new DOMException('Quota surrogate','QuotaExceededError');return put.apply(this,args as [any]);});
 await expect(engine.importLegacyRows(key,'e',1,null,[row],JSON.stringify(['visible','tasks','a']),true)).rejects.toMatchObject({name:'QuotaExceededError'});spy.mockRestore();expect(await engine.legacyProgress(key)).toBeUndefined();expect((await engine.committedEntities({scope:'visible',entity:'tasks',ids:['a'],epoch:'e'})).rows[0]?.value).toBeNull();
 expect(await engine.importLegacyRows(key,'e',1,null,[row],JSON.stringify(['visible','tasks','a']),true)).toMatchObject({seen:1,copied:1,finished:true});expect((await engine.committedEntities({scope:'visible',entity:'tasks',ids:['a'],epoch:'e'})).rows[0]?.value).toEqual({id:'a',title:'Offline'});
});
