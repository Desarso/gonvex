import {expect,it} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {SQLiteIndexedDBMigrationSource} from './sqlite-idb-migration-source.js';
async function seed(factory:IDBFactory,name='legacy'){
 const db=await new Promise<IDBDatabase>((resolve,reject)=>{const opening=factory.open(name,20);opening.onupgradeneeded=()=>{opening.result.createObjectStore('entries',{keyPath:'id'});opening.result.createObjectStore('sessions',{keyPath:'scope'});};opening.onsuccess=()=>resolve(opening.result);opening.onerror=()=>reject(opening.error);});
 await new Promise<void>((resolve,reject)=>{const tx=db.transaction(['entries','sessions'],'readwrite');for(let n=1;n<=101;n++)tx.objectStore('entries').add({id:n,scope:'s',args:{text:'Synthetic '+n}});tx.objectStore('sessions').add({scope:'s',account:'account'});tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});return db;
}
it('fences the old explicit schema version and preserves every source record for bounded read-only migration',async()=>{
 const factory=new IDBFactory(),legacy=await seed(factory);let versionChanges=0;legacy.onversionchange=()=>{versionChanges++;legacy.close();};
 const source=await SQLiteIndexedDBMigrationSource.open('legacy',factory,IDBKeyRange);try{
  expect(versionChanges).toBe(1);expect(source).toMatchObject({sourceVersion:20,fenceVersion:21});expect(source!.stores()).toEqual(['entries','sessions']);expect(await source!.count('entries')).toBe(101);
  let after:IDBValidKey|undefined,ids:number[]=[];for(;;){const page=await source!.page('entries',after,16);expect(page.records.length).toBeLessThanOrEqual(16);ids.push(...page.records.map(row=>(row.value as any).id));if(page.nextKey===null)break;after=page.nextKey;}
  expect(ids).toEqual(Array.from({length:101},(_,n)=>n+1));expect((await source!.page('sessions')).records[0].value).toEqual({scope:'s',account:'account'});
  await expect(new Promise((resolve,reject)=>{const old=factory.open('legacy',20);old.onsuccess=()=>{old.result.close();resolve(old.result);};old.onerror=()=>reject(old.error);})).rejects.toMatchObject({name:'VersionError'});
  source!.close();const resumed=await SQLiteIndexedDBMigrationSource.open('legacy',factory,IDBKeyRange);expect(resumed!.fenceVersion).toBe(21);expect(await resumed!.count('entries')).toBe(101);resumed!.close();
 }finally{source?.close();legacy.close();}
});
it('does not leave an empty database when the source is absent',async()=>{
 const factory=new IDBFactory();expect(await SQLiteIndexedDBMigrationSource.open('missing',factory,IDBKeyRange)).toBeUndefined();expect(await factory.databases()).toEqual([]);
});
it('reports a blocked old tab without mutating data, then resumes after that tab closes',async()=>{
 const factory=new IDBFactory(),legacy=await seed(factory);
 await expect(SQLiteIndexedDBMigrationSource.open('legacy',factory,IDBKeyRange)).rejects.toMatchObject({code:'BUSY'});legacy.close();await new Promise(resolve=>setTimeout(resolve,0));
 const source=await SQLiteIndexedDBMigrationSource.open('legacy',factory,IDBKeyRange);try{expect(await source!.count('entries')).toBe(101);}finally{source?.close();}
});

it('bounds compound-key pages and preserves an oversized legacy record instead of silently skipping it',async()=>{
 const factory=new IDBFactory();const db=await new Promise<IDBDatabase>((resolve,reject)=>{const opening=factory.open('normalized',50);opening.onupgradeneeded=()=>opening.result.createObjectStore('entities',{keyPath:['scope','entity','id']});opening.onsuccess=()=>resolve(opening.result);opening.onerror=()=>reject(opening.error);});
 await new Promise<void>((resolve,reject)=>{const tx=db.transaction('entities','readwrite'),store=tx.objectStore('entities');for(let n=0;n<10;n++)store.add({scope:'s',entity:'tasks',id:String(n),value:'x'.repeat(n===9?150000:20000)});tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error);});db.close();
 const source=await SQLiteIndexedDBMigrationSource.open('normalized',factory,IDBKeyRange);try{
  const page=await source!.page('entities',undefined,128,100000);expect(page.records).toHaveLength(2);expect(page.nextKey).toEqual(['s','tasks','1']);
  await expect(source!.page('entities',['s','tasks','8'])).rejects.toMatchObject({code:'BUDGET'});expect(await source!.count('entities')).toBe(10);
 }finally{source?.close();}
});
