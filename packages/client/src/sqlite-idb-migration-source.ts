import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
const markerStore='__gonvex_sqlite_source_fence',markerKey='owner',protocol='sqlite-bounded-v1';
type Marker={key:string;protocol:string;sourceVersion:number;fenceVersion:number};
export type SQLiteMigrationSourcePage={records:Array<{key:IDBValidKey;value:unknown}>;nextKey:IDBValidKey|null};
const result=<T>(request:IDBRequest<T>)=>new Promise<T>((resolve,reject)=>{request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error??Error('Migration source request failed'));});
/** A current normalized IndexedDB database is upgraded once to fence legacy
 * SDKs which request its older explicit schema version. Existing connections
 * receive versionchange and must close before this succeeds. No application
 * record is changed or deleted. Subsequent migration reads are read-only. */
export class SQLiteIndexedDBMigrationSource {
 private closed=false;
 private constructor(private readonly db:IDBDatabase,readonly sourceVersion:number,readonly fenceVersion:number,private readonly ranges:typeof IDBKeyRange){db.onversionchange=()=>this.close();}
 static async open(name:string,factory:IDBFactory=globalThis.indexedDB,ranges:typeof IDBKeyRange=globalThis.IDBKeyRange):Promise<SQLiteIndexedDBMigrationSource|undefined>{
  if(typeof name!=='string'||!name||name.length>256)throw new TypeError('Invalid migration source name');
  if(!factory||!ranges)throw Object.assign(Error('IndexedDB migration source is unavailable'),{code:'UNSUPPORTED'});
  const existing=await new Promise<IDBDatabase|undefined>((resolve,reject)=>{
   const opening=factory.open(name);let absent=false;
   opening.onupgradeneeded=event=>{if(event.oldVersion===0){absent=true;opening.transaction!.abort();}};
   opening.onerror=()=>absent?resolve(undefined):reject(opening.error??Error('Cannot open migration source'));
   opening.onsuccess=()=>resolve(opening.result);
  });
  if(!existing)return;
  let db=existing;
  try{
   if(!db.objectStoreNames.contains(markerStore)){
    const sourceVersion=db.version;if(!Number.isSafeInteger(sourceVersion)||sourceVersion<1||sourceVersion>=Number.MAX_SAFE_INTEGER)fail('FENCED','Migration source schema version is invalid');
    db.close();
    db=await new Promise<IDBDatabase>((resolve,reject)=>{
     let cancelled=false;const opening=factory.open(name,sourceVersion+1);
     opening.onblocked=()=>{cancelled=true;reject(Object.assign(Error('A legacy tab still holds the migration source; retry after versionchange'),{code:'BUSY'}));};
     opening.onerror=()=>reject(opening.error??Error('Cannot fence migration source'));
     opening.onupgradeneeded=event=>{
      if(cancelled){opening.transaction!.abort();return;}
      if(event.oldVersion!==sourceVersion||opening.result.objectStoreNames.contains(markerStore)){opening.transaction!.abort();return;}
      opening.result.createObjectStore(markerStore,{keyPath:'key'}).add({key:markerKey,protocol,sourceVersion,fenceVersion:sourceVersion+1} satisfies Marker);
     };
     opening.onsuccess=()=>{if(cancelled){opening.result.close();return;}resolve(opening.result);};
    });
   }
   const marker=await result(db.transaction(markerStore,'readonly').objectStore(markerStore).get(markerKey)) as Marker|undefined;
   if(!marker||marker.protocol!==protocol||!Number.isSafeInteger(marker.sourceVersion)||marker.sourceVersion<1||marker.fenceVersion!==db.version||marker.fenceVersion!==marker.sourceVersion+1)throw Object.assign(Error('Migration source fence is incompatible'),{code:'PROTOCOL'});
   return new SQLiteIndexedDBMigrationSource(db,marker.sourceVersion,marker.fenceVersion,ranges);
  }catch(error){db.close();throw error;}
 }
 get name(){return this.db.name;}
 stores(){if(this.closed)fail('FENCED','Migration source is closed');return Array.from(this.db.objectStoreNames).filter(name=>name!==markerStore);}
 private store(name:string){if(this.closed)fail('FENCED','Migration source is closed');if(name===markerStore||!this.db.objectStoreNames.contains(name))throw new TypeError('Unknown migration source store');return this.db.transaction(name,'readonly').objectStore(name);}
 count(store:string){return result(this.store(store).count());}
 async page(store:string,after?:IDBValidKey,limit=32,maxBytes=262144):Promise<SQLiteMigrationSourcePage>{
  if(!Number.isSafeInteger(limit)||limit<1||limit>128||!Number.isSafeInteger(maxBytes)||maxBytes<1024||maxBytes>262144)throw new TypeError('Invalid migration source page budget');
  const range=after===undefined?undefined:this.ranges.lowerBound(structuredClone(after),true),source=this.store(store);
  return new Promise((resolve,reject)=>{
   const records:SQLiteMigrationSourcePage['records']=[];let bytes=64;const cursor=source.openCursor(range);
   cursor.onerror=()=>reject(cursor.error??Error('Migration source cursor failed'));
   cursor.onsuccess=()=>{
    const row=cursor.result;if(!row){resolve({records,nextKey:null});return;}
    if(records.length>=limit){resolve({records,nextKey:records.at(-1)!.key});return;}
    try{
     // Legacy normalized keys are scalars/arrays, and records are plain SDK
     // data. An unsupported giant legacy blob stays intact for fallback or
     // authoritative rebootstrap; it is never silently skipped or truncated.
     const entry={key:row.primaryKey,value:row.value},size=sqlitePayloadBytes(entry,maxBytes);
     if(bytes+size>maxBytes){if(!records.length)fail('BUDGET','Legacy source record exceeds migration page budget');resolve({records,nextKey:records.at(-1)!.key});return;}
     bytes+=size;records.push(entry);row.continue();
    }catch(error){reject(error);}
   };
  });
 }
 close(){if(this.closed)return;this.closed=true;this.db.close();}
}
