import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import type {FunctionReference} from './index.js';
import type {SQLiteLiveQueryUpdate} from './sqlite-native-transport.js';
import type {SQLiteWindowHeader} from './sqlite-window-store.js';
import type {SQLitePendingLivePage} from './sqlite-live-windows.js';
import type {SQLiteNativeWindowRead,SQLiteNativeWindowResult} from './sqlite-native-replica-reader.js';
import {IndexedDBFeedStore} from './indexeddb-feed-store.js';
import {prepareNativeLiveWindow,nativeLiveHash} from './native-live-window-frame.js';
import {sqlitePayloadBytes,decodedJSONBytes,fail} from './sqlite-admission.js';
type Stage={key:string;stream:string;scope:string;signature:string;epoch:string;revision:number;generation:string;header:SQLiteWindowHeader;hash:string};
type Active={key:string;generation:string;header:SQLiteWindowHeader;ids:string[]};
const request=<T>(value:IDBRequest<T>)=>new Promise<T>((resolve,reject)=>{value.onsuccess=()=>resolve(value.result);value.onerror=()=>reject(value.error??Error('Fallback live request failed'));});
const prefix=(stream:string)=>{if(!stream||stream.length>128)throw new TypeError('Invalid live stream');return 'live-stage:'+JSON.stringify(stream)+':';};
const activeKey=(scope:string,signature:string)=>'live-active:'+JSON.stringify([scope,signature]);
/** Exact live results remain bounded frames on disk until the terminal feed
 * revision. Publication stores ordered IDs beside normalized canonical values. */
export class IndexedDBLiveWindows extends IndexedDBFeedStore {
 async stageLiveWindow(schema:SQLiteTypedSchema,artifactHash:string,stream:string,scope:string,signature:string,ref:FunctionReference,frame:Exclude<SQLiteLiveQueryUpdate,{error:string}>){
  const prepared=await prepareNativeLiveWindow(schema,artifactHash,stream,scope,signature,ref,frame),key=prefix(stream)+signature,bodyKey='live-body:'+key,header=prepared.header,hash=await nativeLiveHash(JSON.stringify(header)+'\0'+prepared.rows);
  return this.transaction(['meta'],'readwrite',async tx=>{await this.fence(tx);const store=tx.objectStore('meta'),active=await request(store.get(activeKey(scope,signature))) as Active|undefined,old=await request(store.get(key)) as Stage|undefined;
   for(const previous of [active?.header,old?.header])if(previous?.cursor?.epoch===header.cursor!.epoch){if(previous.cursor.revision>header.cursor!.revision)return false;if(previous.cursor.revision===header.cursor!.revision){if(previous.aggregateDigest!==header.aggregateDigest||JSON.stringify(previous.resultSkeleton)!==JSON.stringify(header.resultSkeleton))fail('INTENT_CONFLICT','Fallback live result changed at the same revision');return false;}}
   if(!old&&await request(store.count(this.ranges.bound(prefix(stream),prefix(stream)+'\uffff')))>=32)fail('BUDGET','Fallback live stage limit exceeded');
   await request(store.put({key,stream,scope,signature,epoch:header.cursor!.epoch,revision:header.cursor!.revision,generation:crypto.randomUUID(),header,hash} satisfies Stage));await request(store.put({key:bodyKey,rows:prepared.rows}));return true;
  });
 }
 private async nextStage(tx:IDBTransaction,stream:string,scope:string,epoch:string,through:number,signature?:string){
  return this.transactionTask(tx,32768,()=>new Promise<Stage|undefined>((resolve,reject)=>{const store=tx.objectStore('meta'),cursor=store.openCursor(signature?this.ranges.only(prefix(stream)+signature):this.ranges.bound(prefix(stream),prefix(stream)+'\uffff'));cursor.onerror=()=>reject(cursor.error);cursor.onsuccess=()=>{const item=cursor.result;if(!item){resolve(undefined);return;}const stage=item.value as Stage;if(stage.scope===scope&&stage.epoch===epoch&&stage.revision<=through){try{sqlitePayloadBytes(stage,32768);resolve(stage);}catch(error){reject(error);}return;}item.continue();};}));
 }
 private async decode(tx:IDBTransaction,stage:Stage){
  const body=await this.transactionTask(tx,262144,()=>request(tx.objectStore('meta').get('live-body:'+stage.key)) as Promise<{rows:string}|undefined>);if(!body||typeof body.rows!=='string')fail('INTENT_CORRUPT','Fallback live body missing');decodedJSONBytes(body.rows,131072);
  if(await nativeLiveHash(body.rows)!==stage.header.aggregateDigest||await nativeLiveHash(JSON.stringify(stage.header)+'\0'+body.rows)!==stage.hash||stage.header.cursor?.epoch!==stage.epoch||stage.header.cursor.revision!==stage.revision||stage.header.signature!==stage.signature||stage.header.entityCoverage!==undefined)fail('INTENT_CORRUPT','Fallback live frame hash differs');
  const rows=JSON.parse(body.rows) as Array<Record<string,import('@gonvex/protocol').JsonValue>>;if(!Array.isArray(rows)||rows.length>256)fail('INTENT_CORRUPT','Fallback live frame exceeds row budget');return rows;
 }
 async publishLiveInTransaction(tx:IDBTransaction,stream:string,scope:string,epoch:string,through:number){
  let published=0;for(;;){const stage=await this.nextStage(tx,stream,scope,epoch,through);if(!stage)break;const rows=await this.decode(tx,stage);for(let offset=0;offset<Math.max(rows.length,1);offset+=64)await this.authorityInTransaction(tx,scope,{epoch,revision:stage.revision},rows.slice(offset,offset+64).map(row=>({entity:stage.header.entity,id:String(row[stage.header.key]),row})));
   await this.transactionTask(tx,65536,async()=>{const store=tx.objectStore('meta'),active:Active={key:activeKey(scope,stage.signature),generation:stage.generation,header:stage.header,ids:rows.map(row=>String(row[stage.header.key]))};sqlitePayloadBytes(active,65536);await request(store.put(active));await request(store.delete(stage.key));await request(store.delete('live-body:'+stage.key));});published++;
  }return published;
 }
 async pendingLivePage(stream:string,scope:string,signature:string,epoch:string,options:{offset?:number;limit?:number;generation?:string}={}):Promise<SQLitePendingLivePage|null>{
  const offset=options.offset??0,limit=options.limit??50;if(!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>128||offset>0&&!options.generation)throw new TypeError('Pending live continuation requires generation');
  return this.transaction(['meta'],'readwrite',async tx=>{await this.fence(tx);return this.keepAlive(tx,async()=>{const stage=await this.nextStage(tx,stream,scope,epoch,Number.MAX_SAFE_INTEGER,signature);if(!stage){if(options.generation)fail('PAGE_CHANGED','Fallback live stage removed');return null;}if(options.generation&&options.generation!==stage.generation)fail('PAGE_CHANGED','Fallback live stage changed');const values=await this.decode(tx,stage);let take=limit;for(;;){const selected=values.slice(offset,offset+take),page:SQLitePendingLivePage={complete:false,serverVerified:false,generation:stage.generation,header:stage.header,offset,nextOffset:offset+selected.length<values.length?offset+selected.length:null,receivedRows:values.length,rows:selected.map(value=>({id:String(value[stage.header.key]),value}))};try{sqlitePayloadBytes(page,131072);return page;}catch(error){if((error as {code?:string})?.code!=='BUDGET'||take===1)throw error;take=Math.max(1,Math.floor(take/2));}}});});
 }
 async livePage(input:SQLiteNativeWindowRead&{epoch:string;artifactHash:string}):Promise<SQLiteNativeWindowResult|null>{
  sqlitePayloadBytes(input,8192);const owned=structuredClone(input),offset=owned.offset??0,limit=owned.limit??50;if(!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>128||offset>0&&!owned.generation)throw new TypeError('Live continuation requires generation');
  return this.transaction(['meta','canonical','predicted'],'readonly',async tx=>{const owner=await this.fence(tx,owned.expected),store=tx.objectStore('meta'),active=await request(store.get(activeKey(owned.scope,owned.signature))) as Active|undefined,canonical=await request(store.get('canonical:'+owned.scope)) as {epoch:string}|undefined,progress=await request(store.get('feed-progress:'+owned.scope)) as {epoch:string;unclosed:number;applied:number}|undefined;
   if(!active){if(owned.generation)fail('PAGE_CHANGED','Fallback live window removed');return null;}if(active.header.cursor?.epoch!==owned.epoch||active.header.artifactHash!==owned.artifactHash||canonical&&canonical.epoch!==owned.epoch||progress&&(progress.epoch!==owned.epoch||progress.unclosed>progress.applied)||owned.generation&&owned.generation!==active.generation)fail('PAGE_CHANGED','Fallback live window awaits a complete admitted cohort');
   let take=limit;for(;;){const ids=active.ids.slice(offset,offset+take),values=await this.values(tx,owned.scope,active.header.entity,ids,active.header.columns),byID=new Map(values.map(row=>[row.id,row.row])),page:SQLiteNativeWindowResult={generation:{epoch:owner.epoch,sequence:owner.sequence},window:{generation:active.generation,header:active.header,total:active.ids.length},offset,nextOffset:offset+ids.length<active.ids.length?offset+ids.length:null,rows:ids.map(id=>({id,value:byID.get(id)??null}))};try{sqlitePayloadBytes(page,131072);return page;}catch(error){if((error as {code?:string})?.code!=='BUDGET'||take===1)throw error;take=Math.max(1,Math.floor(take/2));}}
  });
 }
 async retireLive(stream:string,signature?:string){const selected=prefix(stream);return this.transaction(['meta'],'readwrite',async tx=>{await this.fence(tx);const store=tx.objectStore('meta'),range=signature?this.ranges.only(selected+signature):this.ranges.bound(selected,selected+'\uffff');await new Promise<void>((resolve,reject)=>{const cursor=store.openCursor(range);cursor.onerror=()=>reject(cursor.error);cursor.onsuccess=()=>{const item=cursor.result;if(!item){resolve();return;}store.delete('live-body:'+item.key);item.delete();item.continue();};});});}
 async clearLiveStaging(){return this.transaction(['meta'],'readwrite',async tx=>{await this.fence(tx);for(const name of ['live-stage:','live-body:'])await request(tx.objectStore('meta').delete(this.ranges.bound(name,name+'\uffff')));});}
}
