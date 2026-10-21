import {prepareNativeLiveWindow,NATIVE_LIVE_DECODE_BYTES} from './native-live-window-frame.js';
import type {JsonValue} from '@gonvex/protocol';
import type {FunctionReference} from './index.js';
import type {SQLiteLiveQueryUpdate} from './sqlite-native-transport.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {SQLiteWindowStore,type SQLiteWindowHeader} from './sqlite-window-store.js';
import {fail,sqlitePayloadBytes,decodedJSONBytes} from './sqlite-admission.js';
type Frame=Exclude<SQLiteLiveQueryUpdate,{error:string}>;
export type SQLitePendingLivePage={complete:false;serverVerified:false;generation:string;header:SQLiteWindowHeader;offset:number;nextOffset:number|null;receivedRows:number;rows:Array<{id:string;value:Record<string,JsonValue>}>};
type Stage={stream:string;scope:string;signature:string;epoch:string;revision:number;generation:string;header:string;rows:string;fingerprint:string};
const schema=`CREATE TABLE IF NOT EXISTS gv_live_window_stages(stream TEXT NOT NULL,scope TEXT NOT NULL,signature TEXT NOT NULL,epoch TEXT NOT NULL,revision INTEGER NOT NULL,generation TEXT NOT NULL,header TEXT NOT NULL CHECK(length(header)*2<=16384),rows TEXT NOT NULL CHECK(length(rows)*2<=131072),fingerprint TEXT NOT NULL,PRIMARY KEY(stream,scope,signature)) WITHOUT ROWID;CREATE TABLE IF NOT EXISTS gv_live_window_chunks(stream TEXT NOT NULL,scope TEXT NOT NULL,signature TEXT NOT NULL,generation TEXT NOT NULL,position INTEGER NOT NULL,rows TEXT NOT NULL CHECK(length(rows)*2<=131072),hash TEXT NOT NULL,PRIMARY KEY(stream,scope,signature,generation,position)) WITHOUT ROWID;`;
const sha=async(value:string)=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');
/** Scan encoded rows before parsing, so wide multi-row frames never allocate
 * a single over-budget decoded graph. Each yielded batch fits the turn budget. */
let liveDecodePeakBytes=0;
export const liveStageBudgetStats=()=>({name:'live-stage-decoder',bytes:0,entries:0,maxBytes:NATIVE_LIVE_DECODE_BYTES,maxEntries:256,peakBytes:liveDecodePeakBytes,refusals:0,evictions:0});
export function* decodeLiveStageBatches(text:string):Generator<Array<Record<string,JsonValue>>>{
 let start=1,depth=0,quoted=false,escaped=false,batch:Array<Record<string,JsonValue>>=[],bytes=64;
 if(text[0]!=='['||text.at(-1)!==']')fail('INTENT_CORRUPT','Invalid live stage rows');
 for(let i=1;i<text.length;i++){
  const c=text[i];
  if(quoted){if(escaped)escaped=false;else if(c==='\\')escaped=true;else if(c==='"')quoted=false;continue;}
  if(c==='"'){quoted=true;continue;}
  if(c==='{'||c==='[')depth++;else if(c==='}'||c===']')depth--;
  if(c===','&&depth===0||i===text.length-1){
   const body=text.slice(start,i).trim();start=i+1;if(!body)continue;
   const size=decodedJSONBytes(body,NATIVE_LIVE_DECODE_BYTES-64);
   if(batch.length&&bytes+size>NATIVE_LIVE_DECODE_BYTES){yield batch;batch=[];bytes=64;}
   const row=JSON.parse(body);if(!row||Array.isArray(row)||typeof row!=='object')fail('INTENT_CORRUPT','Invalid live stage row');
   batch.push(row);bytes+=size;liveDecodePeakBytes=Math.max(liveDecodePeakBytes,bytes);
  }
 }
 if(batch.length)yield batch;
}
async function decodeStage(stage:Stage,owner:SQLiteOwnerProgramContext){
 decodedJSONBytes(stage.header,16384);const header=JSON.parse(stage.header) as SQLiteWindowHeader;
 if(header.signature!==stage.signature||header.kind!=='live'||header.source!=='server'||header.entityCoverage!==undefined||header.cursor?.epoch!==stage.epoch||header.cursor?.revision!==stage.revision||await sha(stage.header+'\0'+stage.rows)!==stage.fingerprint)fail('INTENT_CORRUPT','Pending live window differs from its durable frame');
 const manifest=JSON.parse(stage.rows) as {chunks:string[]};if(!Array.isArray(manifest.chunks)||manifest.chunks.length>256)fail('INTENT_CORRUPT','Invalid live chunk manifest');
 async function* batches(){for(let position=0;position<manifest.chunks.length;position++){
  const chunk=await owner.database.getFirstAsync<{rows:string;hash:string}>('SELECT rows,hash FROM gv_live_window_chunks WHERE stream=? AND scope=? AND signature=? AND generation=? AND position=?',stage.stream,stage.scope,stage.signature,stage.generation,position);
  if(!chunk||chunk.hash!==manifest.chunks[position]||await sha(chunk.rows)!==chunk.hash)fail('INTENT_CORRUPT','Pending live chunk differs from its manifest');
  yield* decodeLiveStageBatches(chunk.rows);
 }}
 return {batches,header};
}
/** Exact server windows share the normalized replica. Only bounded frames are
 * decoded in JS; ordered memberships and result metadata persist in SQLite.
 * The owner publishes these stages with the replica terminal watermark and
 * prediction rebase. A limited Live Query never grants entity completeness. */
export class SQLiteLiveWindows {
 private readonly windows:SQLiteWindowStore;private initialized?:Promise<void>;
 constructor(private readonly owner:SQLiteOwnerProgramContext,private readonly artifactHash:string){this.windows=new SQLiteWindowStore(owner.database,()=>owner.engine.verifyOwner());}
 ready(){return this.initialized??= (async()=>{await this.windows.ready();await this.owner.database.execAsync(schema);await this.owner.database.runAsync('DELETE FROM gv_live_window_chunks');await this.owner.database.runAsync('DELETE FROM gv_live_window_stages');})();}
 private check(){if(!this.owner.database.ownerTransactionDepth)throw Error('Live windows require an owner transaction');}
 async stage(stream:string,scope:string,signature:string,ref:FunctionReference,frame:Frame){
  this.check();const {header,rows,hash,cursor}=await prepareNativeLiveWindow(this.owner.engine.schema,this.artifactHash,stream,scope,signature,ref,frame);
  await this.ready();await this.owner.engine.verifyOwner();const active=await this.windows.header(scope,signature);if(active?.header.cursor?.epoch===cursor.epoch){if(active.header.cursor.revision>cursor.sequence)return false;if(active.header.cursor.revision===cursor.sequence){if(active.header.aggregateDigest!==hash||JSON.stringify(active.header.resultSkeleton)!==JSON.stringify(header.resultSkeleton))fail('INTENT_CONFLICT','Live query changed at the same authoritative revision');return false;}}
  const previous=await this.owner.database.getFirstAsync<Stage>('SELECT * FROM gv_live_window_stages WHERE stream=? AND scope=? AND signature=?',stream,scope,signature);if(previous?.epoch===cursor.epoch&&previous.revision>cursor.sequence)return false;
  if(previous?.epoch===cursor.epoch&&previous.revision===cursor.sequence){if(JSON.parse(previous.header).aggregateDigest!==hash||previous.header!==JSON.stringify(header))fail('INTENT_CONFLICT','Pending live query changed at the same revision');return false;}
  const count=await this.owner.database.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_live_window_stages WHERE stream=?',stream);if(!previous&&(count?.n??0)>=32)fail('BUDGET','Native live window stage allowance exceeded');
  const generation=crypto.randomUUID(),chunks:string[]=[];
  await this.owner.database.runAsync('DELETE FROM gv_live_window_chunks WHERE stream=? AND scope=? AND signature=?',stream,scope,signature);
  for(const batch of decodeLiveStageBatches(rows)){
   // A decoded batch may encode beyond the SQL bridge cap. Split it again
   // before binding, preserving the outer authority transaction.
   let offset=0;while(offset<batch.length){let take=batch.length-offset,encoded:string;for(;;){encoded=JSON.stringify(batch.slice(offset,offset+take));if(encoded.length*2<=131072)break;if(take===1)fail('BUDGET','Single live row exceeds encoded chunk budget');take=Math.max(1,Math.floor(take/2));}
    const digest=await sha(encoded);await this.owner.database.runAsync('INSERT INTO gv_live_window_chunks VALUES(?,?,?,?,?,?,?)',stream,scope,signature,generation,chunks.length,encoded,digest);chunks.push(digest);offset+=take;
   }
  }
  const manifest=JSON.stringify({chunks});
  await this.owner.database.runAsync('INSERT INTO gv_live_window_stages VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(stream,scope,signature) DO UPDATE SET epoch=excluded.epoch,revision=excluded.revision,generation=excluded.generation,header=excluded.header,rows=excluded.rows,fingerprint=excluded.fingerprint',stream,scope,signature,cursor.epoch,cursor.sequence,generation,JSON.stringify(header),manifest,await sha(JSON.stringify(header)+'\0'+manifest));return true;
 }
 async pending(stream:string,scope:string,epoch:string,through:number){this.check();await this.ready();return !!await this.owner.database.getFirstAsync('SELECT 1 FROM gv_live_window_stages WHERE stream=? AND scope=? AND epoch=? AND revision<=? LIMIT 1',stream,scope,epoch,through);}
 async publish(stream:string,scope:string,epoch:string,through:number){
  this.check();await this.ready();let published=0;
  for(;;){const stage=await this.owner.database.getFirstAsync<Stage>('SELECT * FROM gv_live_window_stages WHERE stream=? AND scope=? AND epoch=? AND revision<=? ORDER BY revision,signature LIMIT 1',stream,scope,epoch,through);if(!stage)break;
   const {batches,header}=await decodeStage(stage,this.owner);
   // Each source has at most 256 rows. Collect old bounded generations before
   // admitting the next one, rather than waiting for the idle collector.
   const old=await this.owner.database.getAllAsync<{generation:string}>('SELECT generation FROM gv_window_generations WHERE scope=? AND signature=? AND generation<>coalesce((SELECT generation FROM gv_windows WHERE scope=? AND signature=?),\'\') LIMIT 3',scope,stage.signature,scope,stage.signature);for(const generation of old)while((await this.windows.collect(scope,stage.signature,generation.generation)).remaining){}
   const ids:string[]=[];for await(const rows of batches()){ids.push(...rows.map(row=>String(row[header.key])));await this.owner.canonical.applyTransaction({cursor:header.cursor!,changes:rows.map(row=>({entity:header.entity,id:String(row[header.key]),operation:'update' as const,newValue:row}))},{entities:{},liveQueries:{}},scope);}
   await this.windows.begin(scope,header,stage.generation);let progress,offset=0,page=0;do{let take=Math.min(64,ids.length-offset),encoded:string;for(;;){encoded=JSON.stringify({ids:ids.slice(offset,offset+take)});if(encoded.length*2<=65536)break;if(take<=1)fail('BUDGET','Live membership identity exceeds its frame budget');take=Math.max(1,Math.floor(take/2));}progress=await this.windows.append(scope,stage.signature,stage.generation,page++,encoded);offset+=take;}while(offset<ids.length);await this.windows.publish(scope,stage.signature,progress!);
   await this.owner.database.runAsync('DELETE FROM gv_live_window_chunks WHERE stream=? AND scope=? AND signature=? AND generation=?',stream,scope,stage.signature,stage.generation);
   await this.owner.database.runAsync('DELETE FROM gv_live_window_stages WHERE stream=? AND scope=? AND signature=? AND generation=?',stream,scope,stage.signature,stage.generation);published++;
  }return published;
 }
 async page(stream:string,scope:string,signature:string,epoch:string,options:{offset?:number;limit?:number;generation?:string}={}):Promise<SQLitePendingLivePage|null>{
  this.check();await this.ready();await this.owner.engine.verifyOwner();const offset=options.offset??0,limit=options.limit??50;if(!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>128||offset>0&&!options.generation)throw new TypeError('Pending live continuation requires its generation and a bounded page');
  const stage=await this.owner.database.getFirstAsync<Stage>('SELECT * FROM gv_live_window_stages WHERE stream=? AND scope=? AND signature=? AND epoch=?',stream,scope,signature,epoch);if(!stage){if(options.generation)fail('PAGE_CHANGED','Pending live window was replaced');return null;}if(options.generation&&options.generation!==stage.generation)fail('PAGE_CHANGED','Pending live generation changed');const {batches,header}=await decodeStage(stage,this.owner);const values:Array<Record<string,JsonValue>>=[];let receivedRows=0;for await(const batch of batches()){for(const row of batch){if(receivedRows>=offset&&values.length<limit)values.push(row);receivedRows++;}}
  let take=limit;for(;;){const selected=values.slice(0,take),next=offset+selected.length<receivedRows?offset+selected.length:null,output:SQLitePendingLivePage={complete:false,serverVerified:false,generation:stage.generation,header,offset,nextOffset:next,receivedRows,rows:selected.map(value=>({id:String(value[header.key]),value}))};try{sqlitePayloadBytes(output,131072);return output;}catch(error){if((error as {code?:string})?.code!=='BUDGET'||take===1)throw error;take=Math.max(1,Math.floor(take/2));}}
 }
 async retire(stream:string,signature?:string){
  this.check();await this.ready();await this.owner.engine.verifyOwner();
  for(const table of ['gv_live_window_chunks','gv_live_window_stages']){
   if(signature)await this.owner.database.runAsync(`DELETE FROM ${table} WHERE stream=? AND signature=?`,stream,signature);
   else await this.owner.database.runAsync(`DELETE FROM ${table} WHERE stream=?`,stream);
  }
 }
}
