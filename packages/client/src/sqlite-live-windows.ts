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
const schema=`CREATE TABLE IF NOT EXISTS gv_live_window_stages(stream TEXT NOT NULL,scope TEXT NOT NULL,signature TEXT NOT NULL,epoch TEXT NOT NULL,revision INTEGER NOT NULL,generation TEXT NOT NULL,header TEXT NOT NULL CHECK(length(header)*2<=16384),rows TEXT NOT NULL CHECK(length(rows)*2<=131072),fingerprint TEXT NOT NULL,PRIMARY KEY(stream,scope,signature)) WITHOUT ROWID;`;
const sha=async(value:string)=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');
async function decodeStage(stage:Stage){
 decodedJSONBytes(stage.rows,NATIVE_LIVE_DECODE_BYTES);decodedJSONBytes(stage.header,16384);const header=JSON.parse(stage.header) as SQLiteWindowHeader;
 if(header.signature!==stage.signature||header.kind!=='live'||header.source!=='server'||header.entityCoverage!==undefined||header.cursor?.epoch!==stage.epoch||header.cursor?.revision!==stage.revision||await sha(stage.rows)!==header.aggregateDigest||await sha(stage.header+'\0'+stage.rows)!==stage.fingerprint)fail('INTENT_CORRUPT','Pending live window differs from its durable frame');
 const rows=JSON.parse(stage.rows) as Array<Record<string,JsonValue>>;return {rows,header};
}
/** Exact server windows share the normalized replica. Only bounded frames are
 * decoded in JS; ordered memberships and result metadata persist in SQLite.
 * The owner publishes these stages with the replica terminal watermark and
 * prediction rebase. A limited Live Query never grants entity completeness. */
export class SQLiteLiveWindows {
 private readonly windows:SQLiteWindowStore;private initialized?:Promise<void>;
 constructor(private readonly owner:SQLiteOwnerProgramContext,private readonly artifactHash:string){this.windows=new SQLiteWindowStore(owner.database,()=>owner.engine.verifyOwner());}
 ready(){return this.initialized??= (async()=>{await this.windows.ready();await this.owner.database.execAsync(schema);await this.owner.database.runAsync('DELETE FROM gv_live_window_stages');})();}
 private check(){if(!this.owner.database.ownerTransactionDepth)throw Error('Live windows require an owner transaction');}
 async stage(stream:string,scope:string,signature:string,ref:FunctionReference,frame:Frame){
  this.check();const {header,rows,hash,cursor}=await prepareNativeLiveWindow(this.owner.engine.schema,this.artifactHash,stream,scope,signature,ref,frame);
  await this.ready();await this.owner.engine.verifyOwner();const active=await this.windows.header(scope,signature);if(active?.header.cursor?.epoch===cursor.epoch){if(active.header.cursor.revision>cursor.sequence)return false;if(active.header.cursor.revision===cursor.sequence){if(active.header.aggregateDigest!==hash||JSON.stringify(active.header.resultSkeleton)!==JSON.stringify(header.resultSkeleton))fail('INTENT_CONFLICT','Live query changed at the same authoritative revision');return false;}}
  const previous=await this.owner.database.getFirstAsync<Stage>('SELECT * FROM gv_live_window_stages WHERE stream=? AND scope=? AND signature=?',stream,scope,signature);if(previous?.epoch===cursor.epoch&&previous.revision>cursor.sequence)return false;
  if(previous?.epoch===cursor.epoch&&previous.revision===cursor.sequence){if(previous.rows!==rows||previous.header!==JSON.stringify(header))fail('INTENT_CONFLICT','Pending live query changed at the same revision');return false;}
  const count=await this.owner.database.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_live_window_stages WHERE stream=?',stream);if(!previous&&(count?.n??0)>=32)fail('BUDGET','Native live window stage allowance exceeded');
  await this.owner.database.runAsync('INSERT INTO gv_live_window_stages VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(stream,scope,signature) DO UPDATE SET epoch=excluded.epoch,revision=excluded.revision,generation=excluded.generation,header=excluded.header,rows=excluded.rows,fingerprint=excluded.fingerprint',stream,scope,signature,cursor.epoch,cursor.sequence,crypto.randomUUID(),JSON.stringify(header),rows,await sha(JSON.stringify(header)+'\0'+rows));return true;
 }
 async pending(stream:string,scope:string,epoch:string,through:number){this.check();await this.ready();return !!await this.owner.database.getFirstAsync('SELECT 1 FROM gv_live_window_stages WHERE stream=? AND scope=? AND epoch=? AND revision<=? LIMIT 1',stream,scope,epoch,through);}
 async publish(stream:string,scope:string,epoch:string,through:number){
  this.check();await this.ready();let published=0;
  for(;;){const stage=await this.owner.database.getFirstAsync<Stage>('SELECT * FROM gv_live_window_stages WHERE stream=? AND scope=? AND epoch=? AND revision<=? ORDER BY revision,signature LIMIT 1',stream,scope,epoch,through);if(!stage)break;
   const {rows,header}=await decodeStage(stage);
   // Each source has at most 256 rows. Collect old bounded generations before
   // admitting the next one, rather than waiting for the idle collector.
   const old=await this.owner.database.getAllAsync<{generation:string}>('SELECT generation FROM gv_window_generations WHERE scope=? AND signature=? AND generation<>coalesce((SELECT generation FROM gv_windows WHERE scope=? AND signature=?),\'\') LIMIT 3',scope,stage.signature,scope,stage.signature);for(const generation of old)while((await this.windows.collect(scope,stage.signature,generation.generation)).remaining){}
   await this.owner.canonical.applyTransaction({cursor:header.cursor!,changes:rows.map(row=>({entity:header.entity,id:String(row[header.key]),operation:'update' as const,newValue:row}))},{entities:{},liveQueries:{}},scope);
   await this.windows.begin(scope,header,stage.generation);let progress,offset=0,page=0;const ids=rows.map(row=>String(row[header.key]));do{let take=Math.min(64,ids.length-offset),encoded:string;for(;;){encoded=JSON.stringify({ids:ids.slice(offset,offset+take)});if(encoded.length*2<=65536)break;if(take<=1)fail('BUDGET','Live membership identity exceeds its frame budget');take=Math.max(1,Math.floor(take/2));}progress=await this.windows.append(scope,stage.signature,stage.generation,page++,encoded);offset+=take;}while(offset<ids.length);await this.windows.publish(scope,stage.signature,progress!);
   await this.owner.database.runAsync('DELETE FROM gv_live_window_stages WHERE stream=? AND scope=? AND signature=? AND generation=?',stream,scope,stage.signature,stage.generation);published++;
  }return published;
 }
 async page(stream:string,scope:string,signature:string,epoch:string,options:{offset?:number;limit?:number;generation?:string}={}):Promise<SQLitePendingLivePage|null>{
  this.check();await this.ready();await this.owner.engine.verifyOwner();const offset=options.offset??0,limit=options.limit??50;if(!Number.isSafeInteger(offset)||offset<0||!Number.isSafeInteger(limit)||limit<1||limit>128||offset>0&&!options.generation)throw new TypeError('Pending live continuation requires its generation and a bounded page');
  const stage=await this.owner.database.getFirstAsync<Stage>('SELECT * FROM gv_live_window_stages WHERE stream=? AND scope=? AND signature=? AND epoch=?',stream,scope,signature,epoch);if(!stage){if(options.generation)fail('PAGE_CHANGED','Pending live window was replaced');return null;}if(options.generation&&options.generation!==stage.generation)fail('PAGE_CHANGED','Pending live generation changed');const {rows:values,header}=await decodeStage(stage);
  let take=limit;for(;;){const selected=values.slice(offset,offset+take),next=offset+selected.length<values.length?offset+selected.length:null,output:SQLitePendingLivePage={complete:false,serverVerified:false,generation:stage.generation,header,offset,nextOffset:next,receivedRows:values.length,rows:selected.map(value=>({id:String(value[header.key]),value}))};try{sqlitePayloadBytes(output,131072);return output;}catch(error){if((error as {code?:string})?.code!=='BUDGET'||take===1)throw error;take=Math.max(1,Math.floor(take/2));}}
 }
 async retire(stream:string,signature?:string){this.check();await this.ready();await this.owner.engine.verifyOwner();if(signature)await this.owner.database.runAsync('DELETE FROM gv_live_window_stages WHERE stream=? AND signature=?',stream,signature);else await this.owner.database.runAsync('DELETE FROM gv_live_window_stages WHERE stream=?',stream);}
}
