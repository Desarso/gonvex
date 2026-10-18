import type {ServerMessage} from '@gonvex/protocol';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import {decodedJSONBytes,fail,pageBudgetError,sqlitePayloadBytes} from './sqlite-admission.js';
type Delta=Extract<ServerMessage,{type:'replica.delta'}>;
export type SQLiteDeferredReplicaFrame={revision:number;frame:Delta};
const encoder=new TextEncoder(),decoder=new TextDecoder('utf-8',{fatal:true});
const schema=`CREATE TABLE IF NOT EXISTS gv_deferred_replica_frames(scope TEXT NOT NULL,signature TEXT NOT NULL,artifact TEXT NOT NULL,epoch TEXT NOT NULL,revision INTEGER NOT NULL,body BLOB NOT NULL,hash TEXT NOT NULL,bytes INTEGER NOT NULL,PRIMARY KEY(scope,signature,artifact,epoch,revision)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_deferred_replica_retire ON gv_deferred_replica_frames(scope,signature,epoch,revision,artifact);
CREATE TABLE IF NOT EXISTS gv_deferred_replica_usage(scope TEXT PRIMARY KEY,bytes INTEGER NOT NULL CHECK(bytes>=0),frames INTEGER NOT NULL CHECK(frames>=0)) WITHOUT ROWID;`;
function identity(scope:string,signature:string,epoch?:string){if(typeof scope!=='string'||!scope||scope.length>512||scope.includes('\u0000')||typeof signature!=='string'||!signature||signature.length>4096||epoch!==undefined&&(typeof epoch!=='string'||!epoch||epoch.length>256))throw new TypeError('Invalid deferred replica identity');}
function semantic(frame:Delta){const {id:_,path:__,...body}=frame;return JSON.stringify(body);}
async function digest(text:string){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(text))),byte=>byte.toString(16).padStart(2,'0')).join('');}
/** Authoritative membership changes can arrive before their base snapshot.
 * Persist them without retaining row graphs or blocking later binary frames.
 * They are replayed/verified after activation, or superseded by a newer verified
 * replacement. This is not an intent journal and never settles user writes. */
export class SQLiteDeferredReplicaFrames {
 private initialized?:Promise<void>;
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly artifact:string,private readonly fence:()=>Promise<unknown>,private readonly limits={maxBytes:67108864,maxFrames:200000}){
  if(typeof artifact!=='string'||!artifact||artifact.length>256||!Number.isSafeInteger(limits.maxBytes)||limits.maxBytes<1||!Number.isSafeInteger(limits.maxFrames)||limits.maxFrames<1)throw new TypeError('Invalid deferred replica limits');this.limits=Object.freeze({...limits});
 }
 ready(){if(!this.initialized){if(this.db.ownerTransactionDepth)fail('FENCED','Prepare deferred frames before their owner transaction');this.initialized=this.db.withTransactionAsync(async()=>{await this.fence();await this.db.execAsync(schema);}).catch(error=>{this.initialized=undefined;throw error;});}return this.initialized;}
 async append(scope:string,signature:string,frame:Delta):Promise<{inserted:boolean;revision:number}>{
  sqlitePayloadBytes(frame,131072);if(!frame||typeof frame!=='object'||Array.isArray(frame))throw new TypeError('Invalid deferred replica delta');identity(scope,signature,frame.cursor?.epoch);
  if(frame.type!=='replica.delta'||typeof frame.id!=='string'||!frame.id||frame.id.length>256||!Number.isSafeInteger(frame.cursor?.revision)||frame.cursor.revision<0||typeof frame.digest!=='string'||!/^(sum128-v1:[0-9a-f]{32}|xor1:[0-9a-f]{64})$/.test(frame.digest))throw new TypeError('Invalid deferred replica delta');
  const encoded=JSON.stringify(frame),owned=JSON.parse(encoded) as Delta,bytes=encoder.encode(encoded);if(bytes.byteLength>131072)fail('BUDGET','Deferred replica frame wire budget exceeded');const charge=bytes.byteLength+(scope.length+signature.length+this.artifact.length+owned.cursor.epoch.length)*8+512;const hash=await digest(semantic(owned));await this.ready();let inserted=false;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const args=[scope,signature,this.artifact,owned.cursor.epoch,owned.cursor.revision];
   const old=await this.db.getFirstAsync<{hash:string}>('SELECT hash FROM gv_deferred_replica_frames WHERE scope=? AND signature=? AND artifact=? AND epoch=? AND revision=?',...args);
   if(old){if(old.hash!==hash)fail('INTENT_CONFLICT','Deferred replica revision changed its payload');return;}
   const usage=await this.db.getFirstAsync<{bytes:number;frames:number}>('SELECT bytes,frames FROM gv_deferred_replica_usage WHERE scope=?',scope);
   if((usage?.bytes??0)+charge>this.limits.maxBytes||(usage?.frames??0)>=this.limits.maxFrames)fail('BUDGET','Deferred replica disk budget exhausted; reconcile from a new snapshot');
   await this.db.runAsync('INSERT INTO gv_deferred_replica_frames VALUES(?,?,?,?,?,?,?,?)',...args,bytes,hash,charge);
   await this.db.runAsync('INSERT INTO gv_deferred_replica_usage VALUES(?,?,1) ON CONFLICT(scope) DO UPDATE SET bytes=bytes+excluded.bytes,frames=frames+1',scope,charge);inserted=true;
  });return {inserted,revision:owned.cursor.revision};
 }
 async read(scope:string,signature:string,epoch:string,after=-1,limit=8):Promise<SQLiteDeferredReplicaFrame[]>{
  identity(scope,signature,epoch);if(!Number.isSafeInteger(after)||after< -1||!Number.isSafeInteger(limit)||limit<1||limit>8)throw new RangeError('Invalid deferred replica page');await this.ready();let frames!:Array<{revision:number;frame:Delta;hash:string}>;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const args=[scope,signature,this.artifact,epoch,after];let take=limit;
   const sizes=await this.db.getAllAsync<{bytes:number}>('SELECT bytes FROM gv_deferred_replica_frames WHERE scope=? AND signature=? AND artifact=? AND epoch=? AND revision>? ORDER BY revision LIMIT ?',...args,take);let bytes=0;take=0;
   for(const size of sizes){if(bytes+size.bytes+512>262144)break;bytes+=size.bytes+512;take++;}if(!take){frames=[];return;}
   for(;;){try{
    const rows=await this.db.getAllAsync<{revision:number;body:Uint8Array;hash:string}>('SELECT revision,body,hash FROM gv_deferred_replica_frames WHERE scope=? AND signature=? AND artifact=? AND epoch=? AND revision>? ORDER BY revision LIMIT ?',...args,take);
    frames=rows.map(row=>{if(!(row.body instanceof Uint8Array))fail('INTENT_CORRUPT','Invalid deferred replica body');let text:string,frame:Delta;try{text=decoder.decode(row.body);decodedJSONBytes(text,262144);frame=JSON.parse(text) as Delta;}catch{fail('INTENT_CORRUPT','Invalid deferred replica JSON');}if(!frame||frame.cursor?.epoch!==epoch||frame.cursor.revision!==row.revision||frame.type!=='replica.delta')fail('INTENT_CORRUPT','Deferred replica header changed');return {revision:row.revision,frame,hash:row.hash};});sqlitePayloadBytes(frames,262144);break;
   }catch(error){if(take===1||!pageBudgetError(error))throw error;take=Math.max(1,Math.floor(take/2));}}
  });
  for(const row of frames)if(await digest(semantic(row.frame))!==row.hash)fail('INTENT_CORRUPT','Deferred replica frame hash differs');
  return frames.map(({revision,frame})=>({revision,frame}));
 }
 /** Only a verified window at this cursor may supersede earlier frames. Retire
  * at most128 disk records per call; unrelated scopes/epochs remain intact. */
 async discardThrough(scope:string,signature:string,epoch:string,revision:number):Promise<{removed:number;remaining:boolean}>{
  identity(scope,signature,epoch);if(!Number.isSafeInteger(revision)||revision<0)throw new RangeError('Invalid deferred retirement cursor');await this.ready();let result!:{removed:number;remaining:boolean};
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const args=[scope,signature,epoch,revision],selection='scope=? AND signature=? AND epoch=? AND revision<=?';
   const size=await this.db.getFirstAsync<{n:number;bytes:number}>(`SELECT count(*) n,coalesce(sum(bytes),0) bytes FROM (SELECT bytes FROM gv_deferred_replica_frames WHERE ${selection} ORDER BY revision,artifact LIMIT 128)`,...args);
   if(size!.n){await this.db.runAsync(`DELETE FROM gv_deferred_replica_frames WHERE ${selection} AND (artifact,revision) IN (SELECT artifact,revision FROM gv_deferred_replica_frames WHERE ${selection} ORDER BY revision,artifact LIMIT 128)`,...args,...args);if(!await this.db.getFirstAsync('UPDATE gv_deferred_replica_usage SET bytes=bytes-?,frames=frames-? WHERE scope=? AND bytes>=? AND frames>=? RETURNING frames',size!.bytes,size!.n,scope,size!.bytes,size!.n))fail('INTENT_CORRUPT','Deferred replica accounting differs');}
   result={removed:size!.n,remaining:!!await this.db.getFirstAsync(`SELECT 1 FROM gv_deferred_replica_frames WHERE ${selection} LIMIT 1`,...args)};
  });return result;
 }
 async summary(scope:string){identity(scope,'summary');await this.ready();await this.fence();return await this.db.getFirstAsync<{bytes:number;frames:number}>('SELECT bytes,frames FROM gv_deferred_replica_usage WHERE scope=?',scope)??{bytes:0,frames:0};}
}
