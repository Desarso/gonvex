import type {ReplicaRow} from './local-replica.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import {decodedJSONBytes,fail,pageBudgetError,sqlitePayloadBytes} from './sqlite-admission.js';
import {replicaRowsHashes} from './replica-integrity.js';
import {ReplicaSetDigest} from './replica-set-digest.js';
import {sameSnapshotPageMetadata,type SnapshotPage} from './snapshot-pages.js';
import {yieldSQLiteTask} from './sqlite-task-yield.js';

type Metadata=Omit<SnapshotPage,'result'|'digest'>;
type Stage={metadata:string;next_page:number;rows:number;bytes:number;receipt:Uint8Array;complete:number;digest:string|null;discarded:number};
export type SQLiteSnapshotProgress={snapshotId:string;nextPage:number;rows:number;complete:boolean;digest?:string};
const schema=`CREATE TABLE IF NOT EXISTS gv_snapshot_stages(
 scope TEXT NOT NULL,subscription TEXT NOT NULL,snapshot TEXT NOT NULL,metadata TEXT NOT NULL,
 next_page INTEGER NOT NULL,rows INTEGER NOT NULL,bytes INTEGER NOT NULL,receipt BLOB NOT NULL,
 complete INTEGER NOT NULL,digest TEXT,discarded INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(scope,subscription,snapshot)) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS gv_snapshot_rows(
 scope TEXT NOT NULL,subscription TEXT NOT NULL,snapshot TEXT NOT NULL,id TEXT NOT NULL,
 position INTEGER NOT NULL,body TEXT NOT NULL,hash TEXT NOT NULL,
 PRIMARY KEY(scope,subscription,snapshot,id),UNIQUE(scope,subscription,snapshot,position)) WITHOUT ROWID;
 CREATE TABLE IF NOT EXISTS gv_snapshot_frames(
 scope TEXT NOT NULL,subscription TEXT NOT NULL,snapshot TEXT NOT NULL,page INTEGER NOT NULL,hash TEXT NOT NULL,
 PRIMARY KEY(scope,subscription,snapshot,page)) WITHOUT ROWID;`;
const encoder=new TextEncoder();
const progress=(snapshotId:string,stage:Stage):SQLiteSnapshotProgress=>({snapshotId,nextPage:stage.next_page,rows:stage.rows,complete:!!stage.complete,...(stage.digest?{digest:stage.digest}:{})});
function identity(scope:string,subscription:string,snapshot:string){
 if(typeof scope!=='string'||!scope||scope.length>512||typeof subscription!=='string'||!subscription||subscription.length>256||typeof snapshot!=='string'||!snapshot||snapshot.length>128)throw new TypeError('Invalid snapshot identity');
}
async function hashFrame(encoded:string){return [...new Uint8Array(await crypto.subtle.digest('SHA-256',encoder.encode(encoded)))].map(b=>b.toString(16).padStart(2,'0')).join('');}

/** Dedicated-owner staging only. Verified rows remain isolated from canonical
 * values and visible windows until a separate authoritative merge publishes
 * them. A first-page Live Query can therefore continue to read/write canonical
 * rows while a collection arrives. No collection-sized graph or hash map is
 * retained, and an incomplete stage can never grant reducer coverage. */
export class SQLiteSnapshotPages {
 private initialized=false;
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly fence:()=>Promise<unknown>){}
 private async initialize(){
  if(this.initialized)return;
  await this.db.withTransactionAsync(async()=>{await this.fence();await this.db.execAsync(schema);});this.initialized=true;
 }
 private stage(scope:string,subscription:string,snapshot:string){return this.db.getFirstAsync<Stage>('SELECT metadata,next_page,rows,bytes,receipt,complete,digest,discarded FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);}
 async append(scope:string,input:SnapshotPage):Promise<SQLiteSnapshotProgress>{
  sqlitePayloadBytes(input,262144);identity(scope,input.id,input.snapshotId);
  // Snapshot caller ownership before the first async boundary.
  const encoded=JSON.stringify(input),page=JSON.parse(encoded) as SnapshotPage;
  if(page.type!=='replica.snapshotPage'||'hashes'in page||!Array.isArray(page.result)||page.result.length>512||!Number.isSafeInteger(page.sequence)||page.sequence<0||typeof page.last!=='boolean'
   ||typeof page.key!=='string'||!page.key||typeof page.scope!=='string'||!page.scope||page.visibilityScope!==scope||typeof page.cursor?.epoch!=='string'||!page.cursor.epoch||!Number.isSafeInteger(page.cursor.revision)||page.cursor.revision<0
   ||page.maxRows!==undefined&&(!Number.isSafeInteger(page.maxRows)||page.maxRows<0)||page.maxBytes!==undefined&&(!Number.isSafeInteger(page.maxBytes)||page.maxBytes<0)
   ||!page.last&&(!page.result.length||page.digest!==undefined))throw new TypeError('invalid-snapshot-page');
  for(const row of page.result)if(!row||Array.isArray(row)||typeof row!=='object'||typeof row[page.key]!=='string'&&typeof row[page.key]!=='number')throw new TypeError('invalid-snapshot-key');
  const frameHash=await hashFrame(encoded);
  await this.initialize();let output!:SQLiteSnapshotProgress;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();let stage=await this.stage(scope,page.id,page.snapshotId);
   const {result,digest,...metadata}=page;sqlitePayloadBytes(metadata,16384);
   if(stage){
    if(stage.discarded)fail('PAGE_CHANGED','Snapshot is being discarded');
    if(!sameSnapshotPageMetadata(metadata,JSON.parse(stage.metadata) as Metadata))fail('INTENT_CONFLICT','Snapshot metadata changed');
    if(page.sequence<stage.next_page){
     const receipt=await this.db.getFirstAsync<{hash:string}>('SELECT hash FROM gv_snapshot_frames WHERE scope=? AND subscription=? AND snapshot=? AND page=?',scope,page.id,page.snapshotId,page.sequence);
     if(receipt?.hash!==frameHash)fail('INTENT_CONFLICT','Snapshot page replay changed its payload');
     output=progress(page.snapshotId,stage);return;
    }
    if(stage.complete)fail('PAGE_CHANGED','Snapshot is already verified');
   }else{
    if(page.sequence!==0)fail('PAGE_CHANGED','Snapshot must start at page zero');
    const active=(await this.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_snapshot_stages WHERE scope=? AND subscription=?',scope,page.id))!.n;
    if(active>=2)fail('BUDGET','Collect obsolete snapshot stages first');
    stage={metadata:JSON.stringify(metadata),next_page:0,rows:0,bytes:0,receipt:new ReplicaSetDigest().encode(),complete:0,digest:null,discarded:0};
    await this.db.runAsync('INSERT INTO gv_snapshot_stages VALUES(?,?,?,?,0,0,0,?,0,NULL,0)',scope,page.id,page.snapshotId,stage.metadata,stage.receipt);
   }
   if(page.sequence!==stage.next_page)fail('PAGE_CHANGED','Snapshot page arrived out of sequence');
   const bodies=result.map(row=>JSON.stringify(row));
   let bytes=stage.bytes;for(const body of bodies)bytes+=encoder.encode(body).byteLength;
   const rows=stage.rows+result.length;
   if(!Number.isSafeInteger(bytes)||!Number.isSafeInteger(rows)||page.maxRows&&rows>page.maxRows||page.maxBytes&&bytes>page.maxBytes)fail('BUDGET','snapshot-page-budget-exceeded');
   const hashes=await replicaRowsHashes(result,page.key,yieldSQLiteTask),state=ReplicaSetDigest.decode(stage.receipt);
   const entries=Object.entries(hashes);
   for(const [id,hash]of entries){
    if(!id)throw new TypeError('invalid-snapshot-key');
    if(await this.db.getFirstAsync('SELECT 1 FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? AND id=?',scope,page.id,page.snapshotId,id))throw new TypeError('duplicate-snapshot-key');
    state.add(hash);
   }
   for(let offset=0;offset<result.length;offset+=50){
    const count=Math.min(50,result.length-offset),values:unknown[]=[];
    for(let i=0;i<count;i++){
     const row=result[offset+i] as Record<string,unknown>,id=String(row[page.key]);
     values.push(scope,page.id,page.snapshotId,id,stage.rows+offset+i,bodies[offset+i],hashes[id]);
    }
    await this.db.runAsync(`INSERT INTO gv_snapshot_rows VALUES ${Array.from({length:count},()=>'(?,?,?,?,?,?,?)').join(',')}`,...values);
   }
   if(page.last){
    const actual=digest?.startsWith('sum128-v1:')?state.digest():digest?.startsWith('xor1:')?state.legacyDigest():undefined;
    if(!actual||actual!==digest)throw new TypeError('snapshot-page-integrity-mismatch');
   }
   const next={...stage,next_page:page.sequence+1,rows,bytes,receipt:state.encode(),complete:Number(page.last),digest:page.last?digest!:null};
   await this.db.runAsync('INSERT INTO gv_snapshot_frames VALUES(?,?,?,?,?)',scope,page.id,page.snapshotId,page.sequence,frameHash);
   await this.db.runAsync('UPDATE gv_snapshot_stages SET next_page=?,rows=?,bytes=?,receipt=?,complete=?,digest=? WHERE scope=? AND subscription=? AND snapshot=?',next.next_page,rows,bytes,next.receipt,next.complete,next.digest,scope,page.id,page.snapshotId);
   output=progress(page.snapshotId,next);
  });return output;
 }
 async read(scope:string,subscription:string,snapshot:string,after=-1,limit=128){
  identity(scope,subscription,snapshot);
  if(!Number.isSafeInteger(after)||after< -1||!Number.isSafeInteger(limit)||limit<1||limit>128)throw new RangeError('Invalid snapshot page');
  await this.initialize();let output!:{rows:Array<{id:string;position:number;row:ReplicaRow;hash:string}>;nextPosition:number|null};
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const stage=await this.stage(scope,subscription,snapshot);
   if(!stage?.complete||stage.discarded)fail('PAGE_CHANGED','Snapshot has not been verified or was discarded');
   let take=limit;
   for(;;){try{
    const entries=await this.db.getAllAsync<{id:string;position:number;body:string;hash:string}>('SELECT id,position,body,hash FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? AND position>? ORDER BY position LIMIT ?',scope,subscription,snapshot,after,take);
    if(!entries.length&&after+1<stage.rows)fail('INVALID_ROW','Verified snapshot lost rows');
    let bytes=64;for(const entry of entries){bytes+=entry.id.length*2+256+decodedJSONBytes(entry.body,262144-bytes);if(bytes>262144)fail('BUDGET','Snapshot decoded page exceeds budget');}
    const rows=entries.map(({id,position,body,hash})=>({id,position,row:JSON.parse(body) as ReplicaRow,hash}));
    const last=entries.at(-1)?.position??after;
    output={rows,nextPosition:last+1<stage.rows?last:null};sqlitePayloadBytes(output,262144);break;
   }catch(error){if(!pageBudgetError(error)||take===1)throw error;take=Math.max(1,Math.floor(take/2));}}
  });return output;
 }
 async discard(scope:string,subscription:string,snapshot:string):Promise<boolean>{
  identity(scope,subscription,snapshot);await this.initialize();let removed=false;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();
   await this.db.runAsync('UPDATE gv_snapshot_stages SET discarded=1 WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);
   const rows=await this.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM (SELECT id FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? LIMIT 128)',scope,subscription,snapshot);
   await this.db.runAsync('DELETE FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? AND id IN (SELECT id FROM gv_snapshot_rows WHERE scope=? AND subscription=? AND snapshot=? LIMIT 128)',scope,subscription,snapshot,scope,subscription,snapshot);
   if((rows?.n??0)<128){
    await this.db.runAsync('DELETE FROM gv_snapshot_frames WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);
    await this.db.runAsync('DELETE FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=?',scope,subscription,snapshot);removed=true;
   }
  });return removed;
 }
}
