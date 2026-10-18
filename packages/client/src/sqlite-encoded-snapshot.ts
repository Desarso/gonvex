import {ByteLedger} from './byte-budget.js';
import {admitSQLiteUTF8Frame,encodedFrameHasCanonicalStructure} from './sqlite-encoded-frame.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
import {ReplicaSetDigest} from './replica-set-digest.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteSnapshotMetadata,SQLiteSnapshotProgress,SQLiteSnapshotStage,SQLiteSnapshotReceipt} from './sqlite-snapshot-pages.js';

type Backend={
 initialize():Promise<void>;
 prepare(scope:string,metadata:SQLiteSnapshotMetadata,hash:string):Promise<SQLiteSnapshotStage|SQLiteSnapshotProgress>;
 finish(scope:string,metadata:SQLiteSnapshotMetadata,digest:string|undefined,hash:string,stage:SQLiteSnapshotStage,state:ReplicaSetDigest,rows:number,bytes:number):Promise<SQLiteSnapshotProgress>;
};
const source='(SELECT value FROM main.gv_snapshot_input WHERE id=1)';
const field=(name:string)=>`json_extract(${source},'$.${name}')`;
const kind=(name:string)=>`json_type(${source},'$.${name}')`;
const metadataSQL=`SELECT hash frameHash,
 ${field('type')} frameType,${kind('canonicalRows')} rowsType,${kind('last')} lastType,
 ${['id','snapshotId','scope','visibilityScope','key','sequence','last','mode','maxRows','maxBytes','truncated','orderBy','orderDirection','path','digest'].map(name=>`${field(name)} "${name}",${kind(name)} "${name}Type"`).join(',')},
 ${field('cursor.epoch')} epoch,${kind('cursor.epoch')} epochType,${field('cursor.revision')} revision,${kind('cursor.revision')} revisionType,
 CASE WHEN ${kind('result')} IS NULL AND ${kind('hashes')} IS NULL THEN 0 ELSE 1 END badExtra
 FROM main.gv_snapshot_input WHERE id=1`;
const rowsSQL=`SELECT CAST(key AS INTEGER) position,value body FROM json_each(${source},'$.canonicalRows')`;
const keyedSQL=`WITH input AS MATERIALIZED(${rowsSQL}),keyed AS MATERIALIZED(
 SELECT position,body,CASE WHEN json_type(body,?)='text' THEN json_extract(body,?) ELSE body -> ? END id FROM input)`;

/** Native snapshot frames carry canonical JSON row strings. SQLite unescapes
 * that outer transport string exactly; SHA-256 sees the server's canonical
 * row bytes without reserializing numbers or materializing row objects in JS.
 * Only bounded metadata and (id,hash) receipts cross the oo1 bridge. */
export class SQLiteEncodedSnapshotPages {
 private scratch?:Uint8Array;
 private busy=false;
 private readonly ledger=new ByteLedger(131072);
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly fence:()=>Promise<unknown>,private readonly backend:Backend){
  this.ledger.register('snapshot-wire-scratch',()=>({name:'snapshot-wire-scratch',bytes:this.scratch?.byteLength??0,maxBytes:131072,entries:Number(!!this.scratch),maxEntries:1,evictions:0,refusals:0}));
 }
 stats(){return {wireBytes:this.ledger.bytes,wireMaxBytes:this.ledger.maxBytes,wireEntries:Number(!!this.scratch)};}
 async appendReceipt(scope:string,input:Uint8Array):Promise<SQLiteSnapshotReceipt>{
  if(!this.db.ingressNativeHash)throw Object.assign(new Error('Native snapshot hashing is unavailable'),{code:'UNSUPPORTED'});
  if(!(input instanceof Uint8Array)||!(input.buffer instanceof ArrayBuffer)||input.byteLength>this.ledger.maxBytes)fail('BUDGET','Snapshot wire frame exceeds budget');
  if(this.busy)fail('FENCED','Snapshot frames must use the owner FIFO');
  this.scratch??=new Uint8Array(this.ledger.maxBytes);this.scratch.set(input);this.busy=true;
  const owned=this.scratch.subarray(0,input.byteLength);let output!:SQLiteSnapshotProgress,ack!:SQLiteSnapshotReceipt['ack'];
  try{
   admitSQLiteUTF8Frame(owned,262144);
   if(owned.includes(0))fail('INVALID_ROW','Unescaped NUL in snapshot JSON');
   await this.backend.initialize();
   await this.db.withTransactionAsync(async()=>{
    await this.fence();await this.db.runAsync('DELETE FROM main.gv_snapshot_input');
    try{
     await this.db.runAsync(`WITH wire AS MATERIALIZED(SELECT CAST(? AS TEXT) value)
      INSERT INTO main.gv_snapshot_input SELECT 1,jsonb(value),gv_ingress_sha256(value) FROM wire WHERE json_valid(value)`,owned);
     const header=await this.db.getFirstAsync<Record<string,unknown>>(metadataSQL);
     if(!header||header.frameType!=='replica.snapshotEncodedPage'||header.rowsType!=='array'||header.badExtra||!encodedFrameHasCanonicalStructure(owned))fail('INVALID_ROW','Invalid encoded snapshot frame');
     for(const name of ['id','snapshotId','scope','visibilityScope','key'])if(header[name+'Type']!=='text')fail('INVALID_ROW','Invalid encoded snapshot identity');
     if(!header.key||!header.scope||header.epochType!=='text'||!header.epoch||!Number.isSafeInteger(header.revision)||Number(header.revision)<0||header.visibilityScope!==scope
      ||!Number.isSafeInteger(header.sequence)||Number(header.sequence)<0||!['true','false'].includes(String(header.lastType)))fail('INVALID_ROW','Invalid encoded snapshot cursor');
     const metadata:SQLiteSnapshotMetadata={type:'replica.snapshotPage',id:header.id as string,snapshotId:header.snapshotId as string,scope:header.scope as string,visibilityScope:scope,key:header.key as string,
      sequence:Number(header.sequence),last:header.lastType==='true',cursor:{epoch:header.epoch as string,revision:Number(header.revision)}};
     for(const name of ['maxRows','maxBytes'] as const)if(header[name+'Type']!==null){
      if(!Number.isSafeInteger(header[name])||Number(header[name])<0)fail('INVALID_ROW','Invalid encoded snapshot budget');metadata[name]=Number(header[name]);
     }
     for(const name of ['orderBy','orderDirection','path','mode'] as const)if(header[name+'Type']!==null){
      if(header[name+'Type']!=='text')fail('INVALID_ROW','Invalid encoded snapshot metadata');
      Object.assign(metadata,{[name]:header[name]});
     }
     if(metadata.mode!==undefined&&!['eager','progressive'].includes(metadata.mode)||metadata.orderDirection!==undefined&&!['asc','desc'].includes(metadata.orderDirection))fail('INVALID_ROW','Invalid encoded snapshot ordering');
     if(header.truncatedType!==null){if(!['true','false'].includes(String(header.truncatedType)))fail('INVALID_ROW','Invalid encoded snapshot truncation');metadata.truncated=header.truncatedType==='true';}
     const digest=header.digestType===null?undefined:header.digestType==='text'?header.digest as string:fail('INVALID_ROW','Invalid snapshot digest');
     if(!metadata.last&&digest!==undefined)fail('INVALID_ROW','Snapshot digest arrived before the final frame');
     sqlitePayloadBytes(metadata,16384);
     ack={type:'replica.snapshotAck',id:metadata.id,snapshotId:metadata.snapshotId,sequence:metadata.sequence};
     const frameHash=header.frameHash as string,prepared=await this.backend.prepare(scope,metadata,frameHash);
     if('snapshotId'in prepared){output=prepared;return;}
     const path='$.'+JSON.stringify(metadata.key);
     const size=await this.db.getFirstAsync<{n:number;bytes:number;bad:number}>(`SELECT count(*) n,coalesce(sum(length(CAST(value AS BLOB))),0) bytes,
      coalesce(sum(CASE WHEN type<>'text' OR NOT json_valid(value) THEN 1 WHEN json_type(value)<>'object' THEN 1
       WHEN NOT coalesce(json_type(value,?) IN ('text','integer','real'),0) THEN 1 ELSE 0 END),0) bad
      FROM json_each(${source},'$.canonicalRows')`,path);
     if(!size||size.bad||size.n>512||!metadata.last&&!size.n)fail('INVALID_ROW','Invalid canonical snapshot rows');
     const paths=[path,path,path];
     if(await this.db.getFirstAsync(`${keyedSQL} SELECT 1 FROM keyed WHERE id IS NULL OR id='' LIMIT 1`,...paths))fail('INVALID_ROW','Snapshot row requires an identity');
     if(await this.db.getFirstAsync(`${keyedSQL} SELECT 1 FROM keyed GROUP BY id HAVING count(*)>1 LIMIT 1`,...paths))fail('INVALID_ROW','duplicate-snapshot-key');
     // The bounded incoming frame must drive indexed durable duplicate probes.
     if(await this.db.getFirstAsync(`${keyedSQL} SELECT 1 FROM keyed w CROSS JOIN gv_snapshot_rows r ON r.scope=? AND r.subscription=? AND r.snapshot=? AND r.id=w.id LIMIT 1`,...paths,scope,metadata.id,metadata.snapshotId))fail('INVALID_ROW','duplicate-snapshot-key');
     const hashes=await this.db.getAllAsync<{id:string;hash:string}>(`${keyedSQL} INSERT INTO gv_snapshot_rows(scope,subscription,snapshot,id,position,body,hash)
      SELECT ?,?,?,id,?+position,body,gv_ingress_sha256(body) FROM keyed RETURNING id,hash`,...paths,scope,metadata.id,metadata.snapshotId,prepared.rows);
     if(hashes.length!==size.n)fail('INVALID_ROW','Snapshot insert receipt differs from the frame');
     const state=ReplicaSetDigest.decode(prepared.receipt);for(const row of hashes)state.add(row.hash);
     output=await this.backend.finish(scope,metadata,digest,frameHash,prepared,state,size.n,size.bytes);
    }finally{await this.db.runAsync('DELETE FROM main.gv_snapshot_input');}
   });return {progress:output,ack};
  }finally{owned.fill(0);this.busy=false;}
 }
}
