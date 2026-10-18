import {ByteLedger} from './byte-budget.js';
import {copyOwnedSQLiteBytes} from './sqlite-owned-buffer.js';
import {SQLiteTaskYield} from './sqlite-task-yield.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteSnapshotPages,SQLiteSnapshotReceipt} from './sqlite-snapshot-pages.js';

type Pending={bytes:Uint8Array;releaseReservation?:()=>void;gate:Promise<void>;done:Promise<SQLiteSnapshotReceipt>;resolve(value:SQLiteSnapshotReceipt):void;reject(error:unknown):void};
type Options={
 database:SQLiteReplicaDatabase;
 snapshots:Pick<SQLiteSnapshotPages,'appendEncodedReceipt'>&Partial<Pick<SQLiteSnapshotPages,'ready'>>;
 scope:string;
 reserve?(bytes:number):()=>void;
 /** The same owner FIFO used by queries, reducers and every storage operation. */
 runOwner<T>(operation:()=>Promise<T>):Promise<T>;
 /** Publish a staging generation inside the same durable batch. This must
  * never advance a server/reducer watermark or grant complete coverage. */
 commitProgress?():Promise<void>;
 sendAck(ack:SQLiteSnapshotReceipt['ack']):void;
 /** Runs inside the atomic batch, before the final staging credit is returned. */
 activateVerified?(receipt:SQLiteSnapshotReceipt):Promise<void>;
};
const maxFrames=8,maxWireBytes=512*1024,frameBytes=128*1024,metadataBytes=256;
const readyGate=Promise.resolve();
const emptyBytes=new Uint8Array(0);
function closed(){return Object.assign(new Error('Snapshot connection is closed'),{code:'FENCED'});}

/** Owned WebSocket binary frames go directly to the SQLite owner. One task
 * boundary coalesces the available credit window into one durable transaction.
 * No canonical row is parsed in JavaScript, and no credit precedes commit.
 * This is an owner-core component, never a main-thread application transport. */
export class SQLiteNativeIngress {
 private readonly ledger=new ByteLedger(maxWireBytes+maxFrames*metadataBytes);
 private readonly tasks=new SQLiteTaskYield();
 private pending:Pending[]=[];
 private active:Pending[]=[];
 private rejectGate?: (error:Error)=>void;
 private draining=false;
 private disposed=false;
 private entries=0;
 private wireBytes=0;
 private refusals=0;
 private releaseBatch?:()=>void;
 private releasedBytes=0;
 constructor(private readonly options:Options){
  if(!options.scope||options.scope.length>512)throw new TypeError('Invalid snapshot connection scope');
  this.ledger.register('native-snapshot-queue',()=>this.stats());
 }
 stats(){return {name:'native-snapshot-queue',bytes:this.wireBytes+this.entries*metadataBytes,maxBytes:this.ledger.maxBytes,entries:this.entries,maxEntries:maxFrames,evictions:0,refusals:this.refusals,releasedBytes:this.releasedBytes};}
 /** Snapshot bytes before queueing. Caller mutations cannot alter an admitted
  * frame; the caller retains its original buffer on success and refusal. */
 accept(buffer:ArrayBuffer,gate:Promise<void>=readyGate):Promise<SQLiteSnapshotReceipt>{
  if(!(gate instanceof Promise))return Promise.reject(new TypeError('Native frame gate must be an owner promise'));
  if(!(buffer instanceof ArrayBuffer))return Promise.reject(new TypeError('Snapshot requires an owned ArrayBuffer'));
  let bytes=new Uint8Array(buffer);
  if(this.disposed){return Promise.reject(closed());}
  if(!bytes.byteLength||bytes.byteLength>frameBytes||this.entries===maxFrames||this.wireBytes+bytes.byteLength>maxWireBytes){
   this.refusals++;return Promise.reject(Object.assign(new Error('Snapshot connection exceeded its credit budget'),{code:'BUDGET'}));
  }
  let releaseReservation:(()=>void)|undefined;
  try{releaseReservation=this.options.reserve?.(bytes.byteLength+metadataBytes);bytes=copyOwnedSQLiteBytes(buffer);}catch(error){releaseReservation?.();return Promise.reject(error);}
  this.entries++;this.wireBytes+=bytes.byteLength;
  let resolve!:(value:SQLiteSnapshotReceipt)=>void,reject!:(error:unknown)=>void;
  const result=new Promise<SQLiteSnapshotReceipt>((yes,no)=>{resolve=yes;reject=no;});this.pending.push({bytes,releaseReservation,gate,done:result,resolve,reject});
  if(!this.draining){this.draining=true;void this.drain();}
  if(this.pending.length===maxFrames||this.wireBytes===maxWireBytes)this.releaseBatch?.();
  return result;
 }
 /** Capture exactly the binary events preceding a text control frame. Later
  * binary events receive that control's promise as their gate. Waiting happens
  * outside the SQL FIFO, and distinct gates never share a transaction. */
 flush():Promise<void>{
  if(this.disposed)return Promise.reject(closed());this.releaseBatch?.();
  return Promise.all([...this.active,...this.pending].map(entry=>entry.done)).then(()=>{});
 }
 private async waitGate(gate:Promise<void>){
  if(this.disposed)throw closed();
  try{await new Promise<void>((resolve,reject)=>{this.rejectGate=reject;gate.then(resolve,reject);});}
  finally{this.rejectGate=undefined;}
  if(this.disposed)throw closed();
 }
 private release(entry:Pending){
  const size=entry.bytes.byteLength;this.entries--;this.wireBytes-=size;this.releasedBytes+=size;
  // Keep retired backing stores visible to the engine's external-memory GC.
  // DOM event backing stores never enter this queue; native consumers have
  // finished with this private byte snapshot before its reference is dropped.
  entry.bytes=emptyBytes;
  entry.releaseReservation?.();entry.releaseReservation=undefined;
 }
 private async drain(){
  try{
   while(this.pending.length&&!this.disposed){
    await this.tasks.yield();
    // Network events can arrive on separate tasks. Wait at most four ms for
    // the bounded credit window instead of forcing a FULL sync after a partial batch.
    if(this.pending.length<maxFrames&&this.wireBytes<maxWireBytes&&!this.disposed)await new Promise<void>(resolve=>{
     const timer=setTimeout(()=>finish(),4);
     const finish=()=>{clearTimeout(timer);this.releaseBatch=undefined;resolve();};
     this.releaseBatch=finish;
    });
    const gate=this.pending[0]?.gate;if(!gate)continue;let end=1;while(end<this.pending.length&&this.pending[end]!.gate===gate)end++;
    const batch=this.pending.splice(0,end);this.active=batch;
    try{
     await this.waitGate(gate);
     const receipts=await this.options.runOwner(async()=>{
      if(this.disposed)throw closed();
      await this.options.snapshots.ready?.();
      const receipts:SQLiteSnapshotReceipt[]=[];
      await this.options.database.withTransactionAsync(async()=>{
       for(const entry of batch){
        if(this.disposed)throw closed();
        const receipt=await this.options.snapshots.appendEncodedReceipt(this.options.scope,entry.bytes,true);
        // Replaying an earlier page can observe an already complete stage.
        // Only the final page schedules publication, which is itself idempotent.
        if(receipt.progress.complete&&receipt.ack.sequence+1===receipt.progress.nextPage)await this.options.activateVerified?.(receipt);
        receipts.push(receipt);
       }
       await this.options.commitProgress?.();
      });return receipts;
     });
     if(this.disposed)throw closed();
     // The outer transaction has committed. A failed socket send has an unknown
     // delivery outcome; immutable frame receipts make reconnect replay safe.
     for(const receipt of receipts)this.options.sendAck(receipt.ack);
     for(let i=0;i<batch.length;i++)batch[i]!.resolve(receipts[i]!);
    }catch(error){for(const entry of batch)entry.reject(error);}
    finally{for(const entry of batch)this.release(entry);this.active=[];}
   }
  }catch(error){for(const entry of this.pending){entry.reject(error);this.release(entry);}this.pending=[];}
  finally{this.draining=false;}
 }
 close(){
  if(this.disposed)return;this.disposed=true;this.tasks.close();this.releaseBatch?.();this.rejectGate?.(closed());
  const pending=this.pending;this.pending=[];
  for(const entry of pending){entry.reject(closed());this.release(entry);}
 }
}
