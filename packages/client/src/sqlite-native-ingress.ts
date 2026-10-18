import {ByteLedger} from './byte-budget.js';
import {releaseOwnedSQLiteBytes} from './sqlite-owned-buffer.js';
import {SQLiteTaskYield} from './sqlite-task-yield.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteSnapshotPages,SQLiteSnapshotReceipt} from './sqlite-snapshot-pages.js';

type Pending={bytes:Uint8Array;resolve(value:SQLiteSnapshotReceipt):void;reject(error:unknown):void};
type Options={
 database:SQLiteReplicaDatabase;
 snapshots:Pick<SQLiteSnapshotPages,'appendEncodedReceipt'>&Partial<Pick<SQLiteSnapshotPages,'ready'>>;
 scope:string;
 /** The same owner FIFO used by queries, reducers and every storage operation. */
 runOwner<T>(operation:()=>Promise<T>):Promise<T>;
 sendAck(ack:SQLiteSnapshotReceipt['ack']):void;
 /** Runs inside the atomic batch, before the final staging credit is returned. */
 activateVerified?(receipt:SQLiteSnapshotReceipt):Promise<void>;
};
const maxFrames=8,maxWireBytes=512*1024,frameBytes=128*1024,metadataBytes=128;
function closed(){return Object.assign(new Error('Snapshot connection is closed'),{code:'FENCED'});}

/** Owned WebSocket binary frames go directly to the SQLite owner. One task
 * boundary coalesces the available credit window into one durable transaction.
 * No canonical row is parsed in JavaScript, and no credit precedes commit.
 * This is an owner-core component, never a main-thread application transport. */
export class SQLiteNativeIngress {
 private readonly ledger=new ByteLedger(maxWireBytes+maxFrames*metadataBytes);
 private readonly tasks=new SQLiteTaskYield();
 private pending:Pending[]=[];
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
 /** Transfers exclusive ownership of the network ArrayBuffer to this actor.
  * The caller must not read or reuse it after calling accept. */
 accept(buffer:ArrayBuffer):Promise<SQLiteSnapshotReceipt>{
  if(!(buffer instanceof ArrayBuffer))return Promise.reject(new TypeError('Snapshot requires an owned ArrayBuffer'));
  const bytes=new Uint8Array(buffer);
  if(this.disposed){releaseOwnedSQLiteBytes(bytes);return Promise.reject(closed());}
  if(!bytes.byteLength||bytes.byteLength>frameBytes||this.entries===maxFrames||this.wireBytes+bytes.byteLength>maxWireBytes){
   this.refusals++;releaseOwnedSQLiteBytes(bytes);return Promise.reject(Object.assign(new Error('Snapshot connection exceeded its credit budget'),{code:'BUDGET'}));
  }
  this.entries++;this.wireBytes+=bytes.byteLength;
  const result=new Promise<SQLiteSnapshotReceipt>((resolve,reject)=>this.pending.push({bytes,resolve,reject}));
  if(!this.draining){this.draining=true;void this.drain();}
  if(this.pending.length===maxFrames||this.wireBytes>maxWireBytes-frameBytes)this.releaseBatch?.();
  return result;
 }
 private release(entry:Pending){this.entries--;this.wireBytes-=entry.bytes.byteLength;this.releasedBytes+=releaseOwnedSQLiteBytes(entry.bytes);}
 private async drain(){
  try{
   while(this.pending.length&&!this.disposed){
    await this.tasks.yield();
    // Network events can arrive on separate tasks. Wait at most four ms for
    // the bounded credit window instead of forcing a FULL sync after a partial batch.
    if(this.pending.length<maxFrames&&this.wireBytes<=maxWireBytes-frameBytes&&!this.disposed)await new Promise<void>(resolve=>{
     const timer=setTimeout(()=>finish(),4);
     const finish=()=>{clearTimeout(timer);this.releaseBatch=undefined;resolve();};
     this.releaseBatch=finish;
    });
    const batch=this.pending;this.pending=[];
    if(!batch.length)continue;
    try{
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
      });return receipts;
     });
     if(this.disposed)throw closed();
     // The outer transaction has committed. A failed socket send has an unknown
     // delivery outcome; immutable frame receipts make reconnect replay safe.
     for(const receipt of receipts)this.options.sendAck(receipt.ack);
     for(let i=0;i<batch.length;i++)batch[i]!.resolve(receipts[i]!);
    }catch(error){for(const entry of batch)entry.reject(error);}
    finally{for(const entry of batch)this.release(entry);}
   }
  }catch(error){for(const entry of this.pending){entry.reject(error);this.release(entry);}this.pending=[];}
  finally{this.draining=false;}
 }
 close(){
  if(this.disposed)return;this.disposed=true;this.tasks.close();this.releaseBatch?.();
  const pending=this.pending;this.pending=[];
  for(const entry of pending){entry.reject(closed());this.release(entry);}
 }
}
