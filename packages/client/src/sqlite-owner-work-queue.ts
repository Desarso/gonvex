import {SQLiteOwnerQueueBusyError} from './sqlite-owner-transactions.js';

type Work={bytes:number;read:boolean;run:()=>Promise<unknown>;resolve:(value:unknown)=>void;reject:(error:unknown)=>void};
/** One fenced connection, with bounded read priority between durable operations.
 * Reads never interrupt a transaction. Eight reads share a snapshot, then a
 * pending writer gets a turn so continuous UI demand cannot starve intents. */
export class SQLiteOwnerWorkQueue{
 private readonly waiting:Work[]=[];
 private running=false;private closed=false;private count=0;private bytes=0;private readBatches=0;
 private idleWaiters:Array<()=>void>=[];
 private readOperations=0;private readSnapshots=0;private writeOperations=0;
 constructor(private readonly fence:()=>Promise<void>,private readonly snapshot:(run:()=>Promise<void>)=>Promise<void>){}
 stats(){return {queuedOperations:this.count,queuedBytes:this.bytes,readOperations:this.readOperations,readSnapshots:this.readSnapshots,writeOperations:this.writeOperations};}
 submit<T>(bytes:number,run:()=>Promise<T>,read=false):Promise<T>{
  if(this.closed)return Promise.reject(this.fenced());
  if(!Number.isSafeInteger(bytes)||bytes<0||bytes>2097152)return Promise.reject(Object.assign(Error('Storage owner operation exceeds queue budget'),{code:'BUSY'}));
  if(this.count>=64||this.bytes+bytes>2097152)return Promise.reject(new SQLiteOwnerQueueBusyError());
  this.count++;this.bytes+=bytes;
  const result=new Promise<unknown>((resolve,reject)=>{this.waiting.push({bytes,read,run,resolve,reject});});
  this.start();return result as Promise<T>;
 }
 private fenced(){return Object.assign(Error('Storage owner is closed'),{code:'FENCED'});}
 private start(){
  if(this.running)return;this.running=true;
  // Yield to incoming worker requests between groups, not between rows.
  setTimeout(()=>{void this.drain();},0);
 }
 private async drain(){
  const reads=this.waiting.filter(work=>work.read);
  const writer=this.waiting.find(work=>!work.read);
  const batch=reads.length&&(!writer||this.readBatches<1)?reads.slice(0,8):writer?[writer]:[];
  if(!batch.length){this.running=false;for(const resolve of this.idleWaiters.splice(0))resolve();return;}
  for(const work of batch)this.waiting.splice(this.waiting.indexOf(work),1);
  const values:Array<{value?:unknown;error?:unknown;failed?:boolean}>=[];
  try{
   if(this.closed)throw this.fenced();await this.fence();
   if(batch[0]!.read){
    this.readSnapshots++;this.readOperations+=batch.length;
    await this.snapshot(async()=>{
     for(const work of batch){
      try{values.push({value:await work.run()});}catch(error){values.push({failed:true,error});}
     }
     await this.fence();
    });this.readBatches++;
   }else{this.writeOperations++;values.push({value:await batch[0]!.run()});this.readBatches=0;}
   for(let i=0;i<batch.length;i++){const value=values[i]!;if(value.failed)batch[i]!.reject(value.error);else batch[i]!.resolve(value.value);}
  }catch(error){for(const work of batch)work.reject(error);}
  finally{for(const work of batch){this.count--;this.bytes-=work.bytes;}this.running=false;this.start();}
 }
 async close(){
  this.closed=true;for(const work of this.waiting.splice(0)){this.count--;this.bytes-=work.bytes;work.reject(this.fenced());}
  if(this.running)await new Promise<void>(resolve=>this.idleWaiters.push(resolve));
 }
}
