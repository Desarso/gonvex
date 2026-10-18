const lockName='gonvex-origin-storage-worker-v1';
/** Both storage adapters hold this lock in their dedicated worker, never in
 * the tab. A parent election may be stolen, but a frozen old worker must finish
 * or terminate before any replacement adapter can become a writer. */
export class OriginStorageWorkerLease {
 private readonly abort=new AbortController();private pending?:Promise<void>;private lifetime?:Promise<unknown>;private release?:()=>void;private held=false;private closed=false;
 constructor(private readonly locks:Pick<LockManager,'request'>|undefined=globalThis.navigator?.locks){}
 acquire():Promise<void>{
  if(this.closed)return Promise.reject(Object.assign(Error('Storage worker lease is closed'),{code:'FENCED'}));if(this.pending)return this.pending;if(!this.locks)return Promise.reject(Object.assign(Error('Storage worker ownership requires Web Locks'),{code:'UNSUPPORTED'}));
  let accept!:()=>void,deny!:(error:unknown)=>void;const ready=this.pending=new Promise<void>((resolve,reject)=>{accept=resolve;deny=reject;});
  try{this.lifetime=this.locks.request(lockName,{mode:'exclusive',signal:this.abort.signal},async()=>{
   if(this.closed)throw Object.assign(Error('Storage worker lease is closed'),{code:'FENCED'});this.held=true;const lifetime=new Promise<void>(resolve=>{this.release=resolve;});accept();try{await lifetime;}finally{this.held=false;this.release=undefined;}
  });void this.lifetime.catch(error=>deny(error));}catch(error){deny(error);}return ready;
 }
 cancelPending(){if(!this.held)this.abort.abort();}
 stats(){return {held:this.held,pending:!!this.pending&&!this.held&&!this.closed,bytes:1024,maxBytes:1024,entries:Number(!!this.pending),maxEntries:1};}
 async close(){if(!this.closed){this.closed=true;this.abort.abort();this.release?.();}await this.lifetime?.catch(()=>{});}
}
