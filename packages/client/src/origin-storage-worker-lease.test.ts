import {expect,it} from 'vitest';
import {OriginStorageWorkerLease} from './origin-storage-worker-lease.js';
import {IndexedDBIntentLedger} from './indexeddb-intent-ledger.js';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
function manager(){
 type Entry={run:()=>Promise<unknown>;resolve(value:unknown):void;reject(error:unknown):void;options:LockOptions;active:boolean};const queue:Entry[]=[],calls:LockOptions[]=[];let active=false;
 const pump=()=>{if(active||!queue.length)return;const entry=queue.shift()!;active=entry.active=true;void Promise.resolve().then(entry.run).then(entry.resolve,entry.reject).finally(()=>{active=false;pump();});};
 return {calls,locks:{request(_name:string,options:LockOptions,run:()=>Promise<unknown>){calls.push(options);return new Promise((resolve,reject)=>{const entry={run,options,resolve,reject,active:false};const cancel=()=>{if(entry.active)return;const index=queue.indexOf(entry);if(index>=0)queue.splice(index,1);reject(new DOMException('Aborted','AbortError'));};options.signal?.addEventListener('abort',cancel,{once:true});if(options.signal?.aborted){cancel();return;}queue.push(entry);pump();});}} as unknown as Pick<LockManager,'request'>};
}
it('holds ownership across a frozen callback and requires release before the other adapter can claim its durable epoch',async()=>{
 const locks=manager(),first=new OriginStorageWorkerLease(locks.locks),second=new OriginStorageWorkerLease(locks.locks),factory=new IDBFactory(),sql=new IndexedDBIntentLedger('shared',factory,IDBKeyRange),idb=new IndexedDBIntentLedger('shared',factory,IDBKeyRange);try{await first.acquire();expect((await sql.claim('sql')).epoch).toBe(1);let acquired=false;const takeover=second.acquire().then(async()=>{acquired=true;return idb.claim('idb');});await new Promise(resolve=>setTimeout(resolve,20));expect(acquired).toBe(false);expect(first.stats().held).toBe(true);expect(second.stats().pending).toBe(true);await first.close();expect((await takeover).epoch).toBe(2);await expect(sql.generation()).rejects.toMatchObject({code:'FENCED'});expect(locks.calls.every(options=>options.mode==='exclusive'&&!options.steal)).toBe(true);}finally{await first.close();await second.close();sql.close();idb.close();}
});
it('cancels a waiting worker without releasing the active writer and closes acquired leases idempotently',async()=>{
 const locks=manager(),first=new OriginStorageWorkerLease(locks.locks),waiting=new OriginStorageWorkerLease(locks.locks);await first.acquire();const next=waiting.acquire(),rejected=expect(next).rejects.toMatchObject({name:'AbortError'});waiting.cancelPending();await rejected;expect(first.stats().held).toBe(true);await waiting.close();await first.close();await first.close();expect(first.stats().held).toBe(false);await expect(first.acquire()).rejects.toMatchObject({code:'FENCED'});
});
