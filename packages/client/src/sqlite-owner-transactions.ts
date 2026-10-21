import {yieldSQLiteTask} from './sqlite-task-yield.js';
import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';

const transactionTimings:Array<{finishedAt:number;durationMs:number}>=[];let longestTransactionMs=0,transactionCount=0;export const sqliteOwnerTransactionStats=()=>({longestTransactionMs,transactionCount,recent:transactionTimings.slice()});

/** Internal owner-actor host. Only the actor's FIFO may call it: a nested
 * adapter transaction belongs to the already admitted operation. The official
 * host itself still rejects concurrent transactions. No transport exposes SQL. */
export function sqliteOwnerTransactions(database: SQLiteReplicaDatabase): SQLiteReplicaDatabase {
  let depth=0, serial=0, poison:{error:unknown}|undefined;
  const checkPoison = () => { if(poison)throw poison.error; };
  return {
    get ownerTransactionDepth(){return depth;},
    ...(database.ingressUnicodeFold ? {ingressUnicodeFold:true as const} : {}),
    ...(database.ingressNativeFastFold ? {ingressNativeFastFold:true as const} : {}),
    ...(database.ingressNativeSnapshot ? {ingressNativeSnapshot:true as const} : {}),
    ...(database.ingressNativeReceipt ? {ingressNativeReceipt:true as const} : {}),
    ...(database.ingressNativeHash ? {ingressNativeHash:true as const} : {}),
    execAsync:sql=>database.execAsync(sql),
    runAsync:(sql,...params)=>database.runAsync(sql,...params),
    getAllAsync:<T>(sql:string,...params:unknown[])=>database.getAllAsync<T>(sql,...params),
    getFirstAsync:<T>(sql:string,...params:unknown[])=>database.getFirstAsync<T>(sql,...params),
    async joinOwnerTransactionAsync(run){
      checkPoison();
      if(!depth)throw Object.assign(new Error('No active owner transaction to join'),{code:'FENCED'});
      try{await run();checkPoison();}catch(error){poison??={error};throw error;}
    },
    async withTransactionAsync(run){
      checkPoison();
      if(depth>=8)throw new RangeError('SQLite owner transaction nesting exceeds budget');
      if(!depth){
        const started=performance.now();try{await database.withTransactionAsync(async()=>{depth++;poison=undefined;try{await run();checkPoison();}finally{depth--;poison=undefined;}});}finally{transactionCount++;const durationMs=performance.now()-started;longestTransactionMs=Math.max(longestTransactionMs,durationMs);transactionTimings.push({finishedAt:Date.now(),durationMs});if(transactionTimings.length>64)transactionTimings.shift();}
        return;
      }
      const name='gv_owner_nested_'+(++serial);
      depth++;
      try{
        await database.execAsync('SAVEPOINT '+name);
        try{await run();await database.execAsync('RELEASE '+name);}
        catch(error){try{await database.execAsync('ROLLBACK TO '+name);await database.execAsync('RELEASE '+name);}catch(failure){poison={error:failure};}throw error;}
      }finally{depth--;}
    },
  };
}

/** Stage progress invalidates owner read leases and notifies mounted pages.
 * It is a local storage generation only: never a server/reducer watermark.
 * The caller joins the same fenced owner TX which durably writes the frames. */
export async function markSQLiteStagingCommit(database:SQLiteReplicaDatabase,fence:()=>Promise<{epoch:number;sequence:number}>){
 if(!database.ownerTransactionDepth)throw Object.assign(Error('Staging publication requires an owner transaction'),{code:'FENCED'});
 const current=await fence();if(!current||!Number.isSafeInteger(current.epoch)||current.epoch<0||!Number.isSafeInteger(current.sequence)||current.sequence<0)throw Object.assign(Error('Invalid owner generation'),{code:'FENCED'});
 const next=await database.getFirstAsync<{sequence:number}>('UPDATE gv_storage_meta SET sequence=sequence+1 WHERE id=1 AND epoch=? AND sequence=? AND sequence<9007199254740991 RETURNING sequence',current.epoch,current.sequence);
 if(!next)throw Object.assign(Error('Staging publication owner changed or sequence exhausted'),{code:'FENCED'});
 return {epoch:current.epoch,sequence:next.sequence};
}

/** Admission failed before the operation entered the SQL FIFO. This is the
 * only BUSY error that an internal feed may retry without recovery/replay. */
export class SQLiteOwnerQueueBusyError extends Error {
 readonly code='BUSY';
 constructor(){super('Storage owner queue is full');this.name='SQLiteOwnerQueueBusyError';}
}
/** Callers already own bounded ingress/intent slots. Wait outside the FIFO
 * rather than closing their socket when another connection uses its credits. */
export async function waitForSQLiteOwnerAdmission<T>(submit:()=>Promise<T>,assertCurrent:()=>void):Promise<T>{
 for(;;){assertCurrent();try{return await submit();}catch(error){if(!(error instanceof SQLiteOwnerQueueBusyError))throw error;await yieldSQLiteTask();}}
}
