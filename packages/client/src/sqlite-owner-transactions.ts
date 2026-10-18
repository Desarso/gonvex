import type { SQLiteReplicaDatabase } from './sqlite-replica-storage.js';

/** Internal owner-actor host. Only the actor's FIFO may call it: a nested
 * adapter transaction belongs to the already admitted operation. The official
 * host itself still rejects concurrent transactions. No transport exposes SQL. */
export function sqliteOwnerTransactions(database: SQLiteReplicaDatabase): SQLiteReplicaDatabase {
  let depth=0, serial=0, poison:{error:unknown}|undefined;
  const checkPoison = () => { if(poison)throw poison.error; };
  return {
    execAsync:sql=>database.execAsync(sql),
    runAsync:(sql,...params)=>database.runAsync(sql,...params),
    getAllAsync:<T>(sql:string,...params:unknown[])=>database.getAllAsync<T>(sql,...params),
    getFirstAsync:<T>(sql:string,...params:unknown[])=>database.getFirstAsync<T>(sql,...params),
    async withTransactionAsync(run){
      if(depth>=8)throw new RangeError('SQLite owner transaction nesting exceeds budget');
      if(!depth){
        await database.withTransactionAsync(async()=>{depth++;poison=undefined;try{await run();checkPoison();}finally{depth--;poison=undefined;}});
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
