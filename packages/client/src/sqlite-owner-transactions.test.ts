import {afterEach,expect,it} from 'vitest';
import {DatabaseSync} from 'node:sqlite';
import {sqliteOwnerTransactions,markSQLiteStagingCommit} from './sqlite-owner-transactions.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
const connections:DatabaseSync[]=[];
afterEach(()=>{for(const db of connections.splice(0))db.close();});
function fixture(failRollback=false){
  const db=new DatabaseSync(':memory:');connections.push(db);db.exec('CREATE TABLE records(id INTEGER PRIMARY KEY)');
  const raw:SQLiteReplicaDatabase={
    async execAsync(sql){if(failRollback&&sql.startsWith('ROLLBACK TO'))throw new Error('injected IO failure');db.exec(sql);},
    async runAsync(sql,...params){return db.prepare(sql).run(...params as any[]);},
    async getFirstAsync<T>(sql:string,...params:unknown[]){return db.prepare(sql).get(...params as any[]) as T??null;},
    async getAllAsync<T>(sql:string,...params:unknown[]){return db.prepare(sql).all(...params as any[]) as T[];},
    async withTransactionAsync(run){db.exec('BEGIN');try{await run();db.exec('COMMIT');}catch(error){db.exec('ROLLBACK');throw error;}},
  };return {db,database:sqliteOwnerTransactions(raw)};
}
it('rolls a caught nested failure back to its savepoint while retaining the outer operation',async()=>{
  const {db,database}=fixture();
  await database.withTransactionAsync(async()=>{
    await database.runAsync('INSERT INTO records VALUES(1)');
    await expect(database.withTransactionAsync(async()=>{await database.runAsync('INSERT INTO records VALUES(2)');throw Error('nested');})).rejects.toThrow('nested');
    await database.runAsync('INSERT INTO records VALUES(3)');
  });expect(db.prepare('SELECT id FROM records ORDER BY id').all()).toEqual([{id:1},{id:3}]);
});
it('poisons the outer transaction when nested rollback fails even if the caller catches it',async()=>{
  const {db,database}=fixture(true);
  await expect(database.withTransactionAsync(async()=>{
    await expect(database.withTransactionAsync(async()=>{await database.runAsync('INSERT INTO records VALUES(1)');throw Error('nested');})).rejects.toThrow('nested');
    await database.runAsync('INSERT INTO records VALUES(2)');
  })).rejects.toThrow('injected IO failure');
  expect(db.prepare('SELECT count(*) n FROM records').get()!.n).toBe(0);
});
it('bounds transaction nesting and permits a subsequent operation after rollback',async()=>{
  const {db,database}=fixture();
  const nested=(remaining:number):Promise<void>=>database.withTransactionAsync(async()=>{await database.runAsync('INSERT INTO records VALUES(?)',remaining);if(remaining)await nested(remaining-1);});
  await expect(nested(10)).rejects.toThrow('nesting');
  expect(db.prepare('SELECT count(*) n FROM records').get()!.n).toBe(0);
  await database.withTransactionAsync(()=>database.runAsync('INSERT INTO records VALUES(99)').then(()=>undefined));
  expect(db.prepare('SELECT id FROM records').get()!.id).toBe(99);
});
it('joins the actor batch without another savepoint and rolls back even a swallowed joined error',async()=>{
 const {db,database}=fixture();
 await expect(database.joinOwnerTransactionAsync!(async()=>{})).rejects.toMatchObject({code:'FENCED'});
 await expect(database.withTransactionAsync(async()=>{
  await database.runAsync('INSERT INTO records VALUES(1)');
  await expect(database.joinOwnerTransactionAsync!(async()=>{await database.runAsync('INSERT INTO records VALUES(2)');throw Error('joined failure');})).rejects.toThrow('joined failure');
 })).rejects.toThrow('joined failure');
 expect(db.prepare('SELECT count(*) n FROM records').get()!.n).toBe(0);expect(database.ownerTransactionDepth).toBe(0);
 await database.withTransactionAsync(async()=>{await database.joinOwnerTransactionAsync!(async()=>{expect(database.ownerTransactionDepth).toBe(1);await database.runAsync('INSERT INTO records VALUES(3)');});});
 expect(db.prepare('SELECT id FROM records').all()).toEqual([{id:3}]);
});

it('publishes staging generations only inside a fenced transaction and rolls them back with failed frames',async()=>{
 const f=fixture();await f.database.execAsync('CREATE TABLE gv_storage_meta(id INTEGER PRIMARY KEY,epoch INTEGER,sequence INTEGER);INSERT INTO gv_storage_meta VALUES(1,1,4)');
 const fence=async()=>({epoch:1,sequence:(await f.database.getFirstAsync<{sequence:number}>('SELECT sequence FROM gv_storage_meta'))!.sequence});
 await expect(markSQLiteStagingCommit(f.database,fence)).rejects.toMatchObject({code:'FENCED'});
 await expect(f.database.withTransactionAsync(async()=>{expect(await markSQLiteStagingCommit(f.database,fence)).toEqual({epoch:1,sequence:5});throw Error('frame failed');})).rejects.toThrow('frame failed');expect(await fence()).toEqual({epoch:1,sequence:4});
 await f.database.withTransactionAsync(async()=>{expect(await markSQLiteStagingCommit(f.database,fence)).toEqual({epoch:1,sequence:5});});
 await expect(f.database.withTransactionAsync(async()=>{await markSQLiteStagingCommit(f.database,async()=>({epoch:0,sequence:5}));})).rejects.toMatchObject({code:'FENCED'});expect(await fence()).toEqual({epoch:1,sequence:5});
 await f.database.runAsync('UPDATE gv_storage_meta SET sequence=9007199254740991');await expect(f.database.withTransactionAsync(async()=>{await markSQLiteStagingCommit(f.database,fence);})).rejects.toMatchObject({code:'FENCED'});
});
it('waits only for pre-execution owner admission and never retries an unknown SQLite outcome',async()=>{
 const {SQLiteOwnerQueueBusyError,waitForSQLiteOwnerAdmission}=await import('./sqlite-owner-transactions.js');let attempts=0,writes=0;
 expect(await waitForSQLiteOwnerAdmission(async()=>{if(attempts++<2)throw new SQLiteOwnerQueueBusyError();writes++;return 'committed';},()=>{})).toBe('committed');expect(attempts).toBe(3);expect(writes).toBe(1);
 attempts=0;await expect(waitForSQLiteOwnerAdmission(async()=>{attempts++;throw Object.assign(Error('late storage BUSY'),{code:'BUSY'});},()=>{})).rejects.toThrow('late storage BUSY');expect(attempts).toBe(1);
 let current=true;await expect(waitForSQLiteOwnerAdmission(async()=>{current=false;throw new SQLiteOwnerQueueBusyError();},()=>{if(!current)throw Object.assign(Error('admission changed'),{code:'FENCED'});})).rejects.toMatchObject({code:'FENCED'});
});
