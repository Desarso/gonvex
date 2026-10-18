import {afterEach,expect,it} from 'vitest';
import {DatabaseSync} from 'node:sqlite';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
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
