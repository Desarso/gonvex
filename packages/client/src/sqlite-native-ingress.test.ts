import {DatabaseSync} from 'node:sqlite';
import {expect,it,vi} from 'vitest';
import {SQLiteNativeIngress} from './sqlite-native-ingress.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteSnapshotReceipt} from './sqlite-snapshot-pages.js';
function fixture(){
 const sql=new DatabaseSync(':memory:');sql.exec('CREATE TABLE frames(sequence INTEGER PRIMARY KEY);CREATE TABLE activated(id TEXT PRIMARY KEY)');
 let transaction=false,commits=0,head=Promise.resolve();const acks:unknown[]=[];
 const database=sqliteOwnerTransactions({
  async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},
  async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},
  async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},
  async withTransactionAsync(run){sql.exec('BEGIN');transaction=true;try{await run();sql.exec('COMMIT');commits++;}catch(error){sql.exec('ROLLBACK');throw error;}finally{transaction=false;}},
 } satisfies SQLiteReplicaDatabase);
 const options={database,scope:'s',runOwner<T>(run:()=>Promise<T>){const next=head.then(run);head=next.then(()=>{},()=>{});return next;},
  snapshots:{async appendEncodedReceipt(_scope:string,input:Uint8Array):Promise<SQLiteSnapshotReceipt>{
   const sequence=input[0]!;if(sequence===255)throw Error('malformed frame');
   await database.runAsync('INSERT OR IGNORE INTO frames VALUES(?)',sequence);
   return {ack:{type:'replica.snapshotAck',id:'source',snapshotId:'generation',sequence},progress:{snapshotId:'generation',nextPage:sequence+1,rows:sequence+1,complete:sequence===7}};
  }},
  sendAck(ack:SQLiteSnapshotReceipt['ack']){expect(transaction).toBe(false);expect(commits).toBeGreaterThan(0);acks.push(ack);},
  async activateVerified(){await database.runAsync("INSERT OR IGNORE INTO activated VALUES('published')");},
 };
 return {sql,options,acks,get commits(){return commits;},get transaction(){return transaction;}};
}
const wire=(sequence:number)=>Uint8Array.of(sequence).buffer;
it('coalesces all eight available credits into one atomic owner transaction and acknowledges only after commit',async()=>{
 const test=fixture(),ingress=new SQLiteNativeIngress(test.options),buffers=Array.from({length:8},(_,sequence)=>wire(sequence));
 try{
  const work=buffers.map(buffer=>ingress.accept(buffer));expect(ingress.stats().entries).toBe(8);
  const receipts=await Promise.all(work);expect(receipts.map(receipt=>receipt.ack.sequence)).toEqual([0,1,2,3,4,5,6,7]);
  expect(test.commits).toBe(1);expect(test.acks).toHaveLength(8);expect(test.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:8});
  expect(test.sql.prepare('SELECT count(*) n FROM activated').get()).toEqual({n:1});expect(ingress.stats()).toMatchObject({entries:0,bytes:0});
  if(typeof ArrayBuffer.prototype.transfer==='function')expect(buffers.every(buffer=>buffer.byteLength===0)).toBe(true);
 }finally{ingress.close();test.sql.close();}
});
it('rolls back the entire batch and returns no credit when any frame or activation fails',async()=>{
 for(const activationFailure of [false,true]){
  const test=fixture();if(activationFailure)test.options.activateVerified=async()=>{throw Error('activation failed');};
  const ingress=new SQLiteNativeIngress(test.options);
  try{
   const outcome=await Promise.allSettled([ingress.accept(wire(0)),ingress.accept(wire(activationFailure?7:255))]);
   expect(outcome.every(value=>value.status==='rejected')).toBe(true);expect(test.acks).toEqual([]);expect(test.commits).toBe(0);
   expect(test.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:0});expect(ingress.stats()).toMatchObject({entries:0,bytes:0});
   await ingress.accept(wire(0));expect(test.acks).toHaveLength(1);
  }finally{ingress.close();test.sql.close();}
 }
});
it('bounds active plus queued network buffers and rejects extra credit without losing admitted frames',async()=>{
 const test=fixture();let resume!:()=>void;const barrier=new Promise<void>(resolve=>{resume=resolve;});
 const append=test.options.snapshots.appendEncodedReceipt;
 test.options.snapshots.appendEncodedReceipt=async(...args)=>{await barrier;return append(...args);};
 const ingress=new SQLiteNativeIngress(test.options);
 try{
  const work=Array.from({length:8},(_,sequence)=>ingress.accept(wire(sequence)));
  await vi.waitFor(()=>expect(test.transaction).toBe(true));
  await expect(ingress.accept(wire(8))).rejects.toMatchObject({code:'BUDGET'});
  expect(ingress.stats()).toMatchObject({entries:8,refusals:1});resume();await Promise.all(work);
  expect(test.acks).toHaveLength(8);expect(ingress.stats().bytes).toBe(0);
 }finally{resume();ingress.close();test.sql.close();}
});
it('close releases waiting frames and suppresses acknowledgements for an already active commit',async()=>{
 const test=fixture();let resume!:()=>void;const barrier=new Promise<void>(resolve=>{resume=resolve;});
 const append=test.options.snapshots.appendEncodedReceipt;
 test.options.snapshots.appendEncodedReceipt=async(...args)=>{await barrier;return append(...args);};
 const ingress=new SQLiteNativeIngress(test.options);
 try{
  const active=ingress.accept(wire(0));const settled=Promise.allSettled([active]);
  await vi.waitFor(()=>expect(test.transaction).toBe(true));
  const queued=ingress.accept(wire(1));const queuedResult=Promise.allSettled([queued]);ingress.close();
  expect((await queuedResult)[0]).toMatchObject({status:'rejected',reason:{code:'FENCED'}});
  resume();expect((await settled)[0]).toMatchObject({status:'rejected',reason:{code:'FENCED'}});
  expect(test.acks).toEqual([]);expect(ingress.stats()).toMatchObject({entries:0,bytes:0});
  // The active transaction may have committed. Reconnect resolves that outcome
  // through the durable frame receipt rather than pretending close rolled back.
  expect(test.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:1});
 }finally{resume();ingress.close();test.sql.close();}
});
it('releases an unstarted queue on close and refuses oversize frames before admission',async()=>{
 const test=fixture(),ingress=new SQLiteNativeIngress(test.options);
 try{
  const pending=ingress.accept(wire(0));const result=Promise.allSettled([pending]);ingress.close();
  expect((await result)[0]).toMatchObject({status:'rejected',reason:{code:'FENCED'}});expect(test.commits).toBe(0);
  expect(ingress.stats().bytes).toBe(0);await expect(ingress.accept(wire(1))).rejects.toMatchObject({code:'FENCED'});
  const other=new SQLiteNativeIngress(test.options);
  try{await expect(other.accept(new ArrayBuffer(131073))).rejects.toMatchObject({code:'BUDGET'});expect(other.stats().bytes).toBe(0);}finally{other.close();}
 }finally{ingress.close();test.sql.close();}
});
