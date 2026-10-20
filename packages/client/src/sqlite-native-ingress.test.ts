import {DatabaseSync} from 'node:sqlite';
import {expect,it,vi} from 'vitest';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
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
  expect(buffers.every(buffer=>buffer.byteLength===1)).toBe(true);for(const buffer of buffers)new Uint8Array(buffer).fill(255);
  const receipts=await Promise.all(work);expect(receipts.map(receipt=>receipt.ack.sequence)).toEqual([0,1,2,3,4,5,6,7]);
  expect(test.commits).toBe(1);expect(test.acks).toHaveLength(8);expect(test.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:8});
  expect(test.sql.prepare('SELECT count(*) n FROM activated').get()).toEqual({n:1});expect(ingress.stats()).toMatchObject({entries:0,bytes:0});
  expect(buffers.every(buffer=>buffer.byteLength===1)).toBe(true);
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

it('drops settled queue references while retaining external-memory accounting for the owned backing store',async()=>{
 const test=fixture(),append=test.options.snapshots.appendEncodedReceipt,inputs:Uint8Array[]=[];
 test.options.snapshots.appendEncodedReceipt=async(...args)=>{inputs.push(args[1]);return append(...args);};const ingress=new SQLiteNativeIngress(test.options),source=wire(0);
 try{await ingress.accept(source);expect(source.byteLength).toBe(1);expect(inputs[0].buffer).not.toBe(source);expect(inputs[0].byteLength).toBe(1);expect(ingress.stats()).toMatchObject({entries:0,bytes:0,releasedBytes:1});}finally{ingress.close();test.sql.close();}
});

it('publishes staging progress atomically before credit and rolls back the entire batch if publication fails',async()=>{
 for(const fails of [false,true]){
  const f=fixture();f.sql.exec('CREATE TABLE progress(n INTEGER)');let calls=0;
  const ingress=new SQLiteNativeIngress({...f.options,async commitProgress(){expect(f.transaction).toBe(true);expect(f.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:2});calls++;await f.options.database.runAsync('INSERT INTO progress VALUES(1)');if(fails)throw Error('progress persistence failed');}});
  try{
   const results=await Promise.allSettled([ingress.accept(wire(0)),ingress.accept(wire(1))]);expect(calls).toBe(1);expect(results.every(result=>result.status===(fails?'rejected':'fulfilled'))).toBe(true);expect(f.acks).toHaveLength(fails?0:2);expect(f.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:fails?0:2});expect(f.sql.prepare('SELECT count(*) n FROM progress').get()).toEqual({n:fails?0:1});
  }finally{ingress.close();f.sql.close();}
 }
});

it('preserves wire control barriers without deadlocking coalesced binary frames or the SQL FIFO',async()=>{
 const f=fixture(),order:Array<string|number>=[],ingress=new SQLiteNativeIngress({...f.options,async commitProgress(){order.push((f.sql.prepare('SELECT count(*) n FROM frames').get() as {n:number}).n);}});
 try{
  const first=ingress.accept(wire(0)),control=ingress.flush().then(()=>{order.push('control');});
  const second=ingress.accept(wire(1),control),terminal=ingress.flush().then(()=>{order.push('terminal');});
  await Promise.all([first,second,terminal]);expect(order).toEqual([1,'control',2,'terminal']);expect(f.commits).toBe(2);expect(f.acks).toHaveLength(2);expect(ingress.stats()).toMatchObject({entries:0,bytes:0});
 }finally{ingress.close();f.sql.close();}
});
it('bounds frames waiting outside the SQL FIFO and cancels a closed control gate without touching disk',async()=>{
 const f=fixture();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;}),ingress=new SQLiteNativeIngress(f.options);
 try{
  const frames=Array.from({length:8},(_,n)=>ingress.accept(wire(n),gate)),waiting=Promise.allSettled([...frames,ingress.flush()]);
  await expect(ingress.accept(wire(9),gate)).rejects.toMatchObject({code:'BUDGET'});await new Promise(resolve=>setTimeout(resolve,10));expect(f.commits).toBe(0);expect(ingress.stats().entries).toBe(8);ingress.close();expect((await waiting).every(result=>result.status==='rejected')).toBe(true);expect(ingress.stats()).toMatchObject({entries:0,bytes:0});release();await new Promise(resolve=>setTimeout(resolve,10));expect(f.commits).toBe(0);expect(f.acks).toEqual([]);
 }finally{ingress.close();release();f.sql.close();}
});

it('shares ingress admission across connections and releases reservations on cancellation and rollback',async()=>{
 const test=fixture(),budget=new SQLiteTransientBudget(514,2);let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;}),first=new SQLiteNativeIngress({...test.options,reserve:bytes=>budget.reserve(bytes)}),second=new SQLiteNativeIngress({...test.options,reserve:bytes=>budget.reserve(bytes)});
 try{
  const a=first.accept(wire(0),gate),b=second.accept(wire(1),gate);void a.catch(()=>{});void b.catch(()=>{});expect(budget.stats()).toMatchObject({bytes:514,entries:2});await expect(second.accept(wire(2),gate)).rejects.toMatchObject({code:'BUDGET'});expect(budget.stats().refusals).toBe(1);
  first.close();second.close();release();await Promise.allSettled([a,b]);expect(budget.stats()).toMatchObject({bytes:0,entries:0});expect(test.acks).toEqual([]);
  const third=new SQLiteNativeIngress({...test.options,reserve:bytes=>budget.reserve(bytes)});try{await expect(third.accept(wire(255))).rejects.toThrow('malformed frame');expect(budget.stats()).toMatchObject({bytes:0,entries:0});}finally{third.close();}
 }finally{first.close();second.close();test.sql.close();}
});

it('coalesces separately prepared sources after all their gates resolve, without returning credit before commit',async()=>{
 const f=fixture(),ingress=new SQLiteNativeIngress(f.options);
 try{
  const work=Array.from({length:8},(_,n)=>ingress.accept(wire(n),Promise.resolve()));
  await Promise.all(work);expect(f.commits).toBe(1);expect(f.acks).toHaveLength(8);
  expect(f.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:8});
 }finally{ingress.close();f.sql.close();}
});

it('keeps an unresolved or rejected later source gate behind already admitted durable frames',async()=>{
 for(const rejected of [false,true]){
  const f=fixture(),ingress=new SQLiteNativeIngress(f.options);let release!:()=>void;
  const gate=new Promise<void>((yes,no)=>{release=()=>rejected?no(Error('later source refused')):yes();});
  try{
   const first=ingress.accept(wire(0),Promise.resolve()),later=ingress.accept(wire(1),gate),settled=Promise.allSettled([later]);
   await first;expect(f.acks).toHaveLength(1);expect(f.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:1});
   release();expect((await settled)[0].status).toBe(rejected?'rejected':'fulfilled');
   expect(f.acks).toHaveLength(rejected?1:2);expect(f.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:rejected?1:2});
  }finally{release();ingress.close();f.sql.close();}
 }
});

it('keeps precommit staging separate from durable acceptance while FULL commit is blocked or fails',async()=>{
 for(const failCommit of [false,true]){
  const f=fixture(),transaction=f.options.database.withTransactionAsync.bind(f.options.database);
  let release!:()=>void,entered!:()=>void;const commitGate=new Promise<void>(resolve=>{release=resolve;}),staged=new Promise<void>(resolve=>{entered=resolve;});
  f.options.database.withTransactionAsync=run=>transaction(async()=>{await run();entered();await commitGate;if(failCommit)throw Error('FULL sync failed');});
  const ingress=new SQLiteNativeIngress(f.options);let accepted=false;
  try{
   const result=ingress.accept(wire(0)).then(()=>{accepted=true;return 'committed';},()=> 'rejected');
   await staged;expect(f.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:1});
   expect(accepted).toBe(false);expect(f.acks).toEqual([]);expect(f.commits).toBe(0);
   release();expect(await result).toBe(failCommit?'rejected':'committed');
   expect(f.acks).toHaveLength(failCommit?0:1);expect(f.sql.prepare('SELECT count(*) n FROM frames').get()).toEqual({n:failCommit?0:1});
  }finally{release();ingress.close();f.sql.close();}
 }
});

it('revokes uncommitted staging display on durable commit and rollback without granting early credit',async()=>{
 for(const failCommit of [false,true]){
  const f=fixture(),tx=f.options.database.withTransactionAsync.bind(f.options.database);
  let release!:()=>void,entered!:()=>void,visible=false,revoked=0;
  const gate=new Promise<void>(resolve=>{release=resolve;}),staged=new Promise<void>(resolve=>{entered=resolve;});
  f.options.database.withTransactionAsync=run=>tx(async()=>{await run();entered();await gate;if(failCommit)throw Error('sync failure');});
  const ingress=new SQLiteNativeIngress({...f.options,async previewStaging(receipt){
   expect(f.transaction).toBe(true);expect(receipt.progress.complete).toBe(false);expect(f.acks).toEqual([]);
   visible=true;return ()=>{visible=false;revoked++;};
  }});
  try{
   const done=ingress.accept(wire(0)).then(()=>true,()=>false);await staged;
   expect(visible).toBe(true);expect(f.acks).toEqual([]);expect(f.commits).toBe(0);
   release();expect(await done).toBe(!failCommit);expect(visible).toBe(false);expect(revoked).toBe(1);
   expect(f.acks).toHaveLength(failCommit?0:1);
  }finally{release();ingress.close();f.sql.close();}
 }
});

it('revokes a staging display immediately on close even while the durable commit is blocked',async()=>{
 const f=fixture(),tx=f.options.database.withTransactionAsync.bind(f.options.database);
 let release!:()=>void,entered!:()=>void,visible=false,revoked=0;
 const gate=new Promise<void>(resolve=>{release=resolve;}),staged=new Promise<void>(resolve=>{entered=resolve;});
 f.options.database.withTransactionAsync=run=>tx(async()=>{await run();entered();await gate;});
 const ingress=new SQLiteNativeIngress({...f.options,async previewStaging(){visible=true;return ()=>{visible=false;revoked++;throw Error('display cleanup failure');};}});
 try{
  const done=Promise.allSettled([ingress.accept(wire(0))]);await staged;expect(visible).toBe(true);
  ingress.close();expect(visible).toBe(false);expect(revoked).toBe(1);expect(f.acks).toEqual([]);
  release();expect((await done)[0].status).toBe('rejected');expect(revoked).toBe(1);expect(f.acks).toEqual([]);
 }finally{release();ingress.close();f.sql.close();}
});
