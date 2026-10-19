import type {ReducerOutboxEntry,ReducerOutboxState,IntentDependencies} from './outbox.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import {decodedJSONBytes,fail,sqlitePayloadBytes,pageBudgetError} from './sqlite-admission.js';
type Draft=Omit<ReducerOutboxEntry,'id'>;
export type SQLiteIntentReceipt={id:number;idempotencyKey:string};
export type SQLiteIntentPage={entries:ReducerOutboxEntry[];nextId:number|null};
const states=['pending','inflight','committed','failed','rejected'] as const;
const schema=`CREATE TABLE IF NOT EXISTS gv_reducer_journal(
 id INTEGER PRIMARY KEY AUTOINCREMENT,scope TEXT NOT NULL,intent_key TEXT NOT NULL,
 state TEXT NOT NULL,next_attempt REAL NOT NULL,created REAL NOT NULL,global_barrier INTEGER NOT NULL,value TEXT NOT NULL,
 UNIQUE(scope,intent_key));
CREATE INDEX IF NOT EXISTS gv_reducer_journal_scope ON gv_reducer_journal(scope,id);
CREATE INDEX IF NOT EXISTS gv_reducer_journal_ready ON gv_reducer_journal(scope,state,next_attempt,id);
CREATE INDEX IF NOT EXISTS gv_reducer_journal_barrier ON gv_reducer_journal(scope,global_barrier,id);
CREATE TABLE IF NOT EXISTS gv_reducer_journal_receipts(scope TEXT NOT NULL,intent_key TEXT NOT NULL,hash TEXT NOT NULL,id INTEGER NOT NULL,PRIMARY KEY(scope,intent_key)) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS gv_reducer_journal_dependencies(scope TEXT NOT NULL,intent_id INTEGER NOT NULL,kind TEXT NOT NULL,entity TEXT NOT NULL,key TEXT NOT NULL,PRIMARY KEY(intent_id,kind,entity,key)) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS gv_reducer_dependency_key ON gv_reducer_journal_dependencies(scope,key,kind,intent_id);
CREATE INDEX IF NOT EXISTS gv_reducer_dependency_entity ON gv_reducer_journal_dependencies(scope,entity,kind,intent_id);`;
const active="p.state IN ('pending','inflight') AND p.scope=c.scope AND p.id<c.id";
// Every earlier active intent contributes dependencies, including an intent
// which is itself blocked or delayed. Committed/parked records do not block.
const ready=`c.state='pending' AND c.next_attempt<=? AND NOT EXISTS(
 SELECT 1 FROM gv_reducer_journal p INDEXED BY gv_reducer_journal_barrier WHERE ${active} AND p.global_barrier=1)
 AND (c.global_barrier=0 OR NOT EXISTS(SELECT 1 FROM gv_reducer_journal p WHERE ${active}))
 AND NOT EXISTS(SELECT 1 FROM gv_reducer_journal_dependencies a
 JOIN gv_reducer_journal_dependencies b ON b.scope=c.scope AND b.key=a.key
 JOIN gv_reducer_journal p ON p.id=b.intent_id
 WHERE a.intent_id=c.id AND ${active} AND (
  a.kind='r' AND b.kind IN ('w','l') OR a.kind='w' AND b.kind IN ('r','w','l') OR a.kind='l' AND b.kind IN ('r','w','l')))
 AND NOT EXISTS(SELECT 1 FROM gv_reducer_journal_dependencies a
 JOIN gv_reducer_journal_dependencies b ON b.scope=c.scope AND b.entity=a.entity
 JOIN gv_reducer_journal p ON p.id=b.intent_id
 WHERE a.intent_id=c.id AND ${active} AND (
  a.kind IN ('w','l') AND b.kind='s' OR a.kind='s' AND b.kind IN ('w','l')))`;
function scope(value:string){if(typeof value!=='string'||!value||value.length>512)throw new TypeError('Invalid intent scope');}
function id(value:number){if(!Number.isSafeInteger(value)||value<1)throw new TypeError('Invalid intent sequence');}
function state(value:string){if(!states.includes(value as ReducerOutboxState))throw new TypeError('Invalid intent state');}
function admit(draft:Draft){
 sqlitePayloadBytes(draft,131072);scope(draft.scope);state(draft.state);
 if(typeof draft.path!=='string'||!draft.path||draft.path.length>1024||typeof draft.idempotencyKey!=='string'||!draft.idempotencyKey||draft.idempotencyKey.length>256||!Number.isSafeInteger(draft.createdAt)||draft.createdAt<0||!Number.isSafeInteger(draft.nextAttemptAt)||draft.nextAttemptAt<0||!Number.isSafeInteger(draft.attempts)||draft.attempts<0||!Array.isArray(draft.entityKeys)||draft.entityKeys.length>4096||draft.entityKeys.some(key=>typeof key!=='string'||key.length>1280))throw new TypeError('Invalid durable intent');
 const deps=draft.dependencies;
 if(deps){
  if(!Array.isArray(deps.reads)||!Array.isArray(deps.writes)||!Array.isArray(deps.scans)||deps.reads.length+deps.writes.length+deps.scans.length>4096)throw new TypeError('Invalid intent dependencies');
  for(const row of [...deps.reads,...deps.writes])if(!row||typeof row.entity!=='string'||!row.entity||row.entity.length>256||typeof row.rowId!=='string'||!row.rowId||row.rowId.length>1024)throw new TypeError('Invalid intent row dependency');
  for(const table of deps.scans)if(typeof table!=='string'||!table||table.length>256)throw new TypeError('Invalid intent scan dependency');
 }
 const encoded=JSON.stringify(draft);decodedJSONBytes(encoded,131072);if(new TextEncoder().encode(encoded).byteLength>130000)fail('BUDGET','Intent wire body exceeds native bridge budget');return encoded;
}
async function hash(text:string){const bytes=new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text)));return Array.from(bytes,byte=>byte.toString(16).padStart(2,'0')).join('');}

/** Worker-owned durable intents, with indexed causal scheduling. There is no
 * corpus-loading method or resident entries map. The owner FIFO serializes all
 * calls. The composable host allows append/update plus predicted row changes
 * in the same outer transaction as a portable reducer execution. */
export class SQLiteIntentJournal {
 private initialized?:Promise<void>;
 constructor(private readonly db:SQLiteReplicaDatabase,private readonly fence:()=>Promise<unknown>){ }
 ready(){
  if(!this.initialized){if(this.db.ownerTransactionDepth)throw new Error('Prepare intent journal before an owner transaction');this.initialized=this.db.withTransactionAsync(async()=>{await this.fence();await this.db.execAsync(schema);}).catch(error=>{this.initialized=undefined;throw error;});}
  return this.initialized;
 }
 private async dependencies(entry:ReducerOutboxEntry){
  await this.db.runAsync('DELETE FROM gv_reducer_journal_dependencies WHERE intent_id=?',entry.id);
  const rows:Array<{kind:string;entity:string;key:string}>=[];
  const add=(kind:string,entity:string,key:string)=>rows.push({kind,entity,key});
  const deps:IntentDependencies|undefined=entry.dependencies;
  if(deps){for(const row of deps.reads)add('r',row.entity,row.entity+':'+row.rowId);for(const row of deps.writes)add('w',row.entity,row.entity+':'+row.rowId);for(const entity of deps.scans)add('s',entity,entity);}
  else for(const key of entry.entityKeys)add('l',key.split(':')[0]!,key);
  // Each JSON frame is bounded independently, even for a wide transaction.
  for(let start=0;start<rows.length;start+=128)await this.db.runAsync(`INSERT OR IGNORE INTO gv_reducer_journal_dependencies SELECT ?,?,json_extract(value,'$.kind'),json_extract(value,'$.entity'),json_extract(value,'$.key') FROM json_each(?)`,entry.scope,entry.id,JSON.stringify(rows.slice(start,start+128)));
 }
 async append(draft:Draft):Promise<SQLiteIntentReceipt>{
  const encoded=admit(draft),owned=JSON.parse(encoded) as Draft,digest=await hash(encoded);await this.ready();let receipt!:SQLiteIntentReceipt;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const old=await this.db.getFirstAsync<{id:number;hash:string}>('SELECT id,hash FROM gv_reducer_journal_receipts WHERE scope=? AND intent_key=?',owned.scope,owned.idempotencyKey);
   if(old){if(old.hash!==digest)fail('INTENT_CONFLICT','Durable intent key reused with different content');receipt={id:old.id,idempotencyKey:owned.idempotencyKey};return;}
   const result=await this.db.getFirstAsync<{id:number}>(`INSERT INTO gv_reducer_journal(scope,intent_key,state,next_attempt,created,global_barrier,value) VALUES(?,?,?,?,?,?,?) RETURNING id`,owned.scope,owned.idempotencyKey,owned.state,owned.nextAttemptAt,owned.createdAt,Number(owned.entityKeys.includes('__gonvex_local_intents')),encoded);
   if(!result)fail('FENCED','Intent allocation failed');id(result.id);const entry={...owned,id:result.id};admit(entry);
   await this.db.runAsync('UPDATE gv_reducer_journal SET value=? WHERE id=?',JSON.stringify(entry),entry.id);await this.dependencies(entry);
   await this.db.runAsync('INSERT INTO gv_reducer_journal_receipts VALUES(?,?,?,?)',entry.scope,entry.idempotencyKey,digest,entry.id);receipt={id:entry.id,idempotencyKey:entry.idempotencyKey};
  });return receipt;
 }
 private async select(query:string,args:unknown[]):Promise<ReducerOutboxEntry[]>{
  const candidates=await this.db.getAllAsync<{id:number;bytes:number}>(query,...args),entries:ReducerOutboxEntry[]=[];let bytes=64;
  for(const candidate of candidates){
   // The metadata query never returns an intent body. Stop before fetching the
   // next body when a page's conservative wire allowance is exhausted.
   if(bytes+candidate.bytes>262144){if(!entries.length)fail('BUDGET','Intent exceeds delivery page budget');break;}
   const row=await this.db.getFirstAsync<{value:string}>('SELECT value FROM gv_reducer_journal WHERE id=?',candidate.id);
   if(!row)fail('FENCED','Intent disappeared during owner read');let size:number;try{size=decodedJSONBytes(row.value,262144-bytes);}catch(error){if(entries.length&&pageBudgetError(error))break;throw error;}
   const entry=JSON.parse(row.value) as ReducerOutboxEntry;bytes+=size;entries.push(entry);
  }
  sqlitePayloadBytes(entries,262144);return entries;
 }
 private limits(limit:number){if(!Number.isSafeInteger(limit)||limit<1||limit>32)throw new RangeError('Intent page limit must be 1..32');}
 /** Legacy cutover rows lack invocation receipts until an admitted owner can
  * bind their original command to the current visibility scope. Metadata is
  * selected before bounded bodies; native receipts skip this work on reload. */
 async discardUnsent(ownerScope:string):Promise<number>{
  scope(ownerScope);await this.ready();let removed=0;
  await this.db.withTransactionAsync(async()=>{await this.fence();
   removed=(await this.db.getFirstAsync<{n:number}>("SELECT count(*) n FROM gv_reducer_journal WHERE scope=? AND state IN ('pending','failed','rejected')",ownerScope))?.n??0;
   await this.db.runAsync("DELETE FROM gv_reducer_journal_dependencies WHERE scope=? AND intent_id IN (SELECT id FROM gv_reducer_journal WHERE scope=? AND state IN ('pending','failed','rejected'))",ownerScope,ownerScope);
   await this.db.runAsync("DELETE FROM gv_reducer_journal WHERE scope=? AND state IN ('pending','failed','rejected')",ownerScope);
  });return removed;
 }
 async unrecovered(ownerScope:string):Promise<ReducerOutboxEntry[]>{
  scope(ownerScope);await this.ready();return this.select(`SELECT j.id,length(CAST(j.value AS BLOB))*2+128 bytes FROM gv_reducer_journal j WHERE j.scope=? AND NOT EXISTS(SELECT 1 FROM gv_reducer_invocations i WHERE i.scope=j.scope AND i.command_id=j.intent_key) ORDER BY j.id LIMIT 8`,[ownerScope]);
 }
 async page(ownerScope:string,after=0,limit=16):Promise<SQLiteIntentPage>{
  scope(ownerScope);this.limits(limit);if(!Number.isSafeInteger(after)||after<0)throw new TypeError('Invalid intent page cursor');await this.ready();let result!:SQLiteIntentPage;
  await this.db.withTransactionAsync(async()=>{await this.fence();const entries=await this.select('SELECT id,length(CAST(value AS BLOB))*2+128 bytes FROM gv_reducer_journal WHERE scope=? AND id>? ORDER BY id LIMIT ?',[ownerScope,after,limit]);
   const last=entries.at(-1)?.id??after,more=await this.db.getFirstAsync('SELECT 1 FROM gv_reducer_journal WHERE scope=? AND id>? LIMIT 1',ownerScope,last);result={entries,nextId:more?last:null};});return result;
 }
 async find(ownerScope:string,command:string):Promise<ReducerOutboxEntry|undefined>{
  scope(ownerScope);if(typeof command!=='string'||!command||command.length>256)throw new TypeError('Invalid intent command');await this.ready();let entry:ReducerOutboxEntry|undefined;
  await this.db.withTransactionAsync(async()=>{await this.fence();entry=(await this.select('SELECT id,length(CAST(value AS BLOB))*2+128 bytes FROM gv_reducer_journal WHERE scope=? AND intent_key=?',[ownerScope,command]))[0];});return entry;
 }
 async readyBatch(ownerScope:string,now:number,limit=8):Promise<ReducerOutboxEntry[]>{
  scope(ownerScope);this.limits(limit);if(!Number.isSafeInteger(now)||now<0)throw new TypeError('Invalid intent scheduling time');await this.ready();let result!:ReducerOutboxEntry[];
  await this.db.withTransactionAsync(async()=>{await this.fence();result=await this.select(`SELECT c.id,length(CAST(c.value AS BLOB))*2+128 bytes FROM gv_reducer_journal c INDEXED BY gv_reducer_journal_scope WHERE c.scope=? AND ${ready} ORDER BY c.id LIMIT ?`,[ownerScope,now,limit]);});return result;
 }
 /** Claim and persist the delivery fence before the socket sees any intent.
  * Recovery may resend the same immutable key after an unknown outcome. */
 async claim(ownerScope:string,now:number,limit=8):Promise<ReducerOutboxEntry[]>{
  scope(ownerScope);this.limits(limit);if(!Number.isSafeInteger(now)||now<0)throw new TypeError('Invalid intent scheduling time');await this.ready();let claimed!:ReducerOutboxEntry[];
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const entries=await this.readyBatch(ownerScope,now,limit);claimed=[];
   for(const entry of entries){const next:ReducerOutboxEntry={...entry,state:'inflight',sentAt:entry.sentAt??now};const encoded=admit(next);await this.db.runAsync("UPDATE gv_reducer_journal SET state='inflight',value=? WHERE scope=? AND id=?",encoded,ownerScope,entry.id);claimed.push(next);}
  });return claimed;
 }
 async update(ownerScope:string,sequence:number,expected:readonly ReducerOutboxState[],change:(entry:ReducerOutboxEntry)=>ReducerOutboxEntry):Promise<ReducerOutboxEntry|undefined>{
  scope(ownerScope);id(sequence);if(!Array.isArray(expected)||!expected.length||expected.length>5)throw new TypeError('Intent transition requires expected states');for(const value of expected)state(value);await this.ready();let result:ReducerOutboxEntry|undefined;
  await this.db.withTransactionAsync(async()=>{
   await this.fence();const rows=await this.select('SELECT id,length(CAST(value AS BLOB))*2+128 bytes FROM gv_reducer_journal WHERE scope=? AND id=?',[ownerScope,sequence]);const before=rows[0];if(!before||!expected.includes(before.state))return;
   const identity=JSON.stringify([before.id,before.scope,before.idempotencyKey,before.path,before.createdAt,before.args]);
   const next=change(before),encoded=admit(next);
   if(JSON.stringify([next.id,next.scope,next.idempotencyKey,next.path,next.createdAt,next.args])!==identity)fail('INTENT_CONFLICT','Intent transition changed immutable identity');
   await this.db.runAsync('UPDATE gv_reducer_journal SET state=?,next_attempt=?,global_barrier=?,value=? WHERE id=? AND scope=?',next.state,next.nextAttemptAt,Number(next.entityKeys.includes('__gonvex_local_intents')),encoded,sequence,ownerScope);await this.dependencies(next);result=next;
  });return result;
 }
 async recoverInflight(ownerScope:string){scope(ownerScope);await this.ready();await this.db.withTransactionAsync(async()=>{await this.fence();await this.db.runAsync(`UPDATE gv_reducer_journal SET state='pending',value=json_set(value,'$.state','pending') WHERE scope=? AND state='inflight'`,ownerScope);});}
 async acknowledge(ownerScope:string,sequences:readonly number[]){
  scope(ownerScope);if(!Array.isArray(sequences)||sequences.length>128)throw new RangeError('Intent acknowledgement exceeds budget');for(const sequence of sequences)id(sequence);await this.ready();if(!sequences.length)return;
  await this.db.withTransactionAsync(async()=>{await this.fence();const keys=JSON.stringify(sequences);await this.db.runAsync('DELETE FROM gv_reducer_journal_dependencies WHERE scope=? AND intent_id IN (SELECT value FROM json_each(?))',ownerScope,keys);await this.db.runAsync('DELETE FROM gv_reducer_journal WHERE scope=? AND id IN (SELECT value FROM json_each(?))',ownerScope,keys);});
 }
 async summary(ownerScope:string){scope(ownerScope);await this.ready();let result!:{count:number;queued:number;failed:number;rejected:number;oldest:number|null};await this.db.withTransactionAsync(async()=>{await this.fence();result=(await this.db.getFirstAsync<typeof result>(`SELECT count(*) count,coalesce(sum(state IN ('pending','inflight','committed')),0) queued,coalesce(sum(state='failed'),0) failed,coalesce(sum(state='rejected'),0) rejected,min(created) oldest FROM gv_reducer_journal WHERE scope=?`,ownerScope))!;});return result;}
}
