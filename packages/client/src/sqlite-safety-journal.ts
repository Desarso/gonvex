import type {ReducerOutboxEntry} from './outbox.js';
import type {LocalReplicaSession} from './local-replica.js';
import type {SQLiteReducerInvocation} from './sqlite-reducer-core.js';
import {decodedJSONBytes,fail,sqlitePayloadBytes} from './sqlite-admission.js';
type Receipt={scope:string;command:string;hash:string;fingerprint:string;sequence:number;settled:boolean;legacyHash?:string};
type Header={sequence:number;scope:string;command:string;bytes:number};
export type SQLiteSafetyEntry={invocation:SQLiteReducerInvocation;legacy?:ReducerOutboxEntry};
export type SQLiteSafetyPage={entries:SQLiteSafetyEntry[];nextSequence:number|null};
const request=<T>(value:IDBRequest<T>)=>new Promise<T>((resolve,reject)=>{value.onsuccess=()=>resolve(value.result);value.onerror=()=>reject(value.error??Error('Intent journal request failed'));});
function scope(value:string){if(typeof value!=='string'||!value||value.length>512)throw new TypeError('Invalid durable command scope');}
async function hash(value:string){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');}
function command(input:SQLiteReducerInvocation){
 sqlitePayloadBytes(input,131072);scope(input.scope);
 if(typeof input.commandId!=='string'||!input.commandId||input.commandId.length>256||typeof input.path!=='string'||!input.path||input.path.length>1024||!Number.isSafeInteger(input.now)||input.now<0||input.intentEntropy!==undefined&&(typeof input.intentEntropy!=='string'||input.intentEntropy.length>256))throw new TypeError('Invalid durable command');
 const value=JSON.stringify({scope:input.scope,path:input.path,commandId:input.commandId,args:input.args,now:input.now,...(input.intentEntropy===undefined?{}:{intentEntropy:input.intentEntropy})});
 return {value,bytes:decodedJSONBytes(value,131072)};
}
/** Independent write-ahead safety journal, separate from the SQLite engine.
 * It contains commands and authenticated session metadata only, never replica
 * rows. Accept here before committing SQLite predictions. Keep the command
 * until authoritative settlement, so an OPFS failure or interrupted migration
 * can recover every pending intent. No memory-only durability downgrade. */
export class SQLiteSafetyJournal {
 private opening?:Promise<IDBDatabase>;
 private closed=false;
 constructor(private readonly name='gonvex-intent-safety-v1',private readonly factory:IDBFactory=globalThis.indexedDB,private readonly ranges:typeof IDBKeyRange=globalThis.IDBKeyRange){}
 private open(){
  if(this.closed)return Promise.reject(Error('Intent safety journal is closed'));
  if(!this.opening)this.opening=new Promise<IDBDatabase>((resolve,reject)=>{
   if(!this.factory||!this.ranges){reject(Object.assign(Error('Durable intent storage is unavailable'),{code:'UNSUPPORTED'}));return;}
   let cancelled=false;const opening=this.factory.open(this.name,1);
   opening.onupgradeneeded=()=>{
    if(cancelled){opening.transaction!.abort();return;}
    const db=opening.result,headers=db.createObjectStore('headers',{keyPath:'sequence',autoIncrement:true});headers.createIndex('scope-sequence',['scope','sequence']);
    db.createObjectStore('bodies',{keyPath:'sequence'});db.createObjectStore('receipts',{keyPath:['scope','command']});db.createObjectStore('sessions',{keyPath:'scope'});
   };
   opening.onerror=()=>reject(opening.error??Error('Cannot open intent safety journal'));
   opening.onblocked=()=>{cancelled=true;reject(Object.assign(Error('Intent safety journal upgrade is blocked'),{code:'BUSY'}));};
   opening.onsuccess=()=>{const db=opening.result;if(this.closed||cancelled){db.close();reject(Error('Intent safety journal is closed'));return;}db.onversionchange=()=>{this.closed=true;db.close();};resolve(db);};
  }).catch(error=>{this.opening=undefined;throw error;});return this.opening;
 }
 private async transaction<T>(stores:string[],mode:IDBTransactionMode,run:(tx:IDBTransaction)=>Promise<T>):Promise<T>{
  const db=await this.open();if(this.closed)throw Error('Intent safety journal is closed');
  // Explicit strict durability when supported; browsers which do not implement
  // the option still use the same atomic native IndexedDB transaction.
  let tx:IDBTransaction;try{tx=db.transaction(stores,mode,{durability:mode==='readwrite'?'strict':'default'});}catch(error){if(!(error instanceof TypeError))throw error;tx=db.transaction(stores,mode);}
  const completion=new Promise<void>((resolve,reject)=>{tx.oncomplete=()=>resolve();tx.onabort=()=>reject(tx.error??Error('Intent safety transaction aborted'));tx.onerror=()=>{};});
  try{const result=await run(tx);await completion;return result;}catch(error){try{tx.abort();}catch{/* Already completed or aborted. */}await completion.catch(()=>{});throw error;}
 }
 append(input:SQLiteReducerInvocation,session:LocalReplicaSession,intentScope:string):Promise<Receipt>{return this.accept(input,session,intentScope);}
 /** Preserve the complete fenced legacy envelope, including its original
  * identity scope, retry state, stable IDs and execution metadata. */
 appendLegacy(input:SQLiteReducerInvocation,legacy:ReducerOutboxEntry,session:LocalReplicaSession,intentScope:string):Promise<Receipt>{
  sqlitePayloadBytes(legacy,131072);
  if(legacy.idempotencyKey!==input.commandId||legacy.path!==input.path||JSON.stringify(legacy.args)!==JSON.stringify(input.args)||typeof legacy.scope!=='string'||!legacy.scope||!Number.isSafeInteger(legacy.id)||legacy.id<1)throw new TypeError('Legacy intent differs from its recovery command');
  return this.accept(input,session,intentScope,JSON.stringify(legacy));
 }
 private async accept(input:SQLiteReducerInvocation,session:LocalReplicaSession,intentScope:string,legacyValue?:string):Promise<Receipt>{
  scope(intentScope);const encoded=command(input),owned=JSON.parse(encoded.value) as SQLiteReducerInvocation;
  const legacyBytes=legacyValue===undefined?0:decodedJSONBytes(legacyValue,131072),bytes=encoded.bytes+legacyBytes+512;
  if(bytes+64>262144)fail('BUDGET','Recovery envelope exceeds safety journal page budget');
  sqlitePayloadBytes(session,131072);const sessionValue=JSON.stringify(session);decodedJSONBytes(sessionValue,131072);
  if(!session?.directive||session.directive.protocolVersion!==1||!session.identity||typeof session.artifactHash!=='string')throw new TypeError('Durable command requires its authenticated session');
  // Hashing and snapshots happen before opening the IDB transaction. Awaiting
  // crypto inside that transaction could let the browser auto-commit it.
  const digest=await hash(encoded.value),fingerprint=await hash(JSON.stringify({...owned,scope:intentScope})),legacyHash=legacyValue===undefined?undefined:await hash(legacyValue);
  return this.transaction(['headers','bodies','receipts','sessions'],'readwrite',async tx=>{
   const receipts=tx.objectStore('receipts'),previous=await request(receipts.get([intentScope,owned.commandId])) as Receipt|undefined;
   if(previous){if(previous.fingerprint!==fingerprint||legacyHash!==undefined&&previous.legacyHash!==legacyHash)fail('INTENT_CONFLICT','Safety journal command reused with different content');return previous;}
   const sequence=Number(await request(tx.objectStore('headers').add({scope:intentScope,command:owned.commandId,bytes})));
   if(!Number.isSafeInteger(sequence)||sequence<1)fail('FENCED','Safety journal sequence exhausted');
   const receipt:Receipt={scope:intentScope,command:owned.commandId,hash:digest,fingerprint,sequence,settled:false,...(legacyHash===undefined?{}:{legacyHash})};
   await request(tx.objectStore('bodies').add({sequence,value:encoded.value,...(legacyValue===undefined?{}:{legacyValue})}));await request(receipts.add(receipt));await request(tx.objectStore('sessions').put({scope:intentScope,value:sessionValue}));return receipt;
  });
 }
 async page(ownerScope:string,after=0,limit=8):Promise<SQLiteSafetyPage>{
  scope(ownerScope);if(!Number.isSafeInteger(after)||after<0||!Number.isSafeInteger(limit)||limit<1||limit>32)throw new TypeError('Invalid safety journal page');
  if(after===Number.MAX_SAFE_INTEGER)return {entries:[],nextSequence:null};
  const result=await this.transaction(['headers','bodies','receipts'],'readonly',async tx=>{
   const index=tx.objectStore('headers').index('scope-sequence'),range=this.ranges.bound([ownerScope,after],[ownerScope,Number.MAX_SAFE_INTEGER],true,false);
   // Metadata first: never clone the next body before it fits the delivery cap.
   const headers:Header[]=await new Promise((resolve,reject)=>{
    const rows:Header[]=[];let bytes=64;const cursor=index.openCursor(range);cursor.onerror=()=>reject(cursor.error);
    cursor.onsuccess=()=>{const value=cursor.result;if(!value){resolve(rows);return;}const header=value.value as Header;
     if(!Number.isSafeInteger(header.bytes)||header.bytes<1||header.bytes>262080){reject(Error('Invalid safety journal body size'));return;}
     if(rows.length>=limit||bytes+header.bytes>262144){resolve(rows);return;}bytes+=header.bytes;rows.push(header);value.continue();};
   });
   const entries:SQLiteSafetyEntry[]=[],checks:Array<{value:string;hash:string}>=[];
   for(const header of headers){
    const body=await request(tx.objectStore('bodies').get(header.sequence)) as {value:string;legacyValue?:string}|undefined;if(!body)fail('FENCED','Safety journal body is missing');
    const size=decodedJSONBytes(body.value,131072)+(body.legacyValue===undefined?0:decodedJSONBytes(body.legacyValue,131072))+512;
    if(size!==header.bytes)fail('INTENT_CORRUPT','Safety journal body size differs');
    const invocation=JSON.parse(body.value) as SQLiteReducerInvocation;if(invocation.commandId!==header.command)fail('INTENT_CORRUPT','Safety journal identity differs');
    const receipt=await request(tx.objectStore('receipts').get([ownerScope,header.command])) as Receipt|undefined;if(!receipt||receipt.sequence!==header.sequence||receipt.settled)fail('INTENT_CORRUPT','Safety journal receipt differs');
    checks.push({value:body.value,hash:receipt.hash});
    let legacy:ReducerOutboxEntry|undefined;
    if(body.legacyValue!==undefined){if(!receipt.legacyHash)fail('INTENT_CORRUPT','Legacy recovery hash is missing');checks.push({value:body.legacyValue,hash:receipt.legacyHash});legacy=JSON.parse(body.legacyValue);if(legacy!.idempotencyKey!==invocation.commandId||legacy!.path!==invocation.path)fail('INTENT_CORRUPT','Legacy recovery identity differs');}
    else if(receipt.legacyHash)fail('INTENT_CORRUPT','Legacy recovery body is missing');
    entries.push({invocation,...(legacy===undefined?{}:{legacy})});
   }
   const last=headers.at(-1)?.sequence??after,more=last===Number.MAX_SAFE_INTEGER?undefined:await request(index.getKey(this.ranges.bound([ownerScope,last],[ownerScope,Number.MAX_SAFE_INTEGER],true,false)));
   sqlitePayloadBytes(entries,262144);return {entries,nextSequence:more===undefined?null:last,checks};
  });
  for(const check of result.checks)if(await hash(check.value)!==check.hash)fail('INTENT_CORRUPT','Safety journal command hash differs');
  return {entries:result.entries,nextSequence:result.nextSequence};
 }
 async saveSession(ownerScope:string,session:LocalReplicaSession){scope(ownerScope);sqlitePayloadBytes(session,131072);const value=JSON.stringify(session);decodedJSONBytes(value,131072);if(!session?.directive||session.directive.protocolVersion!==1||!session.identity||typeof session.artifactHash!=='string')throw new TypeError('Invalid durable session');await this.transaction(['sessions'],'readwrite',async tx=>{await request(tx.objectStore('sessions').put({scope:ownerScope,value}));});}
 async session(ownerScope:string):Promise<LocalReplicaSession|undefined>{scope(ownerScope);return this.transaction(['sessions'],'readonly',async tx=>{const row=await request(tx.objectStore('sessions').get(ownerScope)) as {value:string}|undefined;if(!row)return;decodedJSONBytes(row.value,131072);return JSON.parse(row.value);});}
 /** Called only on an authoritative accepted/rejected result, never merely
  * because SQLite has durably queued or predicted this command. */
 async settle(ownerScope:string,commandID:string){
  scope(ownerScope);if(typeof commandID!=='string'||!commandID||commandID.length>256)throw new TypeError('Invalid durable command ID');
  return this.transaction(['headers','bodies','receipts'],'readwrite',async tx=>{
   const store=tx.objectStore('receipts'),receipt=await request(store.get([ownerScope,commandID])) as Receipt|undefined;if(!receipt||receipt.settled)return false;
   await request(tx.objectStore('headers').delete(receipt.sequence));await request(tx.objectStore('bodies').delete(receipt.sequence));await request(store.put({...receipt,settled:true}));return true;
  });
 }
 async count(ownerScope:string){scope(ownerScope);return this.transaction(['headers'],'readonly',tx=>request(tx.objectStore('headers').index('scope-sequence').count(this.ranges.bound([ownerScope,0],[ownerScope,Number.MAX_SAFE_INTEGER]))));}
 close(){if(this.closed)return;this.closed=true;void this.opening?.then(db=>db.close(),()=>{});}
}
