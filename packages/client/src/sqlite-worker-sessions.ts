import type {LocalReplicaSession} from './local-replica.js';
import type {SQLiteTransportSession} from './sqlite-transport-host.js';
import type {SQLiteLocalReplicaStorage,SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteSessionJournal} from './sqlite-session-journal.js';
import {ByteLedger} from './byte-budget.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';

type Phase='unavailable'|'authenticating'|'online'|'offline'|'quarantined';
type Connection={id:string;intentScope:string;transportGeneration:number;revision:number;phase:Phase;session?:LocalReplicaSession;bytes:number};
export type WorkerSessionStorage={loadSession(scope:string):Promise<LocalReplicaSession|undefined>;saveSession(scope:string,session:LocalReplicaSession|undefined,options:{current():boolean;ifAbsent?:boolean}):Promise<void>};
type Options={
 artifactHash:string;offlineMaxAgeMs:number|null;
 safety:Pick<SQLiteSessionJournal,'session'|'sessionRevoked'|'saveSession'|'revokeSession'>;
 runOwner<T>(bytes:number,run:()=>Promise<T>):Promise<T>;
} & ({storage:WorkerSessionStorage}|{database:SQLiteReplicaDatabase;canonical:Pick<SQLiteLocalReplicaStorage,'loadSession'|'saveSession'>;fence():Promise<unknown>});
const maxConnections=8,maxTabs=64,maxSessionBytes=32768,maxBytes=maxConnections*(maxSessionBytes+2048)+maxTabs*512;
const clone=<T>(value:T):T=>JSON.parse(JSON.stringify(value));
function key(value:string,max:number){if(typeof value!=='string'||!value||value.length>max)throw new TypeError('Invalid worker session identity');}

/** Trusted owner-only session registry. Application RPCs must never call its
 * admission methods or supply a visibility scope. A tab maps to an owner
 * connection; the correlated transport supplies identity and visibility.
 * Network waits stay outside the SQL FIFO. */
export class SQLiteWorkerSessions {
 private readonly connections=new Map<string,Connection>();
 private readonly tabs=new Map<string,string>();
 private readonly ledger=new ByteLedger(maxBytes+2097152+64*128);
 private readonly storage:WorkerSessionStorage;
 private nextRevision=0;
 private refusals=0;
 private durability=Promise.resolve();
 private pendingOperations=0;
 private pendingBytes=0;
 constructor(private readonly options:Options){
  this.storage='storage'in options?options.storage:{loadSession:scope=>options.runOwner(0,()=>options.canonical.loadSession(scope)),saveSession:(scope,session,guard)=>options.runOwner(sqlitePayloadBytes(session??{},32768),()=>options.database.withTransactionAsync(async()=>{await options.fence();if(!guard.current())fail('FENCED','Worker session admission changed');if(guard.ifAbsent&&await options.canonical.loadSession(scope))fail('FENCED','A newer native session won cached migration');await options.canonical.saveSession(scope,session);if(!guard.current())fail('FENCED','Worker session admission changed');}))};
  key(options.artifactHash,256);
  if(options.offlineMaxAgeMs!==null&&(!Number.isSafeInteger(options.offlineMaxAgeMs)||options.offlineMaxAgeMs<0))throw new TypeError('Invalid offline editing window');
  this.ledger.register('worker-sessions',()=>this.stats());
  this.ledger.register('session-durability',()=>({name:'session-durability',bytes:this.pendingBytes+this.pendingOperations*128,maxBytes:2097152+64*128,entries:this.pendingOperations,maxEntries:64,evictions:0,refusals:this.refusals}));
 }
 stats(){let bytes=this.tabs.size*512;for(const record of this.connections.values())bytes+=record.bytes;return {name:'worker-sessions',bytes,maxBytes,entries:this.connections.size+this.tabs.size,maxEntries:maxConnections+maxTabs,evictions:0,refusals:this.refusals};}
 private persist<T>(bytes:number,run:()=>Promise<T>):Promise<T>{
  if(this.pendingOperations>=64||this.pendingBytes+bytes>2097152){this.refusals++;fail('BUDGET','Session durability queue is full');}
  this.pendingOperations++;this.pendingBytes+=bytes;const result=this.durability.then(run);
  this.durability=result.then(()=>{},()=>{});void result.then(()=>{this.pendingOperations--;this.pendingBytes-=bytes;},()=>{this.pendingOperations--;this.pendingBytes-=bytes;});return result;
 }
 budgets(){return this.ledger.snapshot();}
 private revision(){if(this.nextRevision===Number.MAX_SAFE_INTEGER)fail('FENCED','Session generation exhausted');return ++this.nextRevision;}
 private connection(id:string){const record=this.connections.get(id);if(!record)fail('FENCED','Worker connection is unavailable');return record;}
 private validate(session:LocalReplicaSession){
  const bytes=sqlitePayloadBytes(session,maxSessionBytes);
  const directive=session.directive,identity=session.identity;
  if(!directive||directive.protocolVersion!==1||typeof directive.scope!=='string'||!directive.scope||directive.scope.length>512||typeof directive.visibilityScope!=='string'||!directive.visibilityScope||directive.visibilityScope.length>512||directive.visibilityScope.includes('\u0000')||typeof directive.epoch!=='string'||!directive.epoch||directive.epoch.length>256||!identity?.auth?.account?.id||!identity.tenant?.id||!identity.member?.id||session.lastOnlineAtMs!==undefined&&(!Number.isSafeInteger(session.lastOnlineAtMs)||session.lastOnlineAtMs<0))throw new TypeError('Invalid authenticated worker session');
  if(session.artifactHash!==this.options.artifactHash)fail('SCHEMA_MISMATCH','Cached session requires a different client artifact');
  return bytes;
 }
 open(id:string){key(id,128);if(this.connections.has(id))throw new TypeError('Worker connection already exists');if(this.connections.size===maxConnections){this.refusals++;fail('BUDGET','Worker connection allowance reached');}this.connections.set(id,{id,intentScope:'',transportGeneration:0,revision:this.revision(),phase:'unavailable',bytes:2048});}
 attachTab(tab:string,connection:string){key(tab,128);this.connection(connection);if(!this.tabs.has(tab)&&this.tabs.size===maxTabs){this.refusals++;fail('BUDGET','Worker tab allowance reached');}this.tabs.set(tab,connection);}
 detachTab(tab:string){this.tabs.delete(tab);}
 close(id:string){const record=this.connection(id);record.revision=this.revision();record.phase='unavailable';record.session=undefined;this.connections.delete(id);for(const [tab,connection]of this.tabs)if(connection===id)this.tabs.delete(tab);}
 authenticating(id:string,intentScope:string,generation:number){
  key(intentScope,512);if(!Number.isSafeInteger(generation)||generation<1)throw new TypeError('Invalid transport generation');
  const record=this.connection(id);if(generation<record.transportGeneration)fail('FENCED','Obsolete transport generation');
  if(record.intentScope!==intentScope){record.session=undefined;record.bytes=2048;}
  record.intentScope=intentScope;record.transportGeneration=generation;record.revision=this.revision();record.phase='authenticating';
 }
 disconnected(id:string){const record=this.connection(id);record.revision=this.revision();record.phase=record.session?'offline':'unavailable';}
 private current(record:Connection,revision:number,isCurrent:()=>boolean){return this.connections.get(record.id)===record&&record.revision===revision&&isCurrent();}
 private check(record:Connection,revision:number,isCurrent:()=>boolean){if(!this.current(record,revision,isCurrent))fail('FENCED','Worker session admission changed');}
 async restore(id:string,isCurrent:()=>boolean=()=>true){
  const record=this.connection(id),revision=record.revision,scope=record.intentScope;key(scope,512);
  if(record.phase==='quarantined')return false;if(record.phase==='online')return true;
  await this.durability;this.check(record,revision,isCurrent);
  if(await this.options.safety.sessionRevoked(scope)){this.check(record,revision,isCurrent);record.session=undefined;record.phase='quarantined';record.bytes=2048;return false;}
  const saved=await this.options.safety.session(scope)??await this.storage.loadSession(scope);
  this.check(record,revision,isCurrent);if(!saved)return false;
  const session=clone(saved),bytes=this.validate(session);
  // A concurrent definitive denial must win even when fallback loading waited.
  if(await this.options.safety.sessionRevoked(scope))return false;
  this.check(record,revision,isCurrent);record.session=session;record.bytes=2048+bytes;
  // During token refresh reads use the cached admission, but native frames
  // remain quarantined until activate publishes a durable online session.
  if(record.phase!=='authenticating')record.phase='offline';return true;
 }
 /** Seed a fenced legacy cache only before this identity has any native
  * session. The shared durability lane orders this against authoritative auth,
  * and a revocation marker can never be revived by an old database. */
 async importCached(id:string,input:LocalReplicaSession,isCurrent:()=>boolean=()=>true):Promise<boolean>{
  const session=clone(input),bytes=this.validate(session),record=this.connection(id),revision=record.revision,intentScope=record.intentScope;key(intentScope,512);
  if(record.phase!=='authenticating'||record.session)return false;
  return this.persist(bytes,async()=>{
   this.check(record,revision,isCurrent);if(record.phase!=='authenticating'||record.session)return false;
   if(await this.options.safety.sessionRevoked(intentScope))return false;this.check(record,revision,isCurrent);
   const saved=await this.options.safety.session(intentScope)??await this.storage.loadSession(intentScope);this.check(record,revision,isCurrent);if(saved)return false;
   const current=()=>this.current(record,revision,isCurrent)&&record.phase==='authenticating';
   await this.options.safety.saveSession(intentScope,session,false,current);this.check(record,revision,isCurrent);
   await this.storage.saveSession(intentScope,session,{current,ifAbsent:true});return true;
  });
 }
 async activate(id:string,input:SQLiteTransportSession,isCurrent:()=>boolean){
  sqlitePayloadBytes(input,65536);if(!Number.isSafeInteger(input.lastOnlineAtMs)||input.lastOnlineAtMs<0)throw new TypeError('Invalid authenticated session time');const record=this.connection(id),revision=record.revision;
  if(record.phase!=='authenticating'||input.connectionGeneration!==record.transportGeneration||input.intentScope!==record.intentScope)fail('FENCED','Obsolete worker session result');
  this.check(record,revision,isCurrent);
  if(!input.directive||!input.identity){record.session=undefined;record.phase='unavailable';record.bytes=2048;return false;}
  const session:LocalReplicaSession=clone({directive:input.directive,identity:input.identity,artifactHash:input.artifactHash,lastOnlineAtMs:input.lastOnlineAtMs}),bytes=this.validate(session);
  const current=()=>this.current(record,revision,isCurrent);
  return this.persist(bytes,async()=>{
  this.check(record,revision,isCurrent);
  await this.options.safety.saveSession(record.intentScope,session,true,current);this.check(record,revision,isCurrent);
  await this.storage.saveSession(record.intentScope,session,{current});
  this.check(record,revision,isCurrent);record.session=session;record.bytes=2048+bytes;record.phase='online';record.revision=this.revision();return true;
  });
 }
 /** Called synchronously at definitive transport denial. All tabs sharing this
  * identity lose admission before any persistence or socket work can await. */
 quarantine(id:string,generation:number):Promise<void>{
  const source=this.connection(id);if(generation!==source.transportGeneration)return Promise.resolve();
  const scope=source.intentScope;key(scope,512);
  for(const record of this.connections.values())if(record.intentScope===scope){record.revision=this.revision();record.session=undefined;record.phase='quarantined';record.bytes=2048;}
  return this.persist(512,async()=>{await this.options.safety.revokeSession(scope);await this.storage.saveSession(scope,undefined,{current:()=>true});});
 }
 resolveTab(tab:string,write=false,now=Date.now()){
  const id=this.tabs.get(tab);if(!id)fail('FENCED','Tab has no authenticated worker connection');
  const record=this.connection(id),session=record.session;if(!session||record.phase==='quarantined'||record.phase==='unavailable')fail('FENCED','Worker session is unavailable');
  if(write&&record.phase!=='online'&&this.options.offlineMaxAgeMs!==null&&(!Number.isSafeInteger(now)||now<(session.lastOnlineAtMs??0)||now-(session.lastOnlineAtMs??0)>this.options.offlineMaxAgeMs))fail('FENCED','Offline editing window expired. Reconnect before making more changes.');
  return {scope:session.directive.visibilityScope,intentScope:record.intentScope,generation:record.id+':'+record.revision,identity:clone(session.identity),artifactHash:session.artifactHash,online:record.phase==='online'};
 }
 /** Owner-only bounded session capture for independent intent durability. */
 sessionForConnection(id:string){const record=this.connection(id);if(!record.session||record.phase==='quarantined'||record.phase==='unavailable')fail('FENCED','Worker session is unavailable');return clone(record.session);}
 /** A transport frame is attributable only after its exact durable admission.
  * Authentication or disconnect never permits a new binary stage to proceed. */
 admitsFrames(id:string,generation:number){const record=this.connection(id);return record.phase==='online'&&record.transportGeneration===generation;}
 frameAdmission(id:string,generation:number){
  const record=this.connection(id);if(!this.admitsFrames(id,generation)||!record.session)fail('FENCED','Native frame has no durable online admission');
  return {scope:record.session.directive.visibilityScope,token:record.revision};
 }
 assertFrameAdmission(id:string,generation:number,token:number){const record=this.connection(id);if(!this.admitsFrames(id,generation)||record.revision!==token)fail('FENCED','Native frame admission changed');}
}
