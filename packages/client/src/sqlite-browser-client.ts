import type {JsonValue} from '@gonvex/protocol';
import type {FunctionReference,GonvexClientAuth,ConnectionState} from './index.js';
import type {SQLiteReplicaBroker} from './sqlite-broker.js';
import type {SQLiteNativeWindowResult,SQLiteNativeEntityResult} from './sqlite-native-replica-reader.js';
import type {SQLitePendingSnapshotPage} from './sqlite-pending-snapshot-reader.js';
import type {SQLiteReducerReceipt} from './sqlite-reducer-core.js';
import type {SQLiteTokenRequest} from './sqlite-worker-tokens.js';
import {SQLiteBrowserControls} from './sqlite-browser-controls.js';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
import {ByteLedger,BoundedByteCache} from './byte-budget.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';

type Auth=Pick<GonvexClientAuth,'project'|'tenant'|'identity'|'token'>;
type Broker=Pick<SQLiteReplicaBroker,'request'|'subscribeChanges'|'close'|'stats'>;
type Controls={admissionKey?:string|null;tokens:SQLiteTokenRequest[];status:unknown;session:string;failure:{code:string;message:string}|null;authError?:{id:string;message:string;definitive:boolean;code?:string}|null;developerCredential?:{id:string;token:string}};
type Handle={connection:string;identityKey:string};
type Page=SQLiteNativeWindowResult|SQLitePendingSnapshotPage|SQLiteNativeEntityResult|null;
type WatchSnapshot={page:Page|undefined;stale:boolean;version:number};
type Watch={state:WatchSnapshot;retiring?:Promise<void>;key:string;path:string;args:JsonValue;bytes:number;refs:number;source?:{id:string;signature:string};bound:number;opening?:Promise<void>;listeners:Set<()=>void>;unpin?:()=>void;stale:boolean;version:number};
export type SQLiteBrowserOptions={broker:Broker;artifactHash?:string;auth?:Auth;fetchToken?(request:{forceRefreshToken:boolean}):Promise<string|null>};
export type SQLiteReplicaPageOptions={offset?:number;limit?:number;generation?:string;expected?:SQLiteNativeWindowResult['generation']};
export type SQLiteReplicaWatch={
 /** A mounted watch retains one bounded page, never a collection-sized list. */
 page(options?:SQLiteReplicaPageOptions):Promise<SQLiteNativeWindowResult|null>;
 pendingPage(options?:{after?:number;limit?:number;expected?:SQLitePendingSnapshotPage['generation']}):Promise<SQLitePendingSnapshotPage|null>;
 snapshot():{page:Page|undefined;stale:boolean;version:number};
 subscribe(listener:()=>void):()=>void;
 close():void;
};
const stable=(value:JsonValue):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(stable).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+stable(value[key]!)).join(',')+'}';
/** Main-thread bounded RPC facade. It imports no SQLite host, portable reducer
 * runtime or application transport. The elected worker owns all durable state.
 * The generated compatibility client installs this only with verified migration
 * and fallback; this primitive itself never silently creates another adapter. */
export class SQLiteBrowserClient {
 private fetchToken?:GonvexClientAuth['fetchToken'];private readonly controlListeners=new Set<()=>void>();private readonly authListeners=new Set<(message:string,details:{definitive:boolean;code?:string})=>void>();private readonly credentialListeners=new Set<(token:string)=>void>();private lastAuthError?:string;private lastCredential?:string;
 private auth:Auth;private closed=false;private revision=0;private dataRevision=0;private admissionKey?:string|null;private epoch?:number;private handle?:Handle;private connecting?:Promise<Handle>;private connectingRevision=0;
 private readonly watches=new Map<string,Watch>();private watchBytes=0;
 private readonly ledger=new ByteLedger(6291456);private readonly controlQueries:SQLiteBrowserControls;
 private readonly requests=new SQLiteTransientBudget(2097152,16);
 private readonly pages=new BoundedByteCache<string,Page>('browser-row-pages',2097152,64,this.ledger);
 private readonly tokenRequests=new Set<string>();private readonly tokenCancels=new Map<string,()=>void>();private polling?:Promise<void>;private pollAgain=false;
 private readonly releaseChanges:()=>void;private controlsValue?:Pick<Controls,'status'|'session'|'failure'>;
 constructor(readonly url:string,private readonly options:SQLiteBrowserOptions){
  const parsed=new URL(url);if(!['ws:','wss:'].includes(parsed.protocol)||parsed.username||parsed.password||parsed.hash)throw new TypeError('Invalid native browser URL');
  sqlitePayloadBytes(options.auth??{},65536);this.auth=structuredClone(options.auth??{});this.fetchToken=options.fetchToken;this.controlQueries=new SQLiteBrowserControls(input=>this.rpc(input),this.ledger);
  this.ledger.register('browser-watch-metadata',()=>({name:'browser-watch-metadata',bytes:this.watchBytes,entries:this.watches.size,maxBytes:524288,maxEntries:64,evictions:0,refusals:0}));
  this.ledger.register('browser-request-admission',()=>({...this.requests.stats(),name:'browser-request-admission'}));
  this.ledger.register('browser-observers',()=>({name:'browser-observers',bytes:(this.controlListeners.size+this.authListeners.size+this.credentialListeners.size)*128,entries:this.controlListeners.size+this.authListeners.size+this.credentialListeners.size,maxBytes:12288,maxEntries:96,evictions:0,refusals:0}));
  this.ledger.register('browser-auth',()=>({name:'browser-auth',bytes:sqlitePayloadBytes(this.auth,65536),entries:1,maxBytes:65536,maxEntries:1,evictions:0,refusals:0}));
  this.ledger.register('browser-controls',()=>({name:'browser-controls',bytes:this.controlsValue?sqlitePayloadBytes(this.controlsValue,65536):0,entries:Number(!!this.controlsValue),maxBytes:65536,maxEntries:1,evictions:0,refusals:0}));
  this.ledger.register('browser-token-requests',()=>({name:'browser-token-requests',bytes:this.tokenRequests.size*40960,entries:this.tokenRequests.size,maxBytes:327680,maxEntries:8,evictions:0,refusals:0}));
  this.releaseChanges=options.broker.subscribeChanges(()=>{
   const epoch=options.broker.stats().ownerEpoch;if(epoch!==undefined&&this.epoch!==undefined&&epoch!==this.epoch)this.invalidate(false);if(epoch!==undefined)this.epoch=epoch;
   for(const watch of this.watches.values()){watch.stale=true;this.notify(watch);}this.controlQueries.refresh();void this.pollControls().catch(()=>{});
  });
 }
 private current(revision:number){if(this.closed||revision!==this.revision)fail('FENCED','Native browser session changed');}
 private notify(watch:Watch){watch.version++;watch.state={page:this.pages.get(watch.key),stale:watch.stale,version:watch.version};for(const listener of watch.listeners)try{listener();}catch{/* Observer only. */}}
 private invalidate(identity:boolean){this.revision++;this.dataRevision++;if(identity){this.admissionKey=undefined;this.lastAuthError=undefined;this.lastCredential=undefined;}this.handle=undefined;this.controlsValue=undefined;this.controlQueries.invalidate(identity);
  for(const watch of this.watches.values()){watch.bound=-1;watch.source=undefined;watch.opening=undefined;watch.stale=true;if(identity){watch.unpin?.();watch.unpin=undefined;this.pages.delete(watch.key);}this.notify(watch);}
 }
 private admit(key:string|null){
  if(this.admissionKey===key)return;
  const changed=this.admissionKey!==undefined||key===null;this.admissionKey=key;if(!changed)return;this.dataRevision++;this.controlQueries.invalidate(true);
  for(const watch of this.watches.values()){watch.unpin?.();watch.unpin=undefined;this.pages.delete(watch.key);watch.stale=true;this.notify(watch);}
 }
 connect():Promise<Handle>{
  if(this.closed)return Promise.reject(Object.assign(Error('Native browser client is closed'),{code:'FENCED'}));if(this.handle)return Promise.resolve(this.handle);if(this.connecting){const pending=this.connecting,pendingRevision=this.connectingRevision;return pending.then(handle=>this.handle?handle:this.connect(),error=>{if(!this.closed&&pendingRevision!==this.revision)return this.connect();throw error;});}
  const revision=this.revision,operation=this.options.broker.request<Handle>('clientCall',{method:'connect',url:this.url,auth:this.auth,artifactHash:this.options.artifactHash}).then(handle=>{this.current(revision);this.handle=handle;this.epoch=this.options.broker.stats().ownerEpoch;
   // Credential hints must be serviced while authentication is waiting. Never
   // put a token fetch behind the durable connection/session readiness barrier.
   queueMicrotask(()=>{void this.pollControls().catch(()=>{});for(const watch of this.watches.values())void this.open(watch).catch(()=>{});});return handle;
  });this.connecting=operation;this.connectingRevision=revision;void operation.then(()=>{if(this.connecting===operation)this.connecting=undefined;},()=>{if(this.connecting===operation)this.connecting=undefined;});return operation;
 }
 async setAuth(input:GonvexClientAuth){
  if(Object.hasOwn(input,'fetchToken'))this.fetchToken=input.fetchToken;const auth:Auth={};for(const key of ['project','tenant','identity','token'] as const)if(Object.hasOwn(input,key))Object.assign(auth,{[key]:input[key]});
  sqlitePayloadBytes(auth,65536);const identity=stable({project:Object.hasOwn(auth,'project')?auth.project??null:this.auth.project??null,tenant:Object.hasOwn(auth,'tenant')?auth.tenant??null:this.auth.tenant??null,identity:Object.hasOwn(auth,'identity')?auth.identity as JsonValue??null:this.auth.identity as JsonValue??null});
  const old=stable({project:this.auth.project??null,tenant:this.auth.tenant??null,identity:this.auth.identity as JsonValue??null});this.auth={...this.auth,...structuredClone(auth)};
  if(identity!==old||!this.auth.identity&&Object.hasOwn(auth,'token')){this.invalidate(true);await this.connect();return;}
  const revision=this.revision;await this.connect();this.current(revision);await this.options.broker.request('clientCall',{method:'auth',auth:{token:this.auth.token}});this.current(revision);void this.pollControls().catch(()=>{});
 }
 private async rpc<T>(input:unknown,commandID?:string):Promise<T>{const revision=this.revision,dataRevision=this.dataRevision,tenant=!!input&&typeof input==='object'&&'method'in input&&['entities','replicaPage','replicaPendingPage','reducer'].includes(String(input.method));this.current(revision);const bytes=sqlitePayloadBytes(input,131072),release=this.requests.reserve(bytes+512);try{const owned=structuredClone(input);await this.connect();this.current(revision);const value=await this.options.broker.request<T>('clientCall',owned,90000,commandID);this.current(revision);if(tenant&&dataRevision!==this.dataRevision)fail('FENCED','Native page admission changed');return value;}finally{release();}}
 pollControls():Promise<void>{
  if(this.closed)return Promise.resolve();if(this.polling){this.pollAgain=true;return this.polling;}
  const operation=(async()=>{do{this.pollAgain=false;const revision=this.revision,handle=await this.connect(),controls=await this.options.broker.request<Controls>('clientCall',{method:'controls'});this.current(revision);sqlitePayloadBytes(controls,65536);if(controls.admissionKey!==undefined)this.admit(controls.admissionKey);this.controlsValue={status:controls.status,session:controls.session,failure:controls.failure};
   for(const listener of this.controlListeners)try{listener();}catch{/* Observer only. */}this.current(revision);
   if(controls.authError&&controls.authError.id!==this.lastAuthError&&this.authListeners.size){this.lastAuthError=controls.authError.id;for(const listener of this.authListeners)try{listener(controls.authError.message,{definitive:controls.authError.definitive,code:controls.authError.code});}catch{/* Observer only. */}}
   if(controls.developerCredential&&this.credentialListeners.size){if(controls.developerCredential.id!==this.lastCredential){this.lastCredential=controls.developerCredential.id;for(const listener of this.credentialListeners)try{this.current(revision);listener(controls.developerCredential.token);}catch{/* Observer only. */}}await this.options.broker.request('clientCall',{method:'credentialAck',id:controls.developerCredential.id});}
   for(const request of controls.tokens){if(request.connection!==handle.connection||request.identityKey!==handle.identityKey||this.tokenRequests.has(request.id))continue;if(this.tokenRequests.size>=8)fail('BUDGET','Native browser token request budget exhausted');this.tokenRequests.add(request.id);
    void (async()=>{try{let timer:ReturnType<typeof setTimeout>|undefined;let token:string|null|undefined;try{token=this.fetchToken?await Promise.race([this.fetchToken({forceRefreshToken:request.forceRefreshToken}),new Promise<never>((_,reject)=>{this.tokenCancels.set(request.id,()=>reject(Error('Native token request canceled')));timer=setTimeout(()=>reject(Error('Native token provider timed out')),30000);})]):this.auth.token??null;}finally{clearTimeout(timer);}this.current(revision);token??=this.auth.token??null;if(token!==null&&(typeof token!=='string'||token.length>16384))throw new TypeError('Invalid native browser token');await this.options.broker.request('clientCall',{method:'token',response:{...request,token}});}catch{if(!this.closed&&revision===this.revision)await this.options.broker.request('clientCall',{method:'token',response:{...request,error:'Token provider unavailable'}}).catch(()=>{});}finally{this.tokenRequests.delete(request.id);this.tokenCancels.delete(request.id);}})();
   }
  }while(this.pollAgain&&!this.closed);})();this.polling=operation;void operation.then(()=>{this.polling=undefined;},()=>{this.polling=undefined;if(this.pollAgain&&!this.closed){this.pollAgain=false;queueMicrotask(()=>{void this.pollControls().catch(()=>{});});}});return operation;
 }
 watchControlQuery<T extends JsonValue=JsonValue>(ref:FunctionReference,args:JsonValue={}){if(ref.scope!=='control'||ref.kind!=='query'||ref.delivery!=='live')throw new TypeError('Expected a generated live Control query');return this.controlQueries.watch<T>(ref,args);}
 status(){return this.controlsValue;}
 private observe<T>(set:Set<T>,listener:T){if(this.closed)fail('FENCED','Native browser client is closed');if(set.size>=32)fail('BUDGET','Native observer budget exhausted');set.add(listener);return ()=>{set.delete(listener);};}
 onAuthError(listener:(message:string,details:{definitive:boolean;code?:string})=>void){const release=this.observe(this.authListeners,listener);void this.pollControls().catch(()=>{});return release;}
 onDeveloperCredential(listener:(token:string)=>void){const release=this.observe(this.credentialListeners,listener);void this.pollControls().catch(()=>{});return release;}
 connectionState():ConnectionState{return this.controlsValue?.status as ConnectionState??{isWebSocketConnected:false,hasEverConnected:false,connectionCount:0,connectionRetries:0,hasInflightRequests:false,inflightReducers:0,inflightActions:0,inflightOneShotQueries:0};}
 subscribeToConnectionState(listener:(state:ConnectionState)=>void){return this.observe(this.controlListeners,()=>listener(this.connectionState()));}
 localSessionState(){return (this.controlsValue?.session??'unavailable') as 'unavailable'|'authenticated'|'restored';}
 onLocalSessionChange(listener:()=>void){return this.observe(this.controlListeners,listener);}
 onSessionScopeChange(listener:()=>void){let key=this.admissionKey;return this.observe(this.controlListeners,()=>{if(key!==this.admissionKey){key=this.admissionKey;listener();}});}

 private async open(watch:Watch):Promise<void>{
  if(!watch.refs)fail('FENCED','Native watch is closed');if(watch.retiring)await watch.retiring;await this.connect();if(!watch.refs)fail('FENCED','Native watch is closed');if(watch.bound===this.revision&&watch.source)return;if(watch.opening)return watch.opening;
  const revision=this.revision,operation=this.rpc<{id:string;signature:string}>({method:'watchReplica',path:watch.path,args:watch.args}).then(source=>{this.current(revision);watch.source=source;watch.bound=revision;});watch.opening=operation;void operation.then(()=>{if(watch.opening===operation)watch.opening=undefined;},()=>{if(watch.opening===operation)watch.opening=undefined;});return operation;
 }
 private store(watch:Watch,value:Page){
  if(!watch.refs)fail('FENCED','Native watch closed during read');const bytes=sqlitePayloadBytes({key:watch.key,value},262144);watch.unpin?.();watch.unpin=undefined;
  try{this.pages.put(watch.key,value,bytes);}finally{watch.unpin=this.pages.pin(watch.key);}
  watch.stale=false;this.notify(watch);
 }
 watchReplica(ref:Pick<FunctionReference,'path'>,args:JsonValue={}):SQLiteReplicaWatch{
  sqlitePayloadBytes({path:ref.path,args},16384);const key=stable({path:ref.path,args});let watch=this.watches.get(key);
  if(!watch){const bytes=sqlitePayloadBytes({key,args,path:ref.path},65536)+2048;if(this.watches.size>=64||this.watchBytes+bytes>524288)fail('BUDGET','Native browser watch budget exhausted');watch={state:{page:undefined,stale:true,version:0},key,path:ref.path,args:JSON.parse(stable(args)),bytes,refs:0,bound:-1,listeners:new Set(),stale:true,version:0};this.watches.set(key,watch);this.watchBytes+=bytes;}
  const selected=watch;if(selected.refs>=64)fail('BUDGET','Native watch lease budget exhausted');selected.refs++;let closed=false;const listeners=new Set<()=>void>();void this.open(selected).catch(()=>{});
  const read=async<T extends Page>(method:string,options:unknown):Promise<T>=>{const revision=this.revision,dataRevision=this.dataRevision;if(closed)fail('FENCED','Native watch is closed');await this.open(selected);const value=await this.rpc<T>({method,signature:selected.source!.signature,...options as object});if(closed)fail('FENCED','Native watch closed during read');this.current(revision);if(dataRevision!==this.dataRevision)fail('FENCED','Native watch admission changed');this.store(selected,value);return value;};
  return {page:(options={})=>read('replicaPage',options),pendingPage:(options={})=>read('replicaPendingPage',options),snapshot:()=>selected.state,subscribe:listener=>{if(closed)fail('FENCED','Native watch is closed');if(selected.listeners.size>=32)fail('BUDGET','Native watch listener budget exhausted');const wrapped=()=>listener();listeners.add(wrapped);selected.listeners.add(wrapped);return ()=>{listeners.delete(wrapped);selected.listeners.delete(wrapped);};},close:()=>{if(closed)return;closed=true;for(const listener of listeners)selected.listeners.delete(listener);listeners.clear();if(--selected.refs)return;selected.state={page:undefined,stale:true,version:selected.version+1};selected.unpin?.();selected.unpin=undefined;this.pages.delete(selected.key);
   const retirement=(async()=>{try{await selected.opening;if(selected.refs)return;if(selected.source)await this.rpc({method:'unwatchReplica',signature:selected.source.signature});selected.source=undefined;selected.bound=-1;}finally{if(this.watches.get(selected.key)===selected&&!selected.refs){this.watches.delete(selected.key);this.watchBytes-=selected.bytes;}}})();selected.retiring=retirement;void retirement.then(()=>{if(selected.retiring===retirement)selected.retiring=undefined;},()=>{if(selected.retiring===retirement)selected.retiring=undefined;});}};
 }
 entities(entity:string,ids:readonly string[]):Promise<SQLiteNativeEntityResult>{sqlitePayloadBytes({entity,ids},32768);if(ids.length>128)throw new RangeError('Native entity reads are bounded');return this.rpc({method:'entities',entity,ids});}
 reducer(ref:Pick<FunctionReference,'path'>,args:JsonValue={},options:{now?:number;intentEntropy?:string;commandId?:string}={}):Promise<SQLiteReducerReceipt>{if(options.commandId&&options.now===undefined)throw new TypeError('Retryable reducers require their original invocation time');return this.rpc({method:'reducer',path:ref.path,args,now:options.now??Date.now(),intentEntropy:options.intentEntropy},options.commandId??crypto.randomUUID());}
 query<T extends JsonValue>(ref:Pick<FunctionReference,'path'>,args:JsonValue={}):Promise<T>{return this.rpc({method:'query',path:ref.path,args});}
 action<T extends JsonValue>(ref:Pick<FunctionReference,'path'>,args:JsonValue={}):Promise<T>{return this.rpc({method:'action',path:ref.path,args});}
 budgets(){return this.ledger.snapshot();}
 close(){if(this.closed)return;this.closed=true;this.revision++;this.controlQueries.close();this.releaseChanges();for(const watch of this.watches.values()){watch.refs=0;watch.state={page:undefined,stale:true,version:watch.version+1};watch.unpin?.();watch.listeners.clear();}this.watches.clear();this.watchBytes=0;for(const cancel of this.tokenCancels.values())cancel();this.tokenCancels.clear();this.tokenRequests.clear();this.auth={};this.fetchToken=undefined;this.controlListeners.clear();this.authListeners.clear();this.credentialListeners.clear();this.controlsValue=undefined;this.pages.close();this.options.broker.close();}
}
