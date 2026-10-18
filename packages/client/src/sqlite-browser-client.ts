import type {SQLitePendingLivePage} from './sqlite-live-windows.js';
import type {JsonValue} from '@gonvex/protocol';
import type {FunctionReference,GonvexClientAuth,ConnectionState,QueuedReducerOutcome,CallOptions,GonvexClientOptions,ReducerRejectionEvent,LocalExecutionFallbackEvent,GonvexTelemetryEvent,SupportCommand} from './index.js';
import type {SQLiteReplicaBroker} from './sqlite-broker.js';
import type {SQLiteNativeWindowResult,SQLiteNativeEntityResult} from './sqlite-native-replica-reader.js';
import type {SQLitePendingSnapshotPage} from './sqlite-pending-snapshot-reader.js';
import type {SQLiteReducerReceipt} from './sqlite-reducer-core.js';
import type {SQLiteTokenRequest} from './sqlite-worker-tokens.js';
import {GonvexErrorReporter} from './error-reporter.js';
import {GonvexClientError} from './client-error.js';
import type {SQLiteCollectionPageOptions} from './sqlite-collection-plan.js';
import type {SQLiteTypedPage} from './sqlite-typed-engine.js';
import {SQLiteBrowserEntities} from './sqlite-browser-entities.js';
import {SQLiteBrowserControls} from './sqlite-browser-controls.js';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
import {ByteLedger,BoundedByteCache} from './byte-budget.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';

type Auth=Pick<GonvexClientAuth,'project'|'tenant'|'identity'|'token'>;
type Broker=Pick<SQLiteReplicaBroker,'request'|'subscribeChanges'|'close'|'stats'>;
type Controls={supportCommands?:Array<SupportCommand&{sequence:number}>;rejections?:Array<ReducerRejectionEvent&{sequence:number}>;serverInfo?:Readonly<import('@gonvex/protocol').ServerCapabilities>;admissionKey?:string|null;tokens:SQLiteTokenRequest[];status:unknown;authentication?:'unavailable'|'authenticating'|'authenticated'|'denied';session:string;failure:{code:string;message:string}|null;authError?:{id:string;message:string;definitive:boolean;code?:string}|null;developerCredential?:{id:string;token:string}};
type Handle={connection:string;identityKey:string};
export type SQLiteCollectionPageResult=SQLiteTypedPage&{generation:string};
type Page=SQLitePendingLivePage|SQLiteNativeWindowResult|SQLitePendingSnapshotPage|SQLiteNativeEntityResult|SQLiteCollectionPageResult|null;
type WatchSnapshot={page:Page|undefined;stale:boolean;version:number};
type Watch={kind:'replica'|'live';state:WatchSnapshot;retiring?:Promise<void>;key:string;lease:string;path:string;args:JsonValue;bytes:number;refs:number;source?:{id?:string;signature:string};bound:number;opening?:Promise<void>;listeners:Set<()=>void>;unpin?:()=>void;stale:boolean;version:number};
export type SQLiteBrowserOptions={broker:Broker;artifactHash?:string;onUpdateRequired?:GonvexClientOptions['onUpdateRequired'];onLocalExecutionFallback?:GonvexClientOptions['onLocalExecutionFallback'];errorReporting?:GonvexClientOptions['errorReporting'];role?:'control';public?:{tenant:string};auth?:Auth;fetchToken?:GonvexClientAuth['fetchToken']};
export type SQLiteWatchOptions={view?:JsonValue;deferStart?:boolean};
export type SQLiteReplicaPageOptions={offset?:number;limit?:number;generation?:string;expected?:SQLiteNativeWindowResult['generation']};
export type SQLiteReplicaWatch={
 /** A mounted watch retains one bounded page, never a collection-sized list. */
 page(options?:SQLiteReplicaPageOptions):Promise<SQLiteNativeWindowResult|null>;
 queryPage(plan:SQLiteCollectionPageOptions,generation?:string):Promise<(SQLiteTypedPage&{generation:string})|null>;
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
 private fetchToken?:GonvexClientAuth['fetchToken'];private readonly controlListeners=new Set<()=>void>();private readonly authListeners=new Set<(message:string,details:{definitive:boolean;code?:string})=>void>();private readonly credentialListeners=new Set<(token:string)=>void>();private readonly reporter?:GonvexErrorReporter;private lastRejection=0;private lastSupport=0;private readonly supportListeners=new Set<(command:SupportCommand)=>void>();private readonly rejectionListeners=new Set<(event:ReducerRejectionEvent)=>void>();private readonly fallbackListeners=new Set<(event:LocalExecutionFallbackEvent)=>void>();private readonly telemetryListeners=new Set<(event:GonvexTelemetryEvent)=>void>();private authSerial=0;private readonly authWaiters=new Set<()=>void>();private lastUpdateRequired?:string;private lastAuthError?:string;private lastCredential?:string;
 private readonly publicTenant?:string;private auth:Auth;private closed=false;private revision=0;private dataRevision=0;private admissionKey?:string|null;private epoch?:number;private handle?:Handle;private connecting?:Promise<Handle>;private connectingRevision=0;
 private readonly watches=new Map<string,Watch>();private watchBytes=0;
 private readonly ledger=new ByteLedger(12582912);private readonly entityView:SQLiteBrowserEntities;private readonly controlQueries:SQLiteBrowserControls;
 private readonly requests=new SQLiteTransientBudget(2097152,16);
 private readonly pages=new BoundedByteCache<string,Page>('browser-row-pages',2097152,64,this.ledger);
 private readonly tokenRequests=new Set<string>();private readonly tokenCancels=new Map<string,()=>void>();private polling?:Promise<void>;private pollAgain=false;
 private readonly releaseChanges:()=>void;private controlsValue?:Pick<Controls,'status'|'session'|'failure'|'authentication'|'serverInfo'>;
 constructor(readonly url:string,private readonly options:SQLiteBrowserOptions){
  const parsed=new URL(url);if(!['ws:','wss:'].includes(parsed.protocol)||parsed.username||parsed.password||parsed.hash)throw new TypeError('Invalid native browser URL');
  sqlitePayloadBytes(options.auth??{},65536);this.auth=structuredClone(options.auth??{});if(options.public){if(options.role==='control'||!options.public.tenant.trim()||!this.auth.project?.trim()||this.auth.token||this.auth.identity||this.auth.tenant||options.fetchToken)throw new TypeError('Public clients require fixed project/tenant routing without account credentials');this.publicTenant=options.public.tenant.trim();this.auth.tenant=this.publicTenant;parsed.searchParams.set('public','1');this.url=parsed.href;}this.fetchToken=options.fetchToken;if(options.onLocalExecutionFallback)this.fallbackListeners.add(options.onLocalExecutionFallback);this.controlQueries=new SQLiteBrowserControls(input=>this.rpc(input),this.ledger);this.entityView=new SQLiteBrowserEntities((entity,ids,expected)=>this.entities(entity,ids,expected),this.ledger,error=>{if(this.closed)return;this.controlsValue={...this.controlsValue,status:this.controlsValue?.status,session:this.controlsValue?.session??'unavailable',failure:{code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)}};for(const listener of this.controlListeners)try{listener();}catch{/* Observer only. */}});
  this.ledger.register('browser-watch-metadata',()=>({name:'browser-watch-metadata',bytes:this.watchBytes,entries:this.watches.size,maxBytes:524288,maxEntries:64,evictions:0,refusals:0}));
  this.ledger.register('browser-request-admission',()=>({...this.requests.stats(),name:'browser-request-admission'}));
  this.ledger.register('browser-observers',()=>({name:'browser-observers',bytes:(this.controlListeners.size+this.authListeners.size+this.credentialListeners.size+this.rejectionListeners.size+this.fallbackListeners.size+this.telemetryListeners.size)*128+this.authWaiters.size*256,entries:this.controlListeners.size+this.authListeners.size+this.credentialListeners.size+this.rejectionListeners.size+this.fallbackListeners.size+this.telemetryListeners.size+this.authWaiters.size,maxBytes:26624,maxEntries:200,evictions:0,refusals:0}));
  this.ledger.register('browser-auth',()=>({name:'browser-auth',bytes:sqlitePayloadBytes(this.auth,65536),entries:1,maxBytes:65536,maxEntries:1,evictions:0,refusals:0}));
  this.ledger.register('browser-controls',()=>({name:'browser-controls',bytes:this.controlsValue?sqlitePayloadBytes(this.controlsValue,65536):0,entries:Number(!!this.controlsValue),maxBytes:65536,maxEntries:1,evictions:0,refusals:0}));
  this.ledger.register('browser-token-requests',()=>({name:'browser-token-requests',bytes:this.tokenRequests.size*40960,entries:this.tokenRequests.size,maxBytes:327680,maxEntries:8,evictions:0,refusals:0}));
  this.releaseChanges=options.broker.subscribeChanges(()=>{
   const epoch=options.broker.stats().ownerEpoch;if(epoch!==undefined&&this.epoch!==undefined&&epoch!==this.epoch)this.invalidate(false);if(epoch!==undefined)this.epoch=epoch;
   for(const watch of this.watches.values()){watch.stale=true;this.notify(watch);}this.controlQueries.refresh();this.entityView.refresh();void this.pollControls().catch(()=>{});
  });
  if(options.errorReporting&&this.auth.project){this.reporter=new GonvexErrorReporter({...options.errorReporting,project:this.auth.project,tenant:this.auth.tenant,client:this,transport:(kind,payload)=>this.reportError(kind,payload)});this.ledger.register('error-reporting',()=>this.reporter!.memoryBudget());}
 }
 private current(revision:number){if(this.closed||revision!==this.revision)fail('FENCED','Native browser session changed');}
 private notify(watch:Watch){watch.version++;watch.state={page:this.pages.get(watch.key),stale:watch.stale,version:watch.version};for(const listener of watch.listeners)try{listener();}catch{/* Observer only. */}}
 private invalidate(identity:boolean){this.revision++;this.dataRevision++;this.lastRejection=0;this.lastSupport=0;if(identity){this.admissionKey=undefined;this.lastAuthError=undefined;this.lastCredential=undefined;}this.handle=undefined;this.controlsValue=undefined;this.controlQueries.invalidate(identity);this.entityView.invalidate(identity);
  for(const watch of this.watches.values()){watch.bound=-1;watch.source=undefined;watch.opening=undefined;watch.stale=true;if(identity){watch.unpin?.();watch.unpin=undefined;this.pages.delete(watch.key);}this.notify(watch);}
 }
 private admit(key:string|null){
  if(this.admissionKey===key)return;
  const changed=this.admissionKey!==undefined||key===null;this.admissionKey=key;if(!changed)return;this.dataRevision++;this.controlQueries.invalidate(true);this.entityView.invalidate(true);
  for(const watch of this.watches.values()){watch.unpin?.();watch.unpin=undefined;this.pages.delete(watch.key);watch.stale=true;this.notify(watch);}
 }
 connect():Promise<Handle>{
  if(this.closed)return Promise.reject(Object.assign(Error('Native browser client is closed'),{code:'FENCED'}));if(this.handle)return Promise.resolve(this.handle);if(this.connecting){const pending=this.connecting,pendingRevision=this.connectingRevision;return pending.then(handle=>this.handle?handle:this.connect(),error=>{if(!this.closed&&pendingRevision!==this.revision)return this.connect();throw error;});}
  const revision=this.revision,operation=this.options.broker.request<Handle>('clientCall',{method:'connect',url:this.url,auth:this.auth,...(this.options.role?{role:this.options.role}:{}),...(this.publicTenant?{public:true}:{}),artifactHash:this.options.artifactHash}).then(handle=>{this.current(revision);this.handle=handle;this.epoch=this.options.broker.stats().ownerEpoch;
   // Credential hints must be serviced while authentication is waiting. Never
   // put a token fetch behind the durable connection/session readiness barrier.
   queueMicrotask(()=>{void this.pollControls().catch(()=>{});for(const watch of this.watches.values())void this.open(watch).catch(()=>{});});return handle;
  });this.connecting=operation;this.connectingRevision=revision;void operation.then(()=>{if(this.connecting===operation)this.connecting=undefined;},()=>{if(this.connecting===operation)this.connecting=undefined;});return operation;
 }
 async setAuth(input:GonvexClientAuth){
  this.current(this.revision);if(this.publicTenant&&(input.token||input.fetchToken||input.identity||Object.hasOwn(input,'tenant')&&input.tenant!==this.publicTenant||Object.hasOwn(input,'project')&&input.project!==this.auth.project))throw new TypeError('Public client routing is fixed');this.authSerial++;for(const listener of this.controlListeners)try{listener();}catch{/* Observer only. */}
  if(Object.hasOwn(input,'fetchToken'))this.fetchToken=input.fetchToken;const auth:Auth={};for(const key of ['project','tenant','identity','token'] as const)if(Object.hasOwn(input,key))Object.assign(auth,{[key]:input[key]});
  sqlitePayloadBytes(auth,65536);const identity=stable({project:Object.hasOwn(auth,'project')?auth.project??null:this.auth.project??null,tenant:Object.hasOwn(auth,'tenant')?auth.tenant??null:this.auth.tenant??null,identity:Object.hasOwn(auth,'identity')?auth.identity as JsonValue??null:this.auth.identity as JsonValue??null});
  const old=stable({project:this.auth.project??null,tenant:this.auth.tenant??null,identity:this.auth.identity as JsonValue??null});this.auth={...this.auth,...structuredClone(auth)};if(Object.hasOwn(auth,'tenant'))this.reporter?.setTenant(auth.tenant);if(auth.project!==undefined)this.reporter?.setProject(auth.project);
  if(identity!==old||!this.auth.identity&&Object.hasOwn(auth,'token')){this.invalidate(true);await this.connect();return;}
  const revision=this.revision;await this.connect();this.current(revision);await this.options.broker.request('clientCall',{method:'auth',auth:{token:this.auth.token}});this.current(revision);void this.pollControls().catch(()=>{});
 }
 private async rpc<T>(input:unknown,commandID?:string):Promise<T>{const revision=this.revision,dataRevision=this.dataRevision,tenant=!!input&&typeof input==='object'&&'method'in input&&['entities','replicaPage','replicaPendingPage','collectionPage','livePage','livePendingPage','reducer'].includes(String(input.method));this.current(revision);const bytes=sqlitePayloadBytes(input,131072),release=this.requests.reserve(bytes+512);try{const owned=structuredClone(input) as any;if(tenant&&owned.method==='reducer'&&this.admissionKey!==undefined)owned.expectedAdmissionKey=this.admissionKey;await this.connect();this.current(revision);const value=await this.options.broker.request<T>('clientCall',owned,90000,commandID);this.current(revision);if(tenant&&dataRevision!==this.dataRevision)fail('FENCED','Native page admission changed');return value;}finally{release();}}
 pollControls():Promise<void>{
  if(this.closed)return Promise.resolve();if(this.polling){this.pollAgain=true;return this.polling;}
  const operation=(async()=>{do{this.pollAgain=false;const revision=this.revision,handle=await this.connect(),controls=await this.options.broker.request<Controls>('clientCall',{method:'controls'});this.current(revision);sqlitePayloadBytes(controls,65536);if(controls.admissionKey!==undefined)this.admit(controls.admissionKey);this.controlsValue={status:controls.status,serverInfo:controls.serverInfo,authentication:controls.authentication,session:controls.session,failure:controls.failure};
   if(controls.failure?.code==='SCHEMA_MISMATCH'&&this.lastUpdateRequired!==controls.failure.message){this.lastUpdateRequired=controls.failure.message;try{this.options.onUpdateRequired?.(controls.failure.message);}catch{/* Observer only. */}}for(const listener of this.controlListeners)try{listener();}catch{/* Observer only. */}this.current(revision);
   if(this.rejectionListeners.size&&controls.rejections?.length){for(const event of controls.rejections){if(event.sequence<=this.lastRejection)continue;this.lastRejection=event.sequence;for(const listener of this.rejectionListeners)try{listener(event);}catch{/* Observer only. */}}this.current(revision);await this.options.broker.request('clientCall',{method:'rejectionsAck',sequence:this.lastRejection});}
   if(this.supportListeners.size&&controls.supportCommands?.length){for(const event of controls.supportCommands){if(event.sequence<=this.lastSupport)continue;this.lastSupport=event.sequence;for(const listener of this.supportListeners)try{listener({id:event.id,kind:event.kind,payload:event.payload});}catch{/* Observer only. */}}this.current(revision);await this.options.broker.request('clientCall',{method:'supportAck',sequence:this.lastSupport});}
   if(controls.authError&&controls.authError.id!==this.lastAuthError&&this.authListeners.size){this.lastAuthError=controls.authError.id;for(const listener of this.authListeners)try{listener(controls.authError.message,{definitive:controls.authError.definitive,code:controls.authError.code});}catch{/* Observer only. */}}
   if(controls.developerCredential&&this.credentialListeners.size){if(controls.developerCredential.id!==this.lastCredential){this.lastCredential=controls.developerCredential.id;for(const listener of this.credentialListeners)try{this.current(revision);listener(controls.developerCredential.token);}catch{/* Observer only. */}}await this.options.broker.request('clientCall',{method:'credentialAck',id:controls.developerCredential.id});}
   for(const request of controls.tokens){if(request.connection!==handle.connection||request.identityKey!==handle.identityKey||this.tokenRequests.has(request.id))continue;if(this.tokenRequests.size>=8)fail('BUDGET','Native browser token request budget exhausted');this.tokenRequests.add(request.id);
    void (async()=>{try{let timer:ReturnType<typeof setTimeout>|undefined;let token:string|null|undefined;try{token=this.fetchToken?await Promise.race([this.fetchToken({forceRefreshToken:request.forceRefreshToken}),new Promise<never>((_,reject)=>{this.tokenCancels.set(request.id,()=>reject(Error('Native token request canceled')));timer=setTimeout(()=>reject(Error('Native token provider timed out')),30000);})]):this.auth.token??null;}finally{clearTimeout(timer);}this.current(revision);token??=this.auth.token??null;if(token!==null&&(typeof token!=='string'||token.length>16384))throw new TypeError('Invalid native browser token');await this.options.broker.request('clientCall',{method:'token',response:{...request,token}});}catch{if(!this.closed&&revision===this.revision)await this.options.broker.request('clientCall',{method:'token',response:{...request,error:'Token provider unavailable'}}).catch(()=>{});}finally{this.tokenRequests.delete(request.id);this.tokenCancels.delete(request.id);}})();
   }
  }while(this.pollAgain&&!this.closed);})();this.polling=operation;void operation.then(()=>{this.polling=undefined;},()=>{this.polling=undefined;if(this.pollAgain&&!this.closed){this.pollAgain=false;queueMicrotask(()=>{void this.pollControls().catch(()=>{});});}});return operation;
 }
 get localReplica(){return this.entityView;}
 retainReplicaEntities(entity:string,ids:readonly string[]){return this.entityView.retain(entity,ids);}
 watchControlQuery<T extends JsonValue=JsonValue>(ref:FunctionReference,args:JsonValue={}){if(ref.scope!=='control'||ref.kind!=='query'||ref.delivery!=='live')throw new TypeError('Expected a generated live Control query');return this.controlQueries.watch<T>(ref,args);}
 async restoreSession(auth:GonvexClientAuth){await this.setAuth(auth);const restored=await this.rpc<boolean>({method:'restoreSession'});await this.pollControls();return restored;}
 authenticate(auth:GonvexClientAuth):Promise<void>{
  if(this.closed)return Promise.reject(new GonvexClientError('Native browser client is closed',{code:'closed'}));
  if(this.authWaiters.size>=8)return Promise.reject(Object.assign(Error('Authentication waiter allowance exceeded'),{code:'BUDGET'}));
  const serial=this.authSerial+1;
  return new Promise<void>((resolve,reject)=>{let armed=false,settled=false,timer:ReturnType<typeof setTimeout>|undefined;let stop=()=>{};
   const cancel=()=>finish(new GonvexClientError('Client closed during authentication',{code:'closed'}));
   const finish=(error?:Error)=>{if(settled)return;settled=true;clearTimeout(timer);stop();this.authWaiters.delete(cancel);if(error)reject(error);else resolve();};
   const check=()=>{if(settled)return;if(this.authSerial!==serial){finish(new GonvexClientError('Authentication was superseded',{code:'superseded'}));return;}if(!armed)return;const state=this.controlsValue;if(state?.authentication==='authenticated')finish();else if(state?.authentication==='denied')finish(new GonvexClientError('Authentication rejected',{code:'auth'}));};
   try{stop=this.observe(this.controlListeners,check);}catch(error){reject(error);return;}this.authWaiters.add(cancel);timer=setTimeout(()=>finish(new GonvexClientError('Authentication timed out',{code:'timeout'})),30000);
   void this.setAuth(auth).then(()=>this.pollControls()).then(()=>{armed=true;check();},error=>finish(error instanceof Error?error:Error(String(error))));
  });
 }
 async reportError(type:'register'|'envelope'|'heartbeat',payload:unknown):Promise<void>{sqlitePayloadBytes(payload,65536);return this.rpc({method:'reportError',type,payload});}
 serverInfo(){return this.controlsValue?.serverInfo??{};}
 activeArtifactHash(){return this.options.artifactHash??'';}
 get localSessionReady(){return this.rpc<boolean>({method:'restoreSession'});}
 outboxCount(){return this.rpc<number>({method:'outboxCount'});}
 replicaEntities<T extends import('./local-replica.js').ReplicaRow>(entity:string,ids:readonly string[]){if(ids.length>128)throw new RangeError('Entity views are bounded');return ids.map(id=>this.entityView.entity<T>(entity,id));}
 memoryCounters(){const entities=this.entityView.counters();let pageRows=0;for(const watch of this.watches.values())pageRows+=watch.state.page?.rows.length??0;return {...entities,replicaRows:entities.replicaRows+pageRows,replicaBytes:entities.replicaBytes+this.pages.stats().bytes,replicaWindows:this.watches.size,retainedWindows:this.watches.size,pendingCommands:0,queuedStorageOperations:this.requests.stats().entries,querySubscriptions:this.controlQueries.count(),replicaSubscriptions:this.watches.size,pendingCalls:this.requests.stats().entries,outboxHead:0};}
 status(){return this.controlsValue;}


 private observe<T>(set:Set<T>,listener:T){if(this.closed)fail('FENCED','Native browser client is closed');if(set.size>=32)fail('BUDGET','Native observer budget exhausted');set.add(listener);return ()=>{set.delete(listener);};}
 onSupportCommand(listener:(command:SupportCommand)=>void){const release=this.observe(this.supportListeners,listener);void this.pollControls().catch(()=>{});return release;}
 onReducerRejection(listener:(event:ReducerRejectionEvent)=>void){const release=this.observe(this.rejectionListeners,listener);void this.pollControls().catch(()=>{});return release;}
 onLocalExecutionFallback(listener:(event:LocalExecutionFallbackEvent)=>void){return this.observe(this.fallbackListeners,listener);}
 onTelemetry(listener:(event:GonvexTelemetryEvent)=>void){return this.observe(this.telemetryListeners,listener);}
 onAuthError(listener:(message:string,details:{definitive:boolean;code?:string})=>void){const release=this.observe(this.authListeners,listener);void this.pollControls().catch(()=>{});return release;}
 onDeveloperCredential(listener:(token:string)=>void){const release=this.observe(this.credentialListeners,listener);void this.pollControls().catch(()=>{});return release;}
 connectionState():ConnectionState{return this.controlsValue?.status as ConnectionState??{isWebSocketConnected:false,hasEverConnected:false,connectionCount:0,connectionRetries:0,hasInflightRequests:false,inflightReducers:0,inflightActions:0,inflightOneShotQueries:0};}
 subscribeToConnectionState(listener:(state:ConnectionState)=>void){return this.observe(this.controlListeners,()=>listener(this.connectionState()));}
 localSessionState(){return (this.controlsValue?.session??'unavailable') as 'unavailable'|'authenticated'|'restored';}
 onLocalSessionChange(listener:()=>void){return this.observe(this.controlListeners,listener);}
 onSessionScopeChange(listener:()=>void){let key=this.admissionKey;return this.observe(this.controlListeners,()=>{if(key!==this.admissionKey){key=this.admissionKey;listener();}});}

 private async open(watch:Watch):Promise<void>{
  if(!watch.refs)fail('FENCED','Native watch is closed');if(watch.retiring)await watch.retiring;await this.connect();if(!watch.refs)fail('FENCED','Native watch is closed');if(watch.bound===this.revision&&watch.source)return;if(watch.opening)return watch.opening;
  const revision=this.revision,operation=this.rpc<{id?:string;signature:string}>({method:watch.kind==='live'?'watchLive':'watchReplica',path:watch.path,args:watch.args,lease:watch.lease}).then(source=>{this.current(revision);watch.source=source;watch.bound=revision;});watch.opening=operation;void operation.then(()=>{if(watch.opening===operation)watch.opening=undefined;},()=>{if(watch.opening===operation)watch.opening=undefined;});return operation;
 }
 private store(watch:Watch,value:Page){
  if(!watch.refs)fail('FENCED','Native watch closed during read');const bytes=sqlitePayloadBytes({key:watch.key,value},262144);watch.unpin?.();watch.unpin=undefined;
  try{this.pages.put(watch.key,value,bytes);}finally{watch.unpin=this.pages.pin(watch.key);}
  watch.stale=false;this.notify(watch);
 }
 private deferredBounded(ref:Pick<FunctionReference,'path'>,args:JsonValue,options:SQLiteWatchOptions,kind:'replica'|'live'){
  sqlitePayloadBytes({path:ref.path,args,view:options.view??null},32768);const path=ref.path,owned=structuredClone(args),view=structuredClone(options.view);let selected:ReturnType<SQLiteBrowserClient['watchBounded']>|undefined,closed=false,refs=0,held=false;const empty:WatchSnapshot={page:undefined,stale:true,version:0};
  const ensure=()=>{if(closed||this.closed)fail('FENCED','Deferred native watch is closed');return selected??=this.watchBounded({path},owned,{view},kind);};
  return {read:<T>(run:(watch:ReturnType<SQLiteBrowserClient['watchBounded']>)=>Promise<T>)=>{if(!refs)held=true;return run(ensure());},snapshot:()=>selected?.snapshot()??empty,subscribe:(listener:()=>void)=>{const watch=ensure(),stop=watch.subscribe(listener);refs++;let released=false;return ()=>{if(released)return;released=true;stop();if(!--refs&&!held){selected?.close();selected=undefined;}};},close:()=>{if(closed)return;closed=true;selected?.close();selected=undefined;}};
 }
 watchLiveWindow(ref:FunctionReference,args:JsonValue={},options:SQLiteWatchOptions={}):Pick<SQLiteReplicaWatch,'page'|'snapshot'|'subscribe'|'close'>&{pendingPage(options?:{offset?:number;limit?:number;generation?:string}):Promise<SQLitePendingLivePage|null>}{
  if(ref.kind!=='query'||ref.delivery!=='live'||ref.scope==='control'||!ref.live)throw new TypeError('Expected a generated tenant Live Query');
  if(options.deferStart){const watch=this.deferredBounded(ref,args,options,'live');return {page:options=>watch.read(value=>value.page(options)),pendingPage:options=>watch.read(value=>value.pendingLive(options)),snapshot:watch.snapshot,subscribe:watch.subscribe,close:watch.close};}
  const watch=this.watchBounded(ref,args,options,'live');return {...watch,pendingPage:watch.pendingLive};
 }
 watchReplica(ref:Pick<FunctionReference,'path'>,args:JsonValue={},options:SQLiteWatchOptions={}):SQLiteReplicaWatch{
  if(options.deferStart){const watch=this.deferredBounded(ref,args,options,'replica');return {page:options=>watch.read(value=>value.page(options)),queryPage:(plan,generation)=>watch.read(value=>value.queryPage(plan,generation)),pendingPage:options=>watch.read(value=>value.pendingReplica(options)),snapshot:watch.snapshot,subscribe:watch.subscribe,close:watch.close};}
  const watch=this.watchBounded(ref,args,options,'replica');return {...watch,pendingPage:watch.pendingReplica};
 }
 private watchBounded(ref:Pick<FunctionReference,'path'>,args:JsonValue,options:{view?:JsonValue},kind:'replica'|'live'){
  sqlitePayloadBytes({path:ref.path,args,view:options.view??null},32768);const key=stable({kind,path:ref.path,args,view:options.view??null});let watch=this.watches.get(key);
  if(!watch){const bytes=sqlitePayloadBytes({key,args,path:ref.path},65536)+2048;if(this.watches.size>=64||this.watchBytes+bytes>524288)fail('BUDGET','Native browser watch budget exhausted');watch={kind,state:{page:undefined,stale:true,version:0},key,lease:crypto.randomUUID(),path:ref.path,args:JSON.parse(stable(args)),bytes,refs:0,bound:-1,listeners:new Set(),stale:true,version:0};this.watches.set(key,watch);this.watchBytes+=bytes;}
  const selected=watch;if(selected.refs>=64)fail('BUDGET','Native watch lease budget exhausted');selected.refs++;let closed=false;const listeners=new Set<()=>void>();void this.open(selected).catch(()=>{});
  const read=async<T extends Page>(method:string,options:unknown):Promise<T>=>{const revision=this.revision,dataRevision=this.dataRevision;if(closed)fail('FENCED','Native watch is closed');await this.open(selected);const value=await this.rpc<T>({method,signature:selected.source!.signature,...options as object});if(closed)fail('FENCED','Native watch closed during read');this.current(revision);if(dataRevision!==this.dataRevision)fail('FENCED','Native watch admission changed');if(value!==null||this.pages.get(selected.key)===undefined)this.store(selected,value);return value;};
  return {queryPage:(plan:SQLiteCollectionPageOptions,generation?:string)=>read<SQLiteCollectionPageResult|null>('collectionPage',{plan,...(generation?{generation}:{})}),page:(options:SQLiteReplicaPageOptions={})=>read<SQLiteNativeWindowResult|null>(kind==='live'?'livePage':'replicaPage',options),pendingReplica:(options:{after?:number;limit?:number;expected?:SQLitePendingSnapshotPage['generation']}={})=>read<SQLitePendingSnapshotPage|null>('replicaPendingPage',options),pendingLive:(options:{offset?:number;limit?:number;generation?:string}={})=>read<SQLitePendingLivePage|null>('livePendingPage',options),snapshot:()=>selected.state,subscribe:(listener:()=>void)=>{if(closed)fail('FENCED','Native watch is closed');if(selected.listeners.size>=32)fail('BUDGET','Native watch listener budget exhausted');const wrapped=()=>listener();listeners.add(wrapped);selected.listeners.add(wrapped);return ()=>{listeners.delete(wrapped);selected.listeners.delete(wrapped);};},close:()=>{if(closed)return;closed=true;for(const listener of listeners)selected.listeners.delete(listener);listeners.clear();if(--selected.refs)return;selected.state={page:undefined,stale:true,version:selected.version+1};selected.unpin?.();selected.unpin=undefined;this.pages.delete(selected.key);
   const retirement=(async()=>{try{await selected.opening;if(selected.refs)return;if(selected.source)await this.rpc({method:selected.kind==='live'?'unwatchLive':'unwatchReplica',signature:selected.source.signature,lease:selected.lease});selected.source=undefined;selected.bound=-1;}finally{if(this.watches.get(selected.key)===selected&&!selected.refs){this.watches.delete(selected.key);this.watchBytes-=selected.bytes;}}})();selected.retiring=retirement;void retirement.then(()=>{if(selected.retiring===retirement)selected.retiring=undefined;},()=>{if(selected.retiring===retirement)selected.retiring=undefined;});}};
 }
 entities(entity:string,ids:readonly string[],expected?:SQLiteNativeEntityResult['generation']):Promise<SQLiteNativeEntityResult>{sqlitePayloadBytes({entity,ids},32768);if(ids.length>128)throw new RangeError('Native entity reads are bounded');return this.rpc({method:'entities',entity,ids,...(expected?{expected}:{})});}
 invokeReducer(ref:Pick<FunctionReference,'path'>,args:JsonValue={},options:{now?:number;intentEntropy?:string;commandId?:string}={}):Promise<SQLiteReducerReceipt>{if(options.commandId&&options.now===undefined)throw new TypeError('Retryable reducers require their original invocation time');const id=options.commandId??crypto.randomUUID(),sent=Date.now();return this.rpc<SQLiteReducerReceipt>({method:'reducer',path:ref.path,args,now:options.now??sent,intentEntropy:options.intentEntropy},id).then(receipt=>{if(receipt.queuedOnly)for(const listener of this.fallbackListeners)try{listener({path:ref.path,intentId:id,phase:'call',reason:'incompleteReplica',error:'Local prediction awaits complete admitted data'});}catch{/* Observer only. */}const received=Date.now();for(const listener of this.telemetryListeners)try{listener({type:'reducer',id,path:ref.path,outcome:'ok',clientSentAtMs:sent,clientReceivedAtMs:received,clientDurationMs:received-sent});}catch{/* Observer only. */}return receipt;});}
 reducer<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args:Args,options:CallOptions&{offline:'queue'}):Promise<T|QueuedReducerOutcome>;
 reducer<T extends JsonValue=JsonValue,Args extends JsonValue=JsonValue>(ref:FunctionReference<Args,T>,args?:Args,options?:CallOptions):Promise<T>;
 async reducer<T extends JsonValue=JsonValue>(ref:FunctionReference,args:JsonValue={},_options:CallOptions={}):Promise<T|QueuedReducerOutcome>{if(this.publicTenant&&(_options.offline==='queue'||_options.optimistic?.length))throw new GonvexClientError('Public reducers are online-only',{code:'disconnected',path:ref.path,operation:'reducer'});const receipt=await this.invokeReducer(ref,args);return receipt.queuedOnly?{status:'queued',reducerId:receipt.commandId}:receipt.result as T;}
 prepareReducer(ref:Pick<FunctionReference,'path'>):Promise<void>{return this.rpc({method:'prepareReducer',path:ref.path});}
 query<T extends JsonValue>(ref:Pick<FunctionReference,'path'>,args:JsonValue={}):Promise<T>{return this.rpc({method:'query',path:ref.path,args});}
 action<T extends JsonValue>(ref:Pick<FunctionReference,'path'>,args:JsonValue={}):Promise<T>{return this.rpc({method:'action',path:ref.path,args});}
 budgets(){return this.ledger.snapshot();}
 close(){if(this.closed)return;this.closed=true;this.revision++;this.controlQueries.close();this.entityView.close();this.reporter?.close();for(const cancel of this.authWaiters)cancel();this.authWaiters.clear();this.releaseChanges();for(const watch of this.watches.values()){watch.refs=0;watch.state={page:undefined,stale:true,version:watch.version+1};watch.unpin?.();watch.listeners.clear();}this.watches.clear();this.watchBytes=0;for(const cancel of this.tokenCancels.values())cancel();this.tokenCancels.clear();this.tokenRequests.clear();this.auth={};this.fetchToken=undefined;this.controlListeners.clear();this.authListeners.clear();this.credentialListeners.clear();this.rejectionListeners.clear();this.supportListeners.clear();this.fallbackListeners.clear();this.telemetryListeners.clear();this.controlsValue=undefined;this.pages.close();this.options.broker.close();}
}
