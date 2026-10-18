import {ByteLedger} from './byte-budget.js';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
import {SQLiteOwnerMaintenance} from './sqlite-owner-maintenance.js';
import type {ClientContract} from './client-upgrades.js';
import type {JsonValue} from '@gonvex/protocol';
import type {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import type {ReadCoverage} from '@gonvex/local-runtime/read-view';
import {control} from './control.js';
import type {FunctionReference,GonvexClientAuth} from './index.js';
import {SQLiteNativeTransport} from './sqlite-native-transport.js';
import type {SQLiteOwnerCaller,SQLiteOwnerProgram,SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {SQLiteWorkerSessions} from './sqlite-worker-sessions.js';
import {SQLiteWorkerTokens} from './sqlite-worker-tokens.js';
import {SQLiteNativeFeed} from './sqlite-native-feed.js';
import {SQLiteReducerOwner} from './sqlite-reducer-owner.js';
import {SQLiteReducerDelivery} from './sqlite-reducer-delivery.js';
import {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {SQLiteLegacyBootstrap} from './sqlite-legacy-bootstrap.js';
import type {LocalReplicaSession} from './local-replica.js';
import {SQLiteWindowStore} from './sqlite-window-store.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
type Auth=Pick<GonvexClientAuth,'project'|'tenant'|'token'|'identity'>;
type Source={id:string;signature:string;ref:FunctionReference;args:JsonValue;tabs:Set<string>;eager:boolean;bytes:number;closing?:Promise<void>};
type ControlSource={signature:string;ref:FunctionReference;args:JsonValue;tabs:Set<string>;value?:JsonValue;error?:string;version:number;bytes:number;resultBytes:number;close():void};
type Connection={authVersion:number;authError?:{id:string;message:string;definitive:boolean;code?:string};developer?:{id:string;token:string;acked:Set<string>};route:Omit<Auth,'token'>;id:string;url:string;identityKey:string;client:SQLiteNativeTransport;feed:SQLiteNativeFeed;delivery:SQLiteReducerDelivery;tabs:Set<string>;sources:Map<string,Source>;sourceBytes:number;controls:Map<string,ControlSource>;controlBytes:number;restoring?:Promise<void>;migrationBlocked?:unknown;restoreScope?:string;restoreGeneration?:number;migrationTimer?:ReturnType<typeof setTimeout>;migrationSession?:LocalReplicaSession;denied?:boolean;admissionKey?:{generation:string;value:Promise<string>};recovered?:boolean;recovering?:Promise<void>;lastScope?:string;reconnectTimer?:ReturnType<typeof setTimeout>;reconnectAttempts?:number;timer?:ReturnType<typeof setTimeout>;failure?:{code:string;message:string}};
type Binding={artifactHash:string;clientContract?:ClientContract;runtime:Pick<PortableReducerRuntime,'prepare'|'execute'|'isPrepared'>;
 /** Generated public refs plus SDK-owned __local collections. No app RPC
  * supplies its own function metadata, authorization, SQL or visibility plan. */
 legacyStorage?:{replicaName:string;outboxName:string};
 references:readonly FunctionReference[];collections:readonly string[];offlineMaxAgeMs:number|null;
};
const stable=(value:JsonValue):string=>value===null||typeof value!=='object'?JSON.stringify(value):Array.isArray(value)?'['+value.map(stable).join(',')+']':'{'+Object.keys(value).sort().map(key=>JSON.stringify(key)+':'+stable(value[key]!)).join(',')+'}';
async function digest(value:string){return Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(value))),byte=>byte.toString(16).padStart(2,'0')).join('');}
function text(value:unknown,max:number):asserts value is string{if(typeof value!=='string'||!value||value.length>max)throw new TypeError('Invalid native client identifier');}
/** Generated worker program. All server rows and reducer execution stay in the
 * elected dedicated worker. Main-thread calls select generated refs and bounded
 * pages; authenticated scope/identity comes exclusively from the session registry.
 * This assembly is not installed by codegen until the browser facade and verified
 * migration/fallback contract are complete. */
export class SQLiteClientProgram implements SQLiteOwnerProgram {
 private readonly safety=new SQLiteSafetyJournal();private readonly sessions:SQLiteWorkerSessions;private readonly tokens:SQLiteWorkerTokens;
 private readonly reducer:SQLiteReducerOwner;private readonly windows:SQLiteWindowStore;
 private readonly references=new Map<string,FunctionReference>();private readonly connections=new Map<string,Connection>();private readonly tabs=new Map<string,string>();
 private readonly maintenance:SQLiteOwnerMaintenance;private readonly legacy:SQLiteLegacyBootstrap;
 private readonly ledger=new ByteLedger(20971520);private readonly transient=new SQLiteTransientBudget();private readonly transport=new SQLiteTransientBudget(2097152,256);private readonly transportAuth=new SQLiteTransientBudget(524288,8);private readonly catalogBytes:number;
 private connecting=Promise.resolve();private closed=false;
 constructor(private readonly owner:SQLiteOwnerProgramContext,private readonly binding:Binding){
  text(binding.artifactHash,256);sqlitePayloadBytes(binding.clientContract??{},65536);this.catalogBytes=sqlitePayloadBytes({references:binding.references,collections:binding.collections},2097152)+binding.references.length*64;if(this.catalogBytes>2097152)fail('BUDGET','Generated native function catalog exceeds its metadata allowance');if(binding.references.length>4096||binding.collections.length>4096)fail('BUDGET','Generated native function catalog exceeds budget');
  const builtins:FunctionReference[]=[];const collect=(value:unknown):void=>{if(!value||typeof value!=='object')return;if('kind' in value&&'path' in value){builtins.push(value as FunctionReference);return;}for(const child of Object.values(value))collect(child);};collect(control);
  for(const ref of [...builtins,...binding.references]){if(this.references.size>=4096)fail('BUDGET','Native function catalog entry limit exceeded');text(ref.path,1024);if(this.references.has(ref.path))throw new TypeError('Duplicate generated native reference');this.references.set(ref.path,ref);}
  this.catalogBytes+=sqlitePayloadBytes(builtins,262144)+builtins.length*64;if(this.catalogBytes>2097152)fail('BUDGET','Native function catalog exceeds its metadata allowance');
  for(const path of binding.collections){const ref=this.references.get(path);if(ref?.delivery!=='replica'||!ref.replica)throw new TypeError('Generated native dependency is not a Replica Collection');}
  this.sessions=new SQLiteWorkerSessions({artifactHash:binding.artifactHash,offlineMaxAgeMs:binding.offlineMaxAgeMs,database:owner.database,canonical:owner.canonical,safety:this.safety,runOwner:owner.runOwner,fence:()=>owner.engine.verifyOwner()});
  this.windows=new SQLiteWindowStore(owner.database,()=>owner.engine.verifyOwner());
  this.tokens=new SQLiteWorkerTokens({recipient:(id,key,exclude)=>[...(this.connections.get(id)?.tabs??[])].find(tab=>tab!==exclude&&this.tabs.get(tab)===id&&this.connections.get(id)?.identityKey===key),eligible:(tab,id,key)=>this.tabs.get(tab)===id&&this.connections.get(id)?.identityKey===key,notifyTab:tab=>owner.notifyTab(tab)});
  this.reducer=new SQLiteReducerOwner(owner,{runtime:binding.runtime,artifactHash:binding.artifactHash,resolveTab:tab=>this.sessions.resolveTab(tab,true),context:scope=>this.context(scope),persistIntent:async(input,context,prepared)=>{
   const saved=await owner.canonical.loadSession(context.intentScope);if(!saved||saved.directive.visibilityScope!==input.scope||saved.artifactHash!==binding.artifactHash)fail('FENCED','Intent recovery session changed');await this.safety.appendPrepared(input,prepared,saved,context.intentScope);
  }});
  this.legacy=new SQLiteLegacyBootstrap({owner,journal:this.reducer.journal,safety:this.safety,artifactHash:binding.artifactHash,replicaName:binding.legacyStorage?.replicaName??'gonvex-local-replica',outboxName:binding.legacyStorage?.outboxName??'gonvex-outbox'});
  this.ledger.register('native-function-catalog',()=>({name:'native-function-catalog',bytes:this.catalogBytes,entries:this.references.size,maxBytes:2097152,maxEntries:4096,evictions:0,refusals:0}));
  this.ledger.register('native-source-interests',()=>({name:'native-source-interests',bytes:[...this.connections.values()].reduce((sum,value)=>sum+value.sourceBytes,0),entries:[...this.connections.values()].reduce((sum,value)=>sum+value.sources.size,0),maxBytes:2097152,maxEntries:1024,evictions:0,refusals:0}));
  this.ledger.register('native-control-watches',()=>({name:'native-control-watches',bytes:[...this.connections.values()].reduce((sum,connection)=>sum+connection.controlBytes,0),entries:[...this.connections.values()].reduce((sum,connection)=>sum+connection.controls.size,0),maxBytes:2097152,maxEntries:256,evictions:0,refusals:0}));
  this.ledger.register('native-source-descriptors',()=>({name:'native-source-descriptors',bytes:[...this.connections.values()].reduce((sum,value)=>sum+value.feed.stats().sourceBytes,0),entries:[...this.connections.values()].reduce((sum,value)=>sum+value.feed.stats().sources,0),maxBytes:2097152,maxEntries:1024,evictions:0,refusals:0}));
  this.ledger.register('native-transient',()=>this.transient.stats());
  this.ledger.register('native-transport',()=>({...this.transport.stats(),name:'native-transport'}));
  this.ledger.register('native-transport-auth',()=>({...this.transportAuth.stats(),name:'native-transport-auth'}));
  this.ledger.register('native-intent-deliveries',()=>({name:'native-intent-deliveries',bytes:[...this.connections.values()].reduce((sum,value)=>sum+value.delivery.stats().bytes,0),entries:[...this.connections.values()].reduce((sum,value)=>sum+value.delivery.stats().entries,0),maxBytes:2621440,maxEntries:64,evictions:0,refusals:0}));
  for(const snapshot of this.sessions.budgets())this.ledger.register(snapshot.name,()=>this.sessions.budgets().find(value=>value.name===snapshot.name)!);this.ledger.register('worker-token-requests',()=>this.tokens.stats());
  this.ledger.register('native-connections',()=>({name:'native-connections',bytes:this.connections.size*8192+this.tabs.size*512+[...this.connections.values()].reduce((sum,connection)=>sum+(connection.developer?.token.length??0)*2,0),entries:this.connections.size+this.tabs.size,maxBytes:360448,maxEntries:72,evictions:0,refusals:0}));
  this.maintenance=new SQLiteOwnerMaintenance(owner,error=>{for(const connection of this.connections.values()){connection.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};this.notify(connection);}});this.maintenance.start();
 }
 private ref(path:unknown,kind?:string){text(path,1024);const ref=this.references.get(path);if(!ref||kind&&ref.kind!==kind||ref.authorization==='internal')fail('INVALID_ROW','Unknown generated native function');return ref;}
 private connection(tab:string){const id=this.tabs.get(tab),connection=id&&this.connections.get(id);if(!connection)fail('FENCED','Tab has no native connection');return connection;}
 private notify(connection:Connection){for(const tab of connection.tabs)void this.owner.notifyTab(tab).catch(()=>{});}
 private admission(connection:Connection,write=false){for(const tab of connection.tabs){try{return this.sessions.resolveTab(tab,write);}catch{/* Another tab may still have a current admission. */}}return undefined;}
 private async context(scope:string){
  for(const connection of this.connections.values()){
   const admitted=this.admission(connection);if(admitted?.scope!==scope)continue;const saved=await this.owner.canonical.loadSession(admitted.intentScope);if(!saved||saved.directive.visibilityScope!==scope)continue;
   const coverage:Record<string,ReadCoverage[string]>={};
   const rows=await this.owner.database.getAllAsync<{entity:string;key:string;columns:string}>(`SELECT json_extract(g.header,'$.entity') entity,json_extract(g.header,'$.key') key,json_group_array(DISTINCT c.value) columns
    FROM gv_windows w JOIN gv_window_generations g USING(scope,signature,generation),json_each(g.header,'$.columns') c
    WHERE w.scope=? AND json_extract(g.header,'$.entityCoverage')='complete' AND json_extract(g.header,'$.completeness')='complete'
     AND json_extract(g.header,'$.cursor.epoch')=? AND json_extract(g.header,'$.artifactHash')=?
     AND json_extract(g.header,'$.cursor.revision')>=coalesce((SELECT applied FROM gv_native_feed_progress p WHERE p.scope=w.scope AND p.epoch=json_extract(g.header,'$.cursor.epoch')),0) GROUP BY json_extract(g.header,'$.entity'),json_extract(g.header,'$.key')`,scope,saved.directive.epoch,this.binding.artifactHash);
   for(const row of rows)coverage[row.entity]={key:row.key,complete:true,columns:JSON.parse(row.columns) as string[]};
   return {intentScope:admitted.intentScope,identity:admitted.identity,coverage};
  }fail('FENCED','Native reducer scope is not admitted');
 }
 private recover(connection:Connection):Promise<void>{
  if(connection.recovered)return Promise.resolve();if(connection.recovering)return connection.recovering;
  const operation=(async()=>{
   const admitted=this.admission(connection);if(!admitted)return;let after=0;
   for(;;){
    const page=await this.safety.page(admitted.intentScope,after,8);
    for(const entry of page.entries){
     const prepared=entry.prepared,legacy=entry.legacy;if(!prepared&&!legacy)fail('SCHEMA_MISMATCH','Pending command has no prepared recovery envelope');
     await this.owner.runOwner(262144,()=>this.owner.database.withTransactionAsync(async()=>{const current=this.admission(connection);if(!current||current.scope!==admitted.scope||current.intentScope!==admitted.intentScope||current.generation!==admitted.generation)fail('FENCED','Recovery admission changed');if(prepared)await this.reducer.core.recover({...entry.invocation,scope:admitted.scope},prepared);else await this.reducer.core.recoverLegacy({...entry.invocation,scope:admitted.scope},legacy!);if(legacy?.state==='rejected'){const imported=await this.reducer.journal.find(admitted.intentScope,entry.invocation.commandId);if(imported){const prior=await this.owner.database.getFirstAsync<{status:string;intent_id:number}>('SELECT status,intent_id FROM gv_reducer_delivery_settlements WHERE scope=? AND command=?',admitted.intentScope,entry.invocation.commandId);if(prior&&(prior.status!=='rejected'||prior.intent_id!==imported.id))fail('INTENT_CONFLICT','Legacy rejection conflicts with authoritative settlement');await this.owner.database.runAsync("INSERT OR IGNORE INTO gv_reducer_delivery_settlements(scope,command,intent_id,status) VALUES(?,?,?,'rejected')",admitted.intentScope,entry.invocation.commandId,imported.id);}}const latest=this.admission(connection);if(!latest||latest.generation!==admitted.generation)fail('FENCED','Recovery admission changed');}),[admitted.scope]);
    }
    if(page.nextSequence===null)break;after=page.nextSequence;
   }await this.reducer.rebase(admitted.scope);connection.recovered=true;
  })();connection.recovering=operation;void operation.then(()=>{connection.recovering=undefined;},()=>{connection.recovering=undefined;});return operation;
 }
 private checkCaller(caller:SQLiteOwnerCaller,connection:Connection,generation?:string){
  if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id)fail('FENCED','Native caller detached');const current=this.sessions.resolveTab(caller.tabID);if(generation!==undefined&&current.generation!==generation)fail('FENCED','Native read admission changed');return current;
 }
 private async readable(connection:Connection,admitted:ReturnType<SQLiteWorkerSessions['resolveTab']>){
  await this.recover(connection);const saved=await this.owner.runOwner(0,()=>this.owner.canonical.loadSession(admitted.intentScope));if(!saved||saved.directive.visibilityScope!==admitted.scope)fail('FENCED','Cached read session changed');await connection.feed.assertReadable({scope:admitted.scope,epoch:saved.directive.epoch});
 }
 private feedFailed(connection:Connection,error:unknown){
  const code=String((error as any)?.code??'SQLITE');connection.failure={code,message:String((error as any)?.message??error).slice(0,512)};connection.client.close();this.notify(connection);
  if(!['PAGE_CHANGED','INVALID_ROW','BUDGET','INTENT_CONFLICT'].includes(code)||this.closed||this.connections.get(connection.id)!==connection)return;
  clearTimeout(connection.reconnectTimer);const attempts=Math.min(6,(connection.reconnectAttempts??0)+1);connection.reconnectAttempts=attempts;
  connection.reconnectTimer=setTimeout(()=>{connection.reconnectTimer=undefined;if(!this.closed&&this.connections.get(connection.id)===connection&&connection.tabs.size)connection.client.connect();},Math.min(60000,1000*2**(attempts-1)));
 }
 private scheduleDelivery(connection:Connection,delay=0){
  if(this.closed||this.connections.get(connection.id)!==connection)return;if(connection.timer)clearTimeout(connection.timer);
  connection.timer=setTimeout(()=>{connection.timer=undefined;void this.recover(connection).then(()=>connection.delivery.drainOnce()).then(result=>{this.notify(connection);if(result.nextDelay!==null)this.scheduleDelivery(connection,result.nextDelay);},error=>{if(this.closed)return;connection.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};this.notify(connection);this.scheduleDelivery(connection,1000);});},Math.min(60000,Math.max(0,delay)));
 }
 private scheduleMigration(connection:Connection,delay=0){
  clearTimeout(connection.migrationTimer);if(this.closed||!connection.migrationSession||this.connections.get(connection.id)!==connection)return;
  connection.migrationTimer=setTimeout(()=>{connection.migrationTimer=undefined;const admitted=this.admission(connection),session=connection.migrationSession;if(!admitted||!session)return;if(connection.restoring){this.scheduleMigration(connection,100);return;}
   void this.legacy.step(admitted.intentScope,session).then(result=>{if(!result.verified)this.scheduleMigration(connection,100);},error=>{if(this.closed||this.connections.get(connection.id)!==connection)return;connection.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};this.notify(connection);});
  },delay);
 }
 private async watchControl(connection:Connection,ref:FunctionReference,args:JsonValue,tab:string){
  if(ref.scope!=='control'||ref.kind!=='query'||ref.delivery==='replica')fail('INVALID_ROW','Expected a generated Control query');
  const encoded=stable(args),signature=await digest(connection.id+'\0'+ref.path+'\0'+encoded);if(this.tabs.get(tab)!==connection.id)fail('FENCED','Control watch caller detached');
  let source=connection.controls.get(signature);if(!source){const bytes=sqlitePayloadBytes({path:ref.path,args},16384)+512;if(connection.controls.size>=32||connection.controlBytes+bytes+64>262144)fail('BUDGET','Control watch allowance exceeded');
   source={signature,ref,args:JSON.parse(encoded),tabs:new Set(),version:0,bytes,resultBytes:0,close:()=>{}};const selected=source;
   source.close=connection.client.watchControl(ref,source.args,value=>{
    if(this.connections.get(connection.id)!==connection||connection.controls.get(signature)!==selected)return;
    let resultBytes:number;try{resultBytes=sqlitePayloadBytes(value,131072);if(connection.controlBytes-selected.resultBytes+resultBytes>262144)fail('BUDGET','Control result allowance exceeded');}catch{selected.error='Control result exceeds its byte budget';selected.version++;this.notify(connection);return;}
    connection.controlBytes+=resultBytes-selected.resultBytes;selected.resultBytes=resultBytes;selected.value='result'in value?value.result:undefined;selected.error='error'in value?value.error:undefined;selected.version++;this.notify(connection);
   });connection.controls.set(signature,source);connection.controlBytes+=bytes;
  }
  if(!source.tabs.has(tab)){if(connection.controlBytes+64>262144)fail('BUDGET','Control interest allowance exceeded');source.tabs.add(tab);connection.controlBytes+=64;}return {signature};
 }
 private unwatchControl(connection:Connection,source:ControlSource,tab:string){
  if(!source.tabs.delete(tab))return;connection.controlBytes-=64;if(source.tabs.size)return;connection.controls.delete(source.signature);connection.controlBytes-=source.bytes+source.resultBytes;source.close();
 }
 private async addSource(connection:Connection,ref:FunctionReference,args:JsonValue,tab?:string,eager=false):Promise<Source>{
  if(ref.delivery!=='replica'||!ref.replica)fail('INVALID_ROW','Expected a generated Replica Collection');const encoded=stable(args),signature=await digest(this.binding.artifactHash+'\0'+connection.id+'\0'+ref.path+'\0'+encoded);let source=connection.sources.get(signature);
  if(source?.closing){await source.closing.catch(()=>{});return this.addSource(connection,ref,args,tab,eager);}
  if(tab&&this.tabs.get(tab)!==connection.id)fail('FENCED','Native source caller detached');
  if(!source){
   const bytes=sqlitePayloadBytes({path:ref.path,args},16384)+1024;if(connection.sources.size>=128||connection.sourceBytes+bytes+(tab?64:0)>262144)fail('BUDGET','Native subscription allowance exceeded');
   source={id:'source:'+signature,signature,ref,args:JSON.parse(encoded),tabs:new Set(),eager,bytes};
   const completeEntity=!Object.keys(ref.replica.equalFilters??{}).length&&!ref.replica.excludeWhenSet?.length;
   connection.feed.register({id:source.id,signature,entity:ref.replica.table,key:ref.replica.key,path:ref.path,...(completeEntity?{completeEntity:true}:{}),...(ref.replica.columns?{columns:[...ref.replica.columns]}:{})});
   connection.sources.set(signature,source);connection.sourceBytes+=bytes;
   if(connection.client.localSessionState()==='authenticated')this.openSource(connection,source);
  }
  if(tab&&!source.tabs.has(tab)){if(connection.sourceBytes+64>262144)fail('BUDGET','Native source interests exceed budget');source.tabs.add(tab);connection.sourceBytes+=64;}source.eager||=eager;return source;
 }
 private closeSource(connection:Connection,source:Source):Promise<void>{
  if(source.closing)return source.closing;
  source.closing=connection.feed.closeSource(source.id).catch(error=>{if((error as any)?.code!=='FENCED')throw error;}).finally(()=>{if(connection.sources.get(source.signature)===source){connection.sources.delete(source.signature);connection.sourceBytes-=source.bytes;}});return source.closing;
 }
 private openSource(connection:Connection,source:Source){connection.client.sendOwnerMessage({type:'replica.open',id:source.id,path:source.ref.path,args:source.args});}
 private connect(input:any,caller:SQLiteOwnerCaller):Promise<unknown>{
  const work=this.connecting.then(async()=>{
   if(this.closed||caller.signal.aborted)fail('FENCED','Native client is closed');if(input.artifactHash!==undefined&&input.artifactHash!==this.binding.artifactHash)fail('SCHEMA_MISMATCH','Browser and owner module artifacts differ');await this.maintenance.ready();text(input.url,2048);const url=new URL(input.url);if(!['ws:','wss:'].includes(url.protocol)||url.username||url.password||url.hash)throw new TypeError('Invalid native WebSocket URL');
   const auth=input.auth??{};sqlitePayloadBytes(auth,65536);for(const field of Object.keys(auth))if(!['project','tenant','token','identity'].includes(field))throw new TypeError('Unsupported native auth field');for(const field of ['project','tenant'])if(auth[field]!==undefined&&(typeof auth[field]!=='string'||auth[field].length>512))throw new TypeError('Invalid native tenant routing');if(auth.identity!==undefined){text(auth.identity?.sub,512);if(auth.identity.iss!==undefined)text(auth.identity.iss,512);if(Object.keys(auth.identity).some(field=>!['sub','iss'].includes(field)))throw new TypeError('Invalid external identity hint');}if(auth.token!==undefined&&(typeof auth.token!=='string'||auth.token.length>16384))throw new TypeError('Invalid native token');
   const identityKey=await digest(stable({url:url.href,project:auth.project??null,tenant:auth.tenant??null,identity:auth.identity??null,credential:auth.identity?null:auth.token?await digest(auth.token):null})),id='connection:'+identityKey;
   const old=this.tabs.get(caller.tabID);if(old&&old!==id)this.detachTab(caller.tabID);
   let connection=this.connections.get(id);
   if(!connection){
    if(this.connections.size>=8)fail('BUDGET','Native connection allowance exceeded');this.sessions.open(id);
    let record!:Connection;
    const feed=new SQLiteNativeFeed({owner:this.owner,sessions:this.sessions,connection:id,artifactHash:this.binding.artifactHash,reserve:bytes=>this.transient.reserve(bytes),prepare:path=>this.reducer.core.prepare(path),authority:(scope,mutate)=>this.reducer.core.rebase(scope,mutate).then(()=>{}),send:message=>record.client.sendOwnerMessage(message),advanceWatermark:revision=>{record.reconnectAttempts=0;record.failure=undefined;record.client.advanceOwnerWatermark(revision);this.notify(record);},failed:error=>this.feedFailed(record,error)});
    const restore=(scope:string,generation:number)=>{
     feed.authenticating(scope,generation);record.restoreScope=scope;record.restoreGeneration=generation;
     const current=()=>!this.closed&&this.connections.get(id)===record&&record.restoreScope===scope&&record.restoreGeneration===generation;
     const prior=record.restoring;
     const restoring=(async()=>{await prior?.catch(()=>{});if(!current())return;
      const saved=await this.safety.session(scope)??await this.owner.runOwner(0,()=>this.owner.canonical.loadSession(scope));
      let selected:LocalReplicaSession|undefined;
      try{selected=await this.legacy.prepare(scope,saved?.artifactHash===this.binding.artifactHash?saved:undefined,current);}catch(error){if((error as any)?.code!=='SCHEMA_MISMATCH')throw error;}
      if(!current())return;
      if(selected){record.migrationSession=selected;record.recovered=false;}
      try{await this.sessions.restore(id,current);}catch(error){if((error as any)?.code!=='SCHEMA_MISMATCH')throw error;}
     })();record.restoring=restoring;void restoring.then(()=>{if(record.restoring===restoring){record.restoring=undefined;record.migrationBlocked=undefined;}this.scheduleMigration(record);this.notify(record);},error=>{record.migrationBlocked=error;record.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};this.notify(record);});
    };
    const host={opening:restore,authenticating:restore,session:async(session:any,current:()=>boolean)=>{
     await record.restoring?.catch(()=>{});const previous=this.admission(record);if(previous&&JSON.stringify(previous.identity)!==JSON.stringify(session.identity))record.authVersion++;
     const cutover=(async()=>{await feed.session(session,current);
      if(session.directive&&session.identity){const saved=await this.owner.runOwner(0,()=>this.owner.canonical.loadSession(session.intentScope));if(!saved)fail('FENCED','Native session disappeared before legacy cutover');await this.legacy.prepare(session.intentScope,saved,current);record.migrationSession=saved;}
      record.denied=false;record.recovered=false;await this.recover(record);
      if(record.lastScope&&record.lastScope!==session.directive?.visibilityScope)void this.maintenance.retire(record.lastScope,[...record.sources.values()].map(source=>source.id)).catch(()=>{});record.lastScope=session.directive?.visibilityScope;
      if(!session.directive)record.failure=undefined;
      if(session.directive&&session.identity){for(const source of [...record.sources.values()].sort((a,b)=>Number(!!b.tabs.size)-Number(!!a.tabs.size)))if(!source.closing)this.openSource(record,source);this.scheduleDelivery(record);this.scheduleMigration(record);}
      this.notify(record);
     })();record.restoring=cutover;try{await cutover;record.migrationBlocked=undefined;}catch(error){record.migrationBlocked=error;throw error;}finally{if(record.restoring===cutover)record.restoring=undefined;}
    },binary:(frame:ArrayBuffer,generation:number)=>feed.binary(frame,generation),frame:(message:any,generation:number)=>feed.frame(message,generation),quarantined:(generation:number)=>{record.denied=true;for(const source of record.controls.values())if(source.ref.authorization!=='public'){record.controlBytes-=source.resultBytes;source.resultBytes=0;source.value=undefined;source.error='Session unavailable';source.version++;}feed.quarantined(generation);this.notify(record);},authError:(message:string,details:{definitive:boolean;code?:string})=>{record.authError={id:crypto.randomUUID(),message:message.slice(0,512),...details,...(details.code?{code:details.code.slice(0,128)}:{})};this.notify(record);},developerCredential:(token:string)=>{record.developer={id:crypto.randomUUID(),token,acked:new Set()};this.notify(record);},disconnected:()=>{feed.disconnected();this.notify(record);},failed:(error:unknown)=>feed.failed(error)};
    const client=new SQLiteNativeTransport(url.href,{...auth,reserve:bytes=>this.transport.reserve(bytes),releaseAuth:this.transportAuth.reserve(65536),clientContract:this.binding.clientContract,ownerHost:host,fetchToken:request=>this.tokens.fetch(id,identityKey,request.forceRefreshToken)});
    const delivery=new SQLiteReducerDelivery({owner:this.owner,reducer:this.reducer,safety:this.safety,admission:()=>{const current=this.admission(record);return current&&{scope:current.scope,intentScope:current.intentScope,generation:current.generation,online:current.online};},send:entry=>client.callOwnerReducer({...this.ref(entry.path,'reducer'),artifactHash:entry.localExecution?.artifactHash??this.binding.artifactHash,intentEntropy:entry.localExecution?.intentEntropy},entry.args as JsonValue,entry.idempotencyKey),updateRequired:message=>{record.failure={code:'SCHEMA_MISMATCH',message};this.notify(record);}});
    record=connection={authVersion:0,route:{project:auth.project,tenant:auth.tenant,identity:auth.identity},id,url:url.href,identityKey,client,feed,delivery,tabs:new Set(),sources:new Map(),sourceBytes:0,controls:new Map(),controlBytes:0};this.connections.set(id,record);
    for(const path of this.binding.collections){const ref=this.ref(path);if(ref.replica?.maxRows!==undefined&&ref.replica.maxRows>0&&ref.replica.maxRows<=1000)await this.addSource(record,ref,{},undefined,true);}
   }
   if(this.tabs.size>=64&&!this.tabs.has(caller.tabID))fail('BUDGET','Native tab allowance exceeded');this.tabs.set(caller.tabID,id);connection.tabs.add(caller.tabID);this.sessions.attachTab(caller.tabID,id);
   connection.client.connect();return {connection:id,identityKey};
  });this.connecting=work.then(()=>{},()=>{});return work;
 }
 async call(value:unknown,caller:SQLiteOwnerCaller):Promise<unknown>{
  sqlitePayloadBytes(value,131072);if(!value||typeof value!=='object'||Array.isArray(value))throw new TypeError('Invalid native client request');const input=structuredClone(value) as any;if(this.closed||caller.signal.aborted)fail('FENCED','Native client is closed');
  if(input.method==='connect')return this.connect(input,caller);
  const connection=this.connection(caller.tabID);if(['entities','replicaPage','replicaPendingPage','reducer'].includes(input.method)){await connection.restoring;if(connection.migrationBlocked)throw connection.migrationBlocked;}
  switch(input.method){
   case 'controls':{
    const admitted=this.admission(connection);let key:string|null|undefined;
    if(admitted){
     if(connection.admissionKey?.generation!==admitted.generation){
      const encoded=stable({scope:admitted.scope,identity:admitted.identity as JsonValue,artifact:this.binding.artifactHash}),release=this.transient.reserve(encoded.length*8+1024);
      const value=digest(encoded).finally(release);connection.admissionKey={generation:admitted.generation,value};void value.catch(()=>{if(connection.admissionKey?.value===value)connection.admissionKey=undefined;});
     }
     key=await connection.admissionKey!.value;
     if(this.admission(connection)?.generation!==admitted.generation)fail('FENCED','Control admission changed');
    }else if(connection.denied||!connection.restoring)key=null;
    return {tokens:this.tokens.poll(caller.tabID),status:connection.client.connectionState(),session:admitted?admitted.online?'authenticated':'restored':connection.client.localSessionState(),...(key!==undefined?{admissionKey:key}:{}),failure:connection.failure??null,authError:connection.authError??null,...(connection.developer&&!connection.developer.acked.has(caller.tabID)?{developerCredential:{id:connection.developer.id,token:connection.developer.token}}:{})};
   }
   case 'credentialAck':if(connection.developer&&input.id===connection.developer.id)connection.developer.acked.add(caller.tabID);return null;
   case 'watchControl':return this.watchControl(connection,this.ref(input.path,'query'),input.args??{},caller.tabID);
   case 'unwatchControl':{const source=connection.controls.get(input.signature);if(source)this.unwatchControl(connection,source,caller.tabID);return null;}
   case 'controlState':{const source=connection.controls.get(input.signature);if(!source?.tabs.has(caller.tabID))fail('FENCED','Tab does not own that Control watch');if(input.afterVersion===source.version)return {version:source.version};return {version:source.version,...(source.value!==undefined?{result:source.value}:{}),...(source.error?{error:source.error}:{})};}
   case 'token':this.tokens.respond(caller.tabID,input.response);return null;
   case 'auth':{
    const auth=input.auth??{};sqlitePayloadBytes(auth,65536);if(Object.keys(auth).some(field=>!['token'].includes(field)))throw new TypeError('Native identity changes require a new connection');if(auth.token!==undefined&&(typeof auth.token!=='string'||auth.token.length>16384))throw new TypeError('Invalid native token');if(!connection.route.identity)return this.connect({url:connection.url,auth:{...connection.route,...auth}},caller);connection.authVersion++;this.tokens.invalidate(connection.id);connection.client.setAuth(auth);return null;
   }
   case 'watchReplica':{const source=await this.addSource(connection,this.ref(input.path,'query'),input.args??{},caller.tabID);return {id:source.id,signature:source.signature};}
   case 'unwatchReplica':{text(input.signature,128);const source=connection.sources.get(input.signature);if(source){if(source.tabs.delete(caller.tabID))connection.sourceBytes-=64;if(!source.tabs.size&&!source.eager){await this.closeSource(connection,source);}}return null;}
   case 'entities':{
    const admission=this.checkCaller(caller,connection);await this.readable(connection,admission);return this.owner.runOwner(131072,async()=>{this.checkCaller(caller,connection,admission.generation);const result=await this.owner.reader.entities({scope:admission.scope,entity:input.entity,ids:input.ids,expected:input.expected});this.checkCaller(caller,connection,admission.generation);return result;});
   }
   case 'replicaPendingPage':{
    text(input.signature,128);const source=connection.sources.get(input.signature);if(!source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Tab does not own that source');const admitted=this.checkCaller(caller,connection),pending=await connection.feed.pendingSource(source.id);if(!pending)return null;
    return this.owner.runOwner(131072,async()=>{const current=()=>{this.checkCaller(caller,connection,admitted.generation);return connection.sources.get(source.signature)===source&&(source.eager||source.tabs.has(caller.tabID));};if(!current())fail('FENCED','Pending source detached');if(!await this.owner.database.getFirstAsync('SELECT 1 FROM gv_snapshot_stages WHERE scope=? AND subscription=? AND snapshot=? AND discarded=0',admitted.scope,pending.subscription,pending.snapshot)){if(input.expected)fail('PAGE_CHANGED','Pending source was replaced');return null;}if(await this.owner.database.getFirstAsync('SELECT 1 FROM gv_snapshot_activation_receipts WHERE scope=? AND subscription=? AND snapshot=?',admitted.scope,pending.subscription,pending.snapshot)){if(input.expected)fail('PAGE_CHANGED','Pending source has become canonical');return null;}return this.owner.pendingReader.page({scope:admitted.scope,...pending,after:input.after,limit:input.limit,expected:input.expected},current);});
   }
   case 'replicaPage':{
    text(input.signature,128);const source=connection.sources.get(input.signature);if(!source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Tab does not own that source');const admission=this.checkCaller(caller,connection);await this.readable(connection,admission);return this.owner.runOwner(131072,async()=>{this.checkCaller(caller,connection,admission.generation);const result=await this.owner.reader.window({scope:admission.scope,signature:source.signature,offset:input.offset,limit:input.limit,generation:input.generation,expected:input.expected});this.checkCaller(caller,connection,admission.generation);if(connection.sources.get(source.signature)!==source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Native source was detached');return result??null;});
   }
   case 'reducer':{const ref=this.ref(input.path,'reducer');if(ref.scope==='control'){const version=connection.authVersion,result=await connection.client.callOwnerReducer(ref,input.args??{},caller.requestID);if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id||version!==connection.authVersion)fail('FENCED','Control caller session changed');sqlitePayloadBytes(result,131072);return {commandId:caller.requestID,result};}await this.recover(connection);if(ref.localExecution!==1)fail('INVALID_ROW','Native reducer has no generated portable body');let receipt;try{receipt=await this.reducer.invoke({path:ref.path,args:input.args??{},now:input.now,intentEntropy:input.intentEntropy},caller);}catch(error){connection.recovered=false;throw error;}this.scheduleDelivery(connection);return receipt;}
   case 'query':case 'action':{
    const ref=this.ref(input.path,input.method==='query'?'query':'action');if(ref.delivery==='replica'||ref.delivery==='live'&&ref.scope!=='control')fail('INVALID_ROW','Reactive reads require bounded native subscriptions');const version=connection.authVersion;if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id)fail('FENCED','Remote caller detached');const result=await (input.method==='query'?connection.client.query(ref,input.args??{}):connection.client.action(ref,input.args??{}));if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id||version!==connection.authVersion)fail('FENCED','Remote caller session changed');sqlitePayloadBytes(result,131072);return result;
   }
   case 'status':return {connection:connection.client.connectionState(),session:connection.client.localSessionState(),admission:this.admission(connection)??null,failure:connection.failure??null};
   default:fail('INVALID_ROW','Unknown native client operation');
  }
 }
 detachTab(tabID:string){const id=this.tabs.get(tabID);if(!id)return;this.tabs.delete(tabID);this.sessions.detachTab(tabID);const connection=this.connections.get(id);if(!connection)return;connection.tabs.delete(tabID);connection.developer?.acked.delete(tabID);this.tokens.detachTab(tabID);
  for(const source of [...connection.controls.values()])this.unwatchControl(connection,source,tabID);
  for(const source of connection.sources.values())if(source.tabs.delete(tabID)){connection.sourceBytes-=64;if(!source.tabs.size&&!source.eager)void this.closeSource(connection,source).catch(()=>{});}
  if(!connection.tabs.size){if(connection.lastScope)void this.maintenance.retire(connection.lastScope,[...connection.sources.values()].map(source=>source.id)).catch(()=>{});if(connection.timer)clearTimeout(connection.timer);clearTimeout(connection.reconnectTimer);clearTimeout(connection.migrationTimer);this.tokens.invalidate(id);connection.delivery.close();connection.client.dispose();connection.feed.close();this.sessions.close(id);this.connections.delete(id);}
 }
 budgets(){return this.ledger.snapshot();}
 close(){if(this.closed)return;this.closed=true;this.maintenance.close();for(const tab of [...this.tabs.keys()])this.detachTab(tab);this.tokens.close();this.legacy.close();this.safety.close();}
}
