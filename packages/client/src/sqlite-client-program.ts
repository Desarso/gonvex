import {SQLiteOwnerMaintenance} from './sqlite-owner-maintenance.js';
import type {JsonValue} from '@gonvex/protocol';
import type {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import type {ReadCoverage} from '@gonvex/local-runtime/read-view';
import {GonvexClient,type FunctionReference,type GonvexClientAuth} from './index.js';
import type {SQLiteOwnerCaller,SQLiteOwnerProgram,SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {SQLiteWorkerSessions} from './sqlite-worker-sessions.js';
import {SQLiteWorkerTokens} from './sqlite-worker-tokens.js';
import {SQLiteNativeFeed} from './sqlite-native-feed.js';
import {SQLiteReducerOwner} from './sqlite-reducer-owner.js';
import {SQLiteReducerDelivery} from './sqlite-reducer-delivery.js';
import {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {SQLiteWindowStore} from './sqlite-window-store.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
type Auth=Pick<GonvexClientAuth,'project'|'tenant'|'token'|'identity'>;
type Source={id:string;signature:string;ref:FunctionReference;args:JsonValue;tabs:Set<string>;eager:boolean;bytes:number;closing?:Promise<void>};
type Connection={id:string;url:string;identityKey:string;client:GonvexClient;feed:SQLiteNativeFeed;delivery:SQLiteReducerDelivery;tabs:Set<string>;sources:Map<string,Source>;sourceBytes:number;lastScope?:string;timer?:ReturnType<typeof setTimeout>;failure?:{code:string;message:string}};
type Binding={artifactHash:string;runtime:Pick<PortableReducerRuntime,'prepare'|'execute'|'isPrepared'>;
 /** Generated public refs plus SDK-owned __local collections. No app RPC
  * supplies its own function metadata, authorization, SQL or visibility plan. */
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
 private readonly maintenance:SQLiteOwnerMaintenance;
 private connecting=Promise.resolve();private closed=false;
 constructor(private readonly owner:SQLiteOwnerProgramContext,private readonly binding:Binding){
  text(binding.artifactHash,256);sqlitePayloadBytes(binding.references,2097152);if(binding.references.length>4096||binding.collections.length>4096)fail('BUDGET','Generated native function catalog exceeds budget');
  for(const ref of binding.references){text(ref.path,1024);if(this.references.has(ref.path))throw new TypeError('Duplicate generated native reference');this.references.set(ref.path,structuredClone(ref));}
  for(const path of binding.collections){const ref=this.references.get(path);if(ref?.delivery!=='replica'||!ref.replica)throw new TypeError('Generated native dependency is not a Replica Collection');}
  this.sessions=new SQLiteWorkerSessions({artifactHash:binding.artifactHash,offlineMaxAgeMs:binding.offlineMaxAgeMs,database:owner.database,canonical:owner.canonical,safety:this.safety,runOwner:owner.runOwner,fence:()=>owner.engine.verifyOwner()});
  this.windows=new SQLiteWindowStore(owner.database,()=>owner.engine.verifyOwner());
  this.tokens=new SQLiteWorkerTokens({recipient:(id,key,exclude)=>[...(this.connections.get(id)?.tabs??[])].find(tab=>tab!==exclude&&this.tabs.get(tab)===id&&this.connections.get(id)?.identityKey===key),eligible:(tab,id,key)=>this.tabs.get(tab)===id&&this.connections.get(id)?.identityKey===key,notifyTab:tab=>owner.notifyTab(tab)});
  this.reducer=new SQLiteReducerOwner(owner,{runtime:binding.runtime,artifactHash:binding.artifactHash,resolveTab:tab=>this.sessions.resolveTab(tab,true),context:scope=>this.context(scope),persistIntent:async(input,context)=>{
   const saved=await owner.canonical.loadSession(context.intentScope);if(!saved||saved.directive.visibilityScope!==input.scope||saved.artifactHash!==binding.artifactHash)fail('FENCED','Intent recovery session changed');await this.safety.append(input,saved,context.intentScope);
  }});
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
     AND json_extract(g.header,'$.cursor.epoch')=? AND json_extract(g.header,'$.artifactHash')=? GROUP BY json_extract(g.header,'$.entity'),json_extract(g.header,'$.key')`,scope,saved.directive.epoch,this.binding.artifactHash);
   for(const row of rows)coverage[row.entity]={key:row.key,complete:true,columns:JSON.parse(row.columns) as string[]};
   return {intentScope:admitted.intentScope,identity:admitted.identity,coverage};
  }fail('FENCED','Native reducer scope is not admitted');
 }
 private checkCaller(caller:SQLiteOwnerCaller,connection:Connection,generation?:string){
  if(caller.signal.aborted||this.tabs.get(caller.tabID)!==connection.id)fail('FENCED','Native caller detached');const current=this.sessions.resolveTab(caller.tabID);if(generation!==undefined&&current.generation!==generation)fail('FENCED','Native read admission changed');return current;
 }
 private async readable(connection:Connection,admitted:ReturnType<SQLiteWorkerSessions['resolveTab']>){
  const saved=await this.owner.runOwner(0,()=>this.owner.canonical.loadSession(admitted.intentScope));if(!saved||saved.directive.visibilityScope!==admitted.scope)fail('FENCED','Cached read session changed');await connection.feed.assertReadable({scope:admitted.scope,epoch:saved.directive.epoch});
 }
 private scheduleDelivery(connection:Connection,delay=0){
  if(this.closed||this.connections.get(connection.id)!==connection)return;if(connection.timer)clearTimeout(connection.timer);
  connection.timer=setTimeout(()=>{connection.timer=undefined;void connection.delivery.drainOnce().then(result=>{this.notify(connection);if(result.nextDelay!==null)this.scheduleDelivery(connection,result.nextDelay);},error=>{if(this.closed)return;connection.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};this.notify(connection);this.scheduleDelivery(connection,1000);});},Math.min(60000,Math.max(0,delay)));
 }
 private async addSource(connection:Connection,ref:FunctionReference,args:JsonValue,tab?:string,eager=false):Promise<Source>{
  if(ref.delivery!=='replica'||!ref.replica)fail('INVALID_ROW','Expected a generated Replica Collection');const encoded=stable(args),signature=await digest(this.binding.artifactHash+'\0'+connection.id+'\0'+ref.path+'\0'+encoded);let source=connection.sources.get(signature);
  if(source?.closing){await source.closing.catch(()=>{});return this.addSource(connection,ref,args,tab,eager);}
  if(tab&&this.tabs.get(tab)!==connection.id)fail('FENCED','Native source caller detached');
  if(!source){
   const bytes=sqlitePayloadBytes({path:ref.path,args},16384)+1024;if(connection.sources.size>=128||connection.sourceBytes+bytes>262144)fail('BUDGET','Native subscription allowance exceeded');
   source={id:'source:'+signature,signature,ref,args:JSON.parse(encoded),tabs:new Set(),eager,bytes};connection.sources.set(signature,source);connection.sourceBytes+=bytes;
   const completeEntity=!Object.keys(ref.replica.equalFilters??{}).length&&!ref.replica.excludeWhenSet?.length;
   connection.feed.register({id:source.id,signature,entity:ref.replica.table,key:ref.replica.key,path:ref.path,...(completeEntity?{completeEntity:true}:{}),...(ref.replica.columns?{columns:[...ref.replica.columns]}:{})});
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
   if(this.closed||caller.signal.aborted)fail('FENCED','Native client is closed');await this.maintenance.ready();text(input.url,2048);const url=new URL(input.url);if(!['ws:','wss:'].includes(url.protocol)||url.username||url.password||url.hash)throw new TypeError('Invalid native WebSocket URL');
   const auth=input.auth??{};sqlitePayloadBytes(auth,65536);for(const field of Object.keys(auth))if(!['project','tenant','token','identity'].includes(field))throw new TypeError('Unsupported native auth field');for(const field of ['project','tenant'])if(auth[field]!==undefined&&(typeof auth[field]!=='string'||auth[field].length>512))throw new TypeError('Invalid native tenant routing');if(auth.identity!==undefined){text(auth.identity?.sub,512);if(auth.identity.iss!==undefined)text(auth.identity.iss,512);if(Object.keys(auth.identity).some(field=>!['sub','iss'].includes(field)))throw new TypeError('Invalid external identity hint');}if(auth.token!==undefined&&(typeof auth.token!=='string'||auth.token.length>16384))throw new TypeError('Invalid native token');
   const identityKey=await digest(stable({url:url.href,project:auth.project??null,tenant:auth.tenant??null,identity:auth.identity??null})),id='connection:'+identityKey;
   const old=this.tabs.get(caller.tabID);if(old&&old!==id)this.detachTab(caller.tabID);
   let connection=this.connections.get(id);
   if(!connection){
    if(this.connections.size>=8)fail('BUDGET','Native connection allowance exceeded');this.sessions.open(id);
    let record!:Connection;
    const feed=new SQLiteNativeFeed({owner:this.owner,sessions:this.sessions,connection:id,artifactHash:this.binding.artifactHash,prepare:path=>this.reducer.core.prepare(path),authority:(scope,mutate)=>this.reducer.core.rebase(scope,mutate).then(()=>{}),send:message=>record.client.sendOwnerMessage(message),advanceWatermark:revision=>record.client.advanceOwnerWatermark(revision),failed:error=>{record.failure={code:String((error as any)?.code??'SQLITE'),message:String((error as any)?.message??error).slice(0,512)};record.client.close();this.notify(record);}});
    const host={authenticating:(scope:string,generation:number)=>{feed.authenticating(scope,generation);void this.sessions.restore(id).then(()=>this.notify(record),()=>this.notify(record));},session:async(session:any,current:()=>boolean)=>{await feed.session(session,current);if(record.lastScope&&record.lastScope!==session.directive?.visibilityScope)void this.maintenance.retire(record.lastScope,[...record.sources.values()].map(source=>source.id)).catch(()=>{});record.lastScope=session.directive?.visibilityScope;record.failure=undefined;for(const source of record.sources.values())if(!source.closing)this.openSource(record,source);this.scheduleDelivery(record);this.notify(record);},binary:(frame:ArrayBuffer,generation:number)=>feed.binary(frame,generation),frame:(message:any,generation:number)=>feed.frame(message,generation),quarantined:(generation:number)=>{feed.quarantined(generation);this.notify(record);},disconnected:()=>{feed.disconnected();this.notify(record);},failed:(error:unknown)=>feed.failed(error)};
    const client=new GonvexClient(url.href,{...auth,ownerHost:host,fetchToken:request=>this.tokens.fetch(id,identityKey,request.forceRefreshToken)});
    const delivery=new SQLiteReducerDelivery({owner:this.owner,reducer:this.reducer,safety:this.safety,admission:()=>{const current=this.admission(record);return current&&{scope:current.scope,intentScope:current.intentScope,generation:current.generation,online:current.online};},send:entry=>client.callOwnerReducer({...this.ref(entry.path,'reducer'),artifactHash:entry.localExecution?.artifactHash??this.binding.artifactHash,intentEntropy:entry.localExecution?.intentEntropy},entry.args as JsonValue,entry.idempotencyKey),updateRequired:message=>{record.failure={code:'SCHEMA_MISMATCH',message};this.notify(record);}});
    record=connection={id,url:url.href,identityKey,client,feed,delivery,tabs:new Set(),sources:new Map(),sourceBytes:0};this.connections.set(id,record);
    for(const path of this.binding.collections){const ref=this.ref(path);if(ref.replica?.maxRows!==undefined&&ref.replica.maxRows>0&&ref.replica.maxRows<=1000)await this.addSource(record,ref,{},undefined,true);}
   }
   if(this.tabs.size>=64&&!this.tabs.has(caller.tabID))fail('BUDGET','Native tab allowance exceeded');this.tabs.set(caller.tabID,id);connection.tabs.add(caller.tabID);this.sessions.attachTab(caller.tabID,id);
   connection.client.connect();return {connection:id,identityKey};
  });this.connecting=work.then(()=>{},()=>{});return work;
 }
 async call(value:unknown,caller:SQLiteOwnerCaller):Promise<unknown>{
  sqlitePayloadBytes(value,131072);if(!value||typeof value!=='object'||Array.isArray(value))throw new TypeError('Invalid native client request');const input=structuredClone(value) as any;if(this.closed||caller.signal.aborted)fail('FENCED','Native client is closed');
  if(input.method==='connect')return this.connect(input,caller);
  const connection=this.connection(caller.tabID);
  switch(input.method){
   case 'controls':return {tokens:this.tokens.poll(caller.tabID),status:connection.client.connectionState(),session:connection.client.localSessionState(),failure:connection.failure??null};
   case 'token':this.tokens.respond(caller.tabID,input.response);return null;
   case 'auth':{
    const auth=input.auth??{};sqlitePayloadBytes(auth,65536);if(Object.keys(auth).some(field=>!['token'].includes(field)))throw new TypeError('Native identity changes require a new connection');if(auth.token!==undefined&&(typeof auth.token!=='string'||auth.token.length>16384))throw new TypeError('Invalid native token');this.tokens.invalidate(connection.id);connection.client.setAuth(auth);return null;
   }
   case 'watchReplica':{const source=await this.addSource(connection,this.ref(input.path,'query'),input.args??{},caller.tabID);return {id:source.id,signature:source.signature};}
   case 'unwatchReplica':{text(input.signature,128);const source=connection.sources.get(input.signature);if(source){if(source.tabs.delete(caller.tabID))connection.sourceBytes-=64;if(!source.tabs.size&&!source.eager){await this.closeSource(connection,source);}}return null;}
   case 'entities':{
    const admission=this.checkCaller(caller,connection);await this.readable(connection,admission);return this.owner.runOwner(131072,async()=>{this.checkCaller(caller,connection,admission.generation);const result=await this.owner.reader.entities({scope:admission.scope,entity:input.entity,ids:input.ids,expected:input.expected});this.checkCaller(caller,connection,admission.generation);return result;});
   }
   case 'replicaPage':{
    text(input.signature,128);const source=connection.sources.get(input.signature);if(!source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Tab does not own that source');const admission=this.checkCaller(caller,connection);await this.readable(connection,admission);return this.owner.runOwner(131072,async()=>{this.checkCaller(caller,connection,admission.generation);const result=await this.owner.reader.window({scope:admission.scope,signature:source.signature,offset:input.offset,limit:input.limit,expected:input.expected});this.checkCaller(caller,connection,admission.generation);if(connection.sources.get(source.signature)!==source||!source.eager&&!source.tabs.has(caller.tabID))fail('FENCED','Native source was detached');return result;});
   }
   case 'reducer':{const ref=this.ref(input.path,'reducer');if(ref.localExecution!==1)fail('INVALID_ROW','Native reducer has no generated portable body');const receipt=await this.reducer.invoke({path:ref.path,args:input.args??{},now:input.now,intentEntropy:input.intentEntropy},caller);this.scheduleDelivery(connection);return receipt;}
   case 'query':case 'action':{
    const ref=this.ref(input.path,input.method==='query'?'query':'action');if(ref.delivery==='replica'||ref.delivery==='live')fail('INVALID_ROW','Reactive reads require bounded native subscriptions');const admitted=this.checkCaller(caller,connection);const result=await (input.method==='query'?connection.client.query(ref,input.args??{}):connection.client.action(ref,input.args??{}));this.checkCaller(caller,connection,admitted.generation);sqlitePayloadBytes(result,131072);return result;
   }
   case 'status':return {connection:connection.client.connectionState(),session:connection.client.localSessionState(),admission:this.admission(connection)??null,failure:connection.failure??null};
   default:fail('INVALID_ROW','Unknown native client operation');
  }
 }
 detachTab(tabID:string){const id=this.tabs.get(tabID);if(!id)return;this.tabs.delete(tabID);this.sessions.detachTab(tabID);const connection=this.connections.get(id);if(!connection)return;connection.tabs.delete(tabID);this.tokens.detachTab(tabID);
  for(const source of connection.sources.values())if(source.tabs.delete(tabID)){connection.sourceBytes-=64;if(!source.tabs.size&&!source.eager)void this.closeSource(connection,source).catch(()=>{});}
  if(!connection.tabs.size){if(connection.lastScope)void this.maintenance.retire(connection.lastScope,[...connection.sources.values()].map(source=>source.id)).catch(()=>{});if(connection.timer)clearTimeout(connection.timer);this.tokens.invalidate(id);connection.delivery.close();connection.client.close();connection.feed.close();this.sessions.close(id);this.connections.delete(id);}
 }
 close(){if(this.closed)return;this.closed=true;this.maintenance.close();for(const tab of [...this.tabs.keys()])this.detachTab(tab);this.tokens.close();this.safety.close();}
}
