import type {ClientMessage,JsonValue,ServerCapabilities,ServerMessage,ReducerErrorClass} from '@gonvex/protocol';
import type {ConnectionState,FunctionReference,GonvexClientAuth} from './index.js';
import type {ClientContract} from './client-upgrades.js';
import type {SQLiteTransportHost} from './sqlite-transport-host.js';
import {artifactHashFromAuthResult,developerSessionTokenFromAuthResult,localIdentityFromAuthResult,reducerOutboxScope,replicaDirectiveFromAuthResult} from './client-auth-wire.js';
import {GonvexClientError,reducerErrorClassFromMessage} from './client-error.js';
import {sqlitePayloadBytes} from './sqlite-admission.js';

type AdmissionFrame={generation:number;serial:number;value:ArrayBuffer|ServerMessage;bytes:number;release():void};
type Options=GonvexClientAuth&{controlOnly?:boolean;public?:boolean;ownerHost:SQLiteTransportHost;clientContract?:ClientContract;reserve(bytes:number):()=>void;releaseAuth?:()=>void};
export type SQLiteLiveQueryUpdate={result:JsonValue;subscriptionRevision:{epoch:string;sequence:number};windowRevision?:string;replicaScope?:string}|{error:string};
type ControlWatch={live?:true;ref:FunctionReference;args:JsonValue;wireId?:string;authSerial?:number;release():void;receive(value:{result:JsonValue}|SQLiteLiveQueryUpdate):void};
type NativeError={id:string;wire?:string;sent:boolean;timer?:ReturnType<typeof setTimeout>;release():void;resolve():void;reject(error:Error):void};
type Pending={id:string;kind:'query'|'reducer'|'action';path:string;command?:string;control:boolean;public:boolean;wire?:string;sent:boolean;result?:JsonValue;answered:boolean;revision?:number;controlReady:boolean;timer?:ReturnType<typeof setTimeout>;resolve(value:JsonValue):void;reject(error:Error):void;release():void};
const error=(message:string,code:'auth'|'closed'|'disconnected'|'superseded'|'timeout')=>new GonvexClientError(message,{code});
/** Dedicated-owner wire transport. It has no legacy replica, IndexedDB engine,
 * optimistic cache, outbox or query-result cache. The program owns those SQL
 * responsibilities. Calls are sent once; only the durable owner journal may
 * retry an unknown reducer outcome. */
export class SQLiteNativeTransport {
 private auth:GonvexClientAuth;private socket?:WebSocket;private generation=0;private authSerial=0;private identityRevision=0;
 private latestAuth?:string;private activeAuth?:string;private authInFlight=true;private installing=false;private closed=false;private disposed=false;private quarantined=false;
 private capabilities:ServerCapabilities={};private artifact='';private watermark=0;private watermarkFence='';private session:'unavailable'|'authenticated'='unavailable';
 private readonly ephemeral=crypto.randomUUID();private readonly pending=new Map<string,Pending>();private waiting=0;private readonly controlWatches=new Set<ControlWatch>();private readonly nativeErrors=new Map<string,NativeError>();
 private readonly admissionFrames:AdmissionFrame[]=[];private admissionBytes=0;
 private tokenCancel?:()=>void;
 private retry?:ReturnType<typeof setTimeout>;private authTimer?:ReturnType<typeof setTimeout>;private refreshed=false;private refreshedToken?:string;private sendTimer?:ReturnType<typeof setTimeout>;private reconnectAttempts=0;private connectionCount=0;private hasEver=false;
 constructor(private readonly url:string,private readonly options:Options){this.auth={project:options.project,tenant:options.tenant,identity:options.identity,token:options.token,fetchToken:options.fetchToken};}
 private scope(){return reducerOutboxScope(this.url,this.auth,this.ephemeral);}
 private current(socket:WebSocket,generation:number){return !this.closed&&this.socket===socket&&this.generation===generation;}
 private clearAuthTimer(){clearTimeout(this.authTimer);this.authTimer=undefined;}
 connect(){
  if(this.disposed)throw error('Native transport was disposed','closed');
  if(this.socket&&(this.socket.readyState===0||this.socket.readyState===1))return;
  this.closed=false;this.quarantined=false;clearTimeout(this.retry);this.retry=undefined;const generation=++this.generation;
  this.options.ownerHost.opening?.(this.scope(),generation);
  const socket=this.socket=new WebSocket(this.url);socket.binaryType='arraybuffer';this.authInFlight=true;this.installing=false;this.latestAuth=undefined;this.activeAuth=undefined;
  socket.addEventListener('open',()=>{if(!this.current(socket,generation))return;this.hasEver=true;this.connectionCount++;this.refreshed=false;this.refreshedToken=undefined;void this.authenticate(socket,generation,false);});
  socket.addEventListener('message',event=>{if(!this.current(socket,generation))return;try{
   if(event.data instanceof ArrayBuffer){if(!this.quarantined&&this.installing)this.queueAdmission(event.data,generation);else if(!this.quarantined&&!this.authInFlight)this.options.ownerHost.binary(event.data,generation);return;}
   if(typeof event.data!=='string'||event.data.length*2>262144)throw Object.assign(Error('Native control frame exceeds decode budget'),{code:'BUDGET'});
   const message=JSON.parse(event.data) as ServerMessage;if(!message||typeof message!=='object'||typeof message.type!=='string')throw Error('Invalid native control frame');
   this.receive(socket,generation,message);
  }catch(failure){this.options.ownerHost.failed(failure);socket.close();}});
  socket.addEventListener('close',()=>{if(!this.current(socket,generation))return;this.socket=undefined;this.clearAdmission();this.authSerial++;this.tokenCancel?.();this.tokenCancel=undefined;this.session='unavailable';this.authInFlight=true;this.installing=false;this.clearAuthTimer();this.options.ownerHost.disconnected();
   this.rejectAll(error('Connection interrupted; the dispatched outcome may be unknown','disconnected'));this.rejectNativeErrors(Error('Error report connection interrupted'));
   if(!this.closed&&!this.quarantined){const delay=Math.min(30000,250*2**Math.min(this.reconnectAttempts++,7));this.retry=setTimeout(()=>{this.retry=undefined;this.connect();},delay);}
  });
  // Browser close events own reconnect/error outcomes. An error event does not
  // prove whether a preceding server write committed.
  socket.addEventListener('error',()=>{});
 }
 setAuth(input:GonvexClientAuth){
  if(this.disposed)throw error('Native transport was disposed','closed');
  this.clearAdmission();this.identityRevision++;this.authSerial++;this.quarantined=false;this.auth={...this.auth,...input};this.refreshed=false;this.refreshedToken=undefined;this.latestAuth=undefined;this.activeAuth=undefined;
  this.rejectAll(error('Credentials changed while the operation was pending','superseded'));this.rejectNativeErrors(Error('Error report credentials changed'));
  const socket=this.socket;if(socket?.readyState===1)void this.authenticate(socket,this.generation,false);else this.connect();
 }
 private async authenticate(socket:WebSocket,generation:number,forceRefresh:boolean){
  if(!this.current(socket,generation)||socket.readyState!==1)return;const serial=++this.authSerial;this.tokenCancel?.();this.tokenCancel=undefined;
  this.clearAdmission();this.authInFlight=true;this.installing=false;this.session='unavailable';this.latestAuth=undefined;this.clearAuthTimer();
  try{this.options.ownerHost.authenticating(this.scope(),generation);}catch(failure){this.options.ownerHost.failed(failure);socket.close();return;}
  this.authTimer=setTimeout(()=>{if(this.current(socket,generation)&&serial===this.authSerial)socket.close();},30000);
  try{
   const previousScope=this.scope();let token=this.auth.token;if(this.auth.fetchToken){let cancel:(()=>void)|undefined;let fresh:string|null|undefined;try{fresh=await Promise.race([this.auth.fetchToken({forceRefreshToken:forceRefresh}),new Promise<never>((_yes,no)=>{cancel=()=>no(Error('Native token request canceled'));this.tokenCancel=cancel;})]);}finally{if(this.tokenCancel===cancel)this.tokenCancel=undefined;}if(fresh!==null&&fresh!==undefined){token=fresh;if(forceRefresh)this.refreshedToken=fresh;}}
   if(!this.current(socket,generation)||serial!==this.authSerial)return;
   if(token!==undefined&&(typeof token!=='string'||token.length>16384))throw Error('Invalid native authentication token');
   this.auth={...this.auth,token};if(this.scope()!==previousScope){this.identityRevision++;for(const pending of [...this.pending.values()])if(!pending.public)this.finish(pending,undefined,error('Token identity changed before dispatch','superseded'));this.rejectNativeErrors(Error('Error report token identity changed'));this.options.ownerHost.authenticating(this.scope(),generation);}const id=this.latestAuth=crypto.randomUUID();
   this.sendNow({type:'auth',id,token,project:this.auth.project,tenant:this.auth.tenant,controlOnly:this.options.controlOnly||!this.auth.tenant,...(this.options.public?{public:true}:{}),...(this.options.clientContract?{clientContract:this.options.clientContract.version}:{}),
    capabilities:{replicaNativeRows:1,replicaSetDigest:1,replicaSnapshotPages:1,tenantControl:1,replicaReadyMany:1,replicaWatermark:1}});
   this.flush();
  }catch{if(this.current(socket,generation)&&serial===this.authSerial)socket.close();}
 }
 private receive(socket:WebSocket,generation:number,message:ServerMessage){
  if(message.type==='session.ready'){sqlitePayloadBytes(message.capabilities??{},4096);this.capabilities=Object.freeze({...message.capabilities});return;}
  if(message.type==='client.updateRequired'||message.type==='system.reload'){
   this.options.ownerHost.failed(Object.assign(Error(message.type==='client.updateRequired'?message.reason:'Application module changed'),{code:'SCHEMA_MISMATCH'}));socket.close();return;
  }
  if(message.type==='auth.result'||message.type==='auth.error'){
   const unsolicited=message.type==='auth.error'&&(message.id==='membership-changed'||message.id==='session-expired');
   if(unsolicited){
    if(['revoked','member_removed','account_disabled'].includes(message.code??'')){this.deny(generation,message.error,message.code);return;}
    if(message.id==='membership-changed'){this.options.ownerHost.quarantined?.(generation);this.session='unavailable';this.activeAuth=undefined;void this.authenticate(socket,generation,false);return;}
    if(!this.authInFlight)void this.authenticate(socket,generation,true);return;
   }
   if(message.id!==this.latestAuth&&!(message.type==='auth.error'&&!this.latestAuth&&message.id===this.activeAuth))return;
   if(message.type==='auth.error'){
    this.latestAuth=undefined;this.activeAuth=undefined;
    if(message.code!=='transient'&&!this.refreshed&&this.auth.fetchToken&&!['revoked','member_removed','account_disabled'].includes(message.code??'')){this.refreshed=true;void this.authenticate(socket,generation,true);return;}
    const definitive=['revoked','member_removed','account_disabled'].includes(message.code??'')||message.code==='credential_rejected'&&(this.refreshedToken===this.auth.token&&!!this.refreshedToken||/^gvx_(dev|imp)_/.test(this.auth.token??''));
    if(definitive)this.deny(generation,message.error,message.code);else{this.options.ownerHost.authError?.(message.error,{definitive:false,code:message.code});socket.close();}return;
   }
   this.latestAuth=undefined;this.activeAuth=message.id;this.clearAuthTimer();const serial=this.authSerial;this.installing=true;
   const directive=replicaDirectiveFromAuthResult(message.result),identity=localIdentityFromAuthResult(message.result);
   this.artifact=artifactHashFromAuthResult(message.result)??this.artifact;
   const developer=developerSessionTokenFromAuthResult(message.result);if(developer){if(developer.length>16384)throw Error('Developer credential exceeds admission budget');this.auth={...this.auth,token:developer,fetchToken:undefined};this.options.ownerHost.developerCredential?.(developer);}
   const current=()=>this.current(socket,generation)&&serial===this.authSerial&&this.activeAuth===message.id&&!this.latestAuth;
   const fence=JSON.stringify([this.scope(),directive?.visibilityScope??'',directive?.epoch??'']);if(fence!==this.watermarkFence){this.watermarkFence=fence;this.watermark=0;}
   void this.options.ownerHost.session({connectionGeneration:generation,directive,identity,artifactHash:this.artifact,intentScope:this.scope(),lastOnlineAtMs:Date.now(),capabilities:this.capabilities},current).then(()=>{
    if(!current())return;this.clearAuthTimer();this.authInFlight=false;this.installing=false;this.reconnectAttempts=0;this.refreshed=false;this.refreshedToken=undefined;this.session=directive&&identity?'authenticated':'unavailable';try{this.drainAdmission(generation,serial);}catch(error){this.options.ownerHost.failed(error);socket.close();return;}this.options.ownerHost.authenticated?.();this.flush();
   },failure=>{if(current()){this.options.ownerHost.failed(failure);socket.close();}});return;
  }
  if(message.type.startsWith('replica.')){if(!this.quarantined&&this.installing)this.queueAdmission(message,generation);else if(!this.quarantined&&!this.authInFlight)this.options.ownerHost.frame(message,generation);return;}
  if(!('id'in message)||typeof message.id!=='string')return;if(message.type==='error.ack'){const report=this.nativeErrors.get(message.id);if(report?.sent)this.finishNativeError(report,message.error?Error(message.error):undefined);return;}
  const watched=[...this.controlWatches].find(watch=>watch.wireId===message.id&&watch.authSerial===this.authSerial);
  if(watched){if(message.type==='query.result'){sqlitePayloadBytes(message.result,131072);if(watched.live){const revision=message.subscriptionRevision;if(!revision||typeof revision.epoch!=='string'||!revision.epoch||revision.epoch.length>256||!Number.isSafeInteger(revision.sequence)||revision.sequence<0||message.windowRevision!==undefined&&(typeof message.windowRevision!=='string'||message.windowRevision.length>128)||message.replicaScope!==undefined&&(typeof message.replicaScope!=='string'||message.replicaScope.length>512))throw Error('Live window requires a bounded authoritative revision');watched.receive({result:message.result,subscriptionRevision:revision,...(message.windowRevision!==undefined?{windowRevision:message.windowRevision}:{}),...(message.replicaScope!==undefined?{replicaScope:message.replicaScope}:{})});}else watched.receive({result:message.result});}else if(message.type==='query.error')watched.receive({error:message.error.slice(0,512)});return;}
  const pending=this.pending.get(message.id);if(!pending||!pending.sent)return;
  if(message.type==='control.watermark'){pending.controlReady=true;this.settle(pending);return;}
  if(message.type===pending.kind+'.error'&&'error'in message){const classification=pending.kind==='reducer'?reducerErrorClassFromMessage(message as {error:string;class?:ReducerErrorClass}):undefined;
   this.finish(pending,undefined,new GonvexClientError(String(message.error),{code:'server',path:pending.path,operation:pending.kind,...(classification?{errorClass:classification,retryable:'retryable'in message?Boolean(message.retryable):classification==='transient'||classification==='unauthenticated'}:{})}));return;
  }
  if(message.type!==pending.kind+'.result'||!('result'in message))return;
  if(pending.answered)return;
  if(pending.kind==='reducer'&&'originCommandId'in message&&message.originCommandId!==undefined&&message.originCommandId!==pending.command)throw Object.assign(Error('Native reducer receipt differs from its command'),{code:'INTENT_CONFLICT'});
  sqlitePayloadBytes(message.result,131072);pending.result=message.result;pending.answered=true;
  if(pending.kind!=='query'){
   if(!pending.control){clearTimeout(pending.timer);pending.timer=undefined;}
   if('committedRevision'in message&&message.committedRevision!==undefined){if(!Number.isSafeInteger(message.committedRevision)||message.committedRevision<0)throw Error('Invalid committed native revision');pending.revision=message.committedRevision;}
  }
  this.settle(pending);
 }
 private clearAdmission(){for(const frame of this.admissionFrames)frame.release();this.admissionFrames.length=0;this.admissionBytes=0;}
 private queueAdmission(value:ArrayBuffer|ServerMessage,generation:number){
  if(!(value instanceof ArrayBuffer)&&value.type==='replica.watermark'){if(!Number.isSafeInteger(value.revision)||value.revision<0)throw Error('Invalid pending replica watermark');const last=this.admissionFrames.at(-1);if(last&&!(last.value instanceof ArrayBuffer)&&last.value.type==='replica.watermark'){last.value={...last.value,revision:Math.max(last.value.revision,value.revision)};return;}}
  const bytes=(value instanceof ArrayBuffer?value.byteLength:sqlitePayloadBytes(value,262144))+512;if(value instanceof ArrayBuffer&&value.byteLength>131072||this.admissionFrames.length>=64||this.admissionBytes+bytes>1048576)throw Object.assign(Error('Native admission frame allowance exceeded'),{code:'BUDGET'});const release=this.options.reserve(bytes);this.admissionFrames.push({generation,serial:this.authSerial,value,bytes,release});this.admissionBytes+=bytes;
 }
 private drainAdmission(generation:number,serial:number){try{for(const frame of this.admissionFrames){if(frame.generation!==generation||frame.serial!==serial)continue;if(frame.value instanceof ArrayBuffer)this.options.ownerHost.binary(frame.value,generation);else this.options.ownerHost.frame(frame.value,generation);}}finally{this.clearAdmission();}}
 private deny(generation:number,message:string,code?:string){this.clearAdmission();this.rejectNativeErrors(Error('Error reporting session was denied'));this.options.ownerHost.authError?.(message,{definitive:true,code});this.quarantined=true;this.authSerial++;this.tokenCancel?.();this.tokenCancel=undefined;this.latestAuth=undefined;this.activeAuth=undefined;this.authInFlight=false;this.installing=false;this.session='unavailable';this.clearAuthTimer();this.options.ownerHost.quarantined?.(generation);for(const pending of [...this.pending.values()])if(!pending.public)this.finish(pending,undefined,error(message,'auth'));this.flush();}
 private settle(pending:Pending){if(!pending.answered)return;if(pending.kind!=='query'){
  if(pending.control&&this.capabilities.controlWatermark===1&&!pending.controlReady)return;
  if(!pending.control&&!this.options.public&&this.capabilities.replicaWatermark===1&&pending.revision!==undefined&&pending.revision>this.watermark)return;
 }this.finish(pending,pending.result!);}
 advanceOwnerWatermark(revision:number){if(!Number.isSafeInteger(revision)||revision<0)throw Error('Invalid native terminal watermark');this.watermark=Math.max(this.watermark,revision);for(const pending of this.pending.values())this.settle(pending);}
 private sendNow(message:ClientMessage){const wire=JSON.stringify(message);if(wire.length*2>262144)throw Error('Native outbound frame exceeds budget');if(this.socket?.readyState!==1)throw error('Native socket is unavailable','disconnected');this.socket.send(wire);}
 sendOwnerMessage(message:ClientMessage){sqlitePayloadBytes(message,131072);if(this.quarantined)throw error('Native session is quarantined','auth');if(this.authInFlight&&!this.installing)throw error('Native admission is pending','disconnected');this.sendNow(message);}
 private flush(){const socket=this.socket;if(socket?.readyState!==1)return;for(const pending of this.pending.values()){
  if(pending.sent||!pending.wire||this.authInFlight&&(!pending.public||this.options.public))continue;
  if(!pending.control&&!this.options.public&&this.session!=='authenticated')continue;
  if(socket.bufferedAmount>524288){if(!this.sendTimer)this.sendTimer=setTimeout(()=>{this.sendTimer=undefined;this.flush();},5);continue;}
  const wire=pending.wire;pending.wire=undefined;pending.sent=true;try{socket.send(wire);}catch{this.finish(pending,undefined,error('Native send interrupted; its outcome may be unknown','disconnected'));socket.close();}
 }for(const watch of this.controlWatches){
  if(watch.wireId&&watch.authSerial===this.authSerial||watch.live&&(this.authInFlight||this.quarantined||this.session!=='authenticated')||this.authInFlight&&watch.ref.authorization!=='public'||this.quarantined&&watch.ref.authorization!=='public')continue;
  if(socket.bufferedAmount>524288){if(!this.sendTimer)this.sendTimer=setTimeout(()=>{this.sendTimer=undefined;this.flush();},5);break;}
  const id=crypto.randomUUID();try{this.sendNow({type:'query.subscribe',id,path:watch.ref.path,args:watch.args,scope:watch.ref.scope??'tenant'});}catch{socket.close();return;}watch.wireId=id;watch.authSerial=this.authSerial;
 }if(!this.authInFlight&&!this.quarantined)for(const report of this.nativeErrors.values()){if(report.sent||!report.wire)continue;if(socket.bufferedAmount>524288){if(!this.sendTimer)this.sendTimer=setTimeout(()=>{this.sendTimer=undefined;this.flush();},5);break;}const wire=report.wire;report.wire=undefined;report.sent=true;clearTimeout(report.timer);report.timer=setTimeout(()=>this.finishNativeError(report,Error('Native error report acknowledgment timed out')),10000);try{socket.send(wire);}catch{this.finishNativeError(report,Error('Native error report send interrupted'));socket.close();}}}
 watchControl(ref:FunctionReference,args:JsonValue,receive:(value:{result:JsonValue}|{error:string})=>void){
  if(ref.kind!=='query'||ref.scope!=='control'||ref.delivery==='replica')throw new TypeError('Control watches require generated Control queries');
  return this.watch(ref,args,receive,false);
 }
 watchLive(ref:FunctionReference,args:JsonValue,receive:(value:SQLiteLiveQueryUpdate)=>void){
  if(this.options.controlOnly||this.options.public||ref.kind!=='query'||ref.scope==='control'||ref.delivery!=='live'||!ref.live)throw new TypeError('Live windows require a generated tenant Live Query');
  return this.watch(ref,args,receive as ControlWatch['receive'],true);
 }
 private watch(ref:FunctionReference,args:JsonValue,receive:ControlWatch['receive'],live:boolean){
  const bytes=sqlitePayloadBytes({ref:ref.path,args},16384)+512;if(this.controlWatches.size>=32)throw Object.assign(Error('Native query watch allowance exceeded'),{code:'BUDGET'});
  const release=this.options.reserve(bytes),watch:ControlWatch={ref,args,receive,release,...(live?{live:true as const}:{})};try{watch.args=structuredClone(args);this.controlWatches.add(watch);this.connect();this.flush();}catch(error){this.controlWatches.delete(watch);release();throw error;}let closed=false;
  return ()=>{if(closed)return;closed=true;this.controlWatches.delete(watch);release();if(watch.wireId&&watch.authSerial===this.authSerial&&this.socket?.readyState===1)this.sendNow({type:'query.unsubscribe',id:watch.wireId});};
 }
 async reportError(type:'register'|'envelope'|'heartbeat',payload:unknown):Promise<void>{
  if(!['register','envelope','heartbeat'].includes(type))return Promise.reject(new TypeError('Invalid native error report kind'));sqlitePayloadBytes(payload,65536);if(this.nativeErrors.size>=16)return Promise.reject(Object.assign(Error('Native error reporting is pending'),{code:'BUDGET'}));
  if(!payload||typeof payload!=='object'||Array.isArray(payload))throw new TypeError('Native error reports require an object payload');const body=payload as {release?:unknown;environment?:unknown;events?:JsonValue[]};if(type==='register'&&[body.release,body.environment].some(value=>value!==undefined&&(typeof value!=='string'||value.length>512)))throw new TypeError('Invalid error report registration');if(type==='envelope'&&!Array.isArray(body.events))throw new TypeError('Error envelopes require events');
  const id=crypto.randomUUID(),frame=type==='register'?{type:'error.register',id,release:body.release,environment:body.environment}:type==='heartbeat'?{type:'error.heartbeat',id}:{type:'error.envelope',id,events:body.events},wire=JSON.stringify(frame),release=this.options.reserve(wire.length*2+512);try{this.connect();}catch(error){release();throw error;}
  return new Promise<void>((resolve,reject)=>{const report:NativeError={id,wire,sent:false,release,resolve,reject};report.timer=setTimeout(()=>this.finishNativeError(report,Error('Native error report timed out')),30000);this.nativeErrors.set(id,report);this.flush();});
 }
 private finishNativeError(report:NativeError,failure?:Error){if(this.nativeErrors.get(report.id)!==report)return;this.nativeErrors.delete(report.id);clearTimeout(report.timer);report.wire=undefined;report.release();if(failure)report.reject(failure);else report.resolve();}
 private rejectNativeErrors(error:Error){for(const report of [...this.nativeErrors.values()])this.finishNativeError(report,error);}
 private finish(pending:Pending,value?:JsonValue,failure?:Error){if(this.pending.get(pending.id)!==pending)return;this.pending.delete(pending.id);clearTimeout(pending.timer);pending.wire=undefined;pending.release();if(failure)pending.reject(failure);else pending.resolve(value!);}
 private rejectAll(reason:Error){for(const pending of [...this.pending.values()])this.finish(pending,undefined,reason);}
 private async invoke<T extends JsonValue>(kind:Pending['kind'],ref:FunctionReference,args:JsonValue,command?:string):Promise<T>{
  if(this.quarantined&&!(ref.scope==='control'&&ref.authorization==='public'))throw error('Native session is quarantined','auth');
  sqlitePayloadBytes({path:ref.path,args,command:command??null},131072);if(this.waiting+this.pending.size>=64)throw error('Native operation admission is pending','disconnected');
  this.connect();const revision=this.identityRevision,timeout=kind==='action'?60000:20000,deadline=Date.now()+timeout;this.waiting++;
  let releaseMetadata:(()=>void)|undefined,releasePayload:(()=>void)|undefined,waiting=true;
  try{
   try{releaseMetadata=this.options.reserve(512);}catch{throw error('Native operation capacity is pending','disconnected');}
   for(;;){if(this.closed)throw error('Native client closed','closed');if(revision!==this.identityRevision)throw error('Native operation admission changed','superseded');if(Date.now()>=deadline)throw error('Native operation admission timed out before dispatch','timeout');
    try{releasePayload=this.options.reserve(132096);break;}catch{await new Promise(resolve=>setTimeout(resolve,5));}
   }
   const id=crypto.randomUUID(),wire=JSON.stringify({type:kind+'.call',id,path:ref.path,args,...(ref.scope==='control'?{scope:'control'}:{}),...(kind==='reducer'?{idempotencyKey:command??id,...(ref.artifactHash?{artifactHash:ref.artifactHash}:{}),...(ref.receiptPath?{receiptPath:ref.receiptPath}:{}),...(ref.intentEntropy?{intentEntropy:ref.intentEntropy}:{}),...(this.options.clientContract?{clientContract:this.options.clientContract.version}:{})}:{})});
   if(wire.length*2>131072)throw Error('Native invocation exceeds wire budget');
   const metadata=releaseMetadata,payload=releasePayload;releaseMetadata=undefined;releasePayload=undefined;
   this.waiting--;waiting=false;return await new Promise<T>((resolve,reject)=>{const pending:Pending={id,kind,path:ref.path,...(kind==='reducer'?{command:command??id}:{}),control:ref.scope==='control',public:this.options.public||ref.scope==='control'&&ref.authorization==='public',wire,sent:false,answered:false,controlReady:false,resolve:value=>resolve(value as T),reject,release:()=>{metadata();payload();}};
    pending.timer=setTimeout(()=>this.finish(pending,undefined,new GonvexClientError('Native operation timed out; a dispatched outcome may be unknown',{code:'timeout',path:ref.path,operation:kind})),Math.max(1,deadline-Date.now()));this.pending.set(id,pending);this.flush();
   });
  }finally{if(waiting)this.waiting--;releasePayload?.();releaseMetadata?.();}
 }
 query<T extends JsonValue=JsonValue>(ref:FunctionReference,args:JsonValue={}):Promise<T>{return this.invoke('query',ref,args);}
 action<T extends JsonValue=JsonValue>(ref:FunctionReference,args:JsonValue={}):Promise<T>{return this.invoke('action',ref,args);}
 callOwnerReducer<T extends JsonValue=JsonValue>(ref:FunctionReference,args:JsonValue,command:string):Promise<T>{if(!command||command.length>256)return Promise.reject(Error('Invalid durable command ID'));return this.invoke('reducer',ref,args,command);}
 serverInfo(){return this.capabilities;}
 authenticationState(){return this.quarantined?'denied':this.activeAuth&&!this.authInFlight?'authenticated':this.socket?'authenticating':'unavailable';}
 localSessionState(){return this.session;}
 connectionState():ConnectionState{const values=[...this.pending.values()];return {isWebSocketConnected:this.socket?.readyState===1,hasEverConnected:this.hasEver,connectionCount:this.connectionCount,connectionRetries:this.reconnectAttempts,hasInflightRequests:!!values.length,inflightReducers:values.filter(p=>p.kind==='reducer').length,inflightActions:values.filter(p=>p.kind==='action').length,inflightOneShotQueries:values.filter(p=>p.kind==='query').length};}
 dispose(){if(this.disposed)return;this.close();for(const watch of this.controlWatches)watch.release();this.controlWatches.clear();this.disposed=true;this.auth={};this.options.releaseAuth?.();}
 close(){this.clearAdmission();if(this.closed)return;this.closed=true;this.identityRevision++;this.authSerial++;this.tokenCancel?.();this.tokenCancel=undefined;clearTimeout(this.sendTimer);this.sendTimer=undefined;this.session='unavailable';this.clearAuthTimer();clearTimeout(this.retry);this.retry=undefined;this.rejectAll(error('Native client closed; a dispatched outcome may be unknown','closed'));this.rejectNativeErrors(Error('Native error reporting client closed'));const socket=this.socket;this.socket=undefined;socket?.close();this.options.ownerHost.disconnected();}
}
