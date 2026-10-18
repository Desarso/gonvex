import type {ClientMessage,ReplicaCursor,ServerMessage} from '@gonvex/protocol';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import type {SQLiteTransportHost,SQLiteTransportSession} from './sqlite-transport-host.js';
import type {SQLiteWorkerSessions} from './sqlite-worker-sessions.js';
import {SQLiteNativeIngress} from './sqlite-native-ingress.js';
import {SQLiteWindowStore,type SQLiteWindowHeader} from './sqlite-window-store.js';
import {SQLiteFeedGroups} from './sqlite-feed-groups.js';
import {SQLiteDeferredReplicaFrames} from './sqlite-deferred-replica-frames.js';
import {decodeSQLiteNativeDelta,decodeSQLiteNativeTransaction} from './sqlite-native-control.js';
import {waitForSQLiteOwnerAdmission,markSQLiteStagingCommit} from './sqlite-owner-transactions.js';
import {SQLiteReducerPreparationRequiredError} from './sqlite-reducer-core.js';
import {fail,sqlitePayloadBytes} from './sqlite-admission.js';
type Start=Extract<ServerMessage,{type:'replica.snapshotStart'}>;
type Delta=Extract<ServerMessage,{type:'replica.delta'}>;
export type SQLiteFeedSource={id:string;signature:string;entity:string;key:string;path:string;completeEntity?:true;columns?:readonly string[]};
type Source=SQLiteFeedSource&{bytes:number;start?:Start;opening:boolean;closing?:{promise:Promise<void>;resolve():void;reject(error:unknown):void;timer:ReturnType<typeof setTimeout>}};
type State={stream:string;generation:number;scope:string;epoch:string;logicalScope:string;gate:Promise<void>;ingress:SQLiteNativeIngress;admission?:number;closed:boolean;watermark?:number};
type Options={owner:SQLiteOwnerProgramContext;sessions:SQLiteWorkerSessions;connection:string;artifactHash:string;
 send(message:ClientMessage):void;advanceWatermark(revision:number):void;failed(error:unknown):void;
 /** Rebase derived predictions in the same transaction as canonical authority. */
 authority?(scope:string,mutate:()=>Promise<void>):Promise<void>;
 /** Portable chunks are fetched outside the SQL FIFO before atomic retry. */
 prepare?(path:string):Promise<void>;
};
const progressSchema=`CREATE TABLE IF NOT EXISTS gv_native_feed_progress(scope TEXT NOT NULL,epoch TEXT NOT NULL,observed INTEGER NOT NULL,applied INTEGER NOT NULL,unclosed INTEGER NOT NULL,PRIMARY KEY(scope,epoch)) WITHOUT ROWID;`;
const empty={entities:{},liveQueries:{}};
/** One authenticated socket's ordered native feed. Its bounded control lane
 * never waits for future snapshot bytes. Membership changes preceding the last
 * base page live on disk; only a verified window can retire those records. */
export class SQLiteNativeFeed implements SQLiteTransportHost {
 private readonly sources=new Map<string,Source>();
 private sourceBytes=0;private startBytes=0;private closingBytes=0;
 private state?:State;
 private queued=0;private queuedBytes=0;
 private readonly windows:SQLiteWindowStore;private readonly deferred:SQLiteDeferredReplicaFrames;
 private readonly initialized:Promise<void>;private readonly groups:SQLiteFeedGroups;
 constructor(private readonly options:Options){
  const {owner}=options,fence=()=>owner.engine.verifyOwner();this.windows=new SQLiteWindowStore(owner.database,fence);this.deferred=new SQLiteDeferredReplicaFrames(owner.database,options.artifactHash,fence);this.groups=new SQLiteFeedGroups(owner.database,fence);
  this.initialized=owner.runOwner(0,async()=>{await this.windows.ready();await this.deferred.ready();await this.groups.ready();await owner.snapshots.ready();await owner.activation.ready();await owner.database.execAsync(progressSchema);});void this.initialized.catch(()=>{});
 }
 /** Called by the trusted generated program before sending replica.open.
  * Application RPCs cannot supply an entity, visibility scope or SQL plan. */
 register(input:SQLiteFeedSource){
  sqlitePayloadBytes(input,16384);if(input.completeEntity!==undefined&&input.completeEntity!==true||!input.id||input.id.length>256||!input.signature||input.signature.length>4096||!input.path||input.path.length>1024||!Object.hasOwn(this.options.owner.engine.schema,input.entity)||this.options.owner.engine.schema[input.entity]!.key!==input.key)throw new TypeError('Invalid generated native source');
  if(input.columns!==undefined&&(!Array.isArray(input.columns)||input.columns.length>128||new Set(input.columns).size!==input.columns.length||!input.columns.includes(input.key)||input.columns.some(column=>typeof column!=='string'||!Object.hasOwn(this.options.owner.engine.schema[input.entity]!.fields,column))))throw new TypeError('Invalid generated source projection');
  const owned:SQLiteFeedSource={id:input.id,signature:input.signature,entity:input.entity,key:input.key,path:input.path,...(input.completeEntity?{completeEntity:true}:{}),...(input.columns?{columns:[...input.columns]}:{})},bytes=sqlitePayloadBytes(owned,16384);
  if(this.sources.has(input.id))throw new TypeError('Native source already registered');if(this.sources.size>=128||this.sourceBytes+this.startBytes+this.closingBytes+bytes>262144)fail('BUDGET','Native source registry exceeded budget');
  this.sources.set(input.id,{...owned,bytes,opening:true});this.sourceBytes+=bytes;
 }
 private removeSource(id:string){const source=this.sources.get(id);if(!source)return;this.sources.delete(id);if(source.start)this.startBytes-=sqlitePayloadBytes(source.start,16384);if(source.closing){clearTimeout(source.closing.timer);this.closingBytes-=512;}this.sourceBytes-=source.bytes;}
 unregister(id:string){if(this.state&&!this.state.closed)fail('FENCED','Close an online source through its protocol acknowledgement');this.removeSource(id);}
 closeSource(id:string):Promise<void>{
  const source=this.source(id);if(source.closing)return source.closing.promise;const state=this.state;if(!state||state.closed){this.removeSource(id);return Promise.resolve();}this.check(state);
  if(this.sourceBytes+this.startBytes+this.closingBytes+512>262144)fail('BUDGET','Native source close metadata exceeded budget');
  let resolve!:()=>void,reject!:(error:unknown)=>void;const promise=new Promise<void>((yes,no)=>{resolve=yes;reject=no;});source.closing={promise,resolve,reject,timer:setTimeout(()=>this.reject(state,Object.assign(Error('Native source close acknowledgement timed out'),{code:'PAGE_CHANGED'})),30000)};this.closingBytes+=512;
  try{this.options.send({type:'replica.close',id});}catch(error){this.removeSource(id);reject(error);}return promise;
 }
 private source(id:string){const source=this.sources.get(id);if(!source)fail('FENCED','Native frame has no generated subscription');return source;}
 private check(state:State){if(this.state!==state||state.closed||state.admission===undefined)fail('FENCED','Native feed admission changed');this.options.sessions.assertFrameAdmission(this.options.connection,state.generation,state.admission);}
 private stop(){const state=this.state;if(state){state.closed=true;state.ingress.close();void this.initialized.then(()=>this.options.owner.runOwner(0,()=>this.groups.retire(state.stream))).catch(()=>{});}this.state=undefined;for(const source of [...this.sources.values()])if(source.closing){const closing=source.closing;this.removeSource(source.id);closing.reject(Object.assign(Error('Native source connection changed'),{code:'FENCED'}));}}
 authenticating(intentScope:string,generation:number){this.stop();this.startBytes=0;this.options.sessions.authenticating(this.options.connection,intentScope,generation);for(const source of this.sources.values()){source.start=undefined;source.opening=true;}}
 async session(input:SQLiteTransportSession,isCurrent:()=>boolean){
  this.stop();if(!input.directive){await this.options.sessions.activate(this.options.connection,input,isCurrent);return;}
  const {owner}=this.options;let state!:State;
  const ingress=new SQLiteNativeIngress({database:owner.database,snapshots:owner.snapshots,scope:input.directive.visibilityScope,
   runOwner:run=>this.run(state,524288,async()=>{await this.initialized;return run();}),
   commitProgress:async()=>{await markSQLiteStagingCommit(owner.database,()=>owner.engine.verifyOwner());},
   sendAck:ack=>{this.check(state);this.options.send(ack);},
   activateVerified:async receipt=>{
    this.check(state);const source=this.source(receipt.ack.id),start=source.start;if(!start||start.snapshotId!==receipt.ack.snapshotId)fail('FENCED','Snapshot has no matching start header');
    const header:SQLiteWindowHeader={signature:source.signature,entity:source.entity,key:source.key,kind:'replica',source:'server',cursor:start.cursor,aggregateDigest:receipt.progress.digest,completeness:start.truncated?'partial':'complete',...(start.mode?{mode:start.mode}:{}),...(source.completeEntity&&!start.truncated?{entityCoverage:'complete' as const}:{}),columns:source.columns??[source.key],artifactHash:this.options.artifactHash};
    await this.mutate(state.scope,async()=>{await owner.activation.activate({scope:state.scope,subscription:source.id,snapshot:start.snapshotId,header});});
    await this.replay(state,source);
   }});
  state={stream:crypto.randomUUID(),generation:input.connectionGeneration,scope:input.directive.visibilityScope,epoch:input.directive.epoch,logicalScope:input.directive.scope,gate:Promise.resolve(),ingress,closed:false};this.state=state;
  state.gate=(async()=>{await this.initialized;const admitted=await this.options.sessions.activate(this.options.connection,input,()=>this.state===state&&!state.closed&&isCurrent());if(!admitted)fail('FENCED','Native feed has no tenant admission');const current=this.options.sessions.frameAdmission(this.options.connection,state.generation);state.admission=current.token;this.check(state);})();void state.gate.catch(()=>{});await state.gate;
 }
 binary(frame:ArrayBuffer,generation:number){const state=this.state;if(!state||state.generation!==generation||state.closed)fail('FENCED','Binary frame has no native session');void state.ingress.accept(frame,state.gate).catch(error=>this.reject(state,error));}
 frame(message:ServerMessage,generation:number){
  if(!message.type.startsWith('replica.'))return false;
  const state=this.state;if(!state||state.generation!==generation||state.closed)fail('FENCED','Control frame has no native session');
  const bytes=sqlitePayloadBytes(message,262144);if(this.queued>=64||this.queuedBytes+bytes>2097152)fail('BUDGET','Native control queue exceeded budget');
  const owned=structuredClone(message),preceding=state.ingress.flush();void preceding.catch(()=>{});this.queued++;this.queuedBytes+=bytes;
  const next=state.gate.then(()=>preceding).then(async()=>{this.check(state);await this.process(state,owned);});state.gate=next;
  void next.then(()=>{this.queued--;this.queuedBytes-=bytes;},error=>{this.queued--;this.queuedBytes-=bytes;this.reject(state,error);});return true;
 }
 private reject(state:State,error:unknown){if(this.state!==state||state.closed)return;this.stop();this.options.failed(error);}
 private cursor(state:State,cursor:ReplicaCursor){if(!cursor||cursor.epoch!==state.epoch||!Number.isSafeInteger(cursor.revision)||cursor.revision<0)fail('FENCED','Native feed cursor changed epoch');}
 private async run<T>(state:State,bytes:number,run:()=>Promise<T>):Promise<T>{
  const loaded=new Set<string>();let preparationBytes=0;
  for(;;){try{return await waitForSQLiteOwnerAdmission(()=>this.options.owner.runOwner(bytes,async()=>{let value!:T;await this.options.owner.database.withTransactionAsync(async()=>{this.check(state);value=await run();this.check(state);});return value;},[state.scope]),()=>this.check(state));}
   catch(error){if(!(error instanceof SQLiteReducerPreparationRequiredError)||!this.options.prepare)throw error;
    if(loaded.has(error.path))fail('FENCED','Prepared native feed reducer remains unavailable');preparationBytes+=error.path.length*2+128;
    if(loaded.size>=128||preparationBytes>131072)fail('BUDGET','Native feed reducer preparation exceeded budget');loaded.add(error.path);await this.options.prepare(error.path);
   }
  }
 }
 private async mutate(scope:string,run:()=>Promise<void>){if(this.options.authority)await this.options.authority(scope,run);else await run();}
 private async applyDelta(state:State,source:Source,frame:Delta){
  this.cursor(state,frame.cursor);const rows=await decodeSQLiteNativeDelta(frame,source.key),current=await this.windows.header(state.scope,source.signature);
  if(!current||current.header.cursor?.epoch!==state.epoch)fail('PAGE_CHANGED','Native delta needs a verified base window');
  await this.windows.applyDelta({scope:state.scope,signature:source.signature,generation:current.generation,cursor:frame.cursor,upserts:rows.map(({id,hash})=>({id,hash})),deleted:frame.deleted??[],digest:frame.digest??''},()=>this.mutate(state.scope,async()=>{await this.options.owner.canonical.applyTransaction({cursor:frame.cursor,changes:rows.map(({id,row})=>({entity:source.entity,id,operation:'update',newValue:row}))},empty,state.scope);}));
 }
 private async replay(state:State,source:Source){
  const header=await this.windows.header(state.scope,source.signature);if(!header?.header.cursor)fail('PAGE_CHANGED','Native base publication missing');let after=header.header.cursor.revision;
  for(;;){const page=await this.deferred.read(state.scope,source.signature,state.epoch,after);if(!page.length)break;for(const entry of page){await this.applyDelta(state,source,entry.frame);await this.options.owner.database.runAsync('INSERT INTO gv_native_feed_progress VALUES(?,?,0,0,?) ON CONFLICT(scope,epoch) DO UPDATE SET unclosed=max(unclosed,excluded.unclosed)',state.scope,state.epoch,entry.revision);after=entry.revision;}}
  // Each iteration owns bounded records; callers already hold the owner FIFO.
  while((await this.deferred.discardThrough(state.scope,source.signature,state.epoch,after)).remaining){}
 }
 private async publishWatermark(state:State){
  if(state.watermark===undefined||[...this.sources.values()].some(source=>source.opening))return;
  const revision=state.watermark;
  let applied=false;await this.run(state,0,()=>this.options.owner.database.withTransactionAsync(async()=>{
   await this.groups.drain(state.stream,revision,async frame=>{
    if(frame.type==='replica.transactionEncoded'){const keys=Object.fromEntries(Object.entries(this.options.owner.engine.schema).map(([entity,table])=>[entity,table.key]));await this.mutate(state.scope,async()=>{await this.options.owner.canonical.applyTransaction({cursor:frame.cursor,changes:decodeSQLiteNativeTransaction(frame,keys)},empty,state.scope);});}
    else if(frame.type==='replica.delta')await this.applyDelta(state,this.source(frame.id),frame);
    else await this.windows.confirmReady(state.scope,this.source(frame.id).signature,frame.cursor,frame.digest,frame.truncated??false);
   });
   const pending=await this.options.owner.database.getFirstAsync<{unclosed:number;applied:number}>('SELECT unclosed,applied FROM gv_native_feed_progress WHERE scope=? AND epoch=?',state.scope,state.epoch);if(pending&&pending.unclosed>revision)return;
   await this.options.owner.database.runAsync('INSERT INTO gv_native_feed_progress VALUES(?,?,?, ?,0) ON CONFLICT(scope,epoch) DO UPDATE SET observed=max(observed,excluded.observed),applied=max(applied,excluded.applied),unclosed=0',state.scope,state.epoch,revision,revision);if(!pending||pending.applied<revision||pending.unclosed)await markSQLiteStagingCommit(this.options.owner.database,()=>this.options.owner.engine.verifyOwner());applied=true;
  }));this.check(state);if(applied)this.options.advanceWatermark(revision);
 }
 private async process(state:State,message:ServerMessage):Promise<void>{
  const {owner}=this.options;
  switch(message.type){
   case 'replica.snapshotStart':{
    const source=this.source(message.id);this.cursor(state,message.cursor);
    if(message.visibilityScope!==state.scope||message.scope!==state.logicalScope||message.key!==source.key||message.path!==undefined&&message.path!==source.path||!message.snapshotId||message.snapshotId.length>128)fail('INVALID_ROW','Native start differs from generated source');
    if(source.start)fail('PAGE_CHANGED','Native source received overlapping snapshots');const startBytes=sqlitePayloadBytes(message,16384);if(this.sourceBytes+this.startBytes+this.closingBytes+startBytes>262144)fail('BUDGET','Native source header registry exceeded budget');
    await this.run(state,16384,async()=>{await owner.snapshots.prepareTyped({scope:state.scope,subscription:source.id,snapshot:message.snapshotId,entity:source.entity});});source.start=message;this.startBytes+=startBytes;source.opening=true;return;
   }
   case 'replica.closed':{
    const source=this.source(message.id),closing=source.closing;if(!closing)fail('INVALID_ROW','Unexpected native source close acknowledgement');
    await this.run(state,16384,async()=>{
     const stage=source.start?{subscription:source.id,snapshot:source.start.snapshotId}:await owner.database.getFirstAsync<{subscription:string;snapshot:string}>('SELECT a.subscription,a.snapshot FROM gv_windows w JOIN gv_snapshot_window_alias a USING(scope,signature,generation) WHERE w.scope=? AND w.signature=?',state.scope,source.signature);
     if(stage)await owner.snapshots.discard(state.scope,stage.subscription,stage.snapshot);
    });
    this.removeSource(source.id);closing.resolve();await this.publishWatermark(state);return;
   }
   case 'replica.delta':{
    const source=this.source(message.id);this.cursor(state,message.cursor);
    await this.run(state,262144,async()=>{const header=await this.windows.header(state.scope,source.signature);if(source.start||!header||header.header.cursor?.epoch!==state.epoch)await this.deferred.append(state.scope,source.signature,message);else await this.groups.append(state.stream,message);});return;
   }
   case 'replica.transactionEncoded':{
    this.cursor(state,message.cursor);
    // Validate before taking disk space; canonical authority stays unchanged
    // until the terminal watermark publishes this complete group.
    decodeSQLiteNativeTransaction(message,Object.fromEntries(Object.entries(owner.engine.schema).map(([entity,table])=>[entity,table.key])));
    await this.run(state,262144,()=>this.groups.append(state.stream,message));return;
   }
   case 'replica.readyMany':for(const ready of message.ready)await this.process(state,{type:'replica.ready',...ready});return;
   case 'replica.ready':{
    const source=this.source(message.id);this.cursor(state,message.cursor);
    if(!source.opening){await this.run(state,16384,()=>this.groups.append(state.stream,message));return;}
    await this.run(state,16384,async()=>{await this.replay(state,source);await this.windows.confirmReady(state.scope,source.signature,message.cursor,message.digest,message.truncated??false);});if(source.start)this.startBytes-=sqlitePayloadBytes(source.start,16384);source.start=undefined;source.opening=false;await this.publishWatermark(state);return;
   }
   case 'replica.watermark':if(!Number.isSafeInteger(message.revision)||message.revision<0)fail('INVALID_ROW','Invalid native watermark');state.watermark=Math.max(state.watermark??0,message.revision);await this.publishWatermark(state);return;
   case 'replica.error':fail('PAGE_CHANGED','Native source requires reconciliation');
   default:fail('INVALID_ROW','Unexpected native replica control');
  }
 }
 /** A verified read cannot expose half a transaction/membership feed group.
  * Pending first-sync pages use their separate incomplete display contract. */
 async assertReadable(cached?:{scope:string;epoch:string}){
  const state=this.state;
  if(!state){
   if(!cached)fail('FENCED','Native feed unavailable');sqlitePayloadBytes(cached,4096);
   await this.options.owner.runOwner(0,async()=>{await this.options.owner.engine.verifyOwner();const progress=await this.options.owner.database.getFirstAsync<{unclosed:number}>('SELECT unclosed FROM gv_native_feed_progress WHERE scope=? AND epoch=?',cached.scope,cached.epoch);if(progress?.unclosed)fail('PAGE_CHANGED','Cached feed requires reconciliation of its unfinished group');});return;
  }
  await state.gate;await state.ingress.flush();await this.run(state,0,async()=>{if(await this.groups.pending(state.stream))fail('PAGE_CHANGED','Native feed awaits its terminal watermark');const progress=await this.options.owner.database.getFirstAsync<{unclosed:number}>('SELECT unclosed FROM gv_native_feed_progress WHERE scope=? AND epoch=?',state.scope,state.epoch);if(progress?.unclosed)fail('PAGE_CHANGED','Native feed awaits its terminal watermark');});
 }
 quarantined(generation:number){this.stop();void this.options.sessions.quarantine(this.options.connection,generation).catch(error=>this.options.failed(error));}
 disconnected(){this.stop();this.options.sessions.disconnected(this.options.connection);}
 failed(error:unknown){this.stop();this.options.failed(error);}
 close(){this.stop();this.sources.clear();this.sourceBytes=0;this.startBytes=0;this.closingBytes=0;}
 stats(){return {sources:this.sources.size,sourceBytes:this.sourceBytes+this.startBytes+this.closingBytes,queued:this.queued,queuedBytes:this.queuedBytes,ingress:this.state?.ingress.stats()};}
}
