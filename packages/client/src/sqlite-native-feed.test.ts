import {afterEach,expect,it,vi} from 'vitest';
import {GonvexClient} from './index.js';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {initializePinnedSQLite} from '../test/sqlite-wasm-host.mjs';
import {officialSQLiteDatabase} from './sqlite-official-host.js';
import {SQLiteOwnerQueueBusyError,sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {SQLiteSnapshotActivator} from './sqlite-snapshot-activation.js';
import {SQLiteWorkerSessions} from './sqlite-worker-sessions.js';
import {SQLiteSafetyJournal} from './sqlite-safety-journal.js';
import {SQLiteReducerPreparationRequiredError} from './sqlite-reducer-core.js';
import {SQLiteNativeFeed} from './sqlite-native-feed.js';
import {SQLiteWindowStore} from './sqlite-window-store.js';
import {SQLiteDeferredReplicaFrames} from './sqlite-deferred-replica-frames.js';
import {decodeSQLiteNativeDelta,decodeSQLiteNativeTransaction} from './sqlite-native-control.js';
import {replicaRowsHashes,replicaSumDigest} from './replica-integrity.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
const sqlite=await initializePinnedSQLite(),cleanup:Array<()=>void>=[];
afterEach(()=>{vi.restoreAllMocks();for(const close of cleanup.splice(0))close();});
const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},n:{kind:'number'}},indexes:[]}} as const;
const cursor=(revision:number)=>({epoch:'e',revision});
const stable=(row:Record<string,unknown>)=>JSON.stringify(Object.fromEntries(Object.keys(row).sort().map(key=>[key,row[key]])));
const rows=[{id:'a',name:'Before café 東京',n:1},{id:'b',name:'Second',n:2}],changed={id:'a',name:'Newer canonical',n:12};
const digest=async(values=rows)=>replicaSumDigest(await replicaRowsHashes(values,'id'));
async function fixture(extra:Partial<ConstructorParameters<typeof SQLiteNativeFeed>[0]>={},completeEntity=false){
 const raw=new sqlite.oo1.DB(':memory:'),host=officialSQLiteDatabase(sqlite,raw),db=sqliteOwnerTransactions(host.database),engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open('feed');const canonical=sqliteTypedReplicaStorage(db,schema);await canonical.ready();
 const fence=()=>engine.verifyOwner(),snapshots=new SQLiteSnapshotPages(db,fence,schema),activation=new SQLiteSnapshotActivator(db,canonical,fence,schema),safety=new SQLiteSafetyJournal('feed',new IDBFactory(),IDBKeyRange);
 let lane=Promise.resolve();const runOwner=<T>(_bytes:number,run:()=>Promise<T>)=>{const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;};
 const sessions=new SQLiteWorkerSessions({artifactHash:'artifact',offlineMaxAgeMs:5000,database:db,canonical,safety,runOwner,fence});sessions.open('one');sessions.attachTab('tab','one');
 const owner={database:db,engine,canonical,snapshots,activation,runOwner} as SQLiteOwnerProgramContext;
 const sends:any[]=[],watermarks:number[]=[],failures:unknown[]=[];const feed=new SQLiteNativeFeed({owner,sessions,connection:'one',artifactHash:'artifact',send:message=>sends.push(message),advanceWatermark:revision=>watermarks.push(revision),failed:error=>failures.push(error),...extra});
 cleanup.push(()=>{feed.close();safety.close();raw.close();});
 feed.register({id:'source',signature:'tasks-all',entity:'tasks',key:'id',path:'tasks/all',...(completeEntity?{completeEntity:true as const}:{})});feed.authenticating('identity',1);
 const session={connectionGeneration:1,directive:{protocolVersion:1 as const,scope:'tenant',visibilityScope:'visible',epoch:'e'},identity:{auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}},artifactHash:'artifact',intentScope:'identity',lastOnlineAtMs:1000,capabilities:{replicaNativeRows:1}};
 await feed.session(session,()=>true);
 const start=async()=>{feed.frame({type:'replica.snapshotStart',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',digest:await digest(),path:'tasks/all'},1);};
 const append=async(part:typeof rows,sequence:number,last:boolean)=>{feed.binary(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',sequence,last,canonicalRows:part.map(stable),path:'tasks/all',...(last?{digest:await digest()}:{})})).buffer,1);};
 const ready=async(revision=10,values=rows)=>feed.frame({type:'replica.ready',id:'source',cursor:cursor(revision),digest:await digest(values)},1);
 const transaction=(revision=12)=>feed.frame({type:'replica.transactionEncoded',cursor:cursor(revision),originCommandId:'intent',canonicalChanges:[stable({entity:'tasks',id:'a',operation:'update',newValue:{...changed,n:revision}})]},1);
 return {owner,db,engine,canonical,feed,sends,watermarks,failures,start,append,ready,transaction,session,sessions,safety,runOwner};
}
it('joins auth, source start, binary staging, newer canonical data, disk deltas and terminal watermark without deadlock',async()=>{
 const f=await fixture();await f.start();await f.append(rows.slice(0,1),0,false);await vi.waitFor(()=>expect(f.sends).toHaveLength(1));
 f.transaction();f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest([changed,rows[1]])},1);
 f.feed.frame({type:'replica.watermark',revision:14},1);await vi.waitFor(async()=>{expect(f.failures).toEqual([]);expect((await f.runOwner(0,()=>new SQLiteDeferredReplicaFrames(f.db,'artifact',()=>f.engine.verifyOwner()).summary('visible'))).frames).toBe(1);});expect(f.watermarks).toEqual([]);await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});
 await f.append(rows.slice(1),1,true);await f.ready(14,[changed,rows[1]]);await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.watermarks).toEqual([14]);expect(f.sends.map(ack=>ack.sequence)).toEqual([0,1]);
 expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).map(entry=>entry.row)).toEqual([changed,rows[1]]);const window=await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all');expect(window?.header.cursor).toEqual(cursor(14));expect(window?.header.aggregateDigest).toBe(await digest([changed,rows[1]]));expect((await new SQLiteDeferredReplicaFrames(f.db,'artifact',()=>f.engine.verifyOwner()).summary('visible')).frames).toBe(0);expect(await f.engine.integrity()).toBe('ok');
});
it('leaves a newer feed group pending when a ready arrives before its terminal watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.transaction(12);f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest([changed,rows[1]])},1);await f.ready(12,[changed,rows[1]]);await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});expect(f.failures).toEqual([]);expect(f.watermarks).toEqual([10]);
 f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();expect(f.watermarks).toEqual([10,12]);
});
it('rolls back final snapshot activation and returns no final credit when a deferred certificate is invalid',async()=>{
 const f=await fixture();await f.start();await f.append(rows.slice(0,1),0,false);await vi.waitFor(()=>expect(f.sends).toHaveLength(1));f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:'sum128-v1:'+'0'.repeat(32)},1);await f.append(rows.slice(1),1,true);
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));expect(f.sends).toHaveLength(1);expect(f.watermarks).toEqual([]);expect(await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all')).toBeUndefined();expect(await f.canonical.loadEntityRows('visible','tasks',['a','b'])).toEqual([]);expect((await f.db.getFirstAsync<{next_page:number}>('SELECT next_page FROM gv_snapshot_stages'))?.next_page).toBe(1);expect(await f.engine.integrity()).toBe('ok');
});
it('fences an already queued control after same-socket reauthentication without failing the new admission',async()=>{
 const f=await fixture();f.transaction();f.feed.authenticating('identity',1);await f.feed.session(f.session,()=>true);await new Promise(resolve=>setTimeout(resolve,10));expect(f.failures).toEqual([]);expect(await f.canonical.loadEntityRows('visible','tasks',['a'])).toEqual([]);expect(f.feed.stats().queued).toBe(0);
});
it('owns binary credits immediately but waits for durable authentication before preparing or writing a stage',async()=>{
 const f=await fixture();const original=f.safety.saveSession.bind(f.safety);let release!:()=>void,entered!:()=>void;const waiting=new Promise<void>(resolve=>{entered=resolve;}),barrier=new Promise<void>(resolve=>{release=resolve;});
 vi.spyOn(f.safety,'saveSession').mockImplementation(async(...args)=>{entered();await barrier;return original(...args);});
 f.feed.authenticating('identity',1);const activation=f.feed.session(f.session,()=>true);await waiting;await f.start();await f.append(rows,0,true);await f.ready();expect(f.sends).toEqual([]);expect(f.feed.stats().ingress?.entries).toBe(1);expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_stages')).toBeNull();
 release();await activation;await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.sends).toHaveLength(1);expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).length).toBe(2);
});
it('rolls back a cold prediction rebase, loads its chunk outside the SQL queue and retries atomically',async()=>{
 let cold=false,loads=0,release!:()=>void,entered!:()=>void;const loading=new Promise<void>(resolve=>{release=resolve;}),waiting=new Promise<void>(resolve=>{entered=resolve;});
 const f=await fixture({async authority(_scope,mutate){if(cold)throw new SQLiteReducerPreparationRequiredError('tasks/increment');await mutate();},async prepare(path){expect(path).toBe('tasks/increment');loads++;entered();await loading;cold=false;}});
 await f.start();await f.append(rows,0,true);await f.ready();await f.feed.assertReadable();cold=true;f.transaction();f.feed.frame({type:'replica.watermark',revision:12},1);await waiting;
 expect((await f.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a'])))[0].row).toEqual(rows[0]);expect(await f.runOwner(0,()=>f.engine.integrity())).toBe('ok');
 release();f.feed.frame({type:'replica.watermark',revision:12},1);await f.feed.assertReadable();expect(loads).toBe(1);expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual(changed);
});
it('processes authenticated GClient socket events through the WASM feed and settles a reducer only after durable terminal progress',async()=>{
 class Socket {
  static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;static current:Socket;
  readyState=0;binaryType='blob';sent:any[]=[];listeners=new Map<string,Array<(event:any)=>void>>();
  constructor(){Socket.current=this;}
  addEventListener(type:string,listener:(event:any)=>void){const values=this.listeners.get(type)??[];values.push(listener);this.listeners.set(type,values);}
  emit(type:string,event:any){for(const listener of this.listeners.get(type)??[])listener(event);}
  send(text:string){this.sent.push(JSON.parse(text));}
  receive(message:unknown){this.emit('message',{data:message instanceof ArrayBuffer?message:JSON.stringify(message)});}
  close(){this.readyState=3;this.emit('close',{});}
 }
 vi.stubGlobal('WebSocket',Socket);vi.stubGlobal('postMessage',()=>{});let client!:GonvexClient;
 const f=await fixture({send:message=>client.sendOwnerMessage(message),advanceWatermark:revision=>client.advanceOwnerWatermark(revision)});
 client=new GonvexClient('ws://synthetic-private.test/ws',{ownerHost:f.feed,errorReporting:false,project:'project',tenant:'tenant',token:'opaque',identity:{sub:'account'}});
 try{
  client.connect();const socket=Socket.current;socket.readyState=1;socket.emit('open',{});socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1,replicaWatermark:1}});const auth=socket.sent.find(message=>message.type==='auth');expect(auth.capabilities.replicaNativeRows).toBe(1);
  socket.receive({type:'auth.result',id:auth.id,result:{localIdentity:f.session.identity,replica:f.session.directive,artifactHash:'artifact'}});
  socket.receive({type:'replica.snapshotStart',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',digest:await digest(),path:'tasks/all'});
  socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:'source',snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:cursor(10),key:'id',sequence:0,last:true,canonicalRows:rows.map(stable),path:'tasks/all',digest:await digest()})).buffer);
  socket.receive({type:'replica.ready',id:'source',cursor:cursor(10),digest:await digest()});socket.receive({type:'replica.watermark',revision:10});await f.feed.assertReadable();expect(client.localSessionState()).toBe('authenticated');expect(socket.sent.filter(message=>message.type==='replica.snapshotAck')).toHaveLength(1);
  let settled=false;const pending=client.callOwnerReducer({kind:'reducer',path:'tasks/change',artifactHash:'artifact'},{id:'a'},'command',0).then(value=>{settled=true;return value;});void pending.catch(()=>{});const call=socket.sent.find(message=>message.type==='reducer.call');
  socket.receive({type:'reducer.result',id:call.id,result:{ok:true},originCommandId:'command',committedRevision:12});expect(settled).toBe(false);
  socket.receive({type:'replica.transactionEncoded',cursor:cursor(12),canonicalChanges:[stable({entity:'tasks',id:'a',operation:'update',newValue:changed})]});socket.receive({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:await digest([changed,rows[1]])});socket.receive({type:'replica.watermark',revision:12});
  expect(await pending).toEqual({ok:true});expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual(changed);expect(f.failures).toEqual([]);expect(client.localReplica.snapshot().entities).toEqual({});expect(await f.engine.integrity()).toBe('ok');
 }finally{client.close();vi.unstubAllGlobals();}
});
it('prunes old orphans through real typed native activation only when the generated source declares complete entity coverage',async()=>{
 const f=await fixture({},true);
 await f.canonical.applyTransaction({cursor:cursor(5),changes:[{entity:'tasks',id:'orphan',operation:'insert',newValue:{id:'orphan',name:'removed before source',n:5}}]},{entities:{},liveQueries:{}},'visible');
 await f.canonical.applyTransaction({cursor:cursor(25),changes:[{entity:'tasks',id:'newer',operation:'insert',newValue:{id:'newer',name:'newer authority',n:25}}]},{entities:{},liveQueries:{}},'visible');
 await f.start();await f.append(rows,0,true);await f.ready();await f.feed.assertReadable();expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['orphan','newer','a','b'])).map(entry=>entry.id)).toEqual(['newer','a','b']);expect((await new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all'))?.header.entityCoverage).toBe('complete');expect(await f.engine.integrity()).toBe('ok');
});
it('keeps admitted frames and their credits while another bounded owner operation temporarily fills the SQL queue',async()=>{
 const f=await fixture(),original=f.owner.runOwner.bind(f.owner);let refused=0;
 vi.spyOn(f.owner,'runOwner').mockImplementation(async(...args:any[])=>{if(refused++<2)throw new SQLiteOwnerQueueBusyError();return (original as any)(...args);});
 await f.start();await f.append(rows,0,true);await f.ready();await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.sends).toHaveLength(1);expect((await f.canonical.loadEntityRows('visible','tasks',['a','b'])).length).toBe(2);expect(refused).toBeGreaterThan(2);
});
it('retires a closing source only after its earlier binary frame commits and the server acknowledges cancellation',async()=>{
 const f=await fixture();await f.start();await f.append(rows.slice(0,1),0,false);let closed=false;const pending=f.feed.closeSource('source').then(()=>{closed=true;});expect(f.sends.at(-1).type).toBe('replica.close');expect(closed).toBe(false);expect(f.feed.stats().sources).toBe(1);f.feed.frame({type:'replica.closed',id:'source'},1);await pending;await f.feed.assertReadable();expect(f.failures).toEqual([]);expect(f.feed.stats().sources).toBe(0);expect(f.sends.filter(frame=>frame.type==='replica.snapshotAck')).toHaveLength(1);expect(await f.db.getFirstAsync('SELECT 1 FROM gv_snapshot_stages')).toBeNull();expect(await f.engine.integrity()).toBe('ok');
});
it('releases a pending source close when its connection changes, without leaking a reply waiter',async()=>{
 const f=await fixture();await f.start();await f.feed.assertReadable();const closing=f.feed.closeSource('source');void closing.catch(()=>{});f.feed.disconnected();await expect(closing).rejects.toMatchObject({code:'FENCED'});expect(f.feed.stats().sources).toBe(0);expect(f.failures).toEqual([]);
});
it('rolls back canonical writes when same-socket admission changes during an awaited disk phase',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();const original=f.canonical.applyTransaction.bind(f.canonical);let entered!:()=>void,release!:()=>void;const waiting=new Promise<void>(resolve=>{entered=resolve;}),barrier=new Promise<void>(resolve=>{release=resolve;});
 vi.spyOn(f.canonical,'applyTransaction').mockImplementation(async(...args)=>{const result=await original(...args);entered();await barrier;return result;});f.transaction();f.feed.frame({type:'replica.watermark',revision:12},1);await waiting;f.feed.authenticating('identity',1);release();await vi.waitFor(()=>expect(f.feed.stats().queued).toBe(0));expect(f.failures).toEqual([]);expect((await f.canonical.loadEntityRows('visible','tasks',['a']))[0].row).toEqual(rows[0]);expect((await f.db.getFirstAsync<{applied:number}>('SELECT applied FROM gv_native_feed_progress'))?.applied).toBe(10);
});
it('hashes canonical delta text before number decoding and rejects oversized or malformed controls before parsing',async()=>{
 const text='{"id":"unsafe","n":9007199254740993,"name":"東京"}',frame={type:'replica.delta' as const,id:'source',cursor:cursor(12),canonicalUpserts:[text],deleted:[]};
 const decoded=await decodeSQLiteNativeDelta(frame,'id');const hash=Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',new TextEncoder().encode(text))),byte=>byte.toString(16).padStart(2,'0')).join('');expect(decoded[0].hash).toBe(hash);expect(decoded[0].row.n).toBe(9007199254740992);
 const parse=vi.spyOn(JSON,'parse');await expect(decodeSQLiteNativeDelta({...frame,canonicalUpserts:[' '.repeat(200000)]},'id')).rejects.toMatchObject({code:'BUDGET'});expect(parse).not.toHaveBeenCalled();parse.mockRestore();await expect(decodeSQLiteNativeDelta({...frame,canonicalUpserts:[text,text]},'id')).rejects.toMatchObject({code:'INVALID_ROW'});
 expect(()=>decodeSQLiteNativeTransaction({type:'replica.transactionEncoded',cursor:cursor(12),canonicalChanges:[stable({entity:'other',id:'a',operation:'delete'})]},{tasks:'id'})).toThrow('Invalid native transaction');
});

it('serves the last consistent offline page after disconnect between transaction, delta and watermark',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();
 f.transaction(12);f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:['b'],digest:await digest([changed])},1);await f.ready(12,[changed]);await expect(f.feed.assertReadable()).rejects.toMatchObject({code:'PAGE_CHANGED'});
 expect((await f.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a','b']))).map(entry=>entry.row)).toEqual(rows);
 f.feed.disconnected();await f.feed.assertReadable({scope:'visible',epoch:'e'});expect((await f.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a','b']))).map(entry=>entry.row)).toEqual(rows);
 expect((await f.runOwner(0,()=>new SQLiteWindowStore(f.db,()=>f.engine.verifyOwner()).header('visible','tasks-all')))?.header.cursor).toEqual(cursor(10));expect(f.watermarks).toEqual([10]);expect(f.failures).toEqual([]);
});
it('rolls the whole terminal group back when a later membership digest fails',async()=>{
 const f=await fixture();await f.start();await f.append(rows,0,true);await f.ready();f.feed.frame({type:'replica.watermark',revision:10},1);await f.feed.assertReadable();f.transaction(12);
 f.feed.frame({type:'replica.delta',id:'source',cursor:cursor(12),canonicalUpserts:[stable(changed)],deleted:[],digest:'sum128-v1:'+'0'.repeat(32)},1);f.feed.frame({type:'replica.watermark',revision:12},1);
 await vi.waitFor(()=>expect(f.failures).toHaveLength(1));expect((await f.runOwner(0,()=>f.canonical.loadEntityRows('visible','tasks',['a','b']))).map(entry=>entry.row)).toEqual(rows);await f.feed.assertReadable({scope:'visible',epoch:'e'});expect(f.watermarks).toEqual([10]);expect(await f.runOwner(0,()=>f.engine.integrity())).toBe('ok');
});
