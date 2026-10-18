import {afterEach,expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
import {reducer,schema,getRow} from '@gonvex/module-sdk';
import {initializePinnedSQLite} from '../test/sqlite-wasm-host.mjs';
import {officialSQLiteDatabase} from './sqlite-official-host.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {SQLiteSnapshotActivator} from './sqlite-snapshot-activation.js';
import {SQLiteNativeReplicaReader} from './sqlite-native-replica-reader.js';
import {SQLitePendingSnapshotReader} from './sqlite-pending-snapshot-reader.js';
import {SQLiteClientProgram} from './sqlite-client-program.js';
import {replicaRowsHashes,replicaSumDigest} from './replica-integrity.js';
import type {SQLiteOwnerProgramContext,SQLiteOwnerCaller} from './sqlite-owner-actor.js';
import type {FunctionReference} from './index.js';
class Socket {
 static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;static instances:Socket[]=[];
 readyState=0;binaryType='blob';sent:any[]=[];listeners=new Map<string,Array<(event:any)=>void>>();
 constructor(readonly url:string){Socket.instances.push(this);}
 addEventListener(type:string,listener:(event:any)=>void){const values=this.listeners.get(type)??[];values.push(listener);this.listeners.set(type,values);}
 emit(type:string,event:any){for(const listener of this.listeners.get(type)??[])listener(event);}
 send(text:string){this.sent.push(JSON.parse(text));}
 receive(message:unknown){this.emit('message',{data:message instanceof ArrayBuffer?message:JSON.stringify(message)});}
 open(){this.readyState=1;this.emit('open',{});}
 close(){if(this.readyState===3)return;this.readyState=3;this.emit('close',{});}
}
const sqlite=await initializePinnedSQLite(),cleanup:Array<()=>Promise<void>>=[];
afterEach(async()=>{for(const close of cleanup.splice(0))await close();vi.unstubAllGlobals();vi.restoreAllMocks();});
const typed={tasks:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'}},indexes:[]}} as const;
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
const refs:FunctionReference[]=[{kind:'query',path:'__local.tasks',delivery:'replica',replica:{table:'tasks',key:'id',columns:['id','n'],maxRows:1000,maxBytes:1000000}},{kind:'reducer',path:'tasks.increment',localExecution:1},{kind:'reducer',path:'tasks.addAll',localExecution:1}];
const caller=(tabID:string,requestID=crypto.randomUUID()):SQLiteOwnerCaller=>({tabID,requestID,signal:new AbortController().signal});
const connect={method:'connect',url:'ws://synthetic-private.test/ws',auth:{project:'project',tenant:'tenant',identity:{sub:'provider-account'}}};
const rowDigest=async(row:{id:string;n:number})=>replicaSumDigest(await replicaRowsHashes([row],'id'));
async function fixture(autoEager=true){
 vi.stubGlobal('indexedDB',new IDBFactory());vi.stubGlobal('IDBKeyRange',IDBKeyRange);vi.stubGlobal('WebSocket',Socket);vi.stubGlobal('postMessage',()=>{});Socket.instances=[];
 const raw=new sqlite.oo1.DB(':memory:'),host=officialSQLiteDatabase(sqlite,raw),db=sqliteOwnerTransactions(host.database),engine=new SQLiteTypedReplicaEngine(db,typed);await engine.open('program');const canonical=sqliteTypedReplicaStorage(db,typed);await canonical.ready();const fence=()=>engine.verifyOwner(),snapshots=new SQLiteSnapshotPages(db,fence,typed),activation=new SQLiteSnapshotActivator(db,canonical,fence,typed),reader=new SQLiteNativeReplicaReader(db,canonical,engine),pendingReader=new SQLitePendingSnapshotReader({database:db,canonical,snapshots,schema:typed,fence});
 let lane=Promise.resolve(),executions=0;const hints:string[]=[],owner={database:db,engine,canonical,snapshots,activation,reader,pendingReader,async notifyTab(tab:string){hints.push(tab);},runOwner<T>(_bytes:number,run:()=>Promise<T>){const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;}} as SQLiteOwnerProgramContext;
 const runtime=new PortableReducerRuntime({schema:{tasks:{key:'id',columns:{id:{type:'text',nullable:false},n:{type:'bigint',nullable:false}}}},artifactHash:'artifact',reducers:{'tasks.increment':reducer({args:schema.any(),result:schema.any(),run:async ctx=>{executions++;const row=await getRow<{id:string;n:number}>(ctx.db,'tasks','id','a');await ctx.db.update('tasks','a',{n:row!.n+1});return row!.n+1;}}),'tasks.addAll':reducer({args:schema.any(),result:schema.any(),run:async ctx=>{const rows=await ctx.db.select<{id:string;n:number}>({table:'tasks'}),n=rows.reduce((total,row)=>total+row.n,0)+1;await ctx.db.update('tasks','a',{n});return n;}})}});
 const program=new SQLiteClientProgram(owner,{artifactHash:'artifact',runtime,references:refs,collections:autoEager?['__local.tasks']:[],offlineMaxAgeMs:5000});cleanup.push(async()=>{program.close();await lane;await new Promise(resolve=>setTimeout(resolve,0));raw.close();});
 const call=(input:unknown,tab='one',request?:string)=>program.call(input,caller(tab,request));
 async function authenticate(){
  const response=await call(connect);if(!autoEager)await call({method:'watchReplica',path:'__local.tasks',args:{}});const socket=Socket.instances[0];socket.open();await vi.waitFor(async()=>expect((await call({method:'controls'}) as any).tokens).toHaveLength(1));const request=(await call({method:'controls'}) as any).tokens[0];expect(hints).toContain('one');await call({method:'token',response:{...request,token:'opaque'}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='auth')).toBe(true));const auth=socket.sent.find(message=>message.type==='auth');socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1,replicaWatermark:1}});socket.receive({type:'auth.result',id:auth.id,result:{artifactHash:'artifact',localIdentity:identity,replica:{protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'e'}}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='replica.open')).toBe(true));
  const open=socket.sent.find(message=>message.type==='replica.open'),row={id:'a',n:1},digest=await rowDigest(row);socket.receive({type:'replica.watermark',revision:1});socket.receive({type:'replica.snapshotStart',id:open.id,snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',digest,path:'__local.tasks'});socket.receive(new TextEncoder().encode(JSON.stringify({type:'replica.snapshotEncodedPage',id:open.id,snapshotId:'base',scope:'tenant',visibilityScope:'visible',cursor:{epoch:'e',revision:1},key:'id',sequence:0,last:true,canonicalRows:[JSON.stringify(row)],digest,path:'__local.tasks'})).buffer);socket.receive({type:'replica.ready',id:open.id,cursor:{epoch:'e',revision:1},digest});await vi.waitFor(()=>expect(socket.sent.filter(message=>message.type==='replica.snapshotAck')).toHaveLength(1));return {socket,open,response};
 }
 return {owner,program,call,authenticate,engine,db,canonical,hints,get executions(){return executions;}};
}
it('composes token-provider authentication, one shared connection, typed pages, local execution, server settlement and original reply replay',async()=>{
 const f=await fixture(),{socket,open,response}=await f.authenticate();expect(await f.call(connect,'two')).toEqual(response);expect(Socket.instances).toHaveLength(1);const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});const page:any=await f.call({method:'replicaPage',signature:source.signature,limit:10});expect(page.rows).toEqual([{id:'a',value:{id:'a',n:1}}]);
 const first:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','command');expect(first.result).toBe(2);expect(f.executions).toBe(1);expect((await f.call({method:'entities',entity:'tasks',ids:['a']},'two') as any).rows).toEqual([{id:'a',value:{id:'a',n:2}}]);await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='reducer.call')).toBe(true));const intent=socket.sent.find(message=>message.type==='reducer.call');expect(intent.idempotencyKey).toBe('command');
 socket.receive({type:'reducer.result',id:intent.id,result:2,committedRevision:2,originCommandId:'command'});const row={id:'a',n:2},digest=await rowDigest(row);socket.receive({type:'replica.transactionEncoded',cursor:{epoch:'e',revision:2},canonicalChanges:[JSON.stringify({entity:'tasks',id:'a',operation:'update',newValue:row})]});socket.receive({type:'replica.delta',id:open.id,cursor:{epoch:'e',revision:2},canonicalUpserts:[JSON.stringify(row)],deleted:[],digest});socket.receive({type:'replica.watermark',revision:2});
 await vi.waitFor(async()=>expect((await f.owner.runOwner(0,()=>f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_reducer_journal')))?.n).toBe(0));expect(await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','command')).toEqual(first);expect(f.executions).toBe(1);expect(await f.engine.integrity()).toBe('ok');f.program.detachTab('one');expect(socket.readyState).toBe(1);expect((await f.call({method:'entities',entity:'tasks',ids:['a']},'two') as any).rows[0].value.n).toBe(2);f.program.detachTab('two');expect(socket.readyState).toBe(3);
});
it('serves verified cached SQL pages and durably predicts an offline reducer while the socket is disconnected',async()=>{
 const f=await fixture(),{socket}=await f.authenticate();const source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});socket.close();expect((await f.call({method:'replicaPage',signature:source.signature,limit:10}) as any).rows[0].value.n).toBe(1);const receipt:any=await f.call({method:'reducer',path:'tasks.increment',args:{},now:100},'one','offline-command');expect(receipt.result).toBe(2);expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(2);expect(socket.sent.filter(message=>message.type==='reducer.call')).toEqual([]);expect((await f.owner.runOwner(0,()=>f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_reducer_journal')))?.n).toBe(1);expect(await f.engine.integrity()).toBe('ok');
});
it('retains certified full-entity projection coverage after unwatch so an offline reducer can scan cached SQL rows',async()=>{
 const f=await fixture(false),{socket,open}=await f.authenticate(),source:any=await f.call({method:'watchReplica',path:'__local.tasks',args:{}});const closing=f.call({method:'unwatchReplica',signature:source.signature});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='replica.close')).toBe(true));socket.receive({type:'replica.closed',id:open.id});await closing;socket.close();
 const result:any=await f.call({method:'reducer',path:'tasks.addAll',args:{},now:100},'one','offline-scan');expect(result.result).toBe(2);expect((await f.call({method:'entities',entity:'tasks',ids:['a']}) as any).rows[0].value.n).toBe(2);expect(socket.sent.filter(message=>message.type==='reducer.call')).toEqual([]);expect(await f.engine.integrity()).toBe('ok');
});
it('rejects unknown function metadata, oversized tokens and unowned native windows',async()=>{
 const f=await fixture();await f.authenticate();await expect(f.call({method:'reducer',path:'unknown',args:{},now:100})).rejects.toMatchObject({code:'INVALID_ROW'});await expect(f.call({method:'replicaPage',signature:'other-user-source'})).rejects.toMatchObject({code:'FENCED'});await expect(f.call({method:'auth',auth:{tenant:'other'}})).rejects.toThrow('identity changes require a new connection');await expect(f.call({method:'auth',auth:{token:'x'.repeat(16385)}})).rejects.toThrow('Invalid native');
});
