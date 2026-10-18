import {afterEach,expect,it,vi} from 'vitest';
import {SQLiteNativeTransport} from './sqlite-native-transport.js';
import {SQLiteTransientBudget} from './sqlite-transient-budget.js';
import type {SQLiteTransportHost,SQLiteTransportSession} from './sqlite-transport-host.js';
import type {GonvexClientAuth} from './index.js';
class Socket {
 static instances:Socket[]=[];static CONNECTING=0;static OPEN=1;static CLOSED=3;
 readyState=0;binaryType='';bufferedAmount=0;sent:any[]=[];listeners=new Map<string,Array<(event:any)=>void>>();
 constructor(readonly url:string){Socket.instances.push(this);}
 addEventListener(type:string,listener:(event:any)=>void){const values=this.listeners.get(type)??[];values.push(listener);this.listeners.set(type,values);}
 emit(type:string,event={}){for(const listener of this.listeners.get(type)??[])listener(event);}
 open(){this.readyState=1;this.emit('open');}
 send(wire:string){if(this.readyState!==1)throw Error('closed socket');this.sent.push(JSON.parse(wire));}
 receive(message:unknown){this.emit('message',{data:message instanceof ArrayBuffer?message:JSON.stringify(message)});}
 close(){if(this.readyState===3)return;this.readyState=3;this.emit('close');}
}
const clients:SQLiteNativeTransport[]=[];
afterEach(()=>{clients.splice(0).forEach(client=>client.close());vi.unstubAllGlobals();vi.useRealTimers();});
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
const result={artifactHash:'artifact',localIdentity:identity,replica:{protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'e'}};
function fixture(auth:Partial<GonvexClientAuth>={},session?:(input:SQLiteTransportSession,current:()=>boolean)=>Promise<void>){
 vi.stubGlobal('WebSocket',Socket);Socket.instances=[];const sessions:SQLiteTransportSession[]=[],failures:unknown[]=[],frames:unknown[]=[],binary:ArrayBuffer[]=[],authenticating:string[]=[],opening:string[]=[],quarantines:number[]=[];
 const host:SQLiteTransportHost={opening:(scope)=>opening.push(scope),authenticating:scope=>authenticating.push(scope),session:async(input,current)=>{await session?.(input,current);if(current())sessions.push(input);},frame:message=>{frames.push(message);return true;},binary:value=>binary.push(value),quarantined:generation=>{quarantines.push(generation);},disconnected:vi.fn(),failed:error=>failures.push(error)};
 const budget=new SQLiteTransientBudget(2097152,256),client=new SQLiteNativeTransport('ws://private.test/ws',{project:'project',tenant:'tenant',identity:{sub:'account'},fetchToken:async()=> 'opaque',...auth,ownerHost:host,clientContract:{version:1,offlineMaxAgeMs:null},reserve:bytes=>budget.reserve(bytes)});clients.push(client);
 client.connect();const socket=Socket.instances.at(-1)!;
 async function open(){socket.open();socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1,replicaWatermark:1,controlWatermark:1}});await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='auth')).toBe(true));return socket.sent.filter(message=>message.type==='auth').at(-1);}
 async function authenticate(){const auth=await open();socket.receive({type:'auth.result',id:auth.id,result});await vi.waitFor(()=>expect(client.localSessionState()).toBe('authenticated'));return auth;}
 return {client,socket,budget,host,sessions,failures,frames,binary,authenticating,opening,quarantines,open,authenticate};
}
it('restores before socket open and forwards native frames only after correlated durable admission',async()=>{
 let release!:()=>void;const held=new Promise<void>(resolve=>{release=resolve;}),f=fixture({},async()=>held);
 expect(f.opening).toEqual(['identity\0ws://private.test/ws\0project\0tenant\0\0account']);
 const auth=await f.open();expect(auth).toMatchObject({clientContract:1,capabilities:{replicaNativeRows:1}});
 f.socket.receive({type:'auth.result',id:'old',result});expect(f.sessions).toEqual([]);
 f.socket.receive(new ArrayBuffer(1));expect(f.binary).toEqual([]);
 f.socket.receive({type:'auth.result',id:auth.id,result});expect(f.client.localSessionState()).toBe('unavailable');
 release();await vi.waitFor(()=>expect(f.client.localSessionState()).toBe('authenticated'));
 f.socket.receive(new ArrayBuffer(2));f.socket.receive({type:'replica.watermark',revision:10});expect(f.binary[0].byteLength).toBe(2);expect(f.frames).toEqual([{type:'replica.watermark',revision:10}]);expect(f.failures).toEqual([]);
});
it('retains a successful reducer until the durable watermark and sends the command once',async()=>{
 const f=fixture();await f.authenticate();let settled=false;
 const pending=f.client.callOwnerReducer({kind:'reducer',path:'tasks.edit',artifactHash:'original',intentEntropy:'entropy'},{id:'a'},'durable-command').then(value=>{settled=true;return value;});
 await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='reducer.call')).toHaveLength(1));const call=f.socket.sent.find(message=>message.type==='reducer.call');expect(call).toMatchObject({idempotencyKey:'durable-command',artifactHash:'original',intentEntropy:'entropy',clientContract:1});
 f.socket.receive({type:'reducer.result',id:call.id,result:'original',committedRevision:10,originCommandId:'durable-command'});await Promise.resolve();expect(settled).toBe(false);
 f.socket.receive({type:'reducer.result',id:call.id,result:'duplicate',committedRevision:10,originCommandId:'durable-command'});
 f.client.advanceOwnerWatermark(9);expect(settled).toBe(false);f.client.advanceOwnerWatermark(10);expect(await pending).toBe('original');expect(f.budget.stats()).toMatchObject({bytes:0,entries:0});expect(f.failures).toEqual([]);
});
it('allows a public Control Action during token fetch and waits for its Control watermark',async()=>{
 const f=fixture({tenant:undefined,identity:undefined,fetchToken:()=>new Promise(()=>{})});f.socket.open();f.socket.receive({type:'session.ready',capabilities:{controlWatermark:1,replicaNativeRows:1}});
 let settled=false;const action=f.client.action({kind:'action',path:'control.auth.passwordLogin',scope:'control',authorization:'public'},{email:'synthetic@example.test'}).then(value=>{settled=true;return value;});
 await vi.waitFor(()=>expect(f.socket.sent.some(message=>message.type==='action.call')).toBe(true));const call=f.socket.sent.find(message=>message.type==='action.call');f.socket.receive({type:'action.result',id:call.id,result:{accessToken:'synthetic'}});expect(settled).toBe(false);f.socket.receive({type:'control.watermark',id:call.id});expect(await action).toEqual({accessToken:'synthetic'});expect(f.budget.stats().bytes).toBe(0);
});
it('ignores stale auth work, updates the shared token namespace and quarantines definitive denial',async()=>{
 let oldToken!:(value:string)=>void;const f=fixture({identity:undefined,fetchToken:()=>new Promise(resolve=>{oldToken=resolve;})});f.socket.open();f.socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1}});
 f.client.setAuth({identity:{sub:'new'},fetchToken:async()=> 'new-token'});await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='auth')).toHaveLength(1));oldToken('late-token');await Promise.resolve();const auth=f.socket.sent.find(message=>message.type==='auth');expect(auth.token).toBe('new-token');
 f.socket.receive({type:'auth.result',id:auth.id,result});await vi.waitFor(()=>expect(f.client.localSessionState()).toBe('authenticated'));
 f.socket.receive({type:'auth.error',id:'membership-changed',code:'revoked',error:'Revoked'});expect(f.quarantines).toEqual([1]);expect(f.client.localSessionState()).toBe('unavailable');
 await expect(f.client.query({kind:'query',path:'private'},{})).rejects.toMatchObject({code:'auth'});
 const login=f.client.action({kind:'action',path:'control.auth.passwordLogin',scope:'control',authorization:'public'},{});await vi.waitFor(()=>expect(f.socket.sent.some(message=>message.type==='action.call')).toBe(true));const call=f.socket.sent.find(message=>message.type==='action.call');f.socket.receive({type:'action.result',id:call.id,result:'public-recovery'});expect(await login).toBe('public-recovery');expect(f.failures).toEqual([]);
});
it('keeps cached admission unrevoked after transient auth failure and never resends a dispatched Action',async()=>{
 const f=fixture();await f.authenticate();const action=f.client.action({kind:'action',path:'external.send'},{});const rejected=expect(action).rejects.toMatchObject({code:'disconnected'});await vi.waitFor(()=>expect(f.socket.sent.some(message=>message.type==='action.call')).toBe(true));f.socket.close();await rejected;
 expect(f.quarantines).toEqual([]);f.client.connect();const replacement=Socket.instances.at(-1)!;replacement.open();replacement.receive({type:'session.ready',capabilities:{replicaNativeRows:1}});await vi.waitFor(()=>expect(replacement.sent.some(message=>message.type==='auth')).toBe(true));replacement.receive({type:'auth.error',id:replacement.sent.find(message=>message.type==='auth').id,code:'transient',error:'Try later'});
 expect(f.quarantines).toEqual([]);expect(replacement.sent.filter(message=>message.type==='action.call')).toEqual([]);expect(f.budget.stats()).toMatchObject({bytes:0,entries:0});
});
it('bounds waiting calls and cancels budget waits on close without leaking metadata',async()=>{
 const f=fixture();await f.authenticate();const release=f.budget.reserve(2097152-1024);const request=f.client.query({kind:'query',path:'bounded'},{});const rejected=expect(request).rejects.toMatchObject({code:'closed'});await Promise.resolve();f.client.close();await rejected;release();expect(f.budget.stats()).toMatchObject({bytes:0,entries:0});
});
it('pumps only undispatched calls after browser backpressure clears',async()=>{
 const f=fixture();await f.authenticate();f.socket.bufferedAmount=600000;const request=f.client.query({kind:'query',path:'bounded'},{});await Promise.resolve();expect(f.socket.sent.filter(message=>message.type==='query.call')).toEqual([]);f.socket.bufferedAmount=0;await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='query.call')).toHaveLength(1));const call=f.socket.sent.find(message=>message.type==='query.call');f.socket.receive({type:'query.result',id:call.id,result:'once'});expect(await request).toBe('once');expect(f.budget.stats().bytes).toBe(0);
});

it('retries an expired credential once and revokes only after the fresh credential is rejected',async()=>{
 const fetched:boolean[]=[];const f=fixture({fetchToken:async({forceRefreshToken})=>{fetched.push(forceRefreshToken);return forceRefreshToken?'fresh':'cached';}});const original=await f.authenticate();
 f.socket.receive({type:'auth.error',id:original.id,code:'credential_rejected',error:'Expired'});await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='auth')).toHaveLength(2));const replacement=f.socket.sent.filter(message=>message.type==='auth').at(-1);expect(replacement.token).toBe('fresh');expect(fetched).toEqual([false,true]);expect(f.quarantines).toEqual([]);
 f.socket.receive({type:'auth.error',id:original.id,code:'revoked',error:'Late'});expect(f.quarantines).toEqual([]);
 f.socket.receive({type:'auth.error',id:replacement.id,code:'credential_rejected',error:'Rejected'});expect(f.quarantines).toEqual([1]);
});
it('does not revoke cached admission when a force-refresh provider has no new credential',async()=>{
 const f=fixture({fetchToken:async({forceRefreshToken})=>forceRefreshToken?null:'cached'}),original=await f.authenticate();f.socket.receive({type:'auth.error',id:original.id,code:'credential_rejected',error:'Expired'});await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='auth')).toHaveLength(2));const replacement=f.socket.sent.filter(message=>message.type==='auth').at(-1);f.socket.receive({type:'auth.error',id:replacement.id,code:'credential_rejected',error:'Not fresh'});expect(f.quarantines).toEqual([]);expect(f.socket.readyState).toBe(3);
});
it('updates the exact persisted namespace before admitting a freshly fetched JWT',async()=>{
 const token='synthetic.'+btoa(JSON.stringify({sub:'account',iss:'issuer'}))+'.signature';const f=fixture({fetchToken:async()=>token});await f.authenticate();expect(f.authenticating).toEqual(['identity\0ws://private.test/ws\0project\0tenant\0\0account','identity\0ws://private.test/ws\0project\0tenant\0issuer\0account']);expect(f.sessions[0].intentScope).toBe(f.authenticating.at(-1));
});
it('rejects a mismatched native command receipt without retiring its unknown outcome',async()=>{
 const f=fixture();await f.authenticate();const pending=f.client.callOwnerReducer({kind:'reducer',path:'tasks.edit'},{},'original');const rejected=expect(pending).rejects.toMatchObject({code:'disconnected'});await vi.waitFor(()=>expect(f.socket.sent.some(message=>message.type==='reducer.call')).toBe(true));const call=f.socket.sent.find(message=>message.type==='reducer.call');f.socket.receive({type:'reducer.result',id:call.id,originCommandId:'other-command',result:'wrong',committedRevision:1});await rejected;expect(f.failures).toMatchObject([{code:'INTENT_CONFLICT'}]);expect(f.budget.stats()).toMatchObject({bytes:0,entries:0});
});
