import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {GonvexClient,type FunctionReference} from './index.js';
import type {SQLiteTransportHost} from './sqlite-transport-host.js';
class Socket {
 static CONNECTING=0;static OPEN=1;static CLOSING=2;static CLOSED=3;static instances:Socket[]=[];
 readyState=0;binaryType='blob';sent:any[]=[];listeners=new Map<string,Array<(event:any)=>void>>();
 constructor(readonly url:string){Socket.instances.push(this);}
 addEventListener(type:string,listener:(event:any)=>void){const values=this.listeners.get(type)??[];values.push(listener);this.listeners.set(type,values);}
 send(value:string){this.sent.push(JSON.parse(value));}
 emit(type:string,event:any){for(const listener of this.listeners.get(type)??[])listener(event);}
 open(){this.readyState=1;this.emit('open',{});}
 receive(message:unknown){this.emit('message',{data:message instanceof ArrayBuffer?message:JSON.stringify(message)});}
 close(){if(this.readyState===3)return;this.readyState=3;this.emit('close',{});}
}
const clients:GonvexClient[]=[];
beforeEach(()=>{vi.stubGlobal('WebSocket',Socket);vi.stubGlobal('postMessage',()=>{});Socket.instances=[];});
afterEach(()=>{for(const client of clients.splice(0))client.close();vi.unstubAllGlobals();vi.restoreAllMocks();});
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
const directive={protocolVersion:1,scope:'tenant',visibilityScope:'visible',epoch:'e'};
const authResult=(id:string,replica=directive)=>({type:'auth.result',id,result:{localIdentity:identity,replica,artifactHash:'artifact'}});
function fixture(session:SQLiteTransportHost['session']=async()=>{}){
 const host:SQLiteTransportHost={authenticating:vi.fn(),session:vi.fn(session),binary:vi.fn(),frame:vi.fn(message=>message.type.startsWith('replica.')),quarantined:vi.fn(),disconnected:vi.fn(),failed:vi.fn()};
 const client=new GonvexClient('ws://private.test/ws',{ownerHost:host,errorReporting:false,project:'project',tenant:'tenant',token:'opaque',identity:{sub:'account'}});clients.push(client);client.connect();const socket=Socket.instances.at(-1)!;socket.open();socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1,replicaWatermark:1}});return {host,client,socket,auth:()=>socket.sent.filter(value=>value.type==='auth').at(-1)};
}
const flush=()=>new Promise(resolve=>setTimeout(resolve,0));
it('uses correlated authentication and holds tenant calls until the native session is durable',async()=>{
 let release!:()=>void;const barrier=new Promise<void>(resolve=>{release=resolve;});const f=fixture(()=>barrier);
 expect(f.host.authenticating).toHaveBeenCalledOnce();expect(f.host.authenticating).toHaveBeenCalledWith(expect.stringContaining('identity'),1);expect(f.socket.binaryType).toBe('arraybuffer');expect(f.auth().controlOnly).toBe(false);expect(f.auth().capabilities.replicaNativeRows).toBe(1);
 f.socket.receive(authResult(f.auth().id));await flush();expect(f.host.session).toHaveBeenCalledOnce();expect(f.client.localSessionState()).not.toBe('authenticated');
 const pending=f.client.query({kind:'query',path:'test/read'} as FunctionReference,{}, {timeoutMs:0});void pending.catch(()=>{});expect(f.socket.sent.some(value=>value.type==='query.call')).toBe(false);
 release();await flush();expect(f.client.localSessionState()).toBe('authenticated');const sent=f.socket.sent.find(value=>value.type==='query.call');expect(sent).toBeDefined();f.socket.receive({type:'query.result',id:sent.id,result:7});expect(await pending).toBe(7);
 expect(f.client.localReplica.snapshot()).toMatchObject({entities:{},liveQueries:{}});
});
it('admits native binary and replica frames through the host without materializing legacy snapshots',async()=>{
 const f=fixture();f.socket.receive(authResult(f.auth().id));await flush();const frame=new Uint8Array([1,2,3]).buffer;f.socket.receive(frame);expect(f.host.binary).toHaveBeenCalledWith(frame,1);
 f.socket.receive({type:'replica.snapshot',id:'large',result:[{id:'row',name:'Synthetic'}]});expect(f.host.frame).toHaveBeenCalled();expect(f.client.localReplica.snapshot().entities).toEqual({});
 f.client.sendOwnerMessage({type:'replica.close',id:'large'});expect(f.socket.sent.at(-1)).toEqual({type:'replica.close',id:'large'});
});
it('does not activate an intermediate auth result or a superseded persistence barrier',async()=>{
 const barriers:Array<{current:()=>boolean;release:()=>void}>=[];const f=fixture(async(_session,current)=>new Promise<void>(resolve=>{barriers.push({current,release:resolve});}));
 const first=f.auth().id;f.client.setAuth({token:'new-token'});f.socket.receive(authResult(first));expect(f.host.session).not.toHaveBeenCalled();const second=f.auth().id;expect(second).not.toBe(first);
 f.socket.receive(authResult(second));await flush();expect(barriers).toHaveLength(1);expect(barriers[0].current()).toBe(true);
 f.client.setAuth({token:'newest-token'});expect(barriers[0].current()).toBe(false);barriers[0].release();await flush();expect(f.client.localSessionState()).not.toBe('authenticated');
 f.socket.receive(authResult(f.auth().id));await flush();expect(barriers).toHaveLength(2);barriers[1].release();await flush();expect(f.client.localSessionState()).toBe('authenticated');
});
it('completes a durably journaled reducer only after the native watermark and retains its immutable network key',async()=>{
 const f=fixture();f.socket.receive(authResult(f.auth().id));await flush();let settled=false;
 const pending=f.client.callOwnerReducer({kind:'reducer',path:'tasks/change',artifactHash:'artifact'}, {id:'a'},'stable-command',0).then(result=>{settled=true;return result;});void pending.catch(()=>{});
 const sent=f.socket.sent.find(value=>value.type==='reducer.call');expect(sent.idempotencyKey).toBe('stable-command');expect(sent.artifactHash).toBe('artifact');
 f.socket.receive({type:'reducer.result',id:sent.id,result:{ok:true},originCommandId:'stable-command',committedRevision:5});await flush();expect(settled).toBe(false);
 f.socket.receive({type:'replica.watermark',revision:5});await flush();expect(settled).toBe(false);f.client.advanceOwnerWatermark(5);expect(await pending).toEqual({ok:true});
});
it('reports session persistence failure without publishing authentication, and closes the transport',async()=>{
 const failure=Error('disk quota surrogate'),f=fixture(async()=>{throw failure;});f.socket.receive(authResult(f.auth().id));await flush();expect(f.host.failed).toHaveBeenCalledWith(failure);expect(f.socket.readyState).toBe(3);expect(f.client.localSessionState()).not.toBe('authenticated');expect(f.host.disconnected).toHaveBeenCalledOnce();
});

it('resets the durable completion fence when server epoch or visibility changes',async()=>{
 const f=fixture();f.socket.receive(authResult(f.auth().id));await flush();f.client.advanceOwnerWatermark(100);
 f.client.setAuth({token:'renewed'});f.socket.receive(authResult(f.auth().id,{...directive,epoch:'new-epoch',visibilityScope:'new-visible'}));await flush();let settled=false;
 const pending=f.client.callOwnerReducer({kind:'reducer',path:'tasks/change'}, {},'next',0).then(result=>{settled=true;return result;});void pending.catch(()=>{});const sent=f.socket.sent.filter(value=>value.type==='reducer.call').at(-1);
 f.socket.receive({type:'reducer.result',id:sent.id,result:1,originCommandId:'next',committedRevision:1});await flush();expect(settled).toBe(false);f.client.advanceOwnerWatermark(1);expect(await pending).toBe(1);
});

it('fences the worker session for a definitive revocation but preserves transient reconnect authorization',async()=>{
 const f=fixture();f.socket.receive(authResult(f.auth().id));await flush();
 f.socket.receive({type:'auth.error',id:'session-expired',error:'temporarily expired',code:'expired'});expect(f.host.quarantined).not.toHaveBeenCalled();
 f.socket.receive({type:'auth.error',id:'membership-changed',error:'membership removed',code:'member_removed'});expect(f.host.quarantined).toHaveBeenCalledWith(1);expect(f.client.localSessionState()).toBe('unavailable');
});
