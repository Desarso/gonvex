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
it('forwards bounded support commands only from an admitted socket',async()=>{
 const f=fixture(),commands:unknown[]=[];f.host.supportCommand=command=>commands.push(command);
 f.socket.receive({type:'support.command',id:'early',result:{kind:'inspect'}});
 await f.authenticate();
 f.socket.receive({type:'support.command',id:'current',result:{kind:'inspect',payload:{task:'synthetic'}}});
 expect(commands).toEqual([{id:'current',kind:'inspect',payload:{task:'synthetic'}}]);
 f.client.setAuth({token:'changed'});
 f.socket.receive({type:'support.command',id:'stale',result:{kind:'inspect'}});
 expect(commands).toHaveLength(1);
});
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
 await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='reducer.call')).toHaveLength(1));const call=f.socket.sent.find(message=>message.type==='reducer.call');expect(call).toMatchObject({id:'durable-command',idempotencyKey:'durable-command',artifactHash:'original',intentEntropy:'entropy',clientContract:1});
 f.socket.receive({type:'reducer.result',id:call.id,result:'original',committedRevision:10,originCommandId:'durable-command'});await Promise.resolve();expect(settled).toBe(false);
 f.socket.receive({type:'reducer.result',id:call.id,result:'duplicate',committedRevision:10,originCommandId:'durable-command'});
 f.client.advanceOwnerWatermark(9);expect(settled).toBe(false);f.client.advanceOwnerWatermark(10);expect(await pending).toBe('original');expect(f.budget.stats()).toMatchObject({bytes:0,entries:0});expect(f.failures).toEqual([]);
});
it('allows a public Control Action during token fetch and waits for its Control watermark',async()=>{
 const f=fixture({tenant:undefined,identity:undefined,fetchToken:()=>new Promise(()=>{})});f.socket.open();f.socket.receive({type:'session.ready',capabilities:{controlWatermark:1,replicaNativeRows:1}});
 let settled=false;const action=f.client.action({kind:'action',path:'control.auth.passwordLogin',scope:'control',authorization:'public'},{email:'synthetic@example.test'}).then(value=>{settled=true;return value;});
 await vi.waitFor(()=>expect(f.socket.sent.some(message=>message.type==='action.call')).toBe(true));const call=f.socket.sent.find(message=>message.type==='action.call');expect(call).toMatchObject({scope:'control',idempotencyKey:call.id,clientContract:1});f.socket.receive({type:'action.result',id:call.id,result:{accessToken:'synthetic'}});expect(settled).toBe(false);f.socket.receive({type:'control.watermark',id:call.id});expect(await action).toEqual({accessToken:'synthetic'});expect(f.budget.stats().bytes).toBe(0);
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

it('streams Control query updates and replaces subscription IDs after reauthentication without accepting stale results',async()=>{
 const f=fixture(),updates:unknown[]=[];await f.authenticate();const ref={kind:'query' as const,path:'control.tenants.mine',scope:'control' as const,delivery:'live' as const};
 const release=f.client.watchControl(ref,{},value=>updates.push(value));const first=f.socket.sent.find(message=>message.type==='query.subscribe');expect(first).toMatchObject({scope:'control',path:ref.path});f.socket.receive({type:'query.result',id:first.id,result:[{id:'tenant'}]});expect(updates).toEqual([{result:[{id:'tenant'}]}]);
 f.client.setAuth({token:'updated'});await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='auth')).toHaveLength(2));f.socket.receive({type:'query.result',id:first.id,result:[{id:'stale'}]});expect(updates).toHaveLength(1);const auth=f.socket.sent.filter(message=>message.type==='auth').at(-1);f.socket.receive({type:'auth.result',id:auth.id,result});await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='query.subscribe')).toHaveLength(2));const replacement=f.socket.sent.filter(message=>message.type==='query.subscribe').at(-1);expect(replacement.id).not.toBe(first.id);f.socket.receive({type:'query.result',id:replacement.id,result:[{id:'new'}]});expect(updates.at(-1)).toEqual({result:[{id:'new'}]});release();expect(f.socket.sent.at(-1)).toEqual({type:'query.unsubscribe',id:replacement.id});f.client.dispose();expect(f.budget.stats().bytes).toBe(0);
});
it('reports definitive auth failures and rotated developer credentials only from correlated auth results',async()=>{
 const f=fixture({fetchToken:undefined,token:'gvx_dev_old'}),errors:unknown[]=[],credentials:string[]=[];f.host.authError=(message,details)=>errors.push({message,...details});f.host.developerCredential=token=>credentials.push(token);const auth=await f.open();f.socket.receive({type:'auth.result',id:'stale',result:{...result,developerSessionToken:'gvx_dev_stale'}});expect(credentials).toEqual([]);
 f.socket.receive({type:'auth.error',id:auth.id,error:'Revoked',code:'revoked'});expect(errors).toEqual([{message:'Revoked',definitive:true,code:'revoked'}]);expect(f.quarantines).toHaveLength(1);
});

it('keeps public tenant calls online-only without waiting for a nonexistent replica watermark',async()=>{
 const f=fixture({fetchToken:undefined,token:undefined} as any);f.client.dispose();
 const budget=new SQLiteTransientBudget(),client=new SQLiteNativeTransport('ws://private.test/ws?public=1',{project:'project',tenant:'public-domain',public:true,ownerHost:f.host,reserve:bytes=>budget.reserve(bytes)});clients.push(client);client.connect();const socket=Socket.instances.at(-1)!;socket.open();const auth=socket.sent.find(message=>message.type==='auth');expect(auth).toMatchObject({public:true,controlOnly:false,tenant:'public-domain'});socket.receive({type:'session.ready',capabilities:{replicaNativeRows:1,replicaWatermark:1}});socket.receive({type:'auth.result',id:auth.id,result:{}});await vi.waitFor(()=>expect(client.authenticationState()).toBe('authenticated'));const pending=client.callOwnerReducer({kind:'reducer',path:'public.submit'},{},'public-command');await vi.waitFor(()=>expect(socket.sent.some(message=>message.type==='reducer.call')).toBe(true));const command=socket.sent.find(message=>message.type==='reducer.call');socket.receive({type:'reducer.result',id:command.id,result:{ok:true},committedRevision:99,originCommandId:'public-command'});await expect(pending).resolves.toEqual({ok:true});client.dispose();expect(budget.stats().bytes).toBe(0);
});

it('sends bounded native error frames once after authentication and releases their admission on ACK or scope change',async()=>{
 const f=fixture(),registration=f.client.reportError('register',{release:'synthetic',environment:'private'});void registration.catch(()=>{});expect(f.socket.sent).toEqual([]);await f.authenticate();const frame=f.socket.sent.find(message=>message.type==='error.register');expect(frame).toMatchObject({release:'synthetic',environment:'private'});expect(frame).not.toHaveProperty('payload');f.socket.receive({type:'error.ack',id:frame.id,accepted:1});await expect(registration).resolves.toBeUndefined();expect(f.budget.stats().bytes).toBe(0);const pending=f.client.reportError('envelope',{events:[{message:'Synthetic bounded event'}]});void pending.catch(()=>{});expect(f.socket.sent.at(-1)).toMatchObject({type:'error.envelope',events:[{message:'Synthetic bounded event'}]});f.client.setAuth({token:'changed'});await expect(pending).rejects.toThrow('credentials changed');expect(f.socket.sent.filter(message=>message.type==='error.envelope')).toHaveLength(1);expect(f.budget.stats().bytes).toBe(0);
});

it('keeps tenant live windows behind admission and forwards their authoritative revision with bounded results',async()=>{
 const f=fixture(),updates:unknown[]=[],ref={kind:'query' as const,path:'tasks/grid',delivery:'live' as const,live:{entity:'tasks',key:'id',resultPath:['page'],plan:{} as any}};
 const release=f.client.watchLive(ref,{offset:0,limit:20},value=>updates.push(value));expect(f.socket.sent.some(message=>message.type==='query.subscribe')).toBe(false);const auth=await f.authenticate();const first=f.socket.sent.find(message=>message.type==='query.subscribe');expect(first).toMatchObject({scope:'tenant',path:ref.path,args:{offset:0,limit:20}});
 f.socket.receive({type:'query.result',id:first.id,result:{page:[{id:'a'}],total:100000},subscriptionRevision:{epoch:'e',sequence:10},windowRevision:'window',replicaScope:'visible'});expect(updates).toEqual([{result:{page:[{id:'a'}],total:100000},subscriptionRevision:{epoch:'e',sequence:10},windowRevision:'window',replicaScope:'visible'}]);
 f.client.setAuth({token:'new'});f.socket.receive({type:'query.result',id:first.id,result:{page:[{id:'stale'}]},subscriptionRevision:{epoch:'e',sequence:11}});expect(updates).toHaveLength(1);await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='auth')).toHaveLength(2));const next=f.socket.sent.filter(message=>message.type==='auth').at(-1);expect(next.id).not.toBe(auth.id);f.socket.receive({type:'auth.result',id:next.id,result});await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='query.subscribe')).toHaveLength(2));release();expect(f.socket.sent.at(-1).type).toBe('query.unsubscribe');expect(f.budget.stats().bytes).toBe(0);
});
it('refuses an unversioned tenant live result rather than granting it canonical authority',async()=>{
 const f=fixture();await f.authenticate();const updates:unknown[]=[];f.client.watchLive({kind:'query',path:'tasks/grid',delivery:'live',live:{entity:'tasks',key:'id',plan:{} as any}},{},value=>updates.push(value));const call=f.socket.sent.find(message=>message.type==='query.subscribe');f.socket.receive({type:'query.result',id:call.id,result:[]});expect(updates).toEqual([]);expect(f.failures).toHaveLength(1);expect(f.socket.readyState).toBe(3);
});
it('holds early replica frames until asynchronous owner admission is installed, coalescing consecutive quiet watermarks',async()=>{
 let release!:()=>void;const barrier=new Promise<void>(resolve=>{release=resolve;}),f=fixture({},async()=>barrier),auth=await f.open(),order:unknown[]=[];const frame=f.host.frame,binary=f.host.binary;f.host.frame=(message,generation)=>{order.push(message);return frame(message,generation);};f.host.binary=(value,generation)=>{order.push(value.byteLength);binary(value,generation);};f.socket.receive({type:'auth.result',id:auth.id,result});for(let revision=1;revision<=100;revision++)f.socket.receive({type:'replica.watermark',revision});f.socket.receive(new ArrayBuffer(2));expect(f.frames).toEqual([]);expect(f.binary).toEqual([]);expect(f.budget.stats().entries).toBe(2);release();await vi.waitFor(()=>expect(f.client.localSessionState()).toBe('authenticated'));expect(order).toEqual([{type:'replica.watermark',revision:100},2]);expect(f.budget.stats()).toMatchObject({bytes:0,entries:0});expect(f.failures).toEqual([]);
});
it('discards an old admission frame prefix when authentication is superseded',async()=>{
 let release!:()=>void;const barrier=new Promise<void>(resolve=>{release=resolve;}),f=fixture({},async()=>barrier),auth=await f.open();f.socket.receive({type:'auth.result',id:auth.id,result});f.socket.receive({type:'replica.watermark',revision:10});expect(f.budget.stats().entries).toBe(1);f.client.setAuth({token:'new'});expect(f.budget.stats().entries).toBe(0);release();await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='auth')).toHaveLength(2));const current=f.socket.sent.filter(message=>message.type==='auth').at(-1);f.socket.receive({type:'auth.result',id:current.id,result});await vi.waitFor(()=>expect(f.client.localSessionState()).toBe('authenticated'));f.socket.receive({type:'replica.watermark',revision:20});expect(f.frames).toEqual([{type:'replica.watermark',revision:20}]);expect(f.failures).toEqual([]);
});

it('does not replace a pending immutable reducer command when another delivery attempts the same ID',async()=>{
 const f=fixture();await f.authenticate();const first=f.client.callOwnerReducer({kind:'reducer',path:'tasks.edit'},{},'stable');
 await vi.waitFor(()=>expect(f.socket.sent.some(message=>message.type==='reducer.call')).toBe(true));
 await expect(f.client.callOwnerReducer({kind:'reducer',path:'tasks.edit'},{},'stable')).rejects.toMatchObject({code:'disconnected'});
 expect(f.socket.sent.filter(message=>message.type==='reducer.call')).toHaveLength(1);
 f.socket.receive({type:'reducer.result',id:'stable',originCommandId:'stable',result:'one',committedRevision:1});f.client.advanceOwnerWatermark(1);
 expect(await first).toBe('one');expect(f.budget.stats()).toMatchObject({bytes:0,entries:0});
});

it('keeps identical auth and metadata-only fetcher changes off the wire without superseding a pending call',async()=>{
 const f=fixture({token:'opaque',fetchToken:undefined});await f.authenticate();
 const pending=f.client.query({kind:'query',path:'tasks.one'});void pending.catch(()=>{});
 await vi.waitFor(()=>expect(f.socket.sent.some(message=>message.type==='query.call')).toBe(true));
 const before=vi.fn();f.client.setAuth({token:'opaque'},before);f.client.setAuth({fetchToken:async()=> 'later-token'},before);
 await new Promise(resolve=>setTimeout(resolve,0));
 expect(f.socket.sent.filter(message=>message.type==='auth')).toHaveLength(1);expect(before).not.toHaveBeenCalled();
 expect(f.authenticating).toHaveLength(1);expect(f.client.localSessionState()).toBe('authenticated');
 const call=f.socket.sent.find(message=>message.type==='query.call');f.socket.receive({type:'query.result',id:call.id,result:'still-current'});
 await expect(pending).resolves.toBe('still-current');
 const order:string[]=[];f.host.authenticating=()=>{order.push('authenticating');};
 f.client.setAuth({token:'replacement',fetchToken:undefined},()=>{order.push('fence');});
 expect(order).toEqual(['fence','authenticating']);await vi.waitFor(()=>expect(f.socket.sent.filter(message=>message.type==='auth')).toHaveLength(2));
});

it('completes reload network authentication before delayed durable replica admission',async()=>{
 let release!:()=>void;const installing=new Promise<void>(resolve=>{release=resolve;});
 const f=fixture({},async()=>installing),notified=vi.fn();f.host.authenticated=notified;
 const auth=await f.open();f.socket.receive({type:'auth.result',id:auth.id,result});
 expect(f.client.authenticationState()).toBe('authenticated');expect(notified).toHaveBeenCalledTimes(1);
 expect(f.client.localSessionState()).toBe('unavailable');
 f.socket.receive({type:'replica.watermark',revision:9});expect(f.frames).toEqual([]);
 release();await vi.waitFor(()=>expect(f.client.localSessionState()).toBe('authenticated'));
 expect(f.frames).toEqual([{type:'replica.watermark',revision:9}]);
 f.socket.close();expect(f.client.authenticationState()).toBe('unavailable');expect(f.client.localSessionState()).toBe('unavailable');
});
