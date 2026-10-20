import {expect,it,vi} from 'vitest';
import {SQLiteBrowserClient} from './sqlite-browser-client.js';
const auth={project:'project',tenant:'tenant',identity:{sub:'account'},token:'credential'};
function storage(){const data=new Map<string,string>();return {data,getItem:(k:string)=>data.get(k)??null,setItem:(k:string,v:string)=>{data.set(k,v);},removeItem:(k:string)=>{data.delete(k);}};}
function broker(blocked=false,control=false,blockReads=false){
 let release!:()=>void;const gate=blocked||blockReads?new Promise<void>(resolve=>{release=resolve;}):Promise.resolve();let reads=0;
 const result={generation:{epoch:1,sequence:2},offset:0,nextOffset:null,rows:[{id:'a',value:{id:'a',name:'Real task'}}],window:{generation:'g',total:1,header:{entity:'tasks',source:'server',completeness:'complete',truncated:false}}};
 return {release:()=>release?.(),reads:()=>reads,async request(_op:string,input:any){switch(input.method){
  case 'connect':if(blocked)await gate;return {connection:'c',identityKey:'i'};
  case 'controls':return {tokens:[],admissionKey:control?null:'admission',status:{isWebSocketConnected:true},authentication:'authenticated',session:'restored',failure:null};
  case 'auth':return null;case 'restoreSession':return true;
  case 'watchReplica':return {id:'source',signature:'source'};
  case 'replicaPage':reads++;if(blockReads)await gate;return structuredClone(result);
  case 'unwatchReplica':return null;
  default:throw Error('Unexpected display test RPC '+input.method);
 }},subscribeChanges(){return ()=>{};},stats(){return {ownerEpoch:1};},close(){}};
}
it.each([[false,false,false],[true,false,false],[true,true,false],[true,false,true],[true,true,true]])('shows stale sealed real names before owner connection, with a control client and initial credential adoption=%s owner-connected=%s token-provider=%s',async(initialCredentials,connectedOwner,tokenProvider)=>{
 const saved=storage();vi.stubGlobal('sessionStorage',saved);
 let first:SQLiteBrowserClient|undefined,second:SQLiteBrowserClient|undefined,stop:(()=>void)|undefined;
 try{
  first=new SQLiteBrowserClient('ws://private/ws',{broker:broker() as any,artifactHash:'artifact',auth});
  await first.restoreSession(auth);const initial=first.watchReplica({path:'tasks/all'} as any);
  stop=initial.onUpdate(()=>{});await vi.waitFor(()=>expect(initial.localReplicaState()?.rows).toEqual([{id:'a',name:'Real task'}]));
  await vi.waitFor(()=>expect(saved.data.size).toBe(1));stop();stop=undefined;initial.close();first.close();first=undefined;
  const control=new SQLiteBrowserClient('ws://private/ws',{broker:broker(false,true) as any,artifactHash:'artifact',role:'control'});
  try{await control.restoreSession(auth);expect(saved.data.size).toBe(1);}finally{control.close();}
  const pending=broker(!connectedOwner,false,connectedOwner);second=new SQLiteBrowserClient('ws://private/ws',{broker:pending as any,artifactHash:'artifact',auth:initialCredentials?{project:auth.project}:auth});
  expect(await second.restoreSession(tokenProvider?{...auth,token:undefined,fetchToken:async()=>auth.token}:auth)).toBe(true);expect(second.localSessionState()).toBe('restored');if(connectedOwner)await second.connect();
  const display=second.watchReplica({path:'tasks/all'} as any);stop=display.onUpdate(()=>{});
  await vi.waitFor(()=>expect(display.localReplicaState()).toMatchObject({rows:[{name:'Real task'}],freshness:connectedOwner?'verifying':'offline',isUpToDate:false}));
  if(!connectedOwner)expect(pending.reads()).toBe(0);pending.release();
  await vi.waitFor(()=>expect(display.localReplicaState()).toMatchObject({rows:[{name:'Real task'}],freshness:'current',isUpToDate:true}));
  expect(pending.reads()).toBeGreaterThan(0);stop();stop=undefined;display.close();
 }finally{stop?.();first?.close();second?.close();vi.unstubAllGlobals();}
});

it('keeps sealed rows and entities off the owner until the rendering caller commits',async()=>{
 const saved=storage();vi.stubGlobal('sessionStorage',saved);let first:SQLiteBrowserClient|undefined,second:SQLiteBrowserClient|undefined,stop:(()=>void)|undefined;
 try{
  first=new SQLiteBrowserClient('ws://private/ws',{broker:broker() as any,artifactHash:'artifact',auth});
  await first.restoreSession(auth);const seed=first.watchReplica({path:'tasks/all'});stop=seed.onUpdate(()=>{});
  await vi.waitFor(()=>expect(saved.data.size).toBe(1));stop();stop=undefined;seed.close();first.close();first=undefined;
  const owner=broker(),request=vi.spyOn(owner,'request');
  second=new SQLiteBrowserClient('ws://private/ws',{broker:owner as any,artifactHash:'artifact',auth});
  second.deferCachedDisplayUntilCommit();await second.restoreSession(auth);await second.connect();
  const watch=second.watchReplica({path:'tasks/all'});stop=watch.onUpdate(()=>{});
  await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[{name:'Real task'}],isUpToDate:false}));
  expect((await second.entities('tasks',['a'])).rows).toEqual([{id:'a',value:{id:'a',name:'Real task'}}]);
  expect(request.mock.calls.some(call=>call[1]?.method==='watchReplica')).toBe(false);expect(owner.reads()).toBe(0);
  second.commitCachedDisplay();
  await vi.waitFor(()=>expect(watch.localReplicaState()?.isUpToDate).toBe(true));expect(owner.reads()).toBeGreaterThan(0);
  stop();stop=undefined;watch.close();
 }finally{stop?.();first?.close();second?.close();vi.unstubAllGlobals();}
});

it('does not hold a cold cache miss behind a rendering acknowledgment',async()=>{
 vi.stubGlobal('sessionStorage',storage());const owner=broker(),client=new SQLiteBrowserClient('ws://private/ws',{broker:owner as any,artifactHash:'artifact',auth});
 try{
  client.deferCachedDisplayUntilCommit();await client.restoreSession(auth);const watch=client.watchReplica({path:'tasks/all'}),stop=watch.onUpdate(()=>{});
  await vi.waitFor(()=>expect(watch.localReplicaState()?.isUpToDate).toBe(true));expect(owner.reads()).toBeGreaterThan(0);
  stop();watch.close();
 }finally{client.close();vi.unstubAllGlobals();}
});

it('publishes a sealed first subscription started before session restore without awaiting the grid commit it enables',async()=>{
 const saved=storage();vi.stubGlobal('sessionStorage',saved);let first:SQLiteBrowserClient|undefined,second:SQLiteBrowserClient|undefined,stop:(()=>void)|undefined;
 try{
  first=new SQLiteBrowserClient('ws://private/ws',{broker:broker() as any,artifactHash:'artifact',auth});
  await first.restoreSession(auth);const seed=first.watchReplica({path:'tasks/all'});stop=seed.onUpdate(()=>{});
  await vi.waitFor(()=>expect(saved.data.size).toBe(1));stop();stop=undefined;seed.close();first.close();first=undefined;
  const owner=broker(),request=vi.spyOn(owner,'request');
  second=new SQLiteBrowserClient('ws://private/ws',{broker:owner as any,artifactHash:'artifact',auth});
  second.deferCachedDisplayUntilCommit();
  // Auth/profile subscriptions can mount before the restore effect. Their
  // stale cached result is required to mount the grid which releases the hold.
  const watch=second.watchReplica({path:'tasks/all'});stop=watch.onUpdate(()=>{});
  await second.restoreSession(auth);
  await vi.waitFor(()=>expect(watch.localReplicaState()).toMatchObject({rows:[{name:'Real task'}],isUpToDate:false}),{timeout:250});
  expect(request.mock.calls.some(call=>call[1]?.method==='watchReplica')).toBe(false);
  second.commitCachedDisplay();
  await vi.waitFor(()=>expect(watch.localReplicaState()?.isUpToDate).toBe(true));stop();stop=undefined;watch.close();
 }finally{stop?.();first?.close();second?.close();vi.unstubAllGlobals();}
});

it('republishes a sealed reference after its first subscriber unmounts before grid commit',async()=>{
 const saved=storage();vi.stubGlobal('sessionStorage',saved);let first:SQLiteBrowserClient|undefined,second:SQLiteBrowserClient|undefined;const cleanup:Array<()=>void>=[];
 try{
  first=new SQLiteBrowserClient('ws://private/ws',{broker:broker() as any,artifactHash:'artifact',auth});
  await first.restoreSession(auth);const seed=first.watchReplica({path:'tasks/all'});const stopSeed=seed.onUpdate(()=>{});
  await vi.waitFor(()=>expect(saved.data.size).toBe(1));stopSeed();seed.close();first.close();first=undefined;
  const owner=broker();second=new SQLiteBrowserClient('ws://private/ws',{broker:owner as any,artifactHash:'artifact',auth});
  second.deferCachedDisplayUntilCommit();await second.restoreSession(auth);
  const a=second.watchReplica({path:'tasks/all'});const stopA=a.onUpdate(()=>{});
  await vi.waitFor(()=>expect(a.localReplicaState()?.rows).toEqual([{id:'a',name:'Real task'}]));
  stopA();a.close();await Promise.resolve();
  const b=second.watchReplica({path:'tasks/all'});cleanup.push(()=>b.close());cleanup.push(b.onUpdate(()=>{}));
  await vi.waitFor(()=>expect(b.localReplicaState()?.rows).toEqual([{id:'a',name:'Real task'}]),{timeout:250});
  expect(owner.reads()).toBe(0);second.commitCachedDisplay();
  await vi.waitFor(()=>expect(b.localReplicaState()?.isUpToDate).toBe(true));
 }finally{cleanup.reverse().forEach(f=>f());first?.close();second?.close();vi.unstubAllGlobals();}
});
