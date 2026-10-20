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
it.each([[false,false],[true,false],[true,true]])('shows stale sealed real names before owner connection, with a control client and initial credential adoption=%s owner-connected=%s',async(initialCredentials,connectedOwner)=>{
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
  expect(await second.restoreSession(auth)).toBe(true);expect(second.localSessionState()).toBe('restored');if(connectedOwner)await second.connect();
  const display=second.watchReplica({path:'tasks/all'} as any);stop=display.onUpdate(()=>{});
  await vi.waitFor(()=>expect(display.localReplicaState()).toMatchObject({rows:[{name:'Real task'}],freshness:connectedOwner?'verifying':'offline',isUpToDate:false}));
  if(!connectedOwner)expect(pending.reads()).toBe(0);pending.release();
  await vi.waitFor(()=>expect(display.localReplicaState()).toMatchObject({rows:[{name:'Real task'}],freshness:'current',isUpToDate:true}));
  expect(pending.reads()).toBeGreaterThan(0);stop();stop=undefined;display.close();
 }finally{stop?.();first?.close();second?.close();vi.unstubAllGlobals();}
});
