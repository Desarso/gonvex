import {afterEach,expect,it,vi} from 'vitest';
import {SQLiteBrowserClient} from './sqlite-browser-client.js';
import type {SQLiteReplicaBroker} from './sqlite-broker.js';
const cleanup:Array<()=>void>=[];afterEach(()=>{for(const close of cleanup.splice(0))close();vi.restoreAllMocks();});
function fixture(fetchToken?:()=>Promise<string|null>){
 let admissionKey:string|null|undefined;let epoch=1,identity='a',connection='',tokenPending=false,closed=false;const changes=new Set<(...args:any[])=>void>(),calls:Array<{input:any;requestID?:string}>=[],responses:any[]=[];
 let handler:((input:any)=>Promise<unknown>|unknown)|undefined;
 const broker={async request<T>(_op:string,input:any,_timeout?:number,requestID?:string):Promise<T>{
  calls.push({input:structuredClone(input),requestID});if(handler){const result=await handler(input);if(result!==undefined)return result as T;}
  if(input.method==='connect'){identity=input.auth.identity?.sub??'public';connection=identity+':'+epoch;tokenPending=true;return {connection,identityKey:identity} as T;}
  if(input.method==='controls')return {...(admissionKey!==undefined?{admissionKey}:{}),tokens:tokenPending?[{id:'token:'+connection,connection,identityKey:identity,forceRefreshToken:false}]:[],status:{connected:true},session:'authenticated',failure:null} as T;
  if(input.method==='token'){responses.push(input.response);tokenPending=false;return null as T;}
  if(input.method==='watchReplica')return {id:'source:'+input.path,signature:identity+':'+input.path} as T;
  if(input.method==='replicaPage')return {generation:{epoch,sequence:1},rows:[{id:'row',value:{id:'row',identity}}],window:{generation:'window',total:1,header:{}}} as T;
  if(input.method==='replicaPendingPage')return null as T;
  if(input.method==='unwatchReplica'||input.method==='auth')return null as T;
  if(input.method==='reducer')return {commandId:requestID,result:2} as T;
  if(input.method==='query')return {identity} as T;
  throw Error('Unexpected mock RPC '+input.method);
 },subscribeChanges(listener:(...args:any[])=>void){changes.add(listener);return ()=>{changes.delete(listener);};},stats(){return {ownerEpoch:epoch} as ReturnType<SQLiteReplicaBroker['stats']>;},close(){closed=true;}};
 const client=new SQLiteBrowserClient('ws://private-synthetic.test/ws',{broker,auth:{project:'project',tenant:'tenant',identity:{sub:'a'}},fetchToken});cleanup.push(()=>client.close());
 return {client,calls,responses,get closed(){return closed;},set handler(next:typeof handler){handler=next;},admit(key:string|null){admissionKey=key;for(const notify of changes)notify('*',1,true);},change(){for(const notify of changes)notify('scope',1);},replaceOwner(){epoch++;connection='';tokenPending=false;for(const notify of changes)notify('*',1,true);}};
}
it('services a single selected-tab token request, shares a watch, retains a bounded stable page snapshot and restores after owner change',async()=>{
 const fetchToken=vi.fn(async()=> 'opaque'),f=fixture(fetchToken),one=f.client.watchReplica({path:'tasks/all'}),two=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>{one.close();two.close();});
 const page=await one.page({limit:10});await vi.waitFor(()=>expect(f.responses).toHaveLength(1));expect(fetchToken).toHaveBeenCalledTimes(1);expect(one.snapshot()).toBe(one.snapshot());expect(one.snapshot().page).toBe(page);expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(1);
 f.change();expect(one.snapshot().stale).toBe(true);expect(one.snapshot().page).toBe(page);await vi.waitFor(async()=>{await f.client.pollControls();expect(f.responses).toHaveLength(1);});
 f.replaceOwner();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='connect')).toHaveLength(2));await two.page();expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(2);one.close();expect(two.snapshot().page).toBeDefined();two.close();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(1));expect(f.client.budgets().find(value=>value.name==='browser-row-pages')?.bytes).toBe(0);
});
it('fences stale page and credential results across an identity change and releases held providers on close',async()=>{
 let release!: (value:string)=>void;const held=new Promise<string>(resolve=>{release=resolve;}),fetch=vi.fn().mockImplementationOnce(()=>held).mockResolvedValue('new-token'),f=fixture(fetch),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());await watch.page();await vi.waitFor(()=>expect(fetch).toHaveBeenCalledTimes(1));
 await f.client.setAuth({identity:{sub:'b'}});expect(watch.snapshot().page).toBeUndefined();release('old-token');await vi.waitFor(()=>expect(f.responses).toHaveLength(1));expect(f.responses[0]).toMatchObject({identityKey:'b',token:'new-token'});expect((await watch.page())?.rows[0].value).toMatchObject({identity:'b'});
 let finish!:()=>void;const barrier=new Promise<void>(resolve=>{finish=resolve;});f.handler=async input=>{if(input.method==='query'){await barrier;return {identity:'b'};}};const result=f.client.query({path:'oneShot'});void result.catch(()=>{});await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='query')).toBe(true));await f.client.setAuth({identity:{sub:'c'}});finish();await expect(result).rejects.toMatchObject({code:'FENCED'});f.client.close();expect(f.closed).toBe(true);expect(watch.snapshot().page).toBeUndefined();
});
it('serializes last-watch cancellation with a new lease and preserves pinned pages when admission fails',async()=>{
 const f=fixture(),first=f.client.watchReplica({path:'tasks/all'});await first.page();let release!:()=>void;const waiting=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='unwatchReplica'){await waiting;return null;}};first.close();await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='unwatchReplica')).toBe(true));const replacement=f.client.watchReplica({path:'tasks/all'}),pending=replacement.page();cleanup.push(()=>replacement.close());await new Promise(resolve=>setTimeout(resolve,0));expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(1);release();await pending;expect(f.calls.filter(call=>call.input.method==='watchReplica')).toHaveLength(2);expect(replacement.snapshot().page).toBeDefined();
 const before=replacement.snapshot().page;f.handler=async input=>input.method==='replicaPage'?{rows:[{id:'giant',value:{text:'x'.repeat(150000)}}]}:undefined;await expect(replacement.page()).rejects.toMatchObject({code:'BUDGET'});expect(replacement.snapshot().page).toBe(before);expect(f.client.budgets().every(value=>value.bytes<=value.maxBytes)).toBe(true);
});
it('bounds requests before waiting for a connection and preserves the caller command ID for immutable retries',async()=>{
 const f=fixture();await f.client.connect();const args={id:'a'};const result=await f.client.invokeReducer({path:'tasks/change'},args,{commandId:'stable-command',now:100});expect(result.commandId).toBe('stable-command');expect(f.calls.find(call=>call.input.method==='reducer')?.requestID).toBe('stable-command');expect(()=>f.client.invokeReducer({path:'tasks/change'},{},{commandId:'stable-command'})).toThrow('original invocation time');
 let release!:()=>void;const waiting=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='query'){await waiting;return null;}};const pending=Array.from({length:16},()=>f.client.query({path:'bounded'}));await expect(f.client.query({path:'excess'})).rejects.toMatchObject({code:'BUDGET'});release();await Promise.all(pending);expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.bytes).toBe(0);
});

it('cancels a token provider which never settles when the facade closes',async()=>{
 const provider=vi.fn(()=>new Promise<string|null>(()=>{})),f=fixture(provider);await f.client.connect();await vi.waitFor(()=>expect(provider).toHaveBeenCalledOnce());expect(f.client.budgets().find(value=>value.name==='browser-token-requests')?.entries).toBe(1);f.client.close();await new Promise(resolve=>setTimeout(resolve,0));expect(f.client.budgets().find(value=>value.name==='browser-token-requests')?.entries).toBe(0);expect(f.responses).toEqual([]);
});

it('clears protected pages on worker admission changes and fences a late page without reconnecting the logical client',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());f.admit('old-permissions');await f.client.pollControls();const original=await watch.page();expect(watch.snapshot().page).toBe(original);
 const connections=f.calls.filter(call=>call.input.method==='connect').length;
 let release!:()=>void;const held=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='replicaPage'){await held;return {...original,rows:[{id:'old',value:{id:'old',protected:true}}]};}};
 const pending=watch.page();const rejected=expect(pending).rejects.toMatchObject({code:'FENCED'});await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(2));
 f.admit('new-permissions');await f.client.pollControls();expect(watch.snapshot().page).toBeUndefined();release();await rejected;expect(watch.snapshot().page).toBeUndefined();expect(f.calls.filter(call=>call.input.method==='connect')).toHaveLength(connections);
 f.handler=undefined;await watch.page();expect(watch.snapshot().page).toBeDefined();f.admit(null);await f.client.pollControls();expect(watch.snapshot().page).toBeUndefined();
});

it('delivers correlated auth errors and developer credentials once, ACKs the secret, and retains no token in status',async()=>{
 const f=fixture(),errors:unknown[]=[],credentials:string[]=[];f.handler=async input=>{if(input.method==='controls')return {tokens:[],status:{isWebSocketConnected:true},session:'authenticated',failure:null,authError:{id:'error',message:'Denied',definitive:true,code:'revoked'},developerCredential:{id:'credential',token:'gvx_dev_synthetic'}};if(input.method==='credentialAck')return null;};
 const stopErrors=f.client.onAuthError((message,details)=>errors.push({message,...details})),stopCredentials=f.client.onDeveloperCredential(token=>credentials.push(token));await f.client.pollControls();await f.client.pollControls();expect(errors).toEqual([{message:'Denied',definitive:true,code:'revoked'}]);expect(credentials).toEqual(['gvx_dev_synthetic']);expect(f.calls.some(call=>call.input.method==='credentialAck'&&call.input.id==='credential')).toBe(true);expect(JSON.stringify(f.client.status())).not.toContain('gvx_dev');stopErrors();stopCredentials();
});
it('updates the main-thread token provider without cloning a function to the owner',async()=>{
 const old=vi.fn(async()=> 'old'),next=vi.fn(async()=> 'next'),f=fixture(old);await f.client.connect();await vi.waitFor(()=>expect(f.responses).toHaveLength(1));await f.client.setAuth({fetchToken:next});f.replaceOwner();await vi.waitFor(()=>expect(f.responses).toHaveLength(2));expect(next).toHaveBeenCalledOnce();expect(f.responses.at(-1).token).toBe('next');expect(f.calls.every(call=>!('fetchToken'in(call.input.auth??{})))).toBe(true);
});

it('waits for correlated authoritative authentication, rejects superseded waiters and cancels on close',async()=>{
 const f=fixture();let authentication='authenticating';f.handler=async input=>{if(input.method==='controls')return {tokens:[],status:{isWebSocketConnected:true},authentication,session:'unavailable',failure:null};};const first=f.client.authenticate({token:'first'});void first.catch(()=>{});await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='auth')).toBe(true));const second=f.client.authenticate({token:'second'});void second.catch(()=>{});await expect(first).rejects.toMatchObject({code:'superseded'});authentication='authenticated';f.change();await expect(second).resolves.toBeUndefined();expect(f.client.localSessionState()).toBe('unavailable');
 authentication='authenticating';const closing=f.client.authenticate({token:'third'});void closing.catch(()=>{});await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='auth')).toHaveLength(3));f.client.close();await expect(closing).rejects.toMatchObject({code:'closed'});
});

it('keeps reducer receipts internal while returning the public local result or queued outcome',async()=>{
 const f=fixture(),ref={kind:'reducer' as const,path:'tasks/change'};expect(await f.client.reducer(ref,{id:'a'})).toBe(2);f.handler=async input=>{if(input.method==='reducer')return {commandId:'queued-command',result:null,queuedOnly:true};};expect(await f.client.reducer(ref,{})).toEqual({status:'queued',reducerId:'queued-command'});
});

it('preserves an already displayed first page while its pending stage becomes canonical',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());const first=await watch.page();expect(await watch.pendingPage()).toBeNull();expect(watch.snapshot().page).toBe(first);
});

it('keeps separately filtered view pages pinned independently while sharing the owner source through distinct leases',async()=>{
 const f=fixture();f.handler=async input=>{if(input.method==='collectionPage')return {rows:[{id:'a',n:input.plan.where.value}],sequence:1,bytes:32,cursor:null,generation:'window'};};const one=f.client.watchReplica({path:'tasks/all'},{},{view:'grid'}),two=f.client.watchReplica({path:'tasks/all'},{},{view:'board'});cleanup.push(()=>{one.close();two.close();});const plan={order:[{field:'n',direction:'asc' as const}],limit:10};const first=await one.queryPage({...plan,where:{field:'n',op:'gte',value:1}}),second=await two.queryPage({...plan,where:{field:'n',op:'gte',value:2}});expect(one.snapshot().page).toBe(first);expect(two.snapshot().page).toBe(second);const opens=f.calls.filter(call=>call.input.method==='watchReplica');expect(opens).toHaveLength(2);expect(opens[0].input.lease).not.toBe(opens[1].input.lease);one.close();await vi.waitFor(()=>expect(f.calls.some(call=>call.input.method==='unwatchReplica'&&call.input.lease===opens[0].input.lease)).toBe(true));expect(two.snapshot().page).toBe(second);
});
