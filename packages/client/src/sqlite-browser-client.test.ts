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
 return {client,calls,responses,get closed(){return closed;},set handler(next:typeof handler){handler=next;},admit(key:string|null){admissionKey=key;for(const notify of changes)notify('*',1,true);},control(){for(const notify of changes)notify('@control',1);},change(scope='scope'){for(const notify of changes)notify(scope,1);},replaceOwner(){epoch++;connection='';tokenPending=false;for(const notify of changes)notify('*',1,true);}};
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
 let release!:()=>void;const waiting=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='query'){await waiting;return null;}};const pending=Array.from({length:128},()=>f.client.query({path:'bounded'}));const queued=f.client.query({path:'queued'});await vi.waitFor(()=>expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.entries).toBe(129));release();await Promise.all([...pending,queued]);expect(f.client.budgets().find(value=>value.name==='browser-request-admission')?.bytes).toBe(0);
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

it('retries a control poll superseded by the initial identity admission',async()=>{
 const f=fixture();let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});let held=false;
 f.handler=async input=>{if(input.method==='controls'&&!held){held=true;await gate;return {tokens:[],status:{},session:'unavailable',failure:null};}};
 await f.client.connect();await vi.waitFor(()=>expect(held).toBe(true));
 const admission=f.client.setAuth({identity:{sub:'b'}});release();await admission;
 await expect(f.client.pollControls()).resolves.toBeUndefined();
 expect(f.calls.filter(call=>call.input.method==='controls').length).toBeGreaterThan(1);
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

it('binds queued reducer RPCs to the current opaque worker admission key',async()=>{
 const f=fixture();f.admit('current-admission');await f.client.pollControls();await f.client.invokeReducer({path:'tasks/change'},{id:'a'});expect(f.calls.find(call=>call.input.method==='reducer')?.input.expectedAdmissionKey).toBe('current-admission');
});

it('delivers each authoritative rejection once and acknowledges only delivered notifications',async()=>{
 const f=fixture(),events:unknown[]=[];const event={sequence:1,reducerId:'command',path:'tasks/change',error:'denied',errorClass:'rejected'};f.handler=async input=>{if(input.method==='controls')return {tokens:[],status:{},session:'authenticated',failure:null,rejections:[event]};if(input.method==='rejectionsAck')return null;};await f.client.connect();await f.client.pollControls();expect(f.calls.some(call=>call.input.method==='rejectionsAck')).toBe(false);
 const release=f.client.onReducerRejection(value=>{events.push(value);throw Error('observer failure');});await f.client.pollControls();await f.client.pollControls();expect(events).toEqual([event]);expect(f.calls.filter(call=>call.input.method==='rejectionsAck').every(call=>call.input.sequence===1)).toBe(true);release();
});
it('delivers support commands once per tab after a listener mounts and reads the durable outbox count',async()=>{
 const f=fixture(),commands:unknown[]=[];const command={sequence:1,id:'support-1',kind:'inspect',payload:{task:'synthetic'}};
 f.handler=async input=>{if(input.method==='controls')return {tokens:[],status:{},session:'authenticated',failure:null,supportCommands:[command]};if(input.method==='supportAck')return null;if(input.method==='outboxCount')return 2;};
 await f.client.connect();await f.client.pollControls();expect(f.calls.some(call=>call.input.method==='supportAck')).toBe(false);
 const release=f.client.onSupportCommand(value=>commands.push(value));await f.client.pollControls();await f.client.pollControls();
 expect(commands).toEqual([{id:'support-1',kind:'inspect',payload:{task:'synthetic'}}]);
 expect(f.calls.filter(call=>call.input.method==='supportAck').every(call=>call.input.sequence===1)).toBe(true);
 expect(await f.client.outboxCount()).toBe(2);release();
});

it('creates inert bounded React watches and releases their page/source with the last subscription',async()=>{
 const f=fixture(),args={workspace:'original'},watch=f.client.watchReplica({path:'tasks/all'},args,{deferStart:true});args.workspace='mutated';const empty=watch.snapshot();expect(watch.snapshot()).toBe(empty);expect(f.calls).toEqual([]);expect(f.client.budgets().find(value=>value.name==='browser-watch-metadata')?.entries).toBe(0);
 const stop=watch.subscribe(()=>{});await watch.page();expect(f.calls.find(call=>call.input.method==='watchReplica')?.input.args).toEqual({workspace:'original'});stop();expect(watch.snapshot()).toBe(empty);await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchReplica')).toHaveLength(1));expect(f.client.budgets().find(value=>value.name==='browser-row-pages')?.bytes).toBe(0);const again=watch.subscribe(()=>{});await watch.page();again();watch.close();
});
it('presents only a bounded compatibility collection page and marks larger sources incomplete',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'},{},{deferStart:true}),updates:unknown[]=[];
 f.handler=async input=>input.method==='replicaPage'?{generation:{epoch:1,sequence:2},rows:[{id:'a',value:{id:'a'}}],nextOffset:1,window:{generation:'one',total:100000,header:{source:'server',completeness:'complete'}}}:undefined;
 const stop=watch.onUpdate(()=>updates.push(watch.localReplicaState()));
 await vi.waitFor(()=>expect(watch.localReplicaState()?.rows).toEqual([{id:'a'}]));
 expect(watch.localReplicaState()).toMatchObject({total:100000,truncated:true,completeness:'partial',isUpToDate:false});
 expect(f.calls.find(call=>call.input.method==='replicaPage')?.input.limit).toBe(128);
 stop();watch.close();expect(updates.length).toBeGreaterThan(0);
});
it('routes separately leased live views through bounded canonical and pending window pages',async()=>{
 const f=fixture(),ref={kind:'query',path:'tasks/grid',delivery:'live' as const,live:{entity:'tasks',key:'id',plan:{} as any}};f.handler=async input=>{if(input.method==='watchLive')return {signature:'live'};if(input.method==='livePage')return {generation:{epoch:1,sequence:1},rows:[{id:'a',value:{id:'a'}}]};if(input.method==='livePendingPage')return {generation:'pending',complete:false,serverVerified:false,rows:[{id:'a',value:{id:'a'}}]};if(input.method==='unwatchLive')return null;};const one=f.client.watchLiveWindow(ref,{},{view:'grid'}),two=f.client.watchLiveWindow(ref,{},{view:'preview'});const pending=await one.pendingPage({limit:10});expect(pending?.complete).toBe(false);await two.page({limit:10});expect(one.snapshot().page).toBe(pending);expect(f.calls.filter(call=>call.input.method==='watchLive')).toHaveLength(2);expect(f.calls.some(call=>call.input.method==='watchReplica')).toBe(false);one.close();two.close();await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='unwatchLive')).toHaveLength(2));
});
it('rebuilds a structured live result from two fenced bounded pages',async()=>{
 const f=fixture(),ref={kind:'query',path:'tasks/grid',delivery:'live' as const,live:{entity:'tasks',key:'id',resultPath:['page'],plan:{} as any}};
 f.handler=async input=>{if(input.method==='watchLive')return {signature:'live'};if(input.method==='livePage')return {generation:{epoch:1,sequence:1},window:{generation:'window',total:129,header:{resultSkeleton:{page:[],total:100000,limit:250},resultPath:['page']}},offset:input.offset??0,nextOffset:input.offset?null:128,rows:input.offset?[{id:'tail',value:{id:'tail'}}]:Array.from({length:128},(_,i)=>({id:String(i),value:{id:String(i)}}))};if(input.method==='unwatchLive')return null;};
 const watch=f.client.watchLiveQuery<{page:Array<{id:string}>;total:number}>(ref,{}, {deferStart:true});const stop=watch.onUpdate(()=>{});
 await vi.waitFor(()=>expect(watch.localLiveQueryResult()?.page).toHaveLength(129));
 expect(watch.localLiveQueryResult()?.total).toBe(100000);expect(watch.localLiveQueryResult()?.page[0]).toEqual({id:'0'});expect(watch.localLiveQueryResult()?.page.at(-1)).toEqual({id:'tail'});
 expect(f.calls.filter(call=>call.input.method==='livePage').map(call=>call.input.limit)).toEqual([128,128]);
 stop();watch.close();
});

it('admits a normal metadata-heavy route beyond 64 watches within the same byte ceilings and releases every watch',async()=>{
 const f=fixture(),watches=Array.from({length:100},(_,n)=>f.client.watchReplica({path:'metadata/table'+n}));
 await Promise.all(watches.map(watch=>watch.page()));
 expect(f.client.memoryCounters().replicaWindows).toBe(100);
 expect(f.client.budgets().find(value=>value.name==='browser-row-pages')).toMatchObject({maxBytes:2097152});
 expect(f.client.budgets().find(value=>value.name==='browser-watch-metadata')).toMatchObject({maxBytes:524288});
 for(const watch of watches)watch.close();await vi.waitFor(()=>expect(f.client.memoryCounters().replicaWindows).toBe(0));
 expect(f.client.budgets().find(value=>value.name==='browser-row-pages')?.bytes).toBe(0);
});

it('retains only canonical live IDs without opening an extra transport query and handles numeric display IDs',async()=>{
 const f=fixture(),ref={kind:'query' as const,path:'tasks/window',delivery:'live' as const,live:{entity:'tasks',key:'_id',resultPath:['page']}};
 const signature=f.client.replicaSignature(ref,{limit:250}),release=f.client.retainReplicaWindow(signature),notify=vi.fn(),stop=f.client.subscribeReplicaWindow(signature,notify);
 expect(f.client.replicaWindowMembership(signature)).toBeUndefined();expect(f.calls.some(call=>call.input.method==='watchLive')).toBe(false);
 f.handler=async input=>{if(input.method==='watchLive')return {signature:'server'};if(input.method==='livePage')return {generation:{epoch:1,sequence:1},window:{generation:'window',total:1,header:{resultSkeleton:{page:[],total:10000,offset:0,limit:250},resultPath:['page']}},offset:0,nextOffset:null,rows:[{id:'canonical',value:{_id:'canonical',id:42,name:'Visible'}}]};if(input.method==='unwatchLive')return null;};
 const watch=f.client.watchLiveQuery(ref,{limit:250}),unwatch=watch.onUpdate(()=>{});
 await vi.waitFor(()=>expect(watch.localLiveQueryResult()).toEqual({page:[{_id:'canonical',id:42,name:'Visible'}],total:10000,offset:0,limit:250}));
 expect(f.client.replicaWindowMembership(signature)).toMatchObject({entity:'tasks',ids:['canonical'],total:10000,limit:250});
 expect(f.calls.filter(call=>call.input.method==='watchLive')).toHaveLength(1);expect(notify).toHaveBeenCalled();
 unwatch();watch.close();stop();release();expect(f.client.budgets().find(value=>value.name==='browser-live-memberships')?.bytes).toBe(0);
});

it('services control hints without invalidating every mounted data page',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());
 const page=await watch.page();const notify=vi.fn(),stop=watch.subscribe(notify);
 f.control();await f.client.pollControls();
 expect(watch.snapshot().stale).toBe(false);expect(watch.snapshot().page).toBe(page);expect(notify).not.toHaveBeenCalled();
 f.change();expect(watch.snapshot().stale).toBe(true);expect(notify).toHaveBeenCalledOnce();stop();
});

it('does not spin the microtask queue after a replica read is fenced or the client closes',async()=>{
 const f=fixture();let reads=0;f.handler=input=>{if(input.method==='replicaPage'){reads++;throw Object.assign(Error('Admission unavailable'),{code:'FENCED'});}};
 const watch=f.client.watchReplica({path:'tasks/all'}),stop=watch.onUpdate(vi.fn());
 await new Promise(resolve=>setTimeout(resolve,50));expect(reads).toBe(1);
 f.change();await vi.waitFor(()=>expect(reads).toBe(2));
 f.client.close();await new Promise(resolve=>setTimeout(resolve,50));expect(reads).toBe(2);stop();watch.close();
});

it('retains a verified replica projection and waits for a feed hint when its next read crosses a cohort',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'}),stop=watch.onUpdate(vi.fn());cleanup.push(()=>{stop();watch.close();});
 await vi.waitFor(()=>expect(watch.localReplicaState()).toBeDefined());const before=watch.localReplicaState();let reads=0;
 f.handler=input=>{if(input.method==='replicaPage'){reads++;throw Object.assign(Error('Cohort not yet committed'),{code:'PAGE_CHANGED'});}};
 f.change();await vi.waitFor(()=>expect(reads).toBe(1));await new Promise(resolve=>setTimeout(resolve,75));
 expect(reads).toBe(1);expect(watch.localReplicaState()).toBe(before);
 f.handler=undefined;f.change();await vi.waitFor(()=>expect(watch.localReplicaState()).not.toBe(before));
});

it('coalesces identical mounted page reads and lets one lease close without canceling another',async()=>{
 const f=fixture(),one=f.client.watchReplica({path:'tasks/all'}),two=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>{one.close();two.close();});
 let release!:()=>void;const gate=new Promise<void>(resolve=>{release=resolve;});f.handler=async input=>{if(input.method==='replicaPage'){await gate;return {generation:{epoch:1,sequence:1},rows:[],window:{header:{}}};}};
 const first=one.page({limit:128}),second=two.page({limit:128}),rejected=expect(first).rejects.toMatchObject({code:'FENCED'});
 await vi.waitFor(()=>expect(f.calls.filter(call=>call.input.method==='replicaPage')).toHaveLength(1));one.close();release();await rejected;
 await expect(second).resolves.toMatchObject({rows:[]});expect(two.snapshot().page).toBeDefined();
 expect(f.client.budgets().find(value=>value.name==='browser-shared-reads')).toMatchObject({entries:0,bytes:0});
});

it('captures a page request before asynchronous source opening',async()=>{
 const f=fixture(),watch=f.client.watchReplica({path:'tasks/all'});cleanup.push(()=>watch.close());
 const options={limit:10},pending=watch.page(options);options.limit=100;await pending;
 expect(f.calls.find(call=>call.input.method==='replicaPage')?.input.limit).toBe(10);
});

it('refreshes pending display pages while coalesced staging hints preserve complete cached pages',async()=>{
 const f=fixture(),one=f.client.watchReplica({path:'tasks/all'}),two=f.client.watchReplica({path:'statuses/all'});cleanup.push(()=>{one.close();two.close();});
 f.handler=input=>input.method==='replicaPage'&&input.signature==='a:tasks/all'?{complete:false,serverVerified:false,rows:[]}:undefined;
 const first=await one.page(),second=await two.page(),oneNotify=vi.fn(),twoNotify=vi.fn(),stopOne=one.subscribe(oneNotify),stopTwo=two.subscribe(twoNotify);
 f.change('@pending');expect(one.snapshot().stale).toBe(true);expect(oneNotify).toHaveBeenCalledOnce();
 expect(two.snapshot().stale).toBe(false);expect(two.snapshot().page).toBe(second);expect(one.snapshot().page).toBe(first);expect(twoNotify).not.toHaveBeenCalled();
 f.change();expect(two.snapshot().stale).toBe(true);expect(twoNotify).toHaveBeenCalledOnce();stopOne();stopTwo();
});
