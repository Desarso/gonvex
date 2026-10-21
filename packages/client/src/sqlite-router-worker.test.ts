import {afterEach,expect,it,vi} from 'vitest';
const hosts=vi.hoisted(()=>[] as any[]);
vi.mock('./sqlite-wasm-bytes.js',()=>({installSQLiteWasmByteCache:()=>()=>{}}));
vi.mock('./sqlite-broker.js',()=>({SQLiteReplicaBroker:class{hostedOwnerID='shared-owner';routeHostedRequest=vi.fn();attachHostedFollower=vi.fn();close=vi.fn();constructor(options:any){options.createWorker();hosts.push(this);}}}));
afterEach(()=>{vi.unstubAllGlobals();vi.restoreAllMocks();hosts.length=0;});
function setup(nested=true){vi.stubGlobal('Worker',nested?class{}:undefined);vi.stubGlobal('navigator',{locks:{}});vi.stubGlobal('onconnect',undefined);vi.spyOn(globalThis,'setInterval').mockReturnValue(0 as any);}
function peer(id:string){const port={onmessage:undefined as any,postMessage:vi.fn(),start(){},close:vi.fn()};(globalThis as any).onconnect({ports:[port]});port.onmessage({data:{type:'hello',protocol:2,tab:id},ports:[]});return port;}
it('hosts one dedicated worker and routes followers without an owning page relay',async()=>{
 setup();vi.resetModules();const {configureSQLiteRouterOwner}=await import('./sqlite-router-worker.js');const createWorker=vi.fn(()=>({} as Worker));configureSQLiteRouterOwner({schemaVersion:'a'.repeat(64),createWorker});configureSQLiteRouterOwner({createWorker});expect(createWorker).toHaveBeenCalledOnce();
 const first=peer('first'),second=peer('second');expect(second.postMessage).toHaveBeenCalledWith({type:'pong',ownerHost:true});
 const direct={close:vi.fn()};second.onmessage({data:{type:'attach',to:'shared-owner',token:'owner'},ports:[direct]});expect(hosts[0].attachHostedFollower).toHaveBeenCalledWith('second','owner',direct);
 const message={type:'request',protocol:2,to:'shared-owner',id:'read',token:'owner',op:'clientCall',args:{method:'entities',entity:'tasks',ids:['a']}};second.onmessage({data:message,ports:[]});expect(hosts[0].routeHostedRequest).toHaveBeenCalledWith({...message,from:'second'});expect(first.postMessage).not.toHaveBeenCalledWith(expect.objectContaining({type:'request'}));
 first.onmessage({data:{type:'bye'},ports:[]});expect(hosts[0].close).not.toHaveBeenCalled();
});
it('reports capability fallback when nested dedicated workers are unavailable',async()=>{
 setup(false);vi.resetModules();const {configureSQLiteRouterOwner}=await import('./sqlite-router-worker.js');const createWorker=vi.fn();configureSQLiteRouterOwner({createWorker});expect(createWorker).not.toHaveBeenCalled();expect(peer('first').postMessage).toHaveBeenCalledWith({type:'pong',ownerHost:false});
});
