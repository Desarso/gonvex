import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {Dexie} from 'dexie';
import {GonvexClient,type FunctionReference} from './index';
import {IndexedDBLocalReplicaStorage} from './indexeddb-replica';
import {replicaHashesDigest,replicaRowsHashes} from './replica-integrity';

class Socket {
  static CONNECTING=0;static OPEN=1;static CLOSED=3;
  static all:Socket[]=[];readyState=0;sent:any[]=[];
  listeners=new Map<string,Array<(event:any)=>void>>();
  constructor(){Socket.all.push(this);}
  addEventListener(type:string,listener:(event:any)=>void){this.listeners.set(type,[...(this.listeners.get(type)??[]),listener]);}
  emit(type:string,event:any={}){for(const listener of this.listeners.get(type)??[])listener(event);}
  send(value:string){this.sent.push(JSON.parse(value));}
  open(){this.readyState=1;this.emit('open');}
  close(){this.readyState=3;this.emit('close');}
  receive(value:unknown){this.emit('message',{data:JSON.stringify(value)});}
}
let client:GonvexClient,storage:IndexedDBLocalReplicaStorage;
const ref:FunctionReference={kind:'query',path:'items.rows',delivery:'replica',replica:{table:'items',key:'_id',columns:['_id','name']}};
const directive={protocolVersion:1 as const,scope:'tenant',visibilityScope:'visible',epoch:'epoch'};
beforeEach(()=>{
  Dexie.dependencies.indexedDB=new IDBFactory();Dexie.dependencies.IDBKeyRange=IDBKeyRange;
  storage=new IndexedDBLocalReplicaStorage('metadata-'+Math.random(),'mounted');
  Socket.all=[];vi.stubGlobal('WebSocket',Socket);
  client=new GonvexClient('ws://metadata.test',{localReplica:{storage,maxResidentRows:2}});
});
afterEach(()=>{client.close();storage.close();vi.unstubAllGlobals();});

it('starts no resources for an abandoned deferred metadata watch and releases mounted listeners',async()=>{
  const internal=client as any;
  const scopes=internal.sessionScopeHandlers.size;
  const watch=client.watchReplicaMetadata(ref,{}, {deferStart:true});
  expect(watch.localReplicaMetadata()).toBeUndefined();
  expect(Socket.all).toHaveLength(0);
  expect(internal.sessionScopeHandlers.size).toBe(scopes);
  const off=watch.onUpdate(()=>{});
  expect(Socket.all).toHaveLength(1);
  expect(internal.sessionScopeHandlers.size).toBe(scopes+1);
  expect(internal.replica.retainedWindows.size).toBe(0);
  off();
  expect(internal.sessionScopeHandlers.size).toBe(scopes);
});

it('persists complete collection coverage without pinning or materializing the collection',async()=>{
  const internal=client as any;
  const retain=vi.spyOn(internal.replica,'retainWindow');
  const snapshot=vi.spyOn(internal.replica,'liveQuerySnapshot');
  const watch=client.watchReplicaMetadata(ref,{}, {deferStart:true});
  const off=watch.onUpdate(()=>{});
  const socket=Socket.all[0];socket.open();socket.receive({type:'session.ready',replica:directive});
  await vi.waitFor(()=>expect(socket.sent.some(m=>m.type==='replica.open')).toBe(true));
  const open=socket.sent.find(m=>m.type==='replica.open');
  const rows=Array.from({length:20},(_,i)=>({_id:'item-'+i,name:'Item '+i}));
  socket.receive({type:'replica.snapshot',id:open.id,path:ref.path,result:rows,key:'_id',cursor:{epoch:'epoch',revision:1}});
  await vi.waitFor(()=>expect(watch.localReplicaMetadata()?.rowCount).toBe(20));
  const digest=await replicaHashesDigest(await replicaRowsHashes(rows,'_id'));
  socket.receive({type:'replica.ready',id:open.id,path:ref.path,digest,cursor:{epoch:'epoch',revision:1},truncated:false});
  await vi.waitFor(()=>expect(watch.localReplicaMetadata()).toMatchObject({completeness:'complete',isUpToDate:true,rowCount:20}));
  const metadata=watch.localReplicaMetadata();expect(watch.localReplicaMetadata()).toBe(metadata);
  expect(Object.isFrozen(metadata)).toBe(true);
  expect(metadata).not.toHaveProperty('rows');expect(metadata).not.toHaveProperty('ids');
  expect(retain).not.toHaveBeenCalled();expect(snapshot).not.toHaveBeenCalled();
  expect(client.localReplica.entityRows('items').length).toBeLessThanOrEqual(2);
  expect((await storage.loadEntityRows('visible','items',rows.map(r=>r._id)))).toHaveLength(20);
  const release=client.retainReplicaEntities('items',['item-0']);
  await vi.waitFor(()=>expect(client.localReplica.entity('items','item-0')).toEqual(rows[0]));
  expect(client.localReplica.entityRows('items').length).toBeLessThanOrEqual(2);
  release();off();
});
