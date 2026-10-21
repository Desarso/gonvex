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

it('verifies a legacy ready frame after unmounted snapshot rows have been evicted',async()=>{
  const stop=client.subscribeReplica(ref,{},()=>{});
  const socket=Socket.all[0];socket.open();socket.receive({type:'session.ready',replica:directive});
  await vi.waitFor(()=>expect(socket.sent.some(m=>m.type==='replica.open')).toBe(true));
  const open=socket.sent.find(m=>m.type==='replica.open');
  const rows=Array.from({length:20},(_,i)=>({_id:'item-'+i,name:'Item '+i}));
  socket.receive({type:'replica.snapshot',id:open.id,path:ref.path,result:rows,key:'_id',cursor:{epoch:'epoch',revision:1}});
  const key=client.replicaSignature(ref,{});
  await vi.waitFor(()=>expect(client.localReplica.getWindow(key)?.ids.length).toBe(20));
  expect(client.localReplica.entityRows('items').length).toBeLessThanOrEqual(2);
  const digest=await replicaHashesDigest(await replicaRowsHashes(rows,'_id'));
  socket.receive({type:'replica.ready',id:open.id,path:ref.path,digest,cursor:{epoch:'epoch',revision:1},truncated:false});
  await vi.waitFor(()=>expect(client.localReplica.getWindow(key)?.completeness).toBe('complete'));
  expect(client.localReplica.getWindow(key)?.ids).toEqual(rows.map(r=>r._id));
  expect(socket.sent.filter(m=>m.type==='replica.open')).toHaveLength(1);
  expect(await storage.loadEntityRows('visible','items',rows.map(r=>r._id))).toHaveLength(20);
  stop();
});

it.each([false,true])('keeps cold legacy certificates through a no-upsert delta, deleted=%s',async(remove)=>{
  // Older generated refs may omit columns and the runtime may lack watermarks.
  // A deletion still changes only membership, never the surviving row images.
  const legacy:FunctionReference={...ref,replica:{table:'items',key:'_id'}};
  const stop=client.subscribeReplica(legacy,{},()=>{});
  const socket=Socket.all[0];socket.open();socket.receive({type:'session.ready',replica:directive});
  await vi.waitFor(()=>expect(socket.sent.some(m=>m.type==='replica.open')).toBe(true));
  const open=socket.sent.find(m=>m.type==='replica.open');
  const rows=Array.from({length:20},(_,i)=>({_id:'item-'+i,name:'Item '+i}));
  const key=client.replicaSignature(legacy,{});
  socket.receive({type:'replica.snapshot',id:open.id,path:legacy.path,result:rows,key:'_id',cursor:{epoch:'epoch',revision:1}});
  await vi.waitFor(()=>expect(client.localReplica.getWindow(key)?.ids.length).toBe(20));
  const firstDigest=await replicaHashesDigest(await replicaRowsHashes(rows,'_id'));
  socket.receive({type:'replica.ready',id:open.id,path:legacy.path,digest:firstDigest,cursor:{epoch:'epoch',revision:1},truncated:false});
  await vi.waitFor(()=>expect(client.localReplica.getWindow(key)?.completeness).toBe('complete'));
  expect(client.localReplica.entityRows('items').length).toBeLessThanOrEqual(2);
  const remaining=remove ? rows.slice(1) : rows;
  socket.receive({type:'replica.delta',id:open.id,path:legacy.path,upserts:[],deleted:remove?['item-0']:[],cursor:{epoch:'epoch',revision:2}});
  await vi.waitFor(()=>expect(client.localReplica.getWindow(key)?.cursor?.revision).toBe(2));
  const expected=await replicaRowsHashes(remaining,'_id');
  expect(client.localReplica.getWindow(key)?.hashes).toEqual(expected);
  socket.receive({type:'replica.ready',id:open.id,path:legacy.path,digest:await replicaHashesDigest(expected),cursor:{epoch:'epoch',revision:2},truncated:false});
  await (client as any).replicaFrames;
  expect(client.localReplica.getWindow(key)?.completeness).toBe('complete');
  expect(socket.sent.filter(m=>m.type==='replica.open')).toHaveLength(1);
  expect((await storage.load('visible'))?.liveQueries[key]?.ids).toEqual(remaining.map(row=>row._id));
  stop();
});

it('verifies legacy partial-image deltas from cold durable rows in bounded pages',async()=>{
  const projected:FunctionReference={...ref,replica:{...ref.replica!,columns:['_id','name','note']}};
  const stop=client.subscribeReplica(projected,{},()=>{});
  const socket=Socket.all[0];socket.open();socket.receive({type:'session.ready',replica:directive});
  await vi.waitFor(()=>expect(socket.sent.some(m=>m.type==='replica.open')).toBe(true));
  const open=socket.sent.find(m=>m.type==='replica.open');
  const rows=Array.from({length:260},(_,i)=>({_id:'item-'+i,name:'Item '+i,note:'Preserved '+i}));
  const key=client.replicaSignature(projected,{});
  socket.receive({type:'replica.snapshot',id:open.id,path:projected.path,result:rows,key:'_id',cursor:{epoch:'epoch',revision:1}});
  // Snapshot persistence is part of the frame lane. Wait for that exact
  // operation, rather than racing its disk work against waitFor's 1s default.
  await (client as any).replicaFrames;
  expect(client.localReplica.getWindow(key)?.ids.length).toBe(260);
  socket.receive({type:'replica.ready',id:open.id,path:projected.path,digest:await replicaHashesDigest(await replicaRowsHashes(rows,'_id')),cursor:{epoch:'epoch',revision:1},truncated:false});
  await vi.waitFor(()=>expect(client.localReplica.getWindow(key)?.completeness).toBe('complete'));
  const pages:number[]=[];const originalReadView=storage.withReadView.bind(storage);
  const readView=vi.spyOn(storage,'withReadView').mockImplementation((scope,coverage,run)=>originalReadView(scope,coverage,view=>run({...view,select:async(read,excluded)=>{if(read.where&&'values' in read.where)pages.push(read.where.values.length);return view.select(read,excluded);}})));
  socket.receive({type:'replica.delta',id:open.id,path:projected.path,upserts:[{_id:'item-0',name:'Changed'}],deleted:[],cursor:{epoch:'epoch',revision:2}});
  await vi.waitFor(()=>expect(client.localReplica.getWindow(key)?.cursor?.revision).toBe(2));
  const changed=rows.map((row,i)=>i===0?{...row,name:'Changed'}:row);
  socket.receive({type:'replica.ready',id:open.id,path:projected.path,digest:await replicaHashesDigest(await replicaRowsHashes(changed,'_id')),cursor:{epoch:'epoch',revision:2},truncated:false});
  await (client as any).replicaFrames;
  expect(client.localReplica.getWindow(key)?.completeness).toBe('complete');
  expect(socket.sent.filter(m=>m.type==='replica.open')).toHaveLength(1);
  expect(client.localReplica.getWindow(key)?.hashes).toEqual(await replicaRowsHashes(changed,'_id'));
  expect(client.localReplica.entityRows('items').length).toBeLessThanOrEqual(2);
  expect(readView).toHaveBeenCalledTimes(1);
  expect(pages).toEqual([128,128,4]);
  stop();
});
