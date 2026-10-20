import test from 'node:test';
import assert from 'node:assert/strict';
const {SQLiteBrowserClient}=await import(new URL('../dist/sqlite-browser-client.js',import.meta.url));

async function until(read,accept){
 const expires=Date.now()+2000;
 while(Date.now()<expires){const value=read();if(accept(value))return value;await new Promise(resolve=>setTimeout(resolve,5));}
 assert.fail('Expected public collection state was not published');
}
function fixture(){
 const changes=new Set();let sequence=1,partial=false,name='original',removed=false,lastPage,payload={tags:['a'],meta:{enabled:true}},extra=false,reversed=false,authorized=true;
 const broker={
  async request(_operation,input){
   switch(input.method){
    case 'connect':return {connection:'connection',identityKey:'identity'};
    case 'controls':return {tokens:[],status:{isWebSocketConnected:true},session:'authenticated',failure:null};
    case 'watchReplica':return {id:'source',signature:'source-signature'};
    case 'replicaPage':{if(!authorized)return lastPage=null;const rows=removed?[]:[{id:'row',value:{id:'row',name,payload:structuredClone(payload)}}];if(extra)rows.push({id:'row2',value:{id:'row2',name:'second'}});if(reversed)rows.reverse();return lastPage={generation:{epoch:1,sequence},offset:0,nextOffset:null,rows,window:{generation:'membership',total:rows.length,header:{source:'server',completeness:partial?'partial':'complete',truncated:partial}}};}
    case 'unwatchReplica':return null;
    default:throw Error('Unexpected diagnostic RPC '+input.method);
   }
  },
  subscribeChanges(listener){changes.add(listener);return ()=>changes.delete(listener);},
  stats(){return {ownerEpoch:1};},close(){changes.clear();},
 };
 const client=new SQLiteBrowserClient('ws://private-synthetic.test/ws',{broker,auth:{project:'project',tenant:'tenant',identity:{sub:'account'}}});
 const watch=client.watchReplica({path:'tasks/all'},{},{deferStart:true});const stop=watch.onUpdate(()=>{});
 return {client,watch,page:()=>lastPage,change(update={}){sequence++;if('name'in update)name=update.name;if('partial'in update)partial=update.partial;if('removed'in update)removed=update.removed;if('payload'in update)payload=update.payload;if('extra'in update)extra=update.extra;if('reversed'in update)reversed=update.reversed;for(const notify of changes)notify('scope',sequence);return sequence;},async revoke(){authorized=false;await client.setAuth({tenant:'other-tenant'});},close(){stop();watch.close();client.close();}};
}

test('identical collection values retain their public rows while current revision and coverage still publish',async()=>{
 const f=fixture();
 try{
  const initial=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===1);
  assert.equal(initial.completeness,'complete');const rows=initial.rows;
  const revision=f.change();const repeated=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===revision);
  assert.deepEqual(repeated.rows,rows);
  assert.strictEqual(repeated.rows[0],f.page().rows[0].value,'Public state and the bounded page cache must share the current row object');
  assert.strictEqual(repeated.rows,rows,'An unrelated canonical publication should not replace an identical collection rows array');
  const partialRevision=f.change({partial:true});const partial=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===partialRevision);
  assert.equal(partial.completeness,'partial');assert.equal(partial.truncated,true);assert.equal(partial.isUpToDate,false);
  assert.strictEqual(partial.rows,rows,'Coverage changes must publish independently of unchanged row values');
  const editedRevision=f.change({name:'edited',partial:false});const edited=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===editedRevision);
  assert.equal(edited.completeness,'complete');assert.notStrictEqual(edited.rows,rows);assert.equal(edited.rows[0].name,'edited');
  const removedRevision=f.change({removed:true});const removed=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===removedRevision);
  assert.deepEqual(removed.rows,[]);assert.notStrictEqual(removed.rows,edited.rows);
 }finally{f.close();}
});


test('nested values and membership changes publish correct public rows and ids',async()=>{
 const f=fixture();try{
  const initial=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===1);
  let revision=f.change({payload:{tags:['b','c'],meta:{enabled:false}}});
  const nested=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===revision);
  assert.notStrictEqual(nested.rows,initial.rows);assert.deepEqual(nested.rows[0].payload,{tags:['b','c'],meta:{enabled:false}});
  revision=f.change({extra:true});const added=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===revision);
  assert.deepEqual(added.ids,['row','row2']);assert.equal(added.total,2);
  revision=f.change({reversed:true});const reordered=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===revision);
  assert.deepEqual(reordered.ids,['row2','row']);assert.deepEqual(reordered.rows.map(row=>row.id),['row2','row']);
  revision=f.change({extra:false});const removed=await until(()=>f.watch.localReplicaState(),value=>value?.computedRevision===revision);
  assert.deepEqual(removed.ids,['row']);assert.equal(removed.total,1);
 }finally{f.close();}
});

test('identity change clears the previously shared public row state',async()=>{
 const f=fixture();try{
  await until(()=>f.watch.localReplicaState(),value=>value?.rows.length===1);
  await f.revoke();
  await until(()=>f.watch.localReplicaState(),value=>value===undefined);
  assert.equal(f.watch.localReplicaResult(),undefined);
 }finally{f.close();}
});
