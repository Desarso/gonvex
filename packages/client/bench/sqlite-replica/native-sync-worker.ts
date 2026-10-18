import {assertSQLiteMembership} from '../../src/sqlite-membership-plan.js';
import {sqliteQueryEntityScopes} from '../../src/sqlite-entity-scope.js';
import {compileSQLiteOptimisticPage} from '../../src/sqlite-typed-query.js';
import {SQLitePredictions} from '../../src/sqlite-predictions.js';
import {openOfficialSQLiteHost} from '../../src/sqlite-official-host.js';
import {SQLiteTypedReplicaEngine} from '../../src/sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from '../../src/sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from '../../src/sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from '../../src/sqlite-snapshot-pages.js';
import {SQLiteSnapshotActivator} from '../../src/sqlite-snapshot-activation.js';
import {SQLiteNativeIngress} from '../../src/sqlite-native-ingress.js';
import {SQLiteWindowStore} from '../../src/sqlite-window-store.js';
const schema={rows:{key:'id',fields:{id:{kind:'text'},text:{kind:'text'},nested:{kind:'json'},value:{kind:'number'}},indexes:[{columns:['id']},{columns:['text']},{columns:['value']}],search:['text'],searchDetail:'column',searchContent:'external'}} as const;
const scope='visible';
let host:Awaited<ReturnType<typeof openOfficialSQLiteHost>>,engine:SQLiteTypedReplicaEngine,database:ReturnType<typeof sqliteOwnerTransactions>,canonical:ReturnType<typeof sqliteTypedReplicaStorage>,snapshots:SQLiteSnapshotPages,activation:SQLiteSnapshotActivator,ingress:SQLiteNativeIngress,socket:WebSocket,manifest:any,header:any;
let lane=Promise.resolve(),completed=false,started=0,verified=0,acks=0,activationMs=0;
const runOwner=<T>(run:()=>Promise<T>):Promise<T>=>{const next=lane.then(run);lane=next.then(()=>{},()=>{});return next;};
function failure(error:unknown){globalThis.postMessage({error:String((error as Error).stack??error)});socket?.close();ingress?.close();}
const handlers={
 async open(){
  host=await openOfficialSQLiteHost();database=sqliteOwnerTransactions(host.database);engine=new SQLiteTypedReplicaEngine(database,schema);await engine.open('native-sync-control');
  canonical=sqliteTypedReplicaStorage(database,schema);snapshots=new SQLiteSnapshotPages(database,()=>engine.verifyOwner(),schema);activation=new SQLiteSnapshotActivator(database,canonical,()=>engine.verifyOwner(),schema);
  await snapshots.ready();await activation.ready();
  await canonical.applyTransaction({cursor:{epoch:'e',revision:20},changes:[{entity:'rows',id:'row-0000',operation:'insert',newValue:{id:'row-0000',text:'newer first page'}}]},{entities:{},liveQueries:{}},scope);
  return {sqliteVersion:(await database.getFirstAsync<{version:string}>('SELECT sqlite_version() version'))?.version,nativeHash:host.nativeHash,nativeReceipt:host.nativeReceipt,nativeSnapshot:host.nativeSnapshot,nativeFold:host.nativeFastFold,wasmBytes:host.memoryBytes()};
 },
 async connect({url}:{url:string}){
  host.takeProfile();started=performance.now();globalThis.postMessage({phase:'ingest'});socket=new WebSocket(url);socket.binaryType='arraybuffer';
  ingress=new SQLiteNativeIngress({database,snapshots,scope,runOwner,sendAck(ack){acks++;socket.send(JSON.stringify(ack));},async activateVerified(receipt){
   globalThis.postMessage({phase:'activation'});
   const at=performance.now();await activation.activate({scope,subscription:receipt.ack.id,snapshot:receipt.ack.snapshotId,header});activationMs+=performance.now()-at;verified++;
  }});
  socket.onmessage=event=>{
   if(event.data instanceof ArrayBuffer){void ingress.accept(event.data).catch(failure);return;}
   try{
    const message=JSON.parse(event.data);
    if(message.type==='proof.manifest'){
     if(manifest)throw Error('Duplicate fixture manifest');manifest=message;
     header={signature:'native-sync-control',entity:'rows',key:'id',kind:'replica',source:'server',completeness:'complete',cursor:message.cursor,aggregateDigest:message.digest};
     void runOwner(()=>snapshots.prepareTyped({scope,subscription:message.id,snapshot:message.snapshotId,entity:'rows'})).catch(failure);
    }else if(message.type==='replica.ready'){
     if(!manifest||message.digest!==manifest.digest)throw Error('Invalid final fixture certificate');
     void runOwner(async()=>{
      const window=await new SQLiteWindowStore(database,()=>engine.verifyOwner()).header(scope,header.signature);
      if(window?.rows!==manifest.rows||verified!==1||acks!==manifest.frames)throw Error('Incomplete native activation');
      const current=(await canonical.loadEntityRows(scope,'rows',['row-0000']))[0]?.row;
      if(current?.text!=='newer first page'||!current.nested)throw Error('Newer canonical projection was lost');
      completed=true;globalThis.postMessage({phase:'ready',completed:{ms:performance.now()-started,rows:window.rows,acks,verified,activationMs,profile:host.takeProfile(),wasmBytes:host.memoryBytes(),queue:ingress.stats(),wire:snapshots.stats(),firstPagePreserved:true}});
     }).catch(failure);
    }else throw Error('Unexpected fixture control frame');
   }catch(error){failure(error);}
  };
  socket.onerror=()=>failure(Error('Native fixture socket failed'));
  socket.onclose=()=>{if(!completed)failure(Error('Native fixture socket closed before completion'));};
  return {started:true};
 },
 async page(){return engine.page(scope,{table:'rows',order:[{field:'id',direction:'asc'}],textOrder:'binary',limit:50});},
 async optimistic({expectedDeepID,expectedFirstIDs,expectedCanonicalIDs}:{expectedDeepID:string;expectedFirstIDs:string[];expectedCanonicalIDs:string[]}){
  if(!completed)throw Error('Import must finish before query control');
  const predictions=new SQLitePredictions(database,canonical,schema,()=>engine.verifyOwner());await predictions.ready();host.takeProfile();
  await predictions.apply(scope,1,[{entity:'rows',rowId:'row-0001',op:'patch',fields:{value:-20}},{entity:'rows',rowId:'row-0002',op:'delete'},{entity:'rows',rowId:'offline-created',op:'insert',fields:{id:'offline-created',text:'Synthetic offline control café 東京',nested:null,value:-10}}]);
  const plan={table:'rows',membership:{signature:header.signature,generation:manifest.snapshotId,requireComplete:true},order:[{field:'value',direction:'asc' as const}],limit:50};
  const timings:number[]=[];let first;
  for(let n=0;n<20;n++){const at=performance.now();first=await predictions.page(scope,plan,true);timings.push(performance.now()-at);}
  const checks={first:JSON.stringify(first!.rows.slice(0,expectedFirstIDs.length).map(row=>row.id))===JSON.stringify(expectedFirstIDs),deletedAbsent:!first!.rows.some(row=>row.id==='row-0002'),deep:false,search:false,canonical:false};
  const at=performance.now(),deep=await predictions.viewPage(scope,'optimistic-control',plan,90000,true),deepMs=performance.now()-at;
  checks.deep=deep.total===100000&&deep.rows[0]?.id===expectedDeepID;
  const searchAt=performance.now(),search=await predictions.page(scope,{...plan,search:'Synthetic offline control'},true),searchMs=performance.now()-searchAt;checks.search=search.rows.length===1&&search.rows[0]?.id==='offline-created';
  await predictions.reset(scope);const emptyAt=performance.now(),empty=await predictions.page(scope,plan,true),emptyMs=performance.now()-emptyAt;checks.canonical=JSON.stringify(empty.rows.slice(0,expectedCanonicalIDs.length).map(row=>row.id))===JSON.stringify(expectedCanonicalIDs);
  const resolved=await sqliteQueryEntityScopes(database,scope,plan),membershipSource=await assertSQLiteMembership(database,schema,scope,plan);const compiled=compileSQLiteOptimisticPage(schema,resolved.physical,plan,'trigram',false,scope,{scope:'\u0000prediction:'+scope,allowLocalInserts:true},resolved.sources,membershipSource);const explanation=(await database.getAllAsync<{detail:string}>('EXPLAIN QUERY PLAN '+compiled.sql,...compiled.bindings)).map(row=>row.detail);
  timings.sort((a,b)=>a-b);return {checks,explanation,iterations:timings.length,p50:timings[Math.floor(timings.length*.5)],p95:timings[Math.floor(timings.length*.95)],deepMs,searchMs,emptyMs,total:deep.total,deepFirstID:deep.rows[0]?.id,canonicalSortValues:'Mostly absent value fields, plus unsafe-integer control: ASC NULLS LAST and binary ID ties',firstIDs:first!.rows.slice(0,4).map(row=>row.id),searchIDs:search.rows.map(row=>row.id),profile:host.takeProfile(),wasmBytes:host.memoryBytes()};
 },
 async delta({row,deleted,digest}:{row:{id:string;text:string;value:number};deleted:string;digest:string}){
  const {replicaRowsHashes}=await import('../../src/replica-integrity.js');const hashes=await replicaRowsHashes([row],'id');
  const windows=new SQLiteWindowStore(database,()=>engine.verifyOwner()),initial=await windows.header(scope,'native-sync-control');if(!initial)throw Error('Missing native collection');
  const input={scope,signature:'native-sync-control',generation:initial.generation,cursor:{epoch:'e',revision:30},upserts:[{id:row.id,hash:hashes[row.id]!}],deleted:[deleted],digest};
  const before=await engine.verifyOwner();let mutations=0;host.takeProfile();const at=performance.now();
  const apply=()=>windows.applyDelta(input,async()=>{mutations++;await canonical.applyTransaction({cursor:input.cursor,changes:[{entity:'rows',id:row.id,operation:'insert',newValue:row},{entity:'rows',id:deleted,operation:'delete'}]},{entities:{},liveQueries:{}},scope);});
  const first=await apply(),ms=performance.now()-at,replay=await apply(),after=await engine.verifyOwner();
  const current=await windows.header(scope,input.signature),values=await canonical.loadEntityRows(scope,'rows',[row.id,deleted]),members=await windows.page(scope,input.signature,initial.generation,99998,2),profile=host.takeProfile();
  if(first.rows!==100000||replay.changed||mutations!==1||after.sequence!==before.sequence+1||current?.header.aggregateDigest!==digest||values.length!==1||values[0].id!==row.id||members.ids.at(-1)!==row.id)throw Error('Native delta composition failed');
  return {first,replay,mutations,ms,after,before,digest:current.header.aggregateDigest,tail:members.ids,profile,wasmBytes:host.memoryBytes(),integrity:await engine.integrity()};
 },
 async prepareFault(){await database.execAsync('CREATE TABLE IF NOT EXISTS gv_fault_probe(id INTEGER PRIMARY KEY,body BLOB NOT NULL)');return true;},
 async dirtyFault(){
  const write=(id:string)=>({table:'rows',id,version:1,row:{id,text:'Synthetic dirty transaction'}});
  await database.withTransactionAsync(async()=>{
   await engine.apply('fault-scope',[write('dirty-first')],{id:'fault-command',payload:{v:1}});
   for(let n=0;n<16;n++)await database.runAsync('INSERT INTO gv_fault_probe VALUES(?,zeroblob(524288))',n);
   await engine.apply('fault-scope',[write('dirty-second')]);
   globalThis.postMessage({faultReady:true});await new Promise(()=>{});
  });return false;
 },
 async auditFault(){return {probeRows:(await database.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_fault_probe'))!.n,rows:(await engine.page('fault-scope',{table:'rows',order:[{field:'id',direction:'asc'}],textOrder:'binary',limit:10})).rows,receipt:!!await database.getFirstAsync('SELECT 1 FROM gv_intent_receipts WHERE scope=? AND id=?','fault-scope','fault-command'),outbox:!!await database.getFirstAsync('SELECT 1 FROM gv_outbox WHERE scope=? AND id=?','fault-scope','fault-command'),snapshot:await new SQLiteWindowStore(database,()=>engine.verifyOwner()).header(scope,'native-sync-control'),integrity:await engine.integrity()};},
 async retryFault(){const writes=[{table:'rows',id:'dirty-first',version:1,row:{id:'dirty-first',text:'Synthetic dirty transaction'}}],intent={id:'fault-command',payload:{v:1}};const first=await engine.apply('fault-scope',writes,intent),second=await engine.apply('fault-scope',writes,intent);return {first,second,audit:await handlers.auditFault()};},
 async heldCommit(){const result=await engine.apply('held-scope',[{table:'rows',id:'held-once',version:1,row:{id:'held-once',text:'Synthetic committed once',value:1}}],{id:'held-command',payload:{v:1}});globalThis.postMessage({committedReady:result});await new Promise(()=>{});return false;},
 async recoverHeld(){const before=await engine.verifyOwner(),receipt=await engine.apply('held-scope',[{table:'rows',id:'held-once',version:1,row:{id:'held-once',text:'Synthetic committed once',value:1}}],{id:'held-command',payload:{v:1}}),after=await engine.verifyOwner();return {before,receipt,after,rows:(await engine.page('held-scope',{table:'rows',order:[{field:'id',direction:'asc'}],textOrder:'binary',limit:10})).rows,receipts:await database.getFirstAsync('SELECT count(*) n FROM gv_intent_receipts WHERE scope=?','held-scope'),outbox:await database.getFirstAsync('SELECT count(*) n FROM gv_outbox WHERE scope=?','held-scope')};},
 async verifyFTS(){const name='"gv_t_0072006f00770073_fts"';await database.runAsync('INSERT INTO '+name+'('+name+',rank) VALUES(?,1)','integrity-check');return true;},
 async check(){return {integrity:await engine.integrity(),completed,queue:ingress?.stats()??null,generation:await engine.verifyOwner()};},
 async close(){completed=true;ingress?.close();socket?.close();host.close();return true;},
};
globalThis.onmessage=({data:{id,op,args}})=>{void runOwner(async()=>{
 try{globalThis.postMessage({id,result:await handlers[op](args)});}catch(error){globalThis.postMessage({id,error:String((error as Error).stack??error)});}
});};
