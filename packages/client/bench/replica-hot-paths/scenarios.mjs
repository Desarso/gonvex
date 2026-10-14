export async function fixture(sdk, count) {
  const storage = {load:async()=>undefined,applyTransaction:async()=>{},replaceWindow:async()=>{},withReadView:async(_scope,_coverage,run)=>run({}),loadWindowRows:async()=>undefined};
  const replica = new sdk.LocalReplica(storage, {maxResidentRows:count,maxResidentBytes:1024*2**20});
  const tables = ['tasks','statuses','priorities','members','tags','taskUsers','customFieldValues', ...Array.from({length:13},(_,i)=>`table${i}`)];
  const entities = {};
  let remaining = count;
  for (let t=0;t<tables.length;t++) {
    const size = t===0 ? Math.floor(count*.6) : Math.floor(remaining/(tables.length-t));
    remaining-=size;
    const rows=Array.from({length:size},(_,i)=>Object.fromEntries([
      ['id',String(i)],['status',i%100===0?'open':'closed'],['rank',(i*7919)%100003],
      ...Array.from({length:t===0?47:7},(_,c)=>[`column${c}`,c%3===0?i+c:`value-${i}-${c}`]),
    ]));
    entities[tables[t]]=Object.fromEntries(rows.map(row=>[row.id,row]));
    await replica.materializeWindow({signature:tables[t],kind:'replica',entity:tables[t],key:'id',rows,completeness:'complete',source:'cache'});
  }
  for(let i=0;i<30;i++)await replica.materializeWindow({signature:`visible${i}`,entity:'tasks',key:'id',rows:Object.values(entities.tasks).slice(i*10,i*10+250),completeness:'partial',source:'cache'});
  let wakes=0;
  replica.subscribe(()=>{wakes++;});
  for(let i=0;i<50;i++)replica.subscribe(()=>{wakes++;},{window:i<20?tables[i]:`visible${i-20}`});
  let readVisible=false;
  let checksum=0;
  replica.subscribe(()=>{if(readVisible)checksum+=replica.liveQuerySnapshot('visible0').rows.length;},{window:'visible0'});
  let revision=0;
  const single=()=>replica.applyTransaction({cursor:{epoch:'bench',revision:++revision},changes:[{entity:'tasks',id:'0',operation:'update',newValue:{column0:revision}}]});
  const singleVisible=async()=>{readVisible=true;try{await single();}finally{readVisible=false;}};
  const burst=()=>replica.applyTransaction({cursor:{epoch:'bench',revision:++revision},changes:Array.from({length:1000},(_,i)=>({entity:'tasks',id:String(i),operation:'update',newValue:{column0:revision+i}}))});
  const runtime=new sdk.PortableReducerRuntime({artifactHash:'bench',schema:{tasks:{key:'id',columns:{id:{type:'text',nullable:false},column0:{type:'bigint',nullable:false}}}},reducers:{edit:sdk.reducer({args:sdk.schema.any(),result:sdk.schema.any(),run:async ctx=>{
    const row=await sdk.getRow(ctx.db,'tasks','id','0');
    await ctx.db.update('tasks','0',{column0:Number(row.column0)+1});return null;
  }})}});
  const execution={scope:'bench',commandId:'edit',now:1,artifactHash:'bench',identity:{auth:{},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}}};
  const execute=()=>replica.withReadView(()=>({tasks:{key:'id',complete:true}}),false,view=>runtime.execute('edit',{},view,execution));
  const optimistic=async()=>{
    const first=await execute();replica.applyOptimistic('edit',first.patches);
    await single();
    const replay=await execute();replica.replaceOptimistic([{commandId:'edit',patches:replay.patches}]);
    replica.rejectCommand('edit');
  };
  // Avoid network and session startup in this query-only benchmark. Invoke the
  // actual client methods on a replica-backed instance with the required fields.
  const client=Object.create(sdk.GonvexClient.prototype);
  Object.assign(client,{replica,offlineResults:new Map(),offlineResultsScope:0,offlineResultRows:0,outboxScopeGeneration:0});
  const ref={kind:'query',path:'tasks:grid',live:{entity:'tasks',key:'id',plan:{table:'tasks',key:'id',where:{operator:'eq',column:'status',value:{literal:'open'}},search:{argument:'search',columns:['column1','column2']},sort:{allowedColumns:['rank'],defaultColumn:'rank',defaultDirection:'asc'},window:{offsetArgument:'offset',limitArgument:'limit',defaultLimit:50,maxLimit:500,count:'exact'}}}};
  const args={};
  const unrelated=async()=>{
    await replica.applyTransaction({cursor:{epoch:'bench',revision:++revision},changes:[{entity:'tags',id:'0',operation:'update',newValue:{column0:revision}}]});
    return client.offlineLiveQuery(ref,args).rows.length;
  };
  const matching=async()=>{await single();return client.offlineLiveQuery(ref,args).rows.length;};
  const fullPlan={...ref.live.plan,where:undefined};
  const sorted=()=>sdk.runOfflineLiveQuery(replica.entityRows('tasks'),fullPlan,args,'complete').rows.length;
  const snapshots=()=>{checksum+=replica.entityRows('tasks').length;checksum+=replica.liveQuerySnapshot('visible0').rows.length;};
  const capture=()=>{const read=replica.captureExecutionRows(false);checksum+=Number(typeof read==='function');};
  const serialized=JSON.stringify({entities,liveQueries:Object.fromEntries(replica.listWindows().map(window=>[window.signature,window]))});
  // Both builds pay JSON decoding. Ownership transfer is an optional adapter
  // capability; the baseline ignores it and makes its historical second copy.
  const startup=async()=>{
    const storage={load:async()=>JSON.parse(serialized),loadWorkingSet:async()=>JSON.parse(serialized),ownsWorkingSetRows:true,applyTransaction:async()=>{}};
    const cold=new sdk.LocalReplica(storage,{maxResidentRows:count,maxResidentBytes:1024*2**20});
    await cold.activateScope('cold');checksum+=cold.entityRows('tasks').length;cold.dispose();
  };
  let indexedDB;
  const prepareStorage=async()=>{
    indexedDB=new sdk.IndexedDBLocalReplicaStorage(count===100000?'hotpaths-shared-100000':`hotpaths-${crypto.randomUUID()}`);
    await indexedDB.initialize();
    const database=indexedDB.database;
    if(count===100000 && (await database.meta.get(['cold','fixtureCount']))?.value===String(count))return;
    const snapshot=JSON.parse(serialized);
    // Fixture seeding is not timed. Use the adapter's normal record builder,
    // batching native requests without its production per-write IO throttle.
    for(const [entity,rows] of Object.entries(snapshot.entities).sort(([a],[b])=>a<b?-1:a>b?1:0)) {
      const entries=Object.entries(rows).sort(([a],[b])=>a<b?-1:a>b?1:0);
      for(let offset=0;offset<entries.length;offset+=128) {
        await database.entities.bulkPut(entries.slice(offset,offset+128).map(([id,row])=>indexedDB.versionedRecord('cold',entity,id,row,undefined,1,undefined)));
      }
    }
    await database.windows.bulkPut(Object.values(snapshot.liveQueries).map(window=>({scope:'cold',signature:window.signature,value:JSON.stringify(window),sequence:1})));
    await database.meta.bulkPut([{scope:'cold',key:'sequence',value:'1'},{scope:'cold',key:'resetSequence',value:'1'},{scope:'cold',key:'fixtureCount',value:String(count)}]);
  };
  const startupIndexedDB=async()=>{
    const cold=new sdk.LocalReplica(indexedDB,{maxResidentRows:count,maxResidentBytes:1024*2**20});
    await cold.activateScope('cold');checksum+=cold.entityRows('tasks').length;cold.dispose();
  };
  return {replica,prepareStorage,operations:{single,singleVisible,burst,optimistic,unrelated,matching,sorted,snapshots,capture,startup,startupIndexedDB},verify:()=>({wakes,checksum,rows:replica.entityRows('tasks').length})};
}

export async function measure(run, iterations, warmups, batchSize=1) {
  const batch=batchSize===1?run:async()=>{for(let i=0;i<batchSize;i++)await run();};
  for(let i=0;i<warmups;i++)await batch();
  const samples=[];
  for(let i=0;i<iterations;i++) {
    const start=performance.now();await batch();samples.push((performance.now()-start)/batchSize);
  }
  return samples;
}
