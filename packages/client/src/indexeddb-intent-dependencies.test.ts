import {expect,it} from 'vitest';
import {IDBFactory,IDBKeyRange} from 'fake-indexeddb';
import {IndexedDBReplicaEngine} from './indexeddb-replica-engine.js';
import {intentsConflict,type IntentDependencies,type ReducerOutboxEntry} from './outbox.js';
import type {SQLitePreparedReducer} from './sqlite-reducer-core.js';
const identity={auth:{account:{id:'account'}},tenant:{id:'tenant'},member:{id:'member',accountId:'account',permissions:{}}};
it('matches primary causal conflicts for row reads/writes, scans, legacy keys, cold barriers and parked intents',async()=>{
 const deps:IntentDependencies[]=[{reads:[{entity:'tasks',rowId:'a'}],writes:[],scans:[]},{reads:[],writes:[{entity:'tasks',rowId:'a'}],scans:[]},{reads:[],writes:[{entity:'tasks',rowId:'b'}],scans:[]},{reads:[],writes:[],scans:['tasks']},{reads:[],writes:[{entity:'labels',rowId:'a'}],scans:[]}],variants=[...deps.map(dependencies=>({dependencies,entityKeys:[]})),{entityKeys:['tasks:a']},{entityKeys:['__gonvex_local_intents']}];
 for(let a=0;a<variants.length;a++)for(let b=0;b<variants.length;b++){
  const engine=new IndexedDBReplicaEngine({},'dependencies',new IDBFactory(),IDBKeyRange);await engine.claim('owner');const append=async(command:string,variant:typeof variants[number])=>{const generation=await engine.generation(),input={scope:'visible',path:'test',commandId:command,args:{},now:100},id=generation.sequence+1,intent:ReducerOutboxEntry={id,scope:'identity',path:'test',args:{},idempotencyKey:command,createdAt:100,attempts:0,nextAttemptAt:100,state:'pending',patches:[],localExecution:{scope:'visible',commandId:command,now:100,artifactHash:'artifact',identity},...variant},prepared:SQLitePreparedReducer={intent,receipt:{intentId:id,commandId:command,result:null,originalGeneration:{epoch:generation.epoch,sequence:id}}};await engine.commit(input,prepared,'identity',generation);return intent;};
  try{const earlier=await append('earlier',variants[a]!),later=await append('later',variants[b]!),records=(await engine.deliveryPage('identity')).records,laterRecord=records[1]!,generation=await engine.generation();expect(await engine.canDeliver(laterRecord,generation),JSON.stringify({a,b})).toBe(!intentsConflict(earlier,later));await engine.metadataAtomic(generation,tx=>engine.updateDeliveryInTransaction(tx,'identity','earlier',records[0]!.hash,['pending'],{state:'failed'}));expect(await engine.canDeliver(laterRecord,generation)).toBe(true);await engine.metadataAtomic(generation,tx=>engine.updateDeliveryInTransaction(tx,'identity','earlier',records[0]!.hash,['failed'],{state:'pending',nextAttemptAt:9999}));expect(await engine.canDeliver(laterRecord,generation)).toBe(!intentsConflict(earlier,later));expect(await engine.generation()).toEqual(generation);
  }finally{engine.close();}
 }
});
