import {installSQLiteOwner} from '../../src/sqlite-owner-actor.js';
import {SQLiteReducerOwner} from '../../src/sqlite-reducer-owner.js';
import {PortableReducerRuntime} from '@gonvex/local-runtime/portable';
const schema={records:{key:'_id',fields:{_id:{kind:'text'},name:{kind:'text'},rank:{kind:'number'},group:{kind:'text'}},indexes:[{columns:['rank']}],search:['name'],counts:[{columns:['group']}]}} as const;
// Synthetic authenticated context for a storage/reducer component control.
// This fixture does not implement the real client's auth/WS session manager.
installSQLiteOwner({schema,createProgram(context){
 const pending=new Set<{timer:ReturnType<typeof setTimeout>;resolve(value:unknown):void}>();
 const seeded=context.runOwner(0,()=>context.canonical.applyTransaction({cursor:{epoch:'proof',revision:1},changes:[{entity:'records',id:'portable-row',operation:'insert',newValue:{_id:'portable-row',name:'Synthetic portable reducer',rank:1,group:'portable'}}]},{entities:{},liveQueries:{}},'portable-proof').then(()=>{}));void seeded.catch(()=>{});
 const runtime=new PortableReducerRuntime({schema:{records:{key:'_id',columns:{_id:{type:'text',nullable:false},name:{type:'text',nullable:true},rank:{type:'bigint',nullable:false},group:{type:'text',nullable:true}}}},artifactHash:'proof',reducers:{'proof/increment':()=>import('./reducer.js').then(module=>module.increment)}});
 const reducers=new SQLiteReducerOwner(context,{runtime,artifactHash:'proof',resolveTab(){return {scope:'portable-proof',generation:'synthetic-session-one'};},async context(){return {intentScope:'durable-proof',identity:{auth:{account:{id:'synthetic-account'}},tenant:{id:'synthetic-tenant'},member:{id:'synthetic-member',accountId:'synthetic-account',permissions:{}}},coverage:{records:{key:'_id',complete:true}}};}});
 return {async call(args:any,caller){
  if(args?.path==='proof/increment'){await seeded;return reducers.invoke(args,caller);}
  if(args?.method!=='delay'||!Number.isSafeInteger(args.ms)||args.ms<0||args.ms>40000)throw Error('Unsupported proof program method');
  return new Promise(resolve=>{const entry={timer:setTimeout(()=>{pending.delete(entry);resolve({completed:true});},args.ms),resolve};pending.add(entry);});
 },close(){for(const entry of pending){clearTimeout(entry.timer);entry.resolve({closed:true});}pending.clear();}};
}});
