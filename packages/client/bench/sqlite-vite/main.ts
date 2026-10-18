import {SQLiteReplicaBroker} from '../../src/sqlite-broker.js';
const state=window as any;state.errors=[];state.changes=[];state.states=[];
addEventListener('error',event=>state.errors.push(event.message));addEventListener('unhandledrejection',event=>state.errors.push(String(event.reason)));
const schema={records:{key:'_id',fields:{_id:{kind:'text'},name:{kind:'text'},rank:{kind:'number'},group:{kind:'text'}},indexes:[{columns:['rank']}],search:['name'],counts:[{columns:['group']}]}} as const;
state.broker=new SQLiteReplicaBroker({schema,createWorker:()=>new Worker(new URL('./owner.ts',import.meta.url),{type:'module'}),createHub:()=>new SharedWorker(new URL('./hub.ts',import.meta.url),{type:'module',name:'gonvex-sqlite-router-v1'}),onState(value){state.states.push({at:Date.now(),state:value});if(state.states.length>100)state.states.shift();document.querySelector('#state')!.textContent=value;},onChange(scope,sequence,reset){state.changes.push({scope,sequence,reset});if(state.changes.length>100)state.changes.shift();}});
state.rpc=(op:string,args={})=>state.broker.request(op,args);
state.write=(id:string)=>state.rpc('apply',{scope:'proof',writes:[{table:'records',id,version:1,row:{_id:id,name:'Unicode café 東京 '+id,rank:1,group:'all'}}],intent:{id,payload:{create:id}}});
state.count=()=>state.rpc('count',{scope:'proof',entity:'records',group:0,values:['all']});
state.page=()=>state.rpc('page',{scope:'proof',plan:{table:'records',order:[{field:'rank',direction:'asc'}],limit:50}});
