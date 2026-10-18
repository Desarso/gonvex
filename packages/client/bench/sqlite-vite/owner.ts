import {installSQLiteOwner} from '../../src/sqlite-owner-actor.js';
const schema={records:{key:'_id',fields:{_id:{kind:'text'},name:{kind:'text'},rank:{kind:'number'},group:{kind:'text'}},indexes:[{columns:['rank']}],search:['name'],counts:[{columns:['group']}]}} as const;
// Representative generated program: a long external wait must not retain the
// SQLite FIFO or make a healthy owner fail its progress watchdog.
installSQLiteOwner({schema,createProgram(){
 const pending=new Set<{timer:ReturnType<typeof setTimeout>;resolve(value:unknown):void}>();
 return {async call(args:any){if(args?.method!=='delay'||!Number.isSafeInteger(args.ms)||args.ms<0||args.ms>40000)throw Error('Unsupported proof program method');return new Promise(resolve=>{const entry={timer:setTimeout(()=>{pending.delete(entry);resolve({completed:true});},args.ms),resolve};pending.add(entry);});},close(){for(const entry of pending){clearTimeout(entry.timer);entry.resolve({closed:true});}pending.clear();}};
}});
