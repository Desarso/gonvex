import type {ReducerOutboxEntry} from './outbox.js';
import {sqlitePayloadBytes,fail} from './sqlite-admission.js';
const result=<T>(request:IDBRequest<T>)=>new Promise<T>((resolve,reject)=>{request.onsuccess=()=>resolve(request.result);request.onerror=()=>reject(request.error??Error('Fallback dependency request failed'));});
type Dependency={scope:string;command:string;sequence:number;kind:'r'|'w'|'s'|'l';entity:string;key:string};
export function indexedDBIntentDependencies(entry:ReducerOutboxEntry){
 const rows:Array<Omit<Dependency,'scope'|'command'|'sequence'>>=[];const add=(kind:Dependency['kind'],entity:string,key:string)=>rows.push({kind,entity,key});
 if(entry.dependencies){for(const row of entry.dependencies.reads)add('r',row.entity,row.entity+':'+row.rowId);for(const row of entry.dependencies.writes)add('w',row.entity,row.entity+':'+row.rowId);for(const entity of entry.dependencies.scans)add('s',entity,entity);}else for(const key of entry.entityKeys)add('l',key.split(':')[0]!,key);
 sqlitePayloadBytes(rows,262144);if(rows.length>4096||rows.some(row=>!row.entity||row.entity.length>256||!row.key||row.key.length>1280))fail('INTENT_CORRUPT','Invalid fallback intent dependencies');return [...new Map(rows.map(row=>[JSON.stringify([row.kind,row.entity,row.key]),row])).values()];
}
export async function writeIndexedDBIntentDependencies(tx:IDBTransaction,scope:string,command:string,sequence:number,entry:ReducerOutboxEntry,state:string,ranges:typeof IDBKeyRange,force=false){
 const states=tx.objectStore('intentStates'),deps=tx.objectStore('dependencies'),active=Number(state==='pending'||state==='inflight'),old=await result(states.get([scope,command])) as {active:number}|undefined;
 if(old?.active===active&&!force)return;await result(deps.delete(ranges.bound([scope,command],[scope,command,[]])));if(active)for(const row of indexedDBIntentDependencies(entry))await result(deps.put({scope,command,sequence,...row} satisfies Dependency));await result(states.put({scope,command,sequence,active,global:Number(entry.entityKeys.includes('__gonvex_local_intents'))}));
}
export async function indexedDBIntentIsReady(tx:IDBTransaction,ranges:typeof IDBKeyRange,scope:string,sequence:number,entry:ReducerOutboxEntry){
 const states=tx.objectStore('intentStates'),deps=tx.objectStore('dependencies');
 const earlier=async(index:IDBIndex,prefix:IDBValidKey[])=>await result(index.getKey(ranges.bound([...prefix,0],[...prefix,sequence],false,true)))!==undefined;
 if(await earlier(states.index('barrier'),[scope,1,1]))return false;if(entry.entityKeys.includes('__gonvex_local_intents')&&await earlier(states.index('sequence'),[scope,1]))return false;
 for(const row of indexedDBIntentDependencies(entry)){
  const kinds=row.kind==='r'?['w','l']:row.kind==='s'?[]:['r','w','l'];for(const kind of kinds)if(await earlier(deps.index('row'),[scope,row.entity,row.key,kind]))return false;
  if(row.kind==='w'||row.kind==='l'){if(await earlier(deps.index('entity'),[scope,row.entity,'s']))return false;}else if(row.kind==='s')for(const kind of ['w','l'])if(await earlier(deps.index('entity'),[scope,row.entity,kind]))return false;
 }return true;
}
