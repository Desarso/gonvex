import type {SQLiteNativeWindowRead,SQLiteNativeWindowResult,SQLiteNativeEntityRead,SQLiteNativeEntityResult} from './sqlite-native-replica-reader.js';
import type {LocalReplicaStorage,ReplicaSnapshot} from './local-replica.js';
import type {SQLiteCanonicalGeneration,SQLiteCanonicalReply} from './sqlite-canonical-dispatch.js';
import {sqlitePayloadBytes} from './sqlite-admission.js';
export type SQLiteCanonicalTransport={
  request<T=unknown>(op:string,args:unknown):Promise<T>;
  subscribeChanges(listener:(scope:string,sequence:number)=>void):()=>void;
};
const empty={entities:{},liveQueries:{}};
const cursorOnly=(snapshot:ReplicaSnapshot)=>({...empty,...(snapshot.cursor?{cursor:snapshot.cursor}:{})});

/** Bounded LocalReplica adapter for the elected worker. No SQLite or generated
 * schema loads in a follower. Multi-read callbacks hold a version-fenced view;
 * a commit/owner change rejects the whole read rather than mixing snapshots. */
export type SQLiteBoundedReplicaStorage=LocalReplicaStorage&{
 readNativeWindowPage(input:SQLiteNativeWindowRead):Promise<SQLiteNativeWindowResult|undefined>;
 readNativeEntityPage(input:SQLiteNativeEntityRead):Promise<SQLiteNativeEntityResult>;
 dispose():void;
};
export function brokerSQLiteStorage(transport:SQLiteCanonicalTransport):SQLiteBoundedReplicaStorage{
  let disposed=false;
  const subscriptions=new Set<()=>void>();
  const request=<T>(op:string,args:unknown):Promise<T>=>{
    if(disposed)return Promise.reject(Error('SQLite replica adapter is closed'));
    sqlitePayloadBytes(args,262144);return transport.request<T>(op,args);
  };
  const call=async<T>(method:string,args:unknown[],scope='default'):Promise<T>=>{
    const result=await request<SQLiteCanonicalReply>('canonicalCall',{scope,method,args:args.map(value=>value===undefined?null:value)});
    return (result.value===null?undefined:result.value) as T;
  };
  return {
    ownsWorkingSetRows:true,ownsHydrationRows:true,
    async readNativeWindowPage(input){return (await request<SQLiteNativeWindowResult|null>('nativeWindowPage',input))??undefined;},
    readNativeEntityPage:input=>request<SQLiteNativeEntityResult>('nativeEntityPage',input),
    load:(scope='default')=>call('loadWorkingSet',[scope,{maxRows:128,maxBytes:131072}],scope),
    loadWorkingSet:(scope,budget)=>call('loadWorkingSet',[scope,budget],scope),
    loadEntityRows:(scope,entity,ids)=>{
      if(ids.length>256)return Promise.reject(Object.assign(Error('Entity hydration exceeds bounded row allowance'),{code:'BUDGET'}));
      return call('loadEntityRows',[scope,entity,ids],scope);
    },
    loadWindowRows:(scope,signature)=>call('loadWindowRows',[scope,signature],scope),
    readViewPage:(scope,signature,plan,offset=0)=>call('readViewPage',[scope,signature,plan,offset],scope),
    readChanges:(scope,sequence,interest)=>call('readChanges',[scope,sequence,interest],scope),
    loadSession:scope=>call('loadSession',[scope],scope),
    saveSession:(scope,session)=>call('saveSession',[scope,session],scope),
    applyTransaction:(transaction,_snapshot,scope='default')=>call('applyTransaction',[transaction,empty,scope],scope),
    advanceWatermark:(windows,cursor,scope='default')=>call('advanceWatermark',[windows,cursor,scope],scope),
    replaceSnapshot:(snapshot,scope='default')=>call('replaceSnapshot',[snapshot,scope],scope),
    replaceWindow:(window,snapshot,scope='default',projection)=>{
      sqlitePayloadBytes(window,262144);
      const rows=projection??window.ids.flatMap(id=>{const row=snapshot.entities[window.entity]?.[id];return row?[row]:[];});
      return call('replaceWindow',[window,cursorOnly(snapshot),scope,rows],scope);
    },
    applyWindowDelta:(window,delta,snapshot,scope='default')=>call('applyWindowDelta',[window,delta,cursorOnly(snapshot),scope],scope),
    removeWindow:(signature,snapshot,scope='default')=>call('removeWindow',[signature,cursorOnly(snapshot),scope],scope),
    clear:(scope='default')=>call('clear',[scope],scope),
    subscribePeer(listener){
      if(disposed)throw Error('SQLite replica adapter is closed');
      const unsubscribe=transport.subscribeChanges(scope=>listener(scope));
      const release=()=>{subscriptions.delete(release);unsubscribe();};subscriptions.add(release);return release;
    },
    async withReadView(scope,coverage,run){
      const expected=await request<SQLiteCanonicalGeneration>('canonicalGeneration',{scope});
      let active=true,failure:{error:unknown}|undefined;
      const pending=new Set<Promise<unknown>>();
      try{
        const result=await run({select(read,excluded=[]){
          if(!active||disposed)return Promise.reject(Error('Canonical read view is released'));
          const operation=request<{generation:SQLiteCanonicalGeneration;value:{rows:Record<string,import('@gonvex/protocol').JsonValue>[];complete:boolean}}>('canonicalRead',{scope,coverage,read,excluded,expected}).then(reply=>reply.value);
          pending.add(operation);
          void operation.then(()=>{pending.delete(operation);},error=>{pending.delete(operation);failure={error};});
          return operation;
        }});
        active=false;await Promise.allSettled([...pending]);
        if(failure)throw failure.error;
        await request('canonicalGeneration',{scope,expected});return result;
      }finally{active=false;}
    },
    dispose(){if(disposed)return;disposed=true;for(const release of subscriptions)release();subscriptions.clear();},
  };
}
