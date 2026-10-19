import {expect,it,vi} from 'vitest';
import {SQLiteBrowserEntities} from './sqlite-browser-entities.js';
import {ByteLedger} from './byte-budget.js';
import type {SQLiteCanonicalGeneration} from './sqlite-canonical-dispatch.js';
import type {SQLiteNativeEntityResult} from './sqlite-native-replica-reader.js';
function fixture(){let generation={epoch:1,sequence:1},revision=1;const calls:Array<{entity:string;ids:string[];expected?:SQLiteCanonicalGeneration}>=[],errors:unknown[]=[],ledger=new ByteLedger(5242880);let handler:((entity:string,ids:string[],expected?:SQLiteCanonicalGeneration)=>Promise<SQLiteNativeEntityResult>)|undefined;
 const view=new SQLiteBrowserEntities(async(entity,ids,expected)=>{calls.push({entity,ids,expected});if(handler)return handler(entity,ids,expected);if(expected&&expected.sequence!==generation.sequence)throw Object.assign(Error('Changed'),{code:'PAGE_CHANGED'});return {generation:{...generation},rows:ids.map(id=>({id,value:{id,n:revision}}))};},ledger,error=>errors.push(error));
 return {view,calls,errors,ledger,set handler(value:typeof handler){handler=value;},change(){revision++;generation={...generation,sequence:generation.sequence+1};view.refresh();}};
}
it('retains only mounted entity IDs, publishes stable rows and frees the cache at last unsubscribe',async()=>{
 const f=fixture(),seen:unknown[]=[],stop=f.view.subscribe(()=>seen.push(f.view.entity('tasks','a')),{entity:'tasks',ids:['a']}),release=f.view.retain('tasks',['a']);await vi.waitFor(()=>expect(f.view.entity('tasks','a')).toEqual({id:'a',n:1}));const original=f.view.entity('tasks','a'),version=f.view.entityVersion('tasks','a');f.view.refresh();await vi.waitFor(()=>expect(f.calls.length).toBeGreaterThan(1));expect(f.view.entity('tasks','a')).toBe(original);expect(f.view.entityVersion('tasks','a')).toBe(version);f.change();await vi.waitFor(()=>expect(f.view.entity('tasks','a')?.n).toBe(2));release();expect(f.view.entity('tasks','a')).toBeDefined();stop();expect(f.view.entity('tasks','a')).toBeUndefined();expect(f.ledger.bytes).toBe(0);expect(f.errors).toEqual([]);f.view.close();
});
it('fences all mounted entity batches to one SQL generation and never publishes a mixed read',async()=>{
 const f=fixture(),releases=[f.view.retain('tasks',Array.from({length:128},(_,n)=>String(n))),f.view.retain('tasks',Array.from({length:128},(_,n)=>String(n+128)))];await vi.waitFor(()=>expect(f.view.entity('tasks','255')).toBeDefined());const before=f.view.entityVersion('tasks','0');let reads=0;f.handler=async(_entity,ids,expected)=>{reads++;if(reads===1){expect(expected).toBeUndefined();return {generation:{epoch:1,sequence:2},rows:ids.map(id=>({id,value:{id,n:2}}))};}expect(expected).toEqual({epoch:1,sequence:2});throw Object.assign(Error('Concurrent commit'),{code:'PAGE_CHANGED'});};f.view.refresh();await vi.waitFor(()=>expect(f.errors).toHaveLength(1));expect(f.view.entity('tasks','0')?.n).toBe(1);expect(f.view.entity('tasks','255')?.n).toBe(1);expect(f.view.entityVersion('tasks','0')).toBe(before);for(const release of releases)release();f.view.close();
});
it('clears old identity rows immediately, rejects late reads and handles teardown after close',async()=>{
 const f=fixture(),release=f.view.retain('tasks',['a']),stop=f.view.subscribe(()=>{},{entity:'tasks',ids:['a']});await vi.waitFor(()=>expect(f.view.entity('tasks','a')).toBeDefined());let finish!:(value:SQLiteNativeEntityResult)=>void;const barrier=new Promise<SQLiteNativeEntityResult>(resolve=>{finish=resolve;});let reads=0;f.handler=async(_entity,ids)=>{if(!reads++)return barrier;return {generation:{epoch:1,sequence:3},rows:ids.map(id=>({id,value:{id,n:3}}))};};f.view.refresh();await vi.waitFor(()=>expect(reads).toBe(1));f.view.invalidate(true);expect(f.view.entity('tasks','a')).toBeUndefined();finish({generation:{epoch:1,sequence:2},rows:[{id:'a',value:{id:'a',n:999}}]});await vi.waitFor(()=>expect(f.view.entity('tasks','a')?.n).toBe(3));f.view.close();release();stop();expect(f.ledger.bytes).toBe(0);
});
it('retains a 250-row grid in one view while every SQL RPC remains at most 128 IDs',async()=>{
 const f=fixture(),ids=Array.from({length:250},(_,n)=>String(n));
 const release=f.view.retain('tasks',ids),stop=f.view.subscribe(()=>{},{entity:'tasks',ids});
 await vi.waitFor(()=>expect(f.view.entity('tasks','249')).toEqual({id:'249',n:1}));
 expect(f.calls.every(call=>call.ids.length<=128)).toBe(true);expect(f.view.counters().replicaRows).toBe(250);
 release();stop();expect(f.ledger.bytes).toBe(0);expect(f.errors).toEqual([]);f.view.close();
});

it('retries unresolved reads on staging progress without rereading certified mounted values',async()=>{
 const f=fixture(),release=f.view.retain('tasks',['a']);await vi.waitFor(()=>expect(f.view.entity('tasks','a')).toBeDefined());
 const original=f.view.entity('tasks','a');f.calls.length=0;
 f.view.refresh(false);await new Promise(resolve=>setTimeout(resolve,0));expect(f.calls).toEqual([]);
 let blocked=true;f.handler=async(_entity,ids)=>{if(blocked)throw Object.assign(Error('Generation changed'),{code:'PAGE_CHANGED'});return {generation:{epoch:1,sequence:2},rows:ids.map(id=>({id,value:{id,n:2}}))};};
 const releaseNew=f.view.retain('tasks',['b']);await vi.waitFor(()=>expect(f.errors).toHaveLength(1));f.calls.length=0;blocked=false;
 f.view.refresh(false);await vi.waitFor(()=>expect(f.view.entity('tasks','b')).toEqual({id:'b',n:2}));
 expect(f.calls).toEqual([{entity:'tasks',ids:['b'],expected:undefined}]);expect(f.view.entity('tasks','a')).toBe(original);
 f.view.refresh();await vi.waitFor(()=>expect(f.view.entity('tasks','a')?.n).toBe(2));release();releaseNew();f.view.close();
});

it('does not lose a mounted read when an empty refresh has settled before its cleanup microtask',async()=>{
 const f=fixture();f.view.refresh(false);
 const release=f.view.retain('tasks',['a']);
 await new Promise(resolve=>setTimeout(resolve,0));
 expect(f.view.entity('tasks','a')).toEqual({id:'a',n:1});
 expect(f.calls).toHaveLength(1);expect(f.errors).toEqual([]);release();f.view.close();
});
