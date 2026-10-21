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

it('retains one decoded row without a second JSON string and shares unchanged row identities',async()=>{
 const f=fixture(),payload={id:'large',text:'x'.repeat(16384),nested:{x:[1,true,null]}};
 f.handler=async()=>({generation:{epoch:1,sequence:1},rows:[{id:'large',value:structuredClone(payload)}]});
 const release=f.view.retain('tasks',['large']);await vi.waitFor(()=>expect(f.view.entity('tasks','large')).toEqual(payload));
 const row=f.view.entity('tasks','large'),before=f.ledger.bytes;
 expect(before).toBeLessThan(40000); // 32 KiB JSON/row estimate plus entry metadata, not a second 32 KiB string.
 f.view.refresh();await vi.waitFor(()=>expect(f.calls).toHaveLength(2));await new Promise<void>(resolve=>queueMicrotask(resolve));
 expect(f.view.entity('tasks','large')).toBe(row);await vi.waitFor(()=>expect(f.ledger.bytes).toBe(before));
 release();f.view.close();expect(f.ledger.bytes).toBe(0);
});

it('hydrates byte-bounded continuations under one generation before publishing a mounted window',async()=>{
 const ledger=new ByteLedger(),failures:unknown[]=[],calls:number[]=[];
 const view=new SQLiteBrowserEntities(async(_entity,ids,expected,offset=0)=>{calls.push(offset);if(offset)expect(expected).toEqual({epoch:1,sequence:2});const take=Math.min(3,ids.length-offset);return {generation:{epoch:1,sequence:2},offset,nextOffset:offset+take<ids.length?offset+take:null,rows:ids.slice(offset,offset+take).map(id=>({id,value:{id,name:'wide'.repeat(500)}}))};},ledger,error=>failures.push(error));
 const ids=Array.from({length:17},(_,n)=>String(n)),release=view.retain('tasks',ids);
 await vi.waitFor(()=>expect(view.entity('tasks','16')).toBeDefined());expect(calls).toEqual([0,3,6,9,12,15]);expect(failures).toEqual([]);release();view.close();expect(ledger.bytes).toBe(0);
});

it('serves a known row during a stuck background refresh and retries its owned deadline',async()=>{
 const f=fixture();const release=f.view.retain('tasks',['a']);await vi.waitFor(()=>expect(f.view.entity('tasks','a')).toEqual({id:'a',n:1}));
 // Use a short clock for this deadline test, not a production timeout change.
 (f.view as any).readTimeoutMs=10;let reads=0;
 f.handler=async(_entity,ids)=>{if(++reads===1)return new Promise(()=>{});return {generation:{epoch:1,sequence:2},rows:ids.map(id=>({id,value:{id,n:2}}))};};
 f.view.refresh();expect(f.view.entity('tasks','a')).toEqual({id:'a',n:1});
 await vi.waitFor(()=>expect(f.view.entity('tasks','a')).toEqual({id:'a',n:2}));
 await vi.waitFor(()=>expect((f.view as any).refreshing).toBeUndefined());expect(f.errors).toEqual([]);release();f.view.close();
});

it('coalesces dialog mounts into one entity read and reuses known rows for later consumers',async()=>{
 const f=fixture(),releases=[];
 for(const id of ['task','category','template','status','priority'])releases.push(f.view.retain('tasks',[id]));
 releases.push(f.view.subscribe(()=>{},{entity:'tasks',ids:['task']}));
 await vi.waitFor(()=>expect(f.view.entity('tasks','priority')).toBeDefined());
 expect(f.calls).toHaveLength(1);expect(f.calls[0].ids).toEqual(['task','category','template','status','priority']);
 releases.push(f.view.retain('tasks',['task']),f.view.subscribe(()=>{},{entity:'tasks',ids:['task']}));
 await new Promise(resolve=>setTimeout(resolve,0));expect(f.calls).toHaveLength(1);
 for(const release of releases)release();f.view.close();
});
