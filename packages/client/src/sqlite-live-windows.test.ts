import {DatabaseSync} from 'node:sqlite';
import {afterEach,expect,it} from 'vitest';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteLiveWindows} from './sqlite-live-windows.js';
import {SQLiteNativeReplicaReader} from './sqlite-native-replica-reader.js';
import type {SQLiteReplicaDatabase} from './sqlite-replica-storage.js';
import type {SQLiteOwnerProgramContext} from './sqlite-owner-actor.js';
import {decodedJSONBytes,sqlitePayloadBytes} from './sqlite-admission.js';
import type {SQLiteTypedSchema} from './sqlite-typed-schema.js';
import type {FunctionReference} from './index.js';
const cleanup:Array<()=>void>=[];afterEach(()=>{for(const close of cleanup.splice(0))close();});
const typed={tasks:{key:'id',fields:{id:{kind:'text'},n:{kind:'number'},detail:{kind:'text'}},indexes:[]}} as const;
const ref:FunctionReference={kind:'query',path:'tasks/grid',delivery:'live',live:{entity:'tasks',key:'id',resultPath:['page'],plan:{columns:['id','n']} as any}};
async function fixture(schema:SQLiteTypedSchema=typed,source:FunctionReference=ref){
 const sql=new DatabaseSync(':memory:');cleanup.push(()=>sql.close());const db=sqliteOwnerTransactions({async execAsync(query){sql.exec(query);},async runAsync(query,...args){return sql.prepare(query).run(...args as any[]);},async getFirstAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).get(...args as any[]) as T??null;},async getAllAsync<T>(query:string,...args:unknown[]){return sql.prepare(query).all(...args as any[]) as T[];},async withTransactionAsync(run){sql.exec('BEGIN');try{await run();sql.exec('COMMIT');}catch(error){sql.exec('ROLLBACK');throw error;}}} satisfies SQLiteReplicaDatabase),engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open('live');const canonical=sqliteTypedReplicaStorage(db,schema);await canonical.ready();const owner={database:db,engine,canonical} as SQLiteOwnerProgramContext,windows=new SQLiteLiveWindows(owner,'artifact'),reader=new SQLiteNativeReplicaReader(db,canonical,engine);await windows.ready();await reader.ready();
 const tx=async<T>(run:()=>Promise<T>)=>{let result!:T;await db.withTransactionAsync(async()=>{result=await run();});return result;};
 const stage=(sequence:number,rows:Array<Record<string,any>>=[{id:'a',n:sequence}])=>tx(()=>windows.stage('stream','visible','grid',source,{result:{page:rows,total:100000},subscriptionRevision:{epoch:'e',sequence},replicaScope:'visible'}));
 const publish=(sequence:number)=>tx(()=>windows.publish('stream','visible','e',sequence));return {sql,db,engine,canonical,windows,reader,tx,stage,publish};
}
it('keeps pending pages explicitly incomplete and publishes bounded memberships over normalized projected rows',async()=>{
 const f=await fixture();await f.canonical.applyTransaction({cursor:{epoch:'e',revision:1},changes:[{entity:'tasks',id:'a',operation:'insert',newValue:{id:'a',n:1,detail:'retained detail'}}]},{entities:{},liveQueries:{}},'visible');await f.stage(2);
 const pending=await f.tx(()=>f.windows.page('stream','visible','grid','e',{limit:10}));expect(pending).toMatchObject({complete:false,serverVerified:false,rows:[{id:'a',value:{id:'a',n:2}}],header:{resultSkeleton:{page:[],total:100000}}});expect(await f.publish(1)).toBe(0);expect(await f.publish(2)).toBe(1);expect(await f.tx(()=>f.windows.page('stream','visible','grid','e'))).toBeNull();
 const page=await f.reader.window({scope:'visible',signature:'grid',limit:10});expect(page?.rows).toEqual([{id:'a',value:{id:'a',n:2}}]);expect((await f.reader.entities({scope:'visible',entity:'tasks',ids:['a']})).rows[0]?.value?.detail).toBe('retained detail');expect(page?.window.header.entityCoverage).toBeUndefined();expect(await f.stage(2)).toBe(false);await expect(f.stage(2,[{id:'a',n:3}])).rejects.toMatchObject({code:'INTENT_CONFLICT'});
});
it('rolls canonical rows, membership and stage retirement back together on a late publication failure',async()=>{
 const f=await fixture();await f.stage(2);await f.db.execAsync("CREATE TRIGGER fail_live BEFORE INSERT ON gv_window_members BEGIN SELECT RAISE(ABORT,'live publication rollback');END;");await expect(f.publish(2)).rejects.toThrow('live publication rollback');expect(await f.canonical.loadEntityRows('visible','tasks',['a'])).toEqual([]);expect(await f.reader.window({scope:'visible',signature:'grid',limit:10})).toBeUndefined();expect(await f.tx(()=>f.windows.pending('stream','visible','e',2))).toBe(true);
 await f.db.execAsync('DROP TRIGGER fail_live');expect(await f.publish(2)).toBe(1);expect((await f.reader.window({scope:'visible',signature:'grid',limit:10}))?.rows[0]?.value?.n).toBe(2);expect(await f.engine.integrity()).toBe('ok');
});
it('bounds pending continuations, rejects undeclared columns and fences retirement/replacement',async()=>{
 const f=await fixture();await expect(f.stage(1,[{id:'a',n:1,detail:'not projected'}])).rejects.toThrow('undeclared');await expect(f.stage(1,Array.from({length:257},(_,n)=>({id:String(n),n})))).rejects.toThrow('256');await f.stage(2,[{id:'a',n:2},{id:'b',n:2}]);const first=await f.tx(()=>f.windows.page('stream','visible','grid','e',{limit:1}));expect(first?.nextOffset).toBe(1);await expect(f.tx(()=>f.windows.page('stream','visible','grid','e',{offset:1,limit:1}))).rejects.toThrow('continuation');
 await f.stage(3);await expect(f.tx(()=>f.windows.page('stream','visible','grid','e',{generation:first!.generation}))).rejects.toMatchObject({code:'PAGE_CHANGED'});await f.tx(()=>f.windows.retire('stream','grid'));expect(await f.publish(3)).toBe(0);expect(await f.tx(()=>f.windows.page('stream','visible','grid','e'))).toBeNull();
});
it('keeps repeated generations and long Unicode identities bounded across publication',async()=>{
 const f=await fixture(),ids=Array.from({length:24},(_,n)=>String(n)+'東京"'.repeat(250));for(let sequence=1;sequence<=6;sequence++){await f.stage(sequence,ids.map(id=>({id,n:sequence})));expect(await f.publish(sequence)).toBe(1);}expect((await f.reader.window({scope:'visible',signature:'grid',limit:128}))?.rows.length).toBeGreaterThan(0);expect((await f.db.getFirstAsync<{n:number}>('SELECT count(*) n FROM gv_window_generations'))?.n).toBeLessThanOrEqual(2);expect(await f.engine.integrity()).toBe('ok');
});

it('refuses corrupted durable frames for early display and canonical publication',async()=>{
 const f=await fixture();await f.stage(2);await f.db.runAsync('UPDATE gv_live_window_stages SET rows=?',JSON.stringify([{id:'a',n:999}]));await expect(f.tx(()=>f.windows.page('stream','visible','grid','e'))).rejects.toMatchObject({code:'INTENT_CORRUPT'});await expect(f.publish(2)).rejects.toMatchObject({code:'INTENT_CORRUPT'});expect(await f.canonical.loadEntityRows('visible','tasks',['a'])).toEqual([]);await expect(f.stage(3,[{id:'a',n:1},{id:'a',n:2}])).rejects.toMatchObject({code:'INTENT_CONFLICT'});
});

it('publishes wide bounded frames whose conservative decode charge exceeds their wire charge',async()=>{
 const columns=Array.from({length:52},(_,n)=>'column_'+n),schema:SQLiteTypedSchema={tasks:{key:'id',fields:{id:{kind:'text'},...Object.fromEntries(columns.map(name=>[name,{kind:'text' as const}]))},indexes:[]}},source:FunctionReference={...ref,live:{...ref.live!,plan:{columns:['id',...columns]} as any}};
 const rows=Array.from({length:16},(_,n)=>({id:String(n),...Object.fromEntries(columns.map(name=>[name,null]))}));
 expect(sqlitePayloadBytes(rows,131072)).toBeLessThan(131072);expect(()=>decodedJSONBytes(JSON.stringify(rows),131072)).toThrow('Decoded SQLite page');
 const f=await fixture(schema,source);await f.stage(2,rows);const pending=await f.tx(()=>f.windows.page('stream','visible','grid','e',{limit:4}));expect(pending?.rows).toHaveLength(4);expect(await f.publish(2)).toBe(1);
 expect((await f.reader.window({scope:'visible',signature:'grid',limit:4}))?.rows.map(row=>row.value)).toEqual(rows.slice(0,4));expect(await f.engine.integrity()).toBe('ok');
 await expect(f.stage(3,[{...rows[0],column_0:'x'.repeat(70000)}])).rejects.toMatchObject({code:'BUDGET'});
});
