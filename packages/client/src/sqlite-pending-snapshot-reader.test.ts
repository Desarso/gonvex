import {afterEach,expect,it,vi} from 'vitest';
import {initializePinnedSQLite} from '../test/sqlite-wasm-host.mjs';
import {officialSQLiteDatabase} from './sqlite-official-host.js';
import {SQLiteTypedReplicaEngine} from './sqlite-typed-engine.js';
import {sqliteOwnerTransactions} from './sqlite-owner-transactions.js';
import {sqliteTypedReplicaStorage} from './sqlite-typed-row-layout.js';
import {SQLiteSnapshotPages} from './sqlite-snapshot-pages.js';
import {SQLitePredictions} from './sqlite-predictions.js';
import {SQLitePendingSnapshotReader} from './sqlite-pending-snapshot-reader.js';
import {replicaRowsHashes,replicaSumDigest} from './replica-integrity.js';
const sqlite=await initializePinnedSQLite(),connections:any[]=[];
afterEach(()=>{vi.restoreAllMocks();for(const db of connections.splice(0))db.close();});
const schema={tasks:{key:'id',fields:{id:{kind:'text'},name:{kind:'text'},n:{kind:'number'},detail:{kind:'text'}},indexes:[]}} as const;
const stable=(value:Record<string,unknown>)=>JSON.stringify(Object.fromEntries(Object.keys(value).sort().map(key=>[key,value[key]])));
async function fixture(){
 const raw=new sqlite.oo1.DB(':memory:');connections.push(raw);const host=officialSQLiteDatabase(sqlite,raw),db=sqliteOwnerTransactions(host.database),engine=new SQLiteTypedReplicaEngine(db,schema);await engine.open('pending');const canonical=sqliteTypedReplicaStorage(db,schema);await canonical.ready();
 const snapshots=new SQLiteSnapshotPages(db,()=>engine.verifyOwner(),schema),predictions=new SQLitePredictions(db,canonical,schema,()=>engine.verifyOwner());await snapshots.ready();await predictions.ready();
 const reader=new SQLitePendingSnapshotReader({database:db,canonical,snapshots,predictions,schema,fence:()=>engine.verifyOwner()});
 const metadata={type:'replica.snapshotEncodedPage',id:'collection',snapshotId:'first-sync',scope:'tenant',visibilityScope:'s',key:'id',cursor:{epoch:'e',revision:10}};
 const rows=[{id:'first',name:'Source title',n:10,detail:'Newly received detail café 東京'},{id:'deleted',name:'Deleted source row',n:10},{id:'last',name:'Last frame',n:10}];const digest=replicaSumDigest(await replicaRowsHashes(rows,'id'));
 await snapshots.prepareTyped({scope:'s',subscription:'collection',snapshot:'first-sync',entity:'tasks'});
 const append=(part:typeof rows,sequence:number,last=false)=>snapshots.appendEncoded('s',new TextEncoder().encode(JSON.stringify({...metadata,sequence,last,canonicalRows:part.map(stable),...(last?{digest}:{})})));
 await append(rows.slice(0,2),0);
 const input={scope:'s',entity:'tasks',subscription:'collection',snapshot:'first-sync'};
 const write=(revision:number,changes:any[])=>canonical.applyTransaction({cursor:{epoch:'e',revision},changes},{entities:{},liveQueries:{}},'s');
 return {host,db,engine,canonical,snapshots,predictions,reader,input,rows,append,write};
}
it('publishes received pages with newer canonical fields and intent-owned edits, while retaining newer deletions',async()=>{
 const f=await fixture();await f.write(20,[{entity:'tasks',id:'first',operation:'insert',newValue:{id:'first',name:'Authoritative title',n:20}},{entity:'tasks',id:'deleted',operation:'delete'}]);await f.predictions.apply('s',1,[{entity:'tasks',rowId:'first',op:'patch',fields:{n:99}}]);
 const before=await f.engine.verifyOwner(),first=await f.reader.page({...f.input,limit:2});expect(first).toMatchObject({source:'pending',complete:false,serverVerified:false,receivedRows:2,nextPosition:null});expect(first.rows).toEqual([{id:'first',position:0,value:{id:'first',name:'Authoritative title',n:99,detail:'Newly received detail café 東京'}},{id:'deleted',position:1,value:null}]);
 expect((await f.canonical.loadEntityRows('s','tasks',['first']))[0].row).toEqual({id:'first',name:'Authoritative title',n:20});expect(await f.engine.verifyOwner()).toEqual(before);
 await f.append(f.rows.slice(2),1,true);const last=await f.reader.page({...f.input,after:1,expected:first.generation});expect(last).toMatchObject({source:'pending',complete:false,serverVerified:true,receivedRows:3,nextPosition:null});expect(last.rows[0].value).toEqual(f.rows[2]);expect(last.generation.receivedPages).toBe(2);
 await f.write(30,[{entity:'tasks',id:'last',operation:'update',newValue:{id:'last',name:'Changed'}}]);await expect(f.reader.page({...f.input,after:1,expected:first.generation})).rejects.toMatchObject({code:'PAGE_CHANGED'});
});
it('fences wrong entity, canceled admission and changed prediction generations before publishing values',async()=>{
 const f=await fixture();await expect(f.reader.page({...f.input,entity:'other'})).rejects.toThrow('Invalid pending');await expect(f.reader.page(f.input,()=>false)).rejects.toMatchObject({code:'FENCED'});
 const page=await f.reader.page(f.input);await f.predictions.apply('s',1,[{entity:'tasks',rowId:'local',op:'insert',fields:{id:'local',name:'Local',n:0}}]);await expect(f.reader.page({...f.input,expected:page.generation})).rejects.toMatchObject({code:'PAGE_CHANGED'});
 let current=true;const original=f.snapshots.readPending.bind(f.snapshots);vi.spyOn(f.snapshots,'readPending').mockImplementation(async(...args)=>{const result=await original(...args);current=false;return result;});await expect(f.reader.page(f.input,()=>current)).rejects.toMatchObject({code:'FENCED'});
});
it('shrinks decoded display pages by bytes and preserves every source position for continuation',async()=>{
 const f=await fixture();await f.write(20,[{entity:'tasks',id:'first',operation:'insert',newValue:{id:'first',detail:'x'.repeat(40000)}},{entity:'tasks',id:'deleted',operation:'insert',newValue:{id:'deleted',detail:'y'.repeat(40000)}}]);
 const page=await f.reader.page({...f.input,limit:100});expect(page.rows.length).toBeGreaterThan(0);expect(page.rows).toHaveLength(1);expect(page.nextPosition).toBe(0);expect(page.receivedRows).toBe(2);expect(page.rows[0].value?.detail).toBe('x'.repeat(40000));expect(page.complete).toBe(false);const next=await f.reader.page({...f.input,after:page.nextPosition!,expected:page.generation});expect(next.rows).toHaveLength(1);expect(next.rows[0].id).toBe('deleted');
});

it('reuses a bounded immutable-prefix display page across staging commits but invalidates canonical and prediction changes',async()=>{
 const f=await fixture(),first=await f.reader.page({...f.input,limit:1}),read=vi.spyOn(f.snapshots,'readPending');first.rows[0].value!.name='caller mutation';
 await f.append(f.rows.slice(2),1,true);await f.db.runAsync('UPDATE gv_storage_meta SET sequence=sequence+1 WHERE id=1');const next=await f.reader.page({...f.input,limit:1,expected:first.generation});expect(read).not.toHaveBeenCalled();expect(next.rows[0].value?.name).toBe('Source title');expect(next.receivedRows).toBe(3);expect(next.serverVerified).toBe(true);expect(next.generation.receivedPages).toBe(2);expect(next.nextPosition).toBe(0);
 await f.write(20,[{entity:'tasks',id:'first',operation:'insert',newValue:{id:'first',name:'Changed canonical',n:20}}]);expect((await f.reader.page({...f.input,limit:1})).rows[0].value?.name).toBe('Changed canonical');expect(read).toHaveBeenCalledTimes(1);
 await f.predictions.apply('s',1,[{entity:'tasks',rowId:'first',op:'patch',fields:{n:99}}]);expect((await f.reader.page({...f.input,limit:1})).rows[0].value?.n).toBe(99);expect(read).toHaveBeenCalledTimes(2);for(const budget of f.reader.budgets()){expect(budget.bytes).toBeLessThanOrEqual(budget.maxBytes);expect(budget.entries).toBeLessThanOrEqual(2);}f.reader.clear();expect(f.reader.budgets()[0].bytes).toBe(0);f.reader.close();await expect(f.reader.page(f.input)).rejects.toMatchObject({code:'FENCED'});
});

it('does not retain a precommit display page after its enclosing ingest transaction rolls back',async()=>{
 const f=await fixture();
 await expect(f.db.withTransactionAsync(async()=>{
  await f.append(f.rows.slice(2),1,true);
  const preview=await f.reader.page({...f.input,limit:3});
  expect(preview.rows).toHaveLength(3);expect(preview.serverVerified).toBe(true);expect(preview.complete).toBe(false);
  throw Error('FULL sync failed');
 })).rejects.toThrow('FULL sync failed');
 const committed=await f.reader.page({...f.input,limit:3});
 expect(committed.rows.map(row=>row.id)).toEqual(['first','deleted']);
 expect(committed.serverVerified).toBe(false);expect(committed.receivedRows).toBe(2);
});
