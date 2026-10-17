import { expect, it, vi } from 'vitest';
import { readSQLiteLiveWindow, type SQLiteLiveReadDatabase, type SQLiteLiveReadSnapshot } from './sqlite-live-reader';
import type { LiveQueryPlan } from './query-expression';

const plan: LiveQueryPlan = { table:'tasks',key:'_id',
  sort:{allowedColumns:['id'],defaultColumn:'id',defaultDirection:'asc'},
  window:{offsetArgument:'offset',limitArgument:'limit',defaultLimit:2,maxLimit:250,count:'exact'} };
const budget={maxRows:250,maxTextBytes:1024};
const summary=[0,null,100,0,0];
const item=(id:string)=>[1,JSON.stringify({_id:id,id:1}),null,null,null];
function fixture(records:unknown[][],coverage:'complete'|'partial'='complete',beforeRow?:(index:number)=>void){
  let finalized=0;
  const snapshot:SQLiteLiveReadSnapshot={coverage:vi.fn(async()=>coverage),
    async *rows(){try{for(const[index,row]of records.entries()){beforeRow?.(index);yield row;}}finally{finalized++;}}};
  const database:SQLiteLiveReadDatabase={readSnapshot:vi.fn(async(scope,run)=>{expect(scope).toBe('scope');return run(snapshot);})};
  return {database,snapshot,finalized:()=>finalized};
}
it('reads count, metadata and only the requested row window in one snapshot',async()=>{
 const f=fixture([summary,item('a'),item('b')]);
 expect(await readSQLiteLiveWindow(f.database,plan,{},'scope',{},budget)).toEqual({rows:[{_id:'a',id:1},{_id:'b',id:1}],total:100,offset:0,limit:2,supported:true,completeness:'complete'});
 expect(f.database.readSnapshot).toHaveBeenCalledOnce();expect(f.snapshot.coverage).toHaveBeenCalledOnce();expect(f.finalized()).toBe(1);
});
it.each(['partial','complete'] as const)('preserves %s coverage and does not invent completeness',async coverage=>{
 const f=fixture([[0,null,0,0,1]],coverage);
 expect((await readSQLiteLiveWindow(f.database,plan,{},'scope',{},budget)).completeness).toBe('partial');
});
it('does not invent a count for a countless plan',async()=>{
 const f=fixture([[0,null,null,0,0]]);
 expect(await readSQLiteLiveWindow(f.database,{...plan,window:{...plan.window!,count:undefined}},{},'scope',{},budget)).not.toHaveProperty('total');
});
it.each([['queryTextBudget',{maxRows:250,maxTextBytes:1},[summary,item('a')]],['missingSortField',budget,[[0,null,100,1,0],item('a')]]] as const)('closes the cursor and returns no misleading partial rows on %s',async(reason,limits,records)=>{
 const f=fixture(records.map(r=>[...r]));
 expect(await readSQLiteLiveWindow(f.database,plan,{},'scope',{},limits)).toEqual({rows:[],supported:false,completeness:'partial',unsupportedOperator:reason});
 expect(f.finalized()).toBe(1);
});
it('refuses an over-budget window before opening a transaction',async()=>{
 const f=fixture([]);expect((await readSQLiteLiveWindow(f.database,plan,{limit:251},'scope',{}, {maxRows:249,maxTextBytes:1024})).unsupportedOperator).toBe('queryRowBudget');
 expect(f.database.readSnapshot).not.toHaveBeenCalled();
});
it.each([[],[[1,'{}',null,null,null]],[[0,null,-1,0,0]],[[0,null,0,2,0]],[summary,[1,'[]',null,null,null]],[summary,[1,'null',null,null,null]],[summary,item('a'),item('b'),item('c')]].map(records=>({records})))('rejects malformed or overlong native results %#',async({records})=>{
 const f=fixture(records as unknown[][]);await expect(readSQLiteLiveWindow(f.database,plan,{},'scope',{},budget)).rejects.toThrow();expect(f.finalized()).toBe(1);
});
it('finalizes an in-flight cursor when its consumer is cancelled',async()=>{
 const controller=new AbortController();const reason=new Error('retired view');
 const f=fixture([summary,item('a')],'complete',index=>{if(index===1)controller.abort(reason);});
 await expect(readSQLiteLiveWindow(f.database,plan,{},'scope',{},budget,controller.signal)).rejects.toBe(reason);expect(f.finalized()).toBe(1);
});
it('does not open a transaction for an already retired view',async()=>{
 const f=fixture([]),controller=new AbortController();controller.abort();
 await expect(readSQLiteLiveWindow(f.database,plan,{},'scope',{},budget,controller.signal)).rejects.toThrow();expect(f.database.readSnapshot).not.toHaveBeenCalled();
});
it.each([0,-1,Infinity,NaN,1.5])('rejects an invalid bridge byte budget %s',async maxTextBytes=>{
 const f=fixture([]);await expect(readSQLiteLiveWindow(f.database,plan,{},'scope',{}, {...budget,maxTextBytes})).rejects.toThrow(RangeError);
});

it('rejects retirement during transaction cleanup before publishing a result',async()=>{
 const controller=new AbortController(),reason=new Error('scope left');
 const f=fixture([summary,item('a')]);
 const database:SQLiteLiveReadDatabase={async readSnapshot(scope,run){const result=await f.database.readSnapshot(scope,run);controller.abort(reason);return result;}};
 await expect(readSQLiteLiveWindow(database,plan,{},'scope',{},budget,controller.signal)).rejects.toBe(reason);
 expect(f.finalized()).toBe(1);
});
