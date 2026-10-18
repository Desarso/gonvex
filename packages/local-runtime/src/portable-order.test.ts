import { expect,it } from 'vitest';
import { orderDataRows } from './portable.js';
import { orderedDataRead } from '@gonvex/module-sdk';
it('treats null and missing sort values as one null class before the identity tie-break',()=>{
  for(const direction of ['asc','desc'] as const)for(const nulls of ['first','last'] as const){
    const read=orderedDataRead({table:'tasks',orderBy:[{column:'title',direction,nulls}]});
    const rows=[{_id:'z',title:null},{_id:'a'},{_id:'m',title:'middle'}];
    orderDataRows(rows,read);
    expect(rows.map(row=>row._id)).toEqual(nulls==='first'?['a','z','m']:['m','a','z']);
    orderDataRows(rows,read);
    expect(rows.map(row=>row._id)).toEqual(nulls==='first'?['a','z','m']:['m','a','z']);
  }
});
