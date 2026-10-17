import { expect, it } from 'vitest';
import { compileSQLiteLiveQuery, sqliteLiveQueryCollation, sqliteLiveQueryFunctions } from './sqlite-query-expression';
import type { LiveQueryPlan } from './query-expression';

const plan: LiveQueryPlan = {
  table: 'tasks', key: '_id',
  where: { operator: 'eq', column: 'workspaceId', value: { argument: 'workspace' } },
  sort: { allowedColumns: ['id'], defaultColumn: 'id', defaultDirection: 'asc' },
  window: { offsetArgument: 'offset', limitArgument: 'limit', defaultLimit: 25, maxLimit: 250, count: 'exact' },
};

it.each([[0.1,0.2],[-1e30,1e30],[-1e12,1e12]])('preserves numeric comparison sign through the C int32 collation ABI for %s and %s', (a,b) => {
  expect(sqliteLiveQueryCollation(JSON.stringify(a),JSON.stringify(b)) | 0).toBeLessThan(0);
  expect(sqliteLiveQueryCollation(JSON.stringify(b),JSON.stringify(a)) | 0).toBeGreaterThan(0);
  expect(sqliteLiveQueryCollation(JSON.stringify(a),JSON.stringify(a)) | 0).toBe(0);
});

it('preserves missing, JSON null, boolean, numeric and string distinctions despite SQL affinity', () => {
  const f=sqliteLiveQueryFunctions;
  expect(f.gv_eq(null,'null')).toBe(0);
  expect(f.gv_eq(null,null)).toBe(1);
  expect(f.gv_eq('false','0')).toBe(0);
  expect(f.gv_eq('"1"','1')).toBe(0);
  expect(f.gv_eq('null','null')).toBe(1);
  expect(f.gv_array_contains('[0,false,null]','false')).toBe(1);
  expect(f.gv_array_contains('[0,false,null]','"false"')).toBe(0);
  expect(f.gv_order('null')).toBeNull();
  expect(f.gv_order('"null"')).toBe('"null"');
});

it('keeps routing, data and metadata values as bound parameters in independently executable statements', () => {
  const scope="scope' OR 1=1--",workspace="data');DROP TABLE replica_rows;--";
  const compiled=compileSQLiteLiveQuery({...plan,table:'a strange table',sort:{...plan.sort!,defaultColumn:'a".column',allowedColumns:['a".column']}},{workspace,offset:99,limit:5},scope);
  expect(compiled.supported).toBe(true);
  if(!compiled.supported)throw Error('Expected a compiled window');
  for(const statement of [compiled.rows,compiled.count,compiled.missingSortFields].filter(Boolean)){
    expect(statement!.sql).not.toContain(scope);
    expect(statement!.sql).not.toContain(workspace);
    expect(statement!.sql).not.toContain('a strange table');
    const indices=[...new Set([...statement!.sql.matchAll(/\?(\d+)/g)].map(match=>Number(match[1])))].sort((a,b)=>a-b);
    expect(indices).toEqual(Array.from({length:statement!.bindings.length},(_,i)=>i+1));
    expect(statement!.bindings).toContain(scope);
    expect(statement!.bindings).toContain(JSON.stringify(workspace));
  }
});

it('refuses missing identity rather than compiling an unscoped relation', () => {
  const compiled=compileSQLiteLiveQuery({...plan,where:{operator:'inRelation',column:'_id',relation:{table:'contexts',column:'taskId',where:{operator:'eq',column:'memberId',value:{context:'member.id'}}}}},{},'scope');
  expect(compiled).toEqual({supported:false,unsupportedOperator:'missingIdentityContext'});
});

it('does not turn an unwindowed export into a silently truncated page', () => {
  expect(compileSQLiteLiveQuery({...plan,window:undefined},{},'scope')).toEqual({supported:false,unsupportedOperator:'unwindowedPlan'});
});
