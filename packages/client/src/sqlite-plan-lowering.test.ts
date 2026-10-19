import {expect,it} from 'vitest';
import {lowerSQLiteLivePlan} from './sqlite-plan-lowering.js';
const schema={tasks:{key:'id',fields:{id:{kind:'text' as const},n:{kind:'number' as const},recipients:{kind:'json' as const}},indexes:[]}};
const source:any={table:'tasks',key:'id',columns:['id','n','recipients'],where:{operator:'or',children:[{operator:'arrayContains',column:'recipients',value:{context:'member.id'}},{operator:'eq',column:'recipients',value:{literal:[]}}]},sort:{defaultColumn:'n',defaultDirection:'asc',allowedColumns:['n']},window:{limitArgument:'limit',offsetArgument:'offset',defaultLimit:50,maxLimit:250},search:{argument:'search',columns:[],sources:[{table:'search',key:'id',column:'text'}]},filters:{argument:'filters',allowedColumns:['n'],allowedOperators:['empty','notEmpty']}};
it('lowers generated recipient arrays and empty filters without evaluating unused search sources',()=>{
 const result=lowerSQLiteLivePlan(schema,source,{filters:[{column:'n',operator:'empty'}]},{'member.id':'member'},{textOrder:'binary'});
 expect(result.supported).toBe(true);if(!result.supported)throw Error(result.reason);
 expect(result.plan.where).toMatchObject({op:'and',children:[{op:'or',children:[{op:'arrayContains',field:'recipients',value:'member'},{op:'jsonEq',field:'recipients',value:[]}]},{op:'eq',field:'n',value:null}]});
 expect(lowerSQLiteLivePlan(schema,source,{search:'needle'},{'member.id':'member'},{textOrder:'binary'})).toEqual({supported:false,reason:'searchSources'});
 expect(lowerSQLiteLivePlan(schema,source,{filters:[{column:'n',operator:'contains',value:'1'}]},{'member.id':'member'},{textOrder:'binary'})).toEqual({supported:false,reason:'structuredFilters'});
});
