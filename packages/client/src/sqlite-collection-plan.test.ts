import {expect,it} from 'vitest';
import {sqliteCollectionPagePlan} from './sqlite-collection-plan.js';
import type {FunctionReference} from './index.js';
const ref:FunctionReference={kind:'query',path:'tasks.workspace',delivery:'replica',replica:{table:'tasks',key:'id',columns:['id','workspace_id','archived_at','name'],equalFilters:{workspace:'workspace_id'},excludeWhenSet:['archived_at']}};
const options={order:[{field:'id',direction:'asc' as const}],limit:50};
const membership={signature:'authorized-source',generation:'window'};
it('supplies generated table, projection, membership and mandatory filters instead of accepting caller authority',()=>{
 const result=sqliteCollectionPagePlan(ref,{workspace:'w'},options,membership,new Set());expect(result).toEqual({allowLocalInserts:true,plan:{...options,table:'tasks',select:ref.replica!.columns,membership:{...membership,requireComplete:true},where:{op:'and',children:[{field:'workspace_id',op:'eq',value:'w'},{field:'archived_at',op:'eq',value:null}]}}});expect(()=>sqliteCollectionPagePlan(ref,{}, {...options,table:'secret'} as any,membership,new Set())).toThrow('Unsupported');expect(()=>sqliteCollectionPagePlan(ref,{}, {...options,select:['secret']},membership,new Set())).toThrow('undeclared');
});
it('disables out-of-membership optimistic inserts when a required generated argument is absent',()=>{expect(sqliteCollectionPagePlan(ref,{},options,membership,new Set()).allowLocalInserts).toBe(false);});
it('requires certified related data and bounds predicate structure before SQL compilation',()=>{
 const where={op:'relation' as const,field:'id',table:'assignments',relatedField:'task_id'};expect(()=>sqliteCollectionPagePlan(ref,{workspace:'w'},{...options,where},membership,new Set())).toThrow('complete certified');expect(sqliteCollectionPagePlan(ref,{workspace:'w'},{...options,where},membership,new Set(['assignments'])).plan.where).toBeDefined();let deep:any={field:'id',op:'eq',value:'a'};for(let n=0;n<20;n++)deep={op:'not',child:deep};expect(()=>sqliteCollectionPagePlan(ref,{workspace:'w'},{...options,where:deep},membership,new Set())).toThrow('structural budget');
});

it('permits stale display joins only with admitted membership and preserves every relation and source filter',()=>{
 const where={op:'relation' as const,field:'id',table:'assignments',relatedField:'task_id'};
 const result=sqliteCollectionPagePlan(ref,{workspace:'w'},{...options,where},membership,new Set(),{allowStaleRelations:true});
 expect(result.plan.membership).toEqual({...membership,requireComplete:true});
 expect(result.plan.where).toEqual({op:'and',children:[where,{field:'workspace_id',op:'eq',value:'w'},{field:'archived_at',op:'eq',value:null}]});
 expect(()=>sqliteCollectionPagePlan(ref,{workspace:'w'},{...options,where},membership,new Set())).toThrow('complete certified');
});
