import {reducer,schema,getRow} from '@gonvex/module-sdk';
export const increment=reducer({args:schema.any(),result:schema.any(),run:async(ctx,args:any)=>{
 const row=await getRow<{_id:string;rank:number}>(ctx.db,'records','_id',args.id);if(!row)throw Error('Missing synthetic proof row');
 await ctx.db.update('records',args.id,{rank:row.rank+1});return {id:args.id,rank:row.rank+1};
}});
