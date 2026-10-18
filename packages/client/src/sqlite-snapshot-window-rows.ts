import type {SQLiteMembershipSource} from './sqlite-membership-plan.js';
/** A snapshot is immutable. Disk overlays retain only IDs changed after its
 * certificate. Push ID restrictions into both ordered arms before LIMIT. */
export function sqliteSnapshotWindowRows(scope:string,signature:string,generation:string,source:SQLiteMembershipSource,filter?:{after?:string;ids?:readonly string[]}){
 const bindings:unknown[]=[signature,generation,scope,source.subscription,source.snapshot];
 const restriction=(alias:string)=>{
  if(filter?.ids){bindings.push(JSON.stringify(filter.ids));return ` AND ${alias}.id IN (SELECT value FROM json_each(?))`;}
  if(filter?.after!==undefined){bindings.push(filter.after);return ` AND ${alias}.id>?`;}
  return '';
 };
 const left=restriction('s');bindings.push(scope,signature,generation);const right=restriction('d');
 return {sql:`SELECT s.position,s.id,coalesce(d.hash,s.hash) hash FROM gv_snapshot_rows s
 LEFT JOIN gv_window_mutations d ON d.scope=s.scope AND d.signature=? AND d.generation=? AND d.id=s.id
 WHERE s.scope=? AND s.subscription=? AND s.snapshot=? AND (d.id IS NULL OR (d.deleted=0 AND d.position IS NULL))${left}
 UNION ALL SELECT d.position,d.id,d.hash FROM gv_window_mutations d WHERE d.scope=? AND d.signature=? AND d.generation=? AND d.deleted=0 AND d.position IS NOT NULL${right}`,bindings};
}
