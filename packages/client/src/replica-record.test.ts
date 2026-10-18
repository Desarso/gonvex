import {expect,it} from 'vitest';
import {mergeReplicaRecord} from './replica-record.js';
const at = (revision:number) => ({epoch:'e',revision});

it('retains newer fields while filling an older missing projection', () => {
  const current=mergeReplicaRecord(undefined,{id:'a',status:'working'},at(5));
  const merged=mergeReplicaRecord(current,{id:'a',status:'new',description:'Loaded later'},at(3));
  expect(merged.row).toEqual({id:'a',status:'working',description:'Loaded later'});
  expect(merged.authority.fields).toEqual({id:5,status:5,description:3});
  expect(mergeReplicaRecord(merged,{description:null},at(6)).row).toEqual({id:'a',status:'working',description:null});
});

it('does not resurrect deleted rows from a delayed tab snapshot', () => {
  const current=mergeReplicaRecord(undefined,{id:'a',status:'working'},at(5));
  const deleted=mergeReplicaRecord(current,null,at(6));
  expect(mergeReplicaRecord(deleted,{id:'a',status:'new'},at(5))).toBe(deleted);
  expect(mergeReplicaRecord(deleted,{id:'a',status:'new'},at(6))).toBe(deleted);
  expect(mergeReplicaRecord(deleted,{id:'a',status:'created again'},at(7)).row).toEqual({id:'a',status:'created again'});
});

it('does not let a delayed deletion erase a newer row', () => {
  const current=mergeReplicaRecord(undefined,{id:'a',name:'Recreated'},at(8));
  expect(mergeReplicaRecord(current,null,at(7))).toBe(current);
});

it('does not carry fields across a server epoch replacement', () => {
  const current=mergeReplicaRecord(undefined,{id:'a',name:'Old'},at(8));
  expect(mergeReplicaRecord(current,{id:'a',status:'New'},{epoch:'replacement',revision:1}).row).toEqual({id:'a',status:'New'});
});

it('preserves a repeated snapshot and its decoded row without rebuilding indexes', () => {
  const current = mergeReplicaRecord(undefined, {id:'a', config:{color:'gray'}, status:'working'}, at(5));
  expect(mergeReplicaRecord(current, {id:'a', config:{color:'gray'}}, at(5))).toBe(current);
  const newer = mergeReplicaRecord(current, {status:'working'}, at(6));
  expect(newer.row).toBe(current.row);
  expect(newer.authority.fields.status).toBe(6);
  expect(current.authority.fields.status).toBe(5);
  expect(mergeReplicaRecord(newer, {status:'done'}, at(6)).row?.status).toBe('done');
  const deleted = mergeReplicaRecord(newer, null, at(7));
  expect(mergeReplicaRecord(deleted, null, at(7))).toBe(deleted);
});

it('uses native baseline only for present fields and keeps later stamps sparse', () => {
  const current = {row:{id:'a',name:'snapshot'},authority:{epoch:'e',base:10,fields:{}}};
  const filled = mergeReplicaRecord(current,{name:'obsolete',description:'older projection'},at(5));
  expect(filled.row).toEqual({id:'a',name:'snapshot',description:'older projection'});
  expect(filled.authority).toEqual({epoch:'e',base:10,fields:{description:5}});
  const next = mergeReplicaRecord(filled,{name:'newer'},at(12));
  expect(next.authority).toEqual({epoch:'e',base:10,fields:{description:5,name:12}});
  expect(mergeReplicaRecord(next,{name:'old'},at(11))).toBe(next);
  expect(mergeReplicaRecord(next,null,at(11))).toBe(next);
  expect(mergeReplicaRecord(current,null,at(9))).toBe(current);
  expect(mergeReplicaRecord(current,{id:'a',name:'snapshot'},at(10))).toBe(current);
  const baseline = mergeReplicaRecord(filled,{description:'now current'},at(10));
  expect(baseline.authority.fields).toEqual({});
  expect(mergeReplicaRecord(baseline,{description:'old'},at(9))).toBe(baseline);
  expect(mergeReplicaRecord(next,{id:'a'},{epoch:'replacement',revision:1}).authority).toEqual({epoch:'replacement',fields:{id:1}});
});
